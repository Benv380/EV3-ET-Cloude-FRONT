import { CurrencyPipe, DatePipe, DecimalPipe } from '@angular/common';
import { Component, DestroyRef, OnInit, computed, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { catchError, forkJoin, interval, of, startWith, switchMap } from 'rxjs';
import { ApiService, mensajeError } from '../../core/api.service';
import { AlertaStream, ConsumidorKafka, MensajeDlt, ResumenAnalitica, VistaEstados } from '../../core/models';

/**
 * Dashboard de streaming: lo que calculan en tiempo real los consumidores Kafka de analitica-service.
 * Se actualiza cada 2 segundos.
 */
@Component({
  selector: 'app-streaming',
  imports: [DecimalPipe, CurrencyPipe, DatePipe],
  template: `
    @if (mensaje()) { <p class="alerta ok">{{ mensaje() }}</p> }
    @if (error()) { <p class="alerta error">{{ error() }}</p> }

    <section class="tarjeta">
      <div class="titulo-fila">
        <h1>Analítica en tiempo real</h1>
        <small class="ayuda">Kafka · topic <code>pedidos.eventos</code> · actualización cada 2 s</small>
      </div>
      <p class="ayuda">
        Cada acción sobre un pedido publica un evento en Kafka. El grupo <code>analitica-eventos</code> (3 consumidores,
        uno por partición) los procesa al instante; el grupo <code>analitica-alertas</code> lee los mismos eventos de
        forma independiente y aplica reglas de negocio.
      </p>
      @if (resumen(); as r) {
        <div class="kpis">
          <div class="kpi"><span>Eventos procesados</span><strong>{{ r.totalEventos }}</strong><small>en la retención (7 días)</small></div>
          <div class="kpi"><span>Monto vendido</span><strong>{{ r.montoVendido | currency: 'CLP' : 'symbol-narrow' : '1.0-0' }}</strong><small>pedidos creados</small></div>
          <div class="kpi"><span>Ticket promedio</span><strong>{{ r.ticketPromedio | currency: 'CLP' : 'symbol-narrow' : '1.0-0' }}</strong><small>monto / pedidos</small></div>
          <div class="kpi"><span>Latencia promedio</span><strong>{{ r.latenciaPromedioMs | number }} ms</strong><small>evento → procesado</small></div>
          <div class="kpi" [class.mal]="r.alertas > 0"><span>Alertas</span><strong>{{ r.alertas }}</strong><small>reglas de negocio</small></div>
          <div class="kpi" [class.mal]="r.mensajesDlt > 0"><span>En DLT</span><strong>{{ r.mensajesDlt }}</strong><small>mensajes con error</small></div>
        </div>
      }
    </section>

    @if (resumen(); as r) {
      <section class="tarjeta dos-graficos">
        <div>
          <h2>Eventos por minuto</h2>
          <div class="barras-v">
            @for (p of r.eventosPorMinuto; track p.minuto) {
              <div class="col" [title]="p.minuto + ': ' + p.eventos + ' eventos'">
                @if (p.eventos > 0) { <span>{{ p.eventos }}</span> }
                <div [style.height.%]="pct(p.eventos, maxMinuto())"></div>
              </div>
            }
          </div>
          <div class="ejes-x">
            @for (p of r.eventosPorMinuto; track p.minuto; let i = $index) {
              <span>{{ i % 3 === 0 ? p.minuto : '' }}</span>
            }
          </div>
        </div>
        <div>
          <h2>Por tipo de evento</h2>
          <div class="barras-h">
            @for (t of entradas(r.porTipo); track t[0]) {
              <div class="fila">
                <span>{{ t[0] }}</span>
                <div class="pista"><div [style.width.%]="pct(t[1], r.totalEventos)"></div></div>
                <strong>{{ t[1] }}</strong>
              </div>
            }
          </div>
          <h2 style="margin-top: 1.25rem">Pedidos por estado actual</h2>
          <p class="ayuda">Desde el topic compactado <code>pedidos.estados</code> ({{ estados()?.pedidos ?? 0 }} pedidos).</p>
          <div class="barras-h">
            @for (t of entradas(r.pedidosPorEstado); track t[0]) {
              <div class="fila">
                <span class="estado" [attr.data-estado]="t[0]">{{ t[0] }}</span>
                <div class="pista"><div [style.width.%]="pct(t[1], estados()?.pedidos ?? 0)"></div></div>
                <strong>{{ t[1] }}</strong>
              </div>
            } @empty { <p class="vacio">Sin pedidos aún</p> }
          </div>
        </div>
      </section>

      <section class="tarjeta dos-graficos">
        <div>
          <h2>Distribución por partición</h2>
          <p class="ayuda">La clave del mensaje es el id del pedido: Kafka la usa para elegir la partición.</p>
          <div class="barras-h">
            @for (t of entradas(r.porParticion); track t[0]) {
              <div class="fila">
                <span>partición {{ t[0] }}</span>
                <div class="pista"><div [style.width.%]="pct(t[1], r.totalEventos)"></div></div>
                <strong>{{ t[1] }}</strong>
              </div>
            }
          </div>
        </div>
        <div>
          <h2>Carga por consumidor</h2>
          <p class="ayuda">Grupo <code>analitica-eventos</code>: cada consumidor atiende su(s) partición(es).</p>
          <div class="barras-h">
            @for (t of entradas(r.porConsumidor); track t[0]) {
              <div class="fila">
                <span>{{ t[0] }}</span>
                <div class="pista"><div [style.width.%]="pct(t[1], r.totalEventos)"></div></div>
                <strong>{{ t[1] }}</strong>
              </div>
            }
          </div>
        </div>
      </section>
    }

    <section class="tarjeta">
      <h2>Alertas en tiempo real</h2>
      <p class="ayuda">Reglas: pedido de $1.000.000 o más → <strong>ALTO_MONTO</strong> · pedido cancelado → <strong>CANCELACION</strong>.</p>
      <div class="tabla-scroll">
        <table>
          <thead><tr><th>Detectada</th><th>Tipo</th><th>Pedido</th><th>Cliente</th><th>Detalle</th><th>Partición@offset</th></tr></thead>
          <tbody>
            @for (a of alertas(); track a.id) {
              <tr [class.nuevo]="a.id > ultimaAlertaVista">
                <td>{{ a.detectadaEn | date: 'HH:mm:ss' }}</td>
                <td><span class="tag" [class.dlq]="a.tipo === 'CANCELACION'">{{ a.tipo }}</span></td>
                <td>#{{ a.pedidoId }}</td>
                <td>{{ a.cliente }}</td>
                <td>{{ a.detalle }}</td>
                <td><code>{{ a.particion }}&#64;{{ a.offset }}</code></td>
              </tr>
            } @empty { <tr><td colspan="6" class="vacio">Sin alertas</td></tr> }
          </tbody>
        </table>
      </div>
    </section>

    <section class="tarjeta">
      <h2>Últimos eventos procesados</h2>
      <div class="tabla-scroll">
        <table>
          <thead><tr><th>Fecha</th><th>Tipo</th><th>Pedido</th><th>Estado</th><th>Total</th><th>Partición@offset</th><th>Consumidor</th><th>Latencia</th></tr></thead>
          <tbody>
            @for (e of resumen()?.ultimos ?? []; track e.particion + '-' + e.offset) {
              <tr>
                <td>{{ e.fechaEvento | date: 'HH:mm:ss' }}</td>
                <td>{{ e.tipo }}</td>
                <td>#{{ e.pedidoId }} <small>{{ e.cliente }}</small></td>
                <td><span class="estado" [attr.data-estado]="e.estado">{{ e.estado }}</span></td>
                <td>{{ e.total | currency: 'CLP' : 'symbol-narrow' : '1.0-0' }}</td>
                <td><code>{{ e.particion }}&#64;{{ e.offset }}</code></td>
                <td>{{ e.consumidor }}</td>
                <td>{{ e.latenciaMs | number }} ms</td>
              </tr>
            } @empty { <tr><td colspan="8" class="vacio">Aún no hay eventos. Cree un pedido.</td></tr> }
          </tbody>
        </table>
      </div>
    </section>

    <section class="tarjeta">
      <h2>Consumidores Kafka</h2>
      <p class="ayuda">
        Al pausar un consumidor los eventos <strong>no se pierden</strong>: quedan en Kafka y sube el <em>lag</em> del grupo
        (ver pestaña Kafka o Kafka UI). Al reanudar, continúa desde el último offset confirmado.
      </p>
      <div class="tabla-scroll">
        <table>
          <thead><tr><th>Listener</th><th>Consumer group</th><th>Topic</th><th>Particiones asignadas</th><th>Estado</th><th></th></tr></thead>
          <tbody>
            @for (c of consumidores(); track c.id) {
              <tr>
                <td><code>{{ c.id }}</code></td>
                <td>{{ c.grupo }}</td>
                <td>{{ c.topicos.join(', ') }}</td>
                <td>@for (p of c.particiones; track p) { <span class="chip">{{ p }}</span> }</td>
                <td><span class="estado" [attr.data-estado]="c.pausado ? 'CANCELADO' : 'ENTREGADO'">{{ c.pausado ? 'PAUSADO' : 'ACTIVO' }}</span></td>
                <td>
                  @if (c.pausado) {
                    <button (click)="cambiar(c, 'reanudar')">Reanudar</button>
                  } @else {
                    <button class="peligro" (click)="cambiar(c, 'pausar')">Pausar</button>
                  }
                </td>
              </tr>
            } @empty { <tr><td colspan="6" class="vacio">Sin información</td></tr> }
          </tbody>
        </table>
      </div>
    </section>

    <section class="tarjeta">
      <h2>Dead Letter Topic (<code>pedidos.dlt</code>)</h2>
      <p class="ayuda">Mensajes que fallaron: con error temporal tras 3 reintentos (1 s, 2 s, 4 s), o de inmediato si el payload es inválido.</p>
      <div class="tabla-scroll">
        <table>
          <thead><tr><th>Fecha</th><th>Origen</th><th>Grupo</th><th>Error</th><th>Motivo</th><th>Payload</th></tr></thead>
          <tbody>
            @for (m of dlt(); track m.particionDlt + '-' + m.offsetDlt) {
              <tr class="dlq">
                <td>{{ m.fecha | date: 'HH:mm:ss' }}</td>
                <td><code>{{ m.topicoOriginal }}-{{ m.particionOriginal }}&#64;{{ m.offsetOriginal }}</code></td>
                <td>{{ m.grupoOriginal }}</td>
                <td>{{ m.excepcion }}</td>
                <td><small>{{ m.motivo }}</small></td>
                <td><code class="payload">{{ m.payload }}</code></td>
              </tr>
            } @empty { <tr><td colspan="6" class="vacio">Sin mensajes en el DLT</td></tr> }
          </tbody>
        </table>
      </div>
    </section>
  `,
})
export class Streaming implements OnInit {
  private readonly api = inject(ApiService);
  private readonly destroyRef = inject(DestroyRef);

  protected readonly resumen = signal<ResumenAnalitica | null>(null);
  protected readonly alertas = signal<AlertaStream[]>([]);
  protected readonly estados = signal<VistaEstados | null>(null);
  protected readonly dlt = signal<MensajeDlt[]>([]);
  protected readonly consumidores = signal<ConsumidorKafka[]>([]);
  protected readonly mensaje = signal('');
  protected readonly error = signal('');
  protected ultimaAlertaVista = Number.MAX_SAFE_INTEGER;

  protected readonly maxMinuto = computed(() =>
    Math.max(1, ...(this.resumen()?.eventosPorMinuto ?? []).map((p) => p.eventos)),
  );

  ngOnInit(): void {
    interval(2000)
      .pipe(
        startWith(0),
        switchMap(() =>
          forkJoin({
            resumen: this.api.resumenAnalitica(),
            alertas: this.api.alertasStream(),
            estados: this.api.vistaEstados(),
            dlt: this.api.mensajesDlt(),
            consumidores: this.api.consumidoresKafka(),
          }).pipe(
            catchError((e) => {
              this.error.set(mensajeError(e));
              return of(null);
            }),
          ),
        ),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe((datos) => {
        if (!datos) {
          return;
        }
        this.error.set('');
        const previas = this.alertas();
        this.ultimaAlertaVista = previas.length ? previas[0].id : this.ultimaAlertaVista;
        this.resumen.set(datos.resumen);
        this.alertas.set(datos.alertas);
        this.estados.set(datos.estados);
        this.dlt.set(datos.dlt);
        this.consumidores.set(datos.consumidores);
      });
  }

  protected cambiar(c: ConsumidorKafka, accion: 'pausar' | 'reanudar'): void {
    this.api.cambiarConsumidorKafka(c.id, accion).subscribe({
      next: (r) => {
        this.consumidores.update((lista) => lista.map((x) => (x.id === r.id ? r : x)));
        this.mensaje.set(`Consumidor ${c.id} ${accion === 'pausar' ? 'pausado' : 'reanudado'}`);
      },
      error: (e) => this.error.set(mensajeError(e)),
    });
  }

  protected entradas(mapa: Record<string, number>): [string, number][] {
    return Object.entries(mapa ?? {});
  }

  protected pct(valor: number, total: number): number {
    return total > 0 ? Math.round((valor / total) * 100) : 0;
  }
}
