import { Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { catchError, forkJoin, interval, of, startWith, switchMap } from 'rxjs';
import { ApiService, mensajeError } from '../../core/api.service';
import {
  ClusterInfo,
  GrupoDetalle,
  GrupoResumen,
  PlantillaMensaje,
  SaludKafka,
  TopicoDetalle,
  TopicoResumen,
} from '../../core/models';

const HORA = 3_600_000;

/**
 * Administración de Kafka (kafka-admin-service): clúster y salud, topics (crear, configurar retención y limpieza,
 * aumentar particiones, eliminar), consumer groups (miembros, offsets, lag) y mensajes de negocio de prueba.
 */
@Component({
  selector: 'app-kafka',
  imports: [ReactiveFormsModule],
  template: `
    @if (mensaje()) { <p class="alerta ok">{{ mensaje() }}</p> }
    @if (error()) { <p class="alerta error">{{ error() }}</p> }

    <!-- ===================== Clúster y salud ===================== -->
    <section class="tarjeta">
      <div class="titulo-fila">
        <h1>Administración de Kafka</h1>
        <small class="ayuda">Actualización cada 3 s</small>
      </div>
      @if (salud(); as s) {
        <div class="kpis">
          <div class="kpi" [class.mal]="s.estado !== 'OK'"><span>Estado</span><strong>{{ s.estado }}</strong><small>{{ s.alertas.length }} alertas</small></div>
          <div class="kpi" [class.mal]="s.brokersActivos < s.brokersEsperados"><span>Brokers</span><strong>{{ s.brokersActivos }}/{{ s.brokersEsperados }}</strong><small>controlador: broker {{ cluster()?.controlador }}</small></div>
          <div class="kpi" [class.mal]="s.particionesSubReplicadas > 0"><span>Sub-replicadas</span><strong>{{ s.particionesSubReplicadas }}</strong><small>ISR &lt; réplicas</small></div>
          <div class="kpi" [class.mal]="s.particionesSinLider > 0"><span>Sin líder</span><strong>{{ s.particionesSinLider }}</strong><small>no disponibles</small></div>
          <div class="kpi" [class.mal]="s.mensajesDlt > 0"><span>En DLT</span><strong>{{ s.mensajesDlt }}</strong><small>pedidos.dlt</small></div>
        </div>
        <div class="nodos" style="margin-top: 1rem">
          @for (b of cluster()?.brokers ?? []; track b.id) {
            <div class="nodo">
              <strong>Broker {{ b.id }} @if (b.controlador) { <span class="tag">controlador</span> }</strong>
              <span>{{ b.host }}:{{ b.puerto }}</span>
            </div>
          }
        </div>
        <ul class="alertas" style="margin-top: 1rem">
          @for (a of s.alertas; track $index) {
            <li [attr.data-nivel]="a.nivel">
              <span class="nivel">{{ a.nivel }}</span>
              <div><strong>{{ a.recurso }}</strong> · {{ a.mensaje }}<p>{{ a.impacto }}</p></div>
            </li>
          }
        </ul>
      }
    </section>

    <!-- ===================== Topics ===================== -->
    <section class="tarjeta">
      <h2>Topics</h2>
      <div class="tabla-scroll">
        <table>
          <thead>
            <tr><th>Topic</th><th>Particiones</th><th>Réplicas</th><th>Limpieza</th><th>Retención</th><th>min ISR</th><th>Mensajes</th><th></th></tr>
          </thead>
          <tbody>
            @for (t of topicsVisibles(); track t.nombre) {
              <tr [class.sel]="detalle()?.nombre === t.nombre" [class.dlq]="t.nombre.endsWith('dlt')">
                <td>
                  <code>{{ t.nombre }}</code>
                  @if (t.protegido && !t.interno) { <span class="tag">sistema</span> }
                  @if (t.interno) { <span class="tag">interno</span> }
                  @if (t.particionesSubReplicadas > 0) { <span class="tag dlq">{{ t.particionesSubReplicadas }} sub-replicadas</span> }
                </td>
                <td>{{ t.particiones }}</td>
                <td>{{ t.replicas }}</td>
                <td><span class="tag" [class.topic]="t.cleanupPolicy?.includes('compact')">{{ t.cleanupPolicy }}</span></td>
                <td>{{ t.cleanupPolicy === 'compact' ? 'último valor por clave' : duracion(t.retentionMs) }}</td>
                <td>{{ t.minInsyncReplicas }}</td>
                <td>{{ t.mensajes }}</td>
                <td class="acciones-cola">
                  <button class="sec" (click)="verTopic(t.nombre)">Ver</button>
                  @if (!t.protegido) { <button class="peligro" (click)="eliminarTopic(t.nombre)">Eliminar</button> }
                </td>
              </tr>
            }
          </tbody>
        </table>
      </div>
      <label class="check"><input type="checkbox" [checked]="verInternos()" (change)="verInternos.set(!verInternos())" /> Mostrar topics internos (__consumer_offsets)</label>
    </section>

    @if (detalle(); as d) {
      <section class="tarjeta">
        <div class="titulo-fila">
          <h2>Topic <code>{{ d.nombre }}</code></h2>
          <button class="sec" (click)="detalle.set(null)">Cerrar</button>
        </div>
        <p class="ayuda">
          Cada partición tiene un <strong>líder</strong> (atiende lecturas y escrituras) y copias en otros brokers.
          <strong>ISR</strong> = réplicas en sincronía: si cae el líder, una de ellas toma su lugar.
          @if (d.grupos.length) { Consumer groups: @for (g of d.grupos; track g) { <span class="chip">{{ g }}</span> } }
        </p>
        <div class="tabla-scroll">
          <table>
            <thead><tr><th>Partición</th><th>Líder</th><th>Réplicas</th><th>ISR</th><th>Offset inicial</th><th>Offset final</th><th>Mensajes</th></tr></thead>
            <tbody>
              @for (p of d.particiones; track p.particion) {
                <tr [class.dlq]="p.subReplicada || p.lider === null">
                  <td>{{ p.particion }}</td>
                  <td>{{ p.lider === null ? 'SIN LÍDER' : 'broker ' + p.lider }}</td>
                  <td>{{ p.replicas.join(', ') }}</td>
                  <td>{{ p.isr.join(', ') }}</td>
                  <td>{{ p.offsetInicial }}</td>
                  <td>{{ p.offsetFinal }}</td>
                  <td>{{ p.mensajes }}</td>
                </tr>
              }
            </tbody>
          </table>
        </div>
        <h3>Configuración</h3>
        <div class="tabla-scroll">
          <table>
            <thead><tr><th>Config</th><th>Valor</th><th>Origen</th></tr></thead>
            <tbody>
              @for (c of d.configs; track c.nombre) {
                <tr>
                  <td><code>{{ c.nombre }}</code></td>
                  <td>{{ c.valor }} @if (c.nombre.endsWith('.ms') && c.valor) { <small>({{ duracion(c.valor) }})</small> }</td>
                  <td><small>{{ c.porDefecto ? 'por defecto del broker' : 'definida en el topic' }}</small></td>
                </tr>
              }
            </tbody>
          </table>
        </div>
        @if (!d.interno) {
          <h3>Modificar</h3>
          <form class="grilla" [formGroup]="formConfig" (ngSubmit)="guardarConfig(d)">
            <label>Política de limpieza
              <select formControlName="cleanupPolicy">
                <option value="">(sin cambio)</option>
                <option value="delete">delete · borra por tiempo/tamaño</option>
                <option value="compact">compact · último valor por clave</option>
                <option value="compact,delete">compact,delete · ambas</option>
              </select>
            </label>
            <label>Retención (horas)<input type="number" formControlName="retencionHoras" min="0" placeholder="(sin cambio)" /></label>
            <label>min.insync.replicas<input type="number" formControlName="minInsync" min="1" [max]="d.replicas" placeholder="(sin cambio)" /></label>
            <div class="acciones"><button type="submit">Aplicar configuración</button></div>
          </form>
          <form class="en-linea" [formGroup]="formParticiones" (ngSubmit)="aumentar(d)" style="margin-top: 0.75rem">
            <label>Aumentar particiones a<input type="number" formControlName="total" [min]="d.particiones.length + 1" /></label>
            <button type="submit" class="sec">Aumentar</button>
            <small class="ayuda">Kafka solo permite aumentar. Las claves pueden pasar a otra partición.</small>
          </form>
        }
      </section>
    }

    <section class="tarjeta">
      <h2>Crear topic</h2>
      <form class="grilla" [formGroup]="formTopic" (ngSubmit)="crearTopic()">
        <label>Nombre<input formControlName="nombre" placeholder="pedidos.reportes" /></label>
        <label>Particiones<input type="number" formControlName="particiones" min="1" max="12" /></label>
        <label>Réplicas<input type="number" formControlName="replicas" min="1" max="3" /></label>
        <label>Limpieza
          <select formControlName="cleanupPolicy">
            <option value="delete">delete</option>
            <option value="compact">compact</option>
            <option value="compact,delete">compact,delete</option>
          </select>
        </label>
        <label>Retención (horas)<input type="number" formControlName="retencionHoras" min="1" /></label>
        <label>min.insync.replicas<input type="number" formControlName="minInsync" min="1" max="3" /></label>
        <div class="acciones"><button type="submit" [disabled]="formTopic.invalid">Crear</button></div>
      </form>
    </section>

    <!-- ===================== Consumer groups ===================== -->
    <section class="tarjeta">
      <h2>Consumer groups</h2>
      <p class="ayuda">
        <strong>Lag</strong> = mensajes que el grupo aún no procesa (offset final − offset confirmado). Si sube y no baja,
        el consumo va más lento que la producción. Dentro de un grupo cada partición la atiende un solo consumidor.
      </p>
      <div class="tabla-scroll">
        <table>
          <thead><tr><th>Grupo</th><th>Estado</th><th>Consumidores</th><th>Topics</th><th>Lag</th><th></th></tr></thead>
          <tbody>
            @for (g of grupos(); track g.grupo) {
              <tr [class.sel]="grupo()?.grupo === g.grupo">
                <td><code>{{ g.grupo }}</code></td>
                <td>{{ g.estado }}</td>
                <td [class.alerta-num]="g.miembros === 0">{{ g.miembros }}</td>
                <td>@for (t of g.topicos; track t) { <span class="chip">{{ t }}</span> }</td>
                <td [class.alerta-num]="g.lagTotal > 0">{{ g.lagTotal }}</td>
                <td><button class="sec" (click)="verGrupo(g.grupo)">Ver</button></td>
              </tr>
            } @empty { <tr><td colspan="6" class="vacio">Sin consumer groups</td></tr> }
          </tbody>
        </table>
      </div>
    </section>

    @if (grupo(); as g) {
      <section class="tarjeta">
        <div class="titulo-fila">
          <h2>Grupo <code>{{ g.grupo }}</code> · {{ g.estado }} · lag {{ g.lagTotal }}</h2>
          <button class="sec" (click)="grupo.set(null)">Cerrar</button>
        </div>
        <p class="ayuda">Coordinador: broker {{ g.coordinador }} · estrategia de asignación: <code>{{ g.asignador }}</code></p>
        <h3>Consumidores y particiones asignadas</h3>
        <ul>
          @for (m of g.miembros; track m.consumidor) {
            <li><code>{{ m.clientId }}</code> ({{ m.host }}) → @for (p of m.particiones; track p) { <span class="chip">{{ p }}</span> }</li>
          } @empty { <li class="vacio">Sin consumidores conectados</li> }
        </ul>
        <h3>Offsets por partición</h3>
        <div class="tabla-scroll">
          <table>
            <thead><tr><th>Topic</th><th>Partición</th><th>Offset confirmado</th><th>Offset final</th><th>Lag</th><th>Consumidor</th></tr></thead>
            <tbody>
              @for (o of g.offsets; track o.topico + o.particion) {
                <tr>
                  <td><code>{{ o.topico }}</code></td>
                  <td>{{ o.particion }}</td>
                  <td>{{ o.offsetConfirmado ?? '—' }}</td>
                  <td>{{ o.offsetFinal }}</td>
                  <td [class.alerta-num]="o.lag > 0">{{ o.lag }}</td>
                  <td><small>{{ o.consumidor ?? '—' }}</small></td>
                </tr>
              }
            </tbody>
          </table>
        </div>
        @if (g.miembros.length === 0 && g.offsets.length) {
          <div class="en-linea" style="margin-top: 0.75rem">
            <button class="sec" (click)="resetear(g, 'INICIO')">Mover offsets al inicio (reprocesar)</button>
            <button class="peligro" (click)="resetear(g, 'FINAL')">Mover offsets al final (saltar pendientes)</button>
          </div>
        } @else {
          <p class="ayuda">Mover offsets solo es posible con el grupo sin consumidores conectados.</p>
        }
      </section>
    }

    <!-- ===================== Mensajes de negocio ===================== -->
    <section class="tarjeta">
      <h2>Probar reacciones del sistema</h2>
      <p class="ayuda">
        Mensajes de negocio listos para publicar. También se pueden copiar y pegar en
        <strong>Kafka UI → Topics → Produce Message</strong> (clave y valor) para ver cómo reacciona el sistema en la pestaña Streaming.
      </p>
      <div class="plantillas">
        @for (p of plantillas(); track p.id) {
          <div class="plantilla">
            <strong>{{ p.titulo }}</strong>
            <small><code>{{ p.topico }}</code> · clave {{ p.clave }}</small>
            <p>{{ p.efectoEsperado }}</p>
            <div class="en-linea">
              <button (click)="publicar(p)">Publicar</button>
              <button class="sec" (click)="copiar(p)">Copiar JSON</button>
            </div>
          </div>
        }
      </div>
    </section>
  `,
})
export class Kafka implements OnInit {
  private readonly api = inject(ApiService);
  private readonly fb = inject(FormBuilder);
  private readonly destroyRef = inject(DestroyRef);

  protected readonly cluster = signal<ClusterInfo | null>(null);
  protected readonly salud = signal<SaludKafka | null>(null);
  protected readonly topics = signal<TopicoResumen[]>([]);
  protected readonly grupos = signal<GrupoResumen[]>([]);
  protected readonly detalle = signal<TopicoDetalle | null>(null);
  protected readonly grupo = signal<GrupoDetalle | null>(null);
  protected readonly plantillas = signal<PlantillaMensaje[]>([]);
  protected readonly verInternos = signal(false);
  protected readonly mensaje = signal('');
  protected readonly error = signal('');

  protected readonly formTopic = this.fb.nonNullable.group({
    nombre: ['', [Validators.required, Validators.pattern(/^[a-zA-Z0-9._-]{1,200}$/)]],
    particiones: [3, [Validators.required, Validators.min(1), Validators.max(12)]],
    replicas: [3, [Validators.required, Validators.min(1), Validators.max(3)]],
    cleanupPolicy: ['delete'],
    retencionHoras: [24, [Validators.required, Validators.min(1)]],
    minInsync: [2, [Validators.required, Validators.min(1), Validators.max(3)]],
  });

  protected readonly formConfig = this.fb.group({
    cleanupPolicy: [''],
    retencionHoras: [null as number | null],
    minInsync: [null as number | null],
  });

  protected readonly formParticiones = this.fb.nonNullable.group({ total: [4, [Validators.min(1)]] });

  ngOnInit(): void {
    this.api.plantillasKafka().subscribe({ next: (p) => this.plantillas.set(p), error: () => undefined });
    interval(3000)
      .pipe(
        startWith(0),
        switchMap(() =>
          forkJoin({
            cluster: this.api.clusterKafka(),
            salud: this.api.saludKafka(),
            topics: this.api.topics(),
            grupos: this.api.gruposKafka(),
          }).pipe(
            catchError((e) => {
              this.error.set(mensajeError(e));
              return of(null);
            }),
          ),
        ),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe((d) => {
        if (!d) {
          return;
        }
        this.cluster.set(d.cluster);
        this.salud.set(d.salud);
        this.topics.set(d.topics);
        this.grupos.set(d.grupos);
        const g = this.grupo();
        if (g) {
          this.api.grupoKafka(g.grupo).subscribe({ next: (x) => this.grupo.set(x), error: () => undefined });
        }
      });
  }

  protected topicsVisibles(): TopicoResumen[] {
    return this.topics().filter((t) => this.verInternos() || !t.interno);
  }

  protected verTopic(nombre: string): void {
    this.api.topic(nombre).subscribe({
      next: (d) => {
        this.detalle.set(d);
        this.formConfig.reset({ cleanupPolicy: '', retencionHoras: null, minInsync: null });
        this.formParticiones.setValue({ total: d.particiones.length + 1 });
      },
      error: (e) => this.error.set(mensajeError(e)),
    });
  }

  protected guardarConfig(d: TopicoDetalle): void {
    const v = this.formConfig.getRawValue();
    const establecer: Record<string, string> = {};
    if (v.cleanupPolicy) establecer['cleanup.policy'] = v.cleanupPolicy;
    if (v.retencionHoras !== null && v.retencionHoras !== undefined) {
      establecer['retention.ms'] = String(Math.round(v.retencionHoras * HORA));
    }
    if (v.minInsync) establecer['min.insync.replicas'] = String(v.minInsync);
    if (!Object.keys(establecer).length) {
      this.error.set('Indique al menos un cambio');
      return;
    }
    this.api.actualizarConfigsTopic(d.nombre, establecer).subscribe({
      next: (r) => this.exito(`Configuración de ${d.nombre} actualizada: ${JSON.stringify(establecer)}`, r),
      error: (e) => this.error.set(mensajeError(e)),
    });
  }

  protected aumentar(d: TopicoDetalle): void {
    const total = this.formParticiones.getRawValue().total;
    if (!confirm(`¿Aumentar ${d.nombre} a ${total} particiones? No se puede deshacer.`)) return;
    this.api.aumentarParticiones(d.nombre, total).subscribe({
      next: (r) => this.exito(`${d.nombre} ahora tiene ${total} particiones`, r),
      error: (e) => this.error.set(mensajeError(e)),
    });
  }

  protected crearTopic(): void {
    const v = this.formTopic.getRawValue();
    this.api
      .crearTopic({
        nombre: v.nombre.trim(),
        particiones: v.particiones,
        replicas: v.replicas,
        configs: {
          'cleanup.policy': v.cleanupPolicy,
          'retention.ms': String(Math.round(v.retencionHoras * HORA)),
          'min.insync.replicas': String(v.minInsync),
        },
      })
      .subscribe({
        next: (r) => {
          this.exito(`Topic ${r.nombre} creado (${r.particiones.length} particiones × ${r.replicas} réplicas)`, r);
          this.formTopic.controls.nombre.reset('');
        },
        error: (e) => this.error.set(mensajeError(e)),
      });
  }

  protected eliminarTopic(nombre: string): void {
    if (!confirm(`¿Eliminar el topic ${nombre} y todos sus mensajes?`)) return;
    this.api.eliminarTopic(nombre).subscribe({
      next: () => {
        this.mensaje.set(`Topic ${nombre} eliminado`);
        this.error.set('');
        if (this.detalle()?.nombre === nombre) this.detalle.set(null);
      },
      error: (e) => this.error.set(mensajeError(e)),
    });
  }

  protected verGrupo(id: string): void {
    this.api.grupoKafka(id).subscribe({ next: (g) => this.grupo.set(g), error: (e) => this.error.set(mensajeError(e)) });
  }

  protected resetear(g: GrupoDetalle, estrategia: 'INICIO' | 'FINAL'): void {
    const topico = g.offsets[0]?.topico;
    if (!topico || !confirm(`¿Mover los offsets de ${g.grupo} en ${topico} al ${estrategia.toLowerCase()}?`)) return;
    this.api.resetearOffsets(g.grupo, topico, estrategia).subscribe({
      next: (r) => {
        this.grupo.set(r);
        this.mensaje.set(`Offsets de ${g.grupo} movidos al ${estrategia.toLowerCase()}`);
      },
      error: (e) => this.error.set(mensajeError(e)),
    });
  }

  protected publicar(p: PlantillaMensaje): void {
    this.api.publicarPlantilla(p.id).subscribe({
      next: (r) => {
        this.mensaje.set(`"${p.titulo}" publicado en ${r.topico}-${r.particion} offset ${r.offset}. ${p.efectoEsperado}`);
        this.error.set('');
      },
      error: (e) => this.error.set(mensajeError(e)),
    });
  }

  protected copiar(p: PlantillaMensaje): void {
    navigator.clipboard
      .writeText(p.valor)
      .then(() => this.mensaje.set(`JSON de "${p.titulo}" copiado. En Kafka UI use la clave ${p.clave}.`))
      .catch(() => this.error.set('No se pudo copiar al portapapeles'));
  }

  protected duracion(ms: string | null): string {
    if (ms === null || ms === undefined) return '—';
    const n = Number(ms);
    if (n < 0) return 'sin límite';
    if (n % (24 * HORA) === 0) return `${n / (24 * HORA)} días`;
    if (n % HORA === 0) return `${n / HORA} h`;
    if (n % 60_000 === 0) return `${n / 60_000} min`;
    return `${n} ms`;
  }

  private exito(texto: string, detalle: TopicoDetalle): void {
    this.mensaje.set(texto);
    this.error.set('');
    this.detalle.set(detalle);
  }
}
