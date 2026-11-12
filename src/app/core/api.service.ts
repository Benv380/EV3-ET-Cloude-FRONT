import { HttpClient, HttpErrorResponse, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { AuthService } from './auth.service';
import {
  AlertaStream,
  ClusterInfo,
  ConsumidorKafka,
  CrearTopico,
  GrupoDetalle,
  GrupoResumen,
  MensajeDlt,
  MensajePublicado,
  PlantillaMensaje,
  ResumenAnalitica,
  SaludKafka,
  TopicoDetalle,
  TopicoResumen,
  VistaEstados,
  BindingInfo,
  ColaInfo,
  CrearBinding,
  CrearCola,
  CrearExchange,
  CrearPedido,
  Documento,
  EstadoConsumidor,
  EstadoPedido,
  EventoAuditoria,
  ExchangeInfo,
  GuardarPolitica,
  NodoInfo,
  Notificacion,
  Pedido,
  PoliticaInfo,
  SaludMensajeria,
  SimulacionFallo,
} from './models';

/**
 * Cliente de la API del backend. Todas las rutas pasan por el API Manager de la nube con la que se
 * inició sesión (Azure API Management o AWS API Gateway).
 */
@Injectable({ providedIn: 'root' })
export class ApiService {
  private readonly http = inject(HttpClient);
  private readonly auth = inject(AuthService);

  private get base(): string {
    return this.auth.apiBaseUrl();
  }

  private get rabbit(): string {
    return `${this.base}/api/admin/rabbit`;
  }

  private get kafka(): string {
    return `${this.base}/api/admin/kafka`;
  }

  // ---------------- Pedidos (pedidos-service) ----------------

  pedidos(): Observable<Pedido[]> {
    return this.http.get<Pedido[]>(`${this.base}/api/pedidos`);
  }

  crearPedido(pedido: CrearPedido): Observable<Pedido> {
    return this.http.post<Pedido>(`${this.base}/api/pedidos`, pedido);
  }

  cambiarEstado(id: number, estado: EstadoPedido, simularFallo: SimulacionFallo): Observable<Pedido> {
    return this.http.patch<Pedido>(`${this.base}/api/pedidos/${id}/estado`, { estado, simularFallo });
  }

  solicitarBoleta(id: number): Observable<{ mensaje: string; eventoId: string }> {
    return this.http.post<{ mensaje: string; eventoId: string }>(`${this.base}/api/pedidos/${id}/boleta`, {});
  }

  // ---------------- Documentos (documentos-service) ----------------

  documentos(pedidoId?: number): Observable<Documento[]> {
    const params = pedidoId ? new HttpParams().set('pedidoId', pedidoId) : undefined;
    return this.http.get<Documento[]>(`${this.base}/api/documentos`, { params });
  }

  contenidoDocumento(id: number): Observable<string> {
    return this.http.get(`${this.base}/api/documentos/${id}/contenido`, { responseType: 'text' });
  }

  // ---------------- Notificaciones y auditoría (notificaciones-service) ----------------

  notificaciones(): Observable<Notificacion[]> {
    return this.http.get<Notificacion[]>(`${this.base}/api/notificaciones`);
  }

  auditoria(): Observable<EventoAuditoria[]> {
    return this.http.get<EventoAuditoria[]>(`${this.base}/api/auditoria`);
  }

  // ---------------- Administración RabbitMQ (rabbit-admin-service) ----------------

  nodos(): Observable<NodoInfo[]> {
    return this.http.get<NodoInfo[]>(`${this.rabbit}/nodes`);
  }

  salud(): Observable<SaludMensajeria> {
    return this.http.get<SaludMensajeria>(`${this.rabbit}/salud`);
  }

  publicarPrueba(cola: string, cantidad: number): Observable<{ publicados: number }> {
    return this.http.post<{ publicados: number }>(`${this.rabbit}/queues/${encodeURIComponent(cola)}/messages`, {
      cantidad,
    });
  }

  reprocesarDlq(dlq: string, max = 100): Observable<{ reprocesados: number; destinos: Record<string, number> }> {
    return this.http.post<{ reprocesados: number; destinos: Record<string, number> }>(
      `${this.rabbit}/queues/${encodeURIComponent(dlq)}/reprocesar`,
      {},
      { params: new HttpParams().set('max', max) },
    );
  }

  politicas(): Observable<PoliticaInfo[]> {
    return this.http.get<PoliticaInfo[]>(`${this.rabbit}/policies`);
  }

  guardarPolitica(nombre: string, politica: GuardarPolitica): Observable<PoliticaInfo> {
    return this.http.put<PoliticaInfo>(`${this.rabbit}/policies/${encodeURIComponent(nombre)}`, politica);
  }

  eliminarPolitica(nombre: string): Observable<void> {
    return this.http.delete<void>(`${this.rabbit}/policies/${encodeURIComponent(nombre)}`);
  }

  // ---------------- Control de consumidores (en cada microservicio consumidor) ----------------

  consumidores(servicio: 'notificaciones' | 'documentos'): Observable<EstadoConsumidor[]> {
    return this.http.get<EstadoConsumidor[]>(`${this.base}/api/${servicio}/consumidores`);
  }

  cambiarConsumidor(
    servicio: 'notificaciones' | 'documentos',
    id: string,
    accion: 'pausar' | 'reanudar',
  ): Observable<EstadoConsumidor> {
    return this.http.post<EstadoConsumidor>(`${this.base}/api/${servicio}/consumidores/${id}/${accion}`, {});
  }

  colas(): Observable<ColaInfo[]> {
    return this.http.get<ColaInfo[]>(`${this.rabbit}/queues`);
  }

  crearCola(cola: CrearCola): Observable<ColaInfo> {
    return this.http.post<ColaInfo>(`${this.rabbit}/queues`, cola);
  }

  eliminarCola(nombre: string): Observable<void> {
    return this.http.delete<void>(`${this.rabbit}/queues/${encodeURIComponent(nombre)}`);
  }

  purgarCola(nombre: string): Observable<{ mensajesEliminados: number }> {
    return this.http.delete<{ mensajesEliminados: number }>(
      `${this.rabbit}/queues/${encodeURIComponent(nombre)}/messages`,
    );
  }

  exchanges(): Observable<ExchangeInfo[]> {
    return this.http.get<ExchangeInfo[]>(`${this.rabbit}/exchanges`);
  }

  crearExchange(exchange: CrearExchange): Observable<ExchangeInfo> {
    return this.http.post<ExchangeInfo>(`${this.rabbit}/exchanges`, exchange);
  }

  eliminarExchange(nombre: string): Observable<void> {
    return this.http.delete<void>(`${this.rabbit}/exchanges/${encodeURIComponent(nombre)}`);
  }

  bindings(): Observable<BindingInfo[]> {
    return this.http.get<BindingInfo[]>(`${this.rabbit}/bindings`);
  }

  crearBinding(binding: CrearBinding): Observable<BindingInfo> {
    return this.http.post<BindingInfo>(`${this.rabbit}/bindings`, binding);
  }

  eliminarBinding(binding: CrearBinding): Observable<void> {
    const params = new HttpParams()
      .set('exchange', binding.exchange)
      .set('cola', binding.cola)
      .set('routingKey', binding.routingKey);
    return this.http.delete<void>(`${this.rabbit}/bindings`, { params });
  }

  // ---------------- Kafka: analítica en tiempo real (analitica-service) ----------------

  resumenAnalitica(): Observable<ResumenAnalitica> {
    return this.http.get<ResumenAnalitica>(`${this.base}/api/analitica/resumen`);
  }

  alertasStream(): Observable<AlertaStream[]> {
    return this.http.get<AlertaStream[]>(`${this.base}/api/analitica/alertas`);
  }

  vistaEstados(): Observable<VistaEstados> {
    return this.http.get<VistaEstados>(`${this.base}/api/analitica/estados`);
  }

  mensajesDlt(): Observable<MensajeDlt[]> {
    return this.http.get<MensajeDlt[]>(`${this.base}/api/analitica/dlt`);
  }

  consumidoresKafka(): Observable<ConsumidorKafka[]> {
    return this.http.get<ConsumidorKafka[]>(`${this.base}/api/analitica/consumidores`);
  }

  cambiarConsumidorKafka(id: string, accion: 'pausar' | 'reanudar'): Observable<ConsumidorKafka> {
    return this.http.post<ConsumidorKafka>(`${this.base}/api/analitica/consumidores/${id}/${accion}`, {});
  }

  // ---------------- Kafka: administración (kafka-admin-service) ----------------

  clusterKafka(): Observable<ClusterInfo> {
    return this.http.get<ClusterInfo>(`${this.kafka}/cluster`);
  }

  saludKafka(): Observable<SaludKafka> {
    return this.http.get<SaludKafka>(`${this.kafka}/salud`);
  }

  topics(): Observable<TopicoResumen[]> {
    return this.http.get<TopicoResumen[]>(`${this.kafka}/topics`);
  }

  topic(nombre: string): Observable<TopicoDetalle> {
    return this.http.get<TopicoDetalle>(`${this.kafka}/topics/${encodeURIComponent(nombre)}`);
  }

  crearTopic(topico: CrearTopico): Observable<TopicoDetalle> {
    return this.http.post<TopicoDetalle>(`${this.kafka}/topics`, topico);
  }

  actualizarConfigsTopic(
    nombre: string,
    establecer: Record<string, string>,
    restablecer: string[] = [],
  ): Observable<TopicoDetalle> {
    return this.http.patch<TopicoDetalle>(`${this.kafka}/topics/${encodeURIComponent(nombre)}/configs`, {
      establecer,
      restablecer,
    });
  }

  aumentarParticiones(nombre: string, total: number): Observable<TopicoDetalle> {
    return this.http.post<TopicoDetalle>(`${this.kafka}/topics/${encodeURIComponent(nombre)}/particiones`, { total });
  }

  eliminarTopic(nombre: string): Observable<void> {
    return this.http.delete<void>(`${this.kafka}/topics/${encodeURIComponent(nombre)}`);
  }

  gruposKafka(): Observable<GrupoResumen[]> {
    return this.http.get<GrupoResumen[]>(`${this.kafka}/grupos`);
  }

  grupoKafka(id: string): Observable<GrupoDetalle> {
    return this.http.get<GrupoDetalle>(`${this.kafka}/grupos/${encodeURIComponent(id)}`);
  }

  resetearOffsets(id: string, topico: string, estrategia: 'INICIO' | 'FINAL'): Observable<GrupoDetalle> {
    return this.http.post<GrupoDetalle>(`${this.kafka}/grupos/${encodeURIComponent(id)}/offsets`, {
      topico,
      estrategia,
    });
  }

  plantillasKafka(): Observable<PlantillaMensaje[]> {
    return this.http.get<PlantillaMensaje[]>(`${this.kafka}/plantillas`);
  }

  publicarPlantilla(id: string): Observable<MensajePublicado> {
    return this.http.post<MensajePublicado>(`${this.kafka}/plantillas/${encodeURIComponent(id)}/publicar`, {});
  }
}

/** Traduce un error HTTP (ProblemDetail del backend o error del API Manager) a un texto legible. */
export function mensajeError(error: unknown): string {
  if (!(error instanceof HttpErrorResponse)) {
    return 'Error inesperado';
  }
  if (error.status === 0) {
    return 'No hay conexión con el backend / API Manager';
  }
  const cuerpo = typeof error.error === 'string' ? safeJson(error.error) : error.error;
  const detalle: string = cuerpo?.detail ?? cuerpo?.message ?? error.statusText;
  const errores = cuerpo?.errores as Record<string, string> | undefined;
  const extra = errores ? ' → ' + Object.entries(errores).map(([c, m]) => `${c}: ${m}`).join('; ') : '';
  return `${error.status} · ${detalle}${extra}`;
}

function safeJson(texto: string): any {
  try {
    return JSON.parse(texto);
  } catch {
    return { detail: texto };
  }
}
