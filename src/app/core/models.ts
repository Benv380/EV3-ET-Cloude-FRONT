export type EstadoPedido = 'CREADO' | 'PAGADO' | 'DESPACHADO' | 'ENTREGADO' | 'CANCELADO';
export type SimulacionFallo = 'NINGUNA' | 'NOTIFICACION' | 'DOCUMENTO' | 'AUDITORIA' | 'ANALITICA';

export interface Pedido {
  id: number;
  cliente: string;
  emailCliente: string;
  descripcion: string;
  total: number;
  estado: EstadoPedido;
  siguientesEstados: EstadoPedido[];
  creadoPor: string;
  fechaCreacion: string;
  fechaActualizacion: string;
}

export interface CrearPedido {
  cliente: string;
  emailCliente: string;
  descripcion: string;
  total: number;
  simularFallo: SimulacionFallo;
}

export interface Documento {
  id: number;
  eventoId: string;
  pedidoId: number;
  tipo: 'TICKET' | 'BOLETA';
  folio: string;
  fechaGeneracion: string;
}

export interface Notificacion {
  id: number;
  eventoId: string;
  pedidoId: number;
  destinatario: string;
  asunto: string;
  mensaje: string;
  fechaEnvio: string;
}

export interface EventoAuditoria {
  id: number;
  eventoId: string;
  tipo: string;
  pedidoId: number;
  estado: string | null;
  estadoAnterior: string | null;
  usuario: string | null;
  exchange: string;
  routingKey: string;
  fechaEvento: string;
  fechaRegistro: string;
}

// ---------------- RabbitMQ admin ----------------

export type TipoExchange = 'DIRECT' | 'TOPIC' | 'FANOUT' | 'HEADERS';
export type TipoCola = 'CLASSIC' | 'QUORUM';
export type TipoDesborde = 'DROP_HEAD' | 'REJECT_PUBLISH' | 'REJECT_PUBLISH_DLX';

export interface NodoInfo {
  nombre: string;
  activo: boolean;
  tipo: string;
  memoriaUsadaBytes: number;
  memoriaLimiteBytes: number;
  uptimeMs: number;
  alarmaMemoria: boolean;
  alarmaDisco: boolean;
}

export interface RetencionInfo {
  ttlMs: number | null;
  maxMensajes: number | null;
  desborde: string | null;
  expiraMs: number | null;
  origen: string;
}

export interface ColaInfo {
  nombre: string;
  tipo: string;
  durable: boolean;
  autoDelete: boolean;
  nodo: string;
  estado: string;
  mensajes: number;
  mensajesListos: number;
  mensajesSinAck: number;
  consumidores: number;
  utilizacionConsumidores: number | null;
  tasaPublicacion: number;
  tasaEntrega: number;
  tasaAck: number;
  memoriaBytes: number;
  esDlq: boolean;
  protegida: boolean;
  politica: string | null;
  retencion: RetencionInfo;
  ocupacionPct: number | null;
  argumentos: Record<string, unknown>;
}

export interface Alerta {
  nivel: 'CRITICO' | 'ADVERTENCIA' | 'INFO';
  recurso: string;
  mensaje: string;
  impacto: string;
}

export interface SaludMensajeria {
  colasActivas: number;
  dlqs: number;
  mensajesListos: number;
  mensajesSinAck: number;
  mensajesEnDlq: number;
  consumidores: number;
  tasaPublicacion: number;
  tasaEntrega: number;
  tasaAck: number;
  nodosActivos: number;
  nodosTotales: number;
  prefetch: number;
  alertas: Alerta[];
}

export interface EstadoConsumidor {
  id: string;
  colas: string[];
  activo: boolean;
  consumidoresActivos: number;
  /** Microservicio dueño del consumidor (lo agrega el frontend). */
  servicio?: 'notificaciones' | 'documentos';
}

export interface PoliticaInfo {
  nombre: string;
  patron: string;
  aplicaA: string;
  prioridad: number;
  definicion: Record<string, unknown>;
}

export interface GuardarPolitica {
  patron: string;
  prioridad: number | null;
  ttlMs: number | null;
  maxMensajes: number | null;
  desborde: TipoDesborde | null;
  expiraSinUsoMs: number | null;
}

export interface ExchangeInfo {
  nombre: string;
  tipo: TipoExchange;
  durable: boolean;
  autoDelete: boolean;
  protegido: boolean;
}

export interface BindingInfo {
  exchange: string;
  destino: string;
  tipoDestino: string;
  routingKey: string;
}

export interface CrearCola {
  nombre: string;
  tipo: TipoCola;
  durable: boolean;
  conDlq: boolean;
  ttlMs: number | null;
  maxMensajes: number | null;
  desborde: TipoDesborde | null;
  expiraSinUsoMs: number | null;
}

export interface CrearExchange {
  nombre: string;
  tipo: TipoExchange;
  durable: boolean;
}

export interface CrearBinding {
  exchange: string;
  cola: string;
  routingKey: string;
}

// ---------------- Kafka: analítica en tiempo real (analitica-service) ----------------

export interface EventoReciente {
  tipo: string;
  pedidoId: number;
  cliente: string;
  estado: string;
  total: number;
  particion: number;
  offset: number;
  consumidor: string;
  latenciaMs: number;
  fechaEvento: string;
}

export interface ResumenAnalitica {
  totalEventos: number;
  porTipo: Record<string, number>;
  montoVendido: number;
  ticketPromedio: number;
  pedidosPorEstado: Record<string, number>;
  porParticion: Record<string, number>;
  porConsumidor: Record<string, number>;
  eventosPorMinuto: { minuto: string; eventos: number }[];
  latenciaPromedioMs: number;
  alertas: number;
  mensajesDlt: number;
  ultimos: EventoReciente[];
}

export interface AlertaStream {
  id: number;
  eventoId: string;
  tipo: 'ALTO_MONTO' | 'CANCELACION';
  pedidoId: number;
  cliente: string;
  total: number;
  detalle: string;
  particion: number;
  offset: number;
  fechaEvento: string;
  detectadaEn: string;
}

export interface MensajeDlt {
  particionDlt: number;
  offsetDlt: number;
  clave: string | null;
  topicoOriginal: string | null;
  particionOriginal: number | null;
  offsetOriginal: number | null;
  grupoOriginal: string | null;
  excepcion: string | null;
  motivo: string | null;
  payload: string | null;
  fecha: string;
}

export interface ConsumidorKafka {
  id: string;
  grupo: string;
  topicos: string[];
  activo: boolean;
  pausado: boolean;
  particiones: string[];
}

export interface VistaEstados {
  pedidos: number;
  registrosLeidos: number;
  porEstado: Record<string, number>;
  ultimos: { pedidoId: number; cliente: string; estado: string; total: number; actualizadoEn: string }[];
}

// ---------------- Kafka: administración (kafka-admin-service) ----------------

export interface BrokerInfo {
  id: number;
  host: string;
  puerto: number;
  rack: string | null;
  controlador: boolean;
}

export interface ClusterInfo {
  clusterId: string;
  controlador: number | null;
  brokersEsperados: number;
  brokers: BrokerInfo[];
}

export interface SaludKafka {
  estado: 'OK' | 'ADVERTENCIA' | 'CRITICO';
  brokersActivos: number;
  brokersEsperados: number;
  particionesSubReplicadas: number;
  particionesSinLider: number;
  lagPorGrupo: Record<string, number>;
  mensajesDlt: number;
  alertas: Alerta[];
}

export interface TopicoResumen {
  nombre: string;
  particiones: number;
  replicas: number;
  protegido: boolean;
  interno: boolean;
  cleanupPolicy: string | null;
  retentionMs: string | null;
  minInsyncReplicas: string | null;
  mensajes: number;
  particionesSubReplicadas: number;
}

export interface ParticionInfo {
  particion: number;
  lider: number | null;
  replicas: number[];
  isr: number[];
  offsetInicial: number;
  offsetFinal: number;
  mensajes: number;
  subReplicada: boolean;
}

export interface ConfigInfo {
  nombre: string;
  valor: string | null;
  porDefecto: boolean;
  fuente: string;
}

export interface TopicoDetalle {
  nombre: string;
  protegido: boolean;
  interno: boolean;
  replicas: number;
  particiones: ParticionInfo[];
  configs: ConfigInfo[];
  grupos: string[];
}

export interface CrearTopico {
  nombre: string;
  particiones: number;
  replicas: number;
  configs: Record<string, string>;
}

export interface GrupoResumen {
  grupo: string;
  estado: string;
  miembros: number;
  lagTotal: number;
  topicos: string[];
}

export interface OffsetParticion {
  topico: string;
  particion: number;
  offsetConfirmado: number | null;
  offsetFinal: number;
  lag: number;
  consumidor: string | null;
}

export interface GrupoDetalle {
  grupo: string;
  estado: string;
  coordinador: number | null;
  asignador: string;
  miembros: { consumidor: string; clientId: string; host: string; particiones: string[] }[];
  offsets: OffsetParticion[];
  lagTotal: number;
}

export interface PlantillaMensaje {
  id: string;
  titulo: string;
  topico: string;
  clave: string;
  valor: string;
  efectoEsperado: string;
}

export interface MensajePublicado {
  topico: string;
  particion: number;
  offset: number;
  clave: string | null;
}
