package protocol

import "time"

// Constantes de tipo de mensagem.
// Usar constantes evita bugs silenciosos por erro de digitação.
const (
	TipoEleicao       = "ELEICAO"        // Início do Algoritmo do Valentão
	TipoVitoria       = "COORDINATOR"    // Fim do Algoritmo do Valentão
	TipoOkEleicao     = "OK"             // Resposta de que o remetente é maior
	TipoOcorrencia    = "NOVA_TAREFA"    // Um sensor detectou algo
	TipoStatusDrone   = "STATUS_DRONE"   // Um drone reportando conclusão de missão
	TipoSyncEstado    = "SYNC_GLOBAL"    // Coordenador enviando backup de estado
	TipoComandoDrone  = "COMANDO_DRONE"  // Coordenador ordenando drone a se mover
	TipoRegistroDrone = "REGISTRO_DRONE" // Drone se apresentando ao sistema
	TipoACK           = "ACK"            //Resposta do coordenador ao broker que solicitou o serviço
)

// Mensagem é o "envelope" universal do sistema.
// TUDO que trafega no TCP tem este formato.
type Mensagem struct {
	Tipo      string    `json:"tipo"`
	IDOrigem  int       `json:"id_origem"` // ID do broker remetente (0 para sensores/drones)
	Timestamp time.Time `json:"timestamp"`
	Payload   string    `json:"payload"` // JSON de Ocorrencia, Drone, etc.
}

// Ocorrencia representa uma emergência detectada por um sensor.
type Ocorrencia struct {
	ID         string    `json:"id"`
	Prioridade int       `json:"prioridade"` // 3=Crítico, 2=Alerta, 1=Aviso
	Timestamp  time.Time `json:"timestamp"`
	Descricao  string    `json:"descricao"`
	Setor      string    `json:"setor"` // Setor de origem da ocorrência
}

// Drone é a representação digital (Digital Twin) de um drone físico.
type Drone struct {
	ID       string `json:"id"`
	Posicao  string `json:"posicao"` // Endereço TCP onde o drone escuta comandos
	Status   string `json:"status"`  // "disponivel", "em_missao", "recarregando"
	Bateria  int    `json:"bateria"`
	MissaoID string `json:"missao_id"` // ID da ocorrência em atendimento (vazio se livre)
}

// Sensor representa um sensor físico de um setor.
type Sensor struct {
	ID    string `json:"id"`
	Setor string `json:"setor"`
	Tipo  string `json:"tipo"`
}

// ComandoMissao é o payload enviado dentro de TipoComandoDrone.
type ComandoMissao struct {
	OcorrenciaID string `json:"ocorrencia_id"`
	Descricao    string `json:"descricao"`
	Prioridade   int    `json:"prioridade"`
}
