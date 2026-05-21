package protocol

import "time"

// Constantes de tipo de mensagem.
// Usar constantes cria uma padronização de comunicação e evita bugs de digitação.
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
	TipoHandoff       = "Handoff"        //Passa a liderança para o servidor com id menor mais próximo
	TipoPedidoEstado  = "REQ_SYNC"       // Um broker recém-iniciado pedindo o estado global para os outros brokers.
)

// Mensagem é o "envelope" universal do sistema.
// TUDO que trafega no TCP tem este formato.
type Mensagem struct {
	Tipo      string    `json:"tipo"`      // Define a ação a ser tomada no switch case do broker.
	IDOrigem  int       `json:"id_origem"` // ID do broker remetente (0 para sensores/drones)
	Timestamp time.Time `json:"timestamp"` // Relógio lógico da mensagem (usado para desempates e logs).
	Payload   string    `json:"payload"`   // JSON de Ocorrencia, Drone ou ComandoMissao.
}

// Ocorrencia representa uma emergência detectada por um sensor.
type Ocorrencia struct {
	ID         string    `json:"id"`         // Identificador gerado no formato "SensorID-OC0001".
	Prioridade int       `json:"prioridade"` // Nível de urgência: 3=Crítico, 2=Alerta, 1=Aviso
	Timestamp  time.Time `json:"timestamp"`  // Critério de desempate na Heap, ganha quem chega primeiro.
	Descricao  string    `json:"descricao"`  // Detalhamento da ocorrência
	Setor      string    `json:"setor"`      // Setor de origem da ocorrência
}

// Drone é a representação de um drone físico.
type Drone struct {
	ID       string `json:"id"`        // Identificador único do drone (ex: "drone1")
	Posicao  string `json:"posicao"`   // Endereço TCP real onde este drone recebe comandos (ex: "172.16.0.5:9091")
	Status   string `json:"status"`    // Estado atual: "disponivel", "em_missao", "recarregando"
	Bateria  int    `json:"bateria"`   // Nível de bateria atual (0 a 100%)
	MissaoID string `json:"missao_id"` // ID da ocorrência em atendimento (vazio se livre)
}

// Sensor representa um sensor físico de um setor.
type Sensor struct {
	ID    string `json:"id"`    // Ex: "S1A"
	Setor string `json:"setor"` // Ex: "setor-1"
	Tipo  string `json:"tipo"`
}

// ComandoMissao é o payload enviado dentro de TipoComandoDrone.
type ComandoMissao struct {
	OcorrenciaID string `json:"ocorrencia_id"`
	Descricao    string `json:"descricao"`
	Prioridade   int    `json:"prioridade"`
}
