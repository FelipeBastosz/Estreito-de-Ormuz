package protocol

import "time"

// Constantes: Usamos constantes para evitar o clássico erro de digitar "ELEICAO"
// em um arquivo e "ELEICÃO" em outro, o que faria o sistema falhar silenciosamente.
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

// Mensagem: É o "Envelope" universal do nosso sistema.
// TUDO que viaja no Socket TCP tem que ter esse formato exato.
type Mensagem struct {
	// A tag `json:"tipo"` ensina o Go a transformar a variável "Tipo" na chave "tipo" no JSON.
	Tipo string `json:"tipo"`

	// IDOrigem é o remetente. Vital para o Valentão saber se quem mandou a mensagem é "maior" ou "menor".
	IDOrigem int `json:"id_origem"`

	// Timestamp carimba a hora que o envelope foi fechado. Ajuda a ordenar os eventos.
	Timestamp time.Time `json:"timestamp"`

	// Payload é o recheio. O Go vai transformar a struct Ocorrencia ou Drone em uma
	// string (texto puro) e colocar aqui dentro.
	Payload string `json:"payload"`
}

// Ocorrencia: Representa a emergência real enviada pelo sensor.
type Ocorrencia struct {
	ID         string    `json:"id"`
	Prioridade int       `json:"prioridade"` // Ex: 3 (Crítico), 2 (Alerta), 1 (Aviso)
	Timestamp  time.Time `json:"timestamp"`  // Quando o evento físico aconteceu
	Descricao  string    `json:"descricao"`
}

// Drone e Sensor são representações físicas (Digital Twins) dos seus atuadores.
type Drone struct {
	ID       string `json:"id"`
	Posicao  string `json:"posicao"` // Endereço TCP onde o drone escuta comandos
	Status   string `json:"status"`  // "disponivel", "em_missao", "recarregando"
	Bateria  int    `json:"bateria"`
	MissaoID string `json:"missao_id"` // ID da ocorrência em atendimento (vazio se livre)
}

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
