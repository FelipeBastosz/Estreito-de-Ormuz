package state

import (
	"Desbloqueio-do-Estreito-de-Ormuz/Protocol"
	"sync"
)

// fileMutex é o nosso cadeado.
// Impede que duas Goroutines tentem abrir e escrever no arquivo state.json NO MESMO MILISSEGUNDO,
// o que corromperia o arquivo e destruiria os dados.
var fileMutex sync.Mutex

// GlobalState é a foto inteira do sistema no momento exato.
type GlobalState struct {
	Drones     map[string]*protocol.Drone `json:"drones"`
	FilaEspera FilaPrioridade             `json:"fila_espera"`

	// UltimoUpdate serve para o caso de termos vários arquivos. O Coordenador olha
	// quem tem o número maior (timestamp) para saber qual é a versão mais recente.
	UltimoUpdate int64 `json:"ultimo_update"`
}
