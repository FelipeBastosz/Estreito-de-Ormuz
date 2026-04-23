package state

import (
	"Desbloqueio-do-Estreito-de-Ormuz/Protocol"
	"encoding/json"
	"os"
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

// SalvarEstado pega tudo que está na RAM e joga no HD.
func SalvarEstado(nomeArquivo string, estado interface{}) error {
	fileMutex.Lock()         // Tranca a porta: Ninguém mais mexe no arquivo agora!
	defer fileMutex.Unlock() // A palavra 'defer' garante que a porta será destrancada no final, mesmo se der erro no meio.

	// MarshalIndent transforma a struct em um JSON bonitinho, com quebras de linha e espaços ("  ").
	// Facilita muito se você quiser abrir o arquivo no bloco de notas para debugar.
	data, err := json.MarshalIndent(estado, "", "  ")
	if err != nil {
		return err
	}

	// Escreve os bytes no HD. O '0644' são permissões padrão do Linux (leitura/escrita).
	return os.WriteFile(nomeArquivo, data, 0644)
}

// CarregarEstado lê o HD e joga de volta para a RAM.
// Chamamos essa função sempre que o Broker acaba de ligar.
func CarregarEstado(nomeArquivo string, estado interface{}) error {
	data, err := os.ReadFile(nomeArquivo)
	if err != nil {
		return err
	}

	// Pega o texto JSON puro (data) e reconstrói as variáveis dentro da struct (estado)
	return json.Unmarshal(data, estado)
}
