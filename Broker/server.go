package main

import (
	protocol "Desbloqueio-do-Estreito-de-Ormuz/Protocol"
	state "Desbloqueio-do-Estreito-de-Ormuz/State"
	"container/heap"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// Broker representa um nó do sistema distribuído.
// Cada broker gerencia um setor do Estreito de Ormuz.
type Broker struct {
	ID                 int                 // Guarda ID do Broker
	Endereco           string              // Guarda endereço IP do servidor
	OutrosBrokers      map[int]string      //Mapa de Conexão com os outros brokers
	Coordenador        int                 // ID do líder atual (-1 se indefinido)
	Estado             *state.GlobalState  // Estado volátil em RAM (Drones e Fila)
	mu                 sync.Mutex          // Garante exclusão mútua no acesso ao Estado
	MensagensPendentes []protocol.Mensagem // Buffer de espera
}

// ============================================================
// Finalização do Sistema
// ============================================================

// encerrarSistema prepara o broker para encerrar de forma limpa.
func (b *Broker) encerrarSistema() {
	sc := make(chan os.Signal, 1)
	// Escuta se o usuário der Ctrl+C ou o Docker mandar parar
	signal.Notify(sc, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sc // Fica bloqueado aqui até receber o sinal
		fmt.Printf("\n[Broker %d] [SISTEMA] Iniciando desligamento ordenado (Graceful Shutdown)...\n", b.ID)

		b.mu.Lock()
		isCoordenador := (b.ID == b.Coordenador)
		b.mu.Unlock()

		if isCoordenador {
			fmt.Println("[BROKER %d] Sou o Coordenador. Vou passar o controle para o sucessor")

			sucessorID := -1

			for id := range b.OutrosBrokers {
				if id > sucessorID {
					sucessorID = id
				}
			}

			if sucessorID != -1 {
				msgHandoff := protocol.Mensagem{
					Tipo:     protocol.TipoHandoff,
					IDOrigem: b.ID,
				}
				b.enviarMensagem(b.OutrosBrokers[sucessorID], msgHandoff)
				fmt.Printf("[SISTEMA] Comando de liderança enviado para o Broker %d\n", sucessorID)
			}
		}

		//Pausa para garantir o envio da mensagem
		time.Sleep(1 * time.Second)

		fmt.Printf("[Broker %d] [SISTEMA] Servidor encerrado com sucesso. Boa noite!\n", b.ID)
		os.Exit(0)
	}()
}

// ============================================================
// INICIALIZAÇÃO
// ============================================================

// carregarConfiguracao lê o mapa de rede do ficheiro JSON inicial.
func carregarConfiguracao(caminho string) (map[int]string, error) {
	arquivo, err := os.ReadFile(caminho)
	if err != nil {
		return nil, err
	}
	mapaString := make(map[string]string)
	json.Unmarshal(arquivo, &mapaString)

	mapaFinal := make(map[int]string)
	for k, v := range mapaString {
		id, _ := strconv.Atoi(k)
		mapaFinal[id] = v
	}
	return mapaFinal, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: broker [ID]")
		return
	}
	id, _ := strconv.Atoi(os.Args[1])

	// Permite sobrescrever o caminho do config via variável de ambiente
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "../config.json"
	}

	mapaRede, err := carregarConfiguracao(configPath)
	if err != nil {
		fmt.Printf("[Broker %d] Erro ao ler config: %v\n", id, err)
		return
	}

	meuEndereco, existe := mapaRede[id]
	if !existe {
		return
	}

	outros := make(map[int]string)
	for k, v := range mapaRede {
		if k != id {
			outros[k] = v
		}
	}

	// Inicializa o estado vazio na RAM
	estadoInicial := &state.GlobalState{
		Drones:       make(map[string]*protocol.Drone),
		FilaEspera:   make(state.FilaPrioridade, 0),
		UltimoUpdate: time.Now().Unix(),
	}
	heap.Init(&estadoInicial.FilaEspera)

	broker := &Broker{
		ID:                 id,
		Endereco:           meuEndereco,
		OutrosBrokers:      outros,
		Coordenador:        -1,
		Estado:             estadoInicial,
		MensagensPendentes: make([]protocol.Mensagem, 0),
	}

	// Inicia o servidor TCP
	go broker.Start()

	// Fica esperando um CTRL + C ou algum sinal de interrupção para finalizar o sistema
	broker.encerrarSistema()

	// Verifica se o Coordenador está vivo a cada 5 segundos
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			broker.verificarCoordenador()
		}
	}()

	// Dá uma pausa para aguardar todos os brokers subirem antes da eleição
	time.Sleep(2 * time.Second)
	broker.IniciarEleicao()

	select {} // Bloqueia a main para o processo não encerrar
}

// ============================================================
// SERVIDOR E MONITORAMENTO
// ============================================================

func (b *Broker) Start() {
	ln, err := net.Listen("tcp", b.Endereco)
	if err != nil {
		fmt.Printf("[Broker %d] Erro ao iniciar: %v\n", b.ID, err)
		os.Exit(1)
	}
	fmt.Printf("[Broker %d] Escutando em %s\n", b.ID, b.Endereco)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go b.LidarComMensagem(conn)
	}
}

// verificarCoordenador tenta realizar um ping no coordenador atual (Heartbeat).
// Se falhar, convoca uma eleição.
func (b *Broker) verificarCoordenador() {
	// Pega o id do Coordenador
	b.mu.Lock()
	coordID := b.Coordenador
	b.mu.Unlock()

	// Se não há líder ou se EU sou o líder, não precisa fazer o ping
	if coordID == -1 || b.ID == coordID {
		return
	}

	//Tenta realizar uma conexão rápida com o coordenador só para ver se ele tá vivo e espera 2 segundos para receber uma resposta
	conn, err := net.DialTimeout("tcp", b.OutrosBrokers[coordID], 2*time.Second)
	if err != nil {
		//Se o Coordenador não respondeu, assumo que ele morreu. Então, aviso esse problema e inicio uma nova votação
		fmt.Printf("[Broker %d] Líder %d offline! Convocando nova eleição.\n", b.ID, coordID)
		go b.IniciarEleicao()
	} else { //Se ainda existir, apenas fecho a conexão
		conn.Close()
	}
}

// ============================================================
// GESTÃO DE ESTADO E MENSAGENS
// ============================================================

// sincronizarEstado Envia o estado atual para todos os outros brokers.
func (b *Broker) sincronizarEstado() {
	//Pego o estado atual do Coordenador
	b.mu.Lock()
	payload, _ := json.Marshal(b.Estado)
	b.mu.Unlock()

	//Crio uma mensagem do tipo de sincronização e coloco o estado atual
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoSyncEstado,
		IDOrigem:  b.ID,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}

	//Envio o estado para todos os outros Brokers
	for _, endereco := range b.OutrosBrokers {
		go b.enviarMensagem(endereco, msg)
	}
}

// LidarComMensagem É responsável pela lógica dos brokers, ele quem vai decidir quais ações executar com base
// nos tipos de protocolos que receber
func (b *Broker) LidarComMensagem(conn net.Conn) {
	defer conn.Close()

	var msg protocol.Mensagem
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		return
	}

	//Irá direcionar as ações com base no tipo do protocolo
	switch msg.Tipo {
	// ELEIÇÃO usando o Algoritmo do Valentão
	case protocol.TipoEleicao:
		if msg.IDOrigem < b.ID {
			fmt.Printf("[Broker %d] ID %d quer liderar, mas sou maior. Assumindo eleição!\n", b.ID, msg.IDOrigem)
			msgOk := protocol.Mensagem{Tipo: protocol.TipoOkEleicao, IDOrigem: b.ID}
			b.enviarMensagem(b.OutrosBrokers[msg.IDOrigem], msgOk)
			go b.IniciarEleicao()
		}

	// Sincroniza o estado do líder com o meu
	case protocol.TipoSyncEstado:
		// Recebe backup do líder e atualiza a memória local
		var novoEstado state.GlobalState
		if err := json.Unmarshal([]byte(msg.Payload), &novoEstado); err == nil {
			b.mu.Lock()
			b.Estado = &novoEstado
			heap.Init(&b.Estado.FilaEspera) // Re-inicializa o Heap para garantir ordem
			b.mu.Unlock()
			fmt.Printf("[Broker %d] Estado sincronizado com o Coordenador %d\n", b.ID, msg.IDOrigem)
		}

	// Exibe uma mensagem de vitória se algum coordenador venceu e descarrega as requisições que não foram atendidas pelo coordenador
	case protocol.TipoVitoria:
		b.mu.Lock()
		b.Coordenador = msg.IDOrigem

		// Faz uma cópia das mensagens pendentes e limpa o buffer
		pendentes := b.MensagensPendentes
		b.MensagensPendentes = nil
		b.mu.Unlock()

		fmt.Printf("[Broker %d] Novo Coordenador eleito: %d\n", b.ID, msg.IDOrigem)

		// Se EU sou o novo coordenador, processo minhas próprias pendências
		if msg.IDOrigem == b.ID {
			for _, pendente := range pendentes {
				// Um truque elegante: mando a mensagem pra mim mesmo para ela
				// cair no topo do 'LidarComMensagem' como se fosse nova!
				go b.enviarMensagem(b.Endereco, pendente)
			}

			// Processa a fila pendente
			go b.tentarDespacharDrone()

			// Se OUTRO broker ganhou, reenvio as pendências para ele exigindo ACK
		} else {
			for _, pendente := range pendentes {
				fmt.Printf("[Broker %d] Reenviando requisição pendente para o novo líder %d\n", b.ID, msg.IDOrigem)
				go b.enviarMensagemComAck(b.OutrosBrokers[msg.IDOrigem], pendente)
			}
		}

	case protocol.TipoOcorrencia:
		var ocorrencia protocol.Ocorrencia
		if b.ID == b.Coordenador {
			json.Unmarshal([]byte(msg.Payload), &ocorrencia)

			//Se houve um tempo na formatação do tempo, atualizo novamente aqui
			if ocorrencia.Timestamp.IsZero() {
				ocorrencia.Timestamp = time.Now()
			}

			b.mu.Lock()
			//Adiciona a requisição na fila de prioridade
			heap.Push(&b.Estado.FilaEspera, &ocorrencia)
			b.mu.Unlock()

			b.sincronizarEstado() // Notifica os outros sobre a nova tarefa

			//Envia uma resposta de confirmação ao broker que solicitou a ação
			ack := protocol.Mensagem{Tipo: protocol.TipoACK, IDOrigem: b.ID}
			json.NewEncoder(conn).Encode(ack)

			fmt.Printf("[Broker %d] Ocorrência %s enfileirada (Prioridade %d). Fila: %d item(s)\n",
				b.ID, ocorrencia.ID, ocorrencia.Prioridade, b.Estado.FilaEspera.Len())

			// Tenta imediatamente despachar um drone para esta ocorrência
			go b.tentarDespacharDrone()

			//Se não for o coordenador
		} else if b.Coordenador != -1 {
			// Repassa a requisição ao coordenador se falhar, assume que ele caiu e pede eleição
			fmt.Printf("[Broker %d] Repassando ocorrência ao Coordenador %d\n", b.ID, b.Coordenador)

			if !b.enviarMensagemComAck(b.OutrosBrokers[b.Coordenador], msg) {
				fmt.Printf("[Broker %d] O Coordenador %d parou de responder, iniciar nova eleição!\n", b.ID, b.Coordenador)
				//Guarda a mensagem no buffer
				b.mu.Lock()
				b.MensagensPendentes = append(b.MensagensPendentes, msg)
				b.mu.Unlock()

				// Convoca uma eleição
				go b.IniciarEleicao()
			}
		} else {
			fmt.Printf("[Broker %d] Sem coordenador no momento. Ocorrência %s ficará temporariamente em espera.\n",
				b.ID, ocorrencia.ID)
			b.mu.Lock()
			b.MensagensPendentes = append(b.MensagensPendentes, msg)
			b.mu.Unlock()
		}

	// --- REGISTRO DE DRONES ---
	// Drone acabou de subir e se apresenta ao sistema
	case protocol.TipoRegistroDrone:
		if b.ID == b.Coordenador {
			var droneInfo protocol.Drone
			json.Unmarshal([]byte(msg.Payload), &droneInfo)

			b.mu.Lock()
			b.Estado.Drones[droneInfo.ID] = &droneInfo
			b.mu.Unlock()
			b.sincronizarEstado() // Atualiza pool de drones nos outros brokers

			fmt.Printf("[Broker %d] Drone %s registrado (Status: %s, Bateria: %d%%)\n",
				b.ID, droneInfo.ID, droneInfo.Status, droneInfo.Bateria)

			go b.tentarDespacharDrone()

		} else if b.Coordenador != -1 {
			if !b.enviarMensagem(b.OutrosBrokers[b.Coordenador], msg) {
				fmt.Printf("[Broker %d] O Broker Coordenador %d parou de responder, iniciar nova eleição!\n", b.ID, b.Coordenador)
				go b.IniciarEleicao()
			}
		}

	// --- RETORNO DE DRONES ---
	// Drone concluiu missão e reporta status atualizado
	case protocol.TipoStatusDrone:
		if b.ID == b.Coordenador {
			var droneInfo protocol.Drone
			json.Unmarshal([]byte(msg.Payload), &droneInfo)

			b.mu.Lock()
			if drone, existe := b.Estado.Drones[droneInfo.ID]; existe {
				drone.Status = droneInfo.Status
				drone.Bateria = droneInfo.Bateria
				drone.MissaoID = "" //Libera da missão anterior
			}
			b.mu.Unlock()
			b.sincronizarEstado() // Informa que o drone está livre
			fmt.Printf("[Broker %d] Drone %s retornou. Status: %s | Bateria: %d%%\n",
				b.ID, droneInfo.ID, droneInfo.Status, droneInfo.Bateria)

			// Drone livre: tenta atender próxima da fila
			go b.tentarDespacharDrone()
		} else if b.Coordenador != -1 {
			if !b.enviarMensagem(b.OutrosBrokers[b.Coordenador], msg) {
				fmt.Printf("[Broker %d] O Broker Coordenador %d parou de responder, iniciar nova eleição!\n", b.ID, b.Coordenador)
				go b.IniciarEleicao()
			}
		}
	}
}

// ============================================================
// DESPACHO E ELEIÇÃO
// ============================================================
// tentarDespacharDrone verifica se há ocorrências na fila e se possui drones disponíveis.
// Se sim, despacha o drone mais adequado para a ocorrência mais urgente.
// Chamado sempre que: (1) nova ocorrência chega, (2) drone fica livre, (3) eleição termina.
func (b *Broker) tentarDespacharDrone() {
	// Apenas o coordenador gerencia o despacho
	if b.ID != b.Coordenador {
		return
	}

	b.mu.Lock() // 🔒 TRANCA O ESTADO

	// Nada na fila: destranca e sai
	if b.Estado.FilaEspera.Len() == 0 {
		b.mu.Unlock() // 🔓 DESTRANCA ANTES DE SAIR
		return
	}

	// Procura um drone disponível
	var droneEscolhido *protocol.Drone
	for _, drone := range b.Estado.Drones {
		if drone.Status == "disponivel" && drone.Bateria > 10 {
			// Prefere drone com mais bateria (critério de seleção)
			if droneEscolhido == nil || drone.Bateria > droneEscolhido.Bateria {
				droneEscolhido = drone
			}
		}
	}

	// Se não tiver drones disponíveis, mostra no terminal
	if droneEscolhido == nil {
		fmt.Printf("[Broker %d] Fila com %d item(s), mas nenhum drone disponível no momento.\n",
			b.ID, b.Estado.FilaEspera.Len())
		b.mu.Unlock() // 🔓 DESTRANCA ANTES DE SAIR
		return
	}

	// Remove a ocorrência mais urgente (mais crítica) da fila
	ocorrencia := heap.Pop(&b.Estado.FilaEspera).(*protocol.Ocorrencia)

	// Marca o drone como ocupado antes de enviar o comando (evita duplo despacho)
	droneEscolhido.Status = "em_missao"
	droneEscolhido.MissaoID = ocorrencia.ID

	b.mu.Unlock()

	// Sincroniza o estado com os outros brokers
	b.sincronizarEstado()

	fmt.Printf("[Broker %d] ✈ Despachando Drone %s para ocorrência %s (Prioridade %d)\n",
		b.ID, droneEscolhido.ID, ocorrencia.ID, ocorrencia.Prioridade)

	// Envia o comando ao drone
	go b.enviarComandoAoDrone(droneEscolhido.Posicao, droneEscolhido.ID, ocorrencia)
}

// enviarComandoAoDrone envia a ordem de missão para o endereço TCP do drone.
// Se o drone não responder, recoloca a ocorrência na fila e libera o slot.
func (b *Broker) enviarComandoAoDrone(enderecoDrone string, droneID string, ocorrencia *protocol.Ocorrencia) {
	comando := protocol.ComandoMissao{
		OcorrenciaID: ocorrencia.ID,
		Descricao:    ocorrencia.Descricao,
		Prioridade:   ocorrencia.Prioridade,
	}
	payload, _ := json.Marshal(comando)

	msg := protocol.Mensagem{
		Tipo:      protocol.TipoComandoDrone,
		IDOrigem:  b.ID,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}

	if !b.enviarMensagem(enderecoDrone, msg) {
		// Drone inacessível: recoloca a ocorrência na fila e libera o drone
		fmt.Printf("[Broker %d] Drone %s inacessível! Recolocando ocorrência %s na fila.\n",
			b.ID, droneID, ocorrencia.ID)

		b.mu.Lock()
		heap.Push(&b.Estado.FilaEspera, ocorrencia) // 1. Devolve a tarefa
		if drone, ok := b.Estado.Drones[droneID]; ok {
			drone.Status = "indisponivel" // 2. Marca o drone como defeituoso
			drone.MissaoID = ""
		}
		b.mu.Unlock()

		// 3. Avisa os outros brokers que a fila cresceu e um drone quebrou
		b.sincronizarEstado()

		// 4. Tenta achar outro drone imediatamente para não atrasar a emergência
		go b.tentarDespacharDrone()
	}
}

func (b *Broker) IniciarEleicao() {
	b.mu.Lock()
	b.Coordenador = -1
	b.mu.Unlock()

	fmt.Printf("\n[Broker %d] Iniciando eleição...\n", b.ID)
	temMaior := false

	msgEleicao := protocol.Mensagem{
		Tipo:      protocol.TipoEleicao,
		IDOrigem:  b.ID,
		Timestamp: time.Now(),
	}

	for idVizinho, endereco := range b.OutrosBrokers {
		//Envia a eleição para o outros servidores com ID maior
		if idVizinho > b.ID {
			if b.enviarMensagem(endereco, msgEleicao) {
				temMaior = true
				fmt.Printf("[Broker %d] Eleição enviada ao ID %d\n", b.ID, idVizinho)
			}
		}
	}

	if !temMaior {
		// Nenhum broker maior respondeu: sou o coordenador
		b.mu.Lock()
		b.Coordenador = b.ID //Atualiza o ID do coordenador
		b.mu.Unlock()

		fmt.Printf("[Broker %d]Sou o novo Coordenador!\n", b.ID)

		//Cria uma mensagem de vitória, avisando que há um novo coordenador
		msgVitoria := protocol.Mensagem{
			Tipo:      protocol.TipoVitoria,
			IDOrigem:  b.ID,
			Timestamp: time.Now(),
		}

		//Envia a mensagem de vitória aos outros brokers, informando o ID do novo coordenador
		for idVizinho, endereco := range b.OutrosBrokers {
			if idVizinho < b.ID {
				b.enviarMensagem(endereco, msgVitoria)
			}
		}

		// Processa a fila que pode ter acumulado durante a eleição
		go b.tentarDespacharDrone()
	}
}

// ============================================================
// COMUNICAÇÃO
// ============================================================
func (b *Broker) enviarMensagem(ipDestino string, msg protocol.Mensagem) bool {
	conn, err := net.DialTimeout("tcp", ipDestino, 1*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	return json.NewEncoder(conn).Encode(msg) == nil
}

// enviarMensagemComAck Envia uma mensagem e espera o ACK de confirmação do coordenador
func (b *Broker) enviarMensagemComAck(ipDestino string, msg protocol.Mensagem) bool {
	conn, err := net.DialTimeout("tcp", ipDestino, 2*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		return false
	}

	// Aguarda o ACK do coordenador (timeout de 2s)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var resposta protocol.Mensagem
	if err := json.NewDecoder(conn).Decode(&resposta); err != nil || resposta.Tipo != protocol.TipoACK {
		return false
	}
	return true
}
