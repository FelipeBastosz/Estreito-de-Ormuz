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

// Cada broker representa um setor do sistema. Sincroniza e colabora para despachar drones.
type Broker struct {
	ID                 int
	Endereco           string
	OutrosBrokers      map[int]string
	Coordenador        int
	Estado             *state.GlobalState
	mu                 sync.Mutex
	MensagensPendentes []protocol.Mensagem
	UltimaSync         time.Time
}

// Listen para sinais de sistema e executa shutdown limpo.
// Se eu for coordenador, aviso quem deve assumir.
func (b *Broker) encerrarSistema() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sc
		fmt.Printf("\n[Broker %d] [SISTEMA] Desligando o servidor...\n", b.ID)
		b.mu.Lock()
		isCoordenador := (b.ID == b.Coordenador)
		b.mu.Unlock()
		if isCoordenador {
			fmt.Printf("[BROKER %d] Sou o Coordenador. Vou passar o controle para o sucessor\n", b.ID)
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
		time.Sleep(1 * time.Second)
		fmt.Printf("[Broker %d] [SISTEMA] Servidor encerrado com sucesso!\n", b.ID)
		os.Exit(0)
	}()
}

// Carrega configurações do cluster dos brokers
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

// O coordenador envia periodicamente snapshots do estado para garantir sincronização de novos brokers
func (b *Broker) sincronizarPeriodicamente() {
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		if b.isCoordinator() {
			b.sincronizarEstado()
		}
	}
}

// Pede o estado aos peers assim que sobe
func (b *Broker) solicitarEstado() {
	msg := protocol.Mensagem{
		Tipo:     protocol.TipoPedidoEstado,
		IDOrigem: b.ID,
	}
	for _, endereco := range b.OutrosBrokers {
		b.enviarMensagem(endereco, msg) // Não precisa de go routine aqui; poucos peers.
	}
}

// Espera até x segundos ou até receber sync de estado (UltimaSync setado)
func (b *Broker) esperarSync(timeout time.Duration) {
	limite := time.After(timeout)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-limite:
			return
		case <-tick.C:
			b.mu.Lock()
			ok := !b.UltimaSync.IsZero()
			b.mu.Unlock()
			if ok {
				return
			}
		}
	}
}

// Leitura thread-safe de Coordenador (helper simples)
func (b *Broker) isCoordinator() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Coordenador == b.ID
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: broker [ID]")
		return
	}
	id, _ := strconv.Atoi(os.Args[1])
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

	go broker.Start()
	go broker.sincronizarPeriodicamente()
	broker.encerrarSistema()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			broker.verificarCoordenador()
		}
	}()

	// Sincronização do estado antes de tentar tomar a liderança (importante!)
	broker.solicitarEstado()
	broker.esperarSync(5 * time.Second) // usa um valor generoso

	time.Sleep(2 * time.Second)
	broker.mu.Lock()
	semLider := (broker.Coordenador == -1)
	broker.mu.Unlock()
	if semLider {
		broker.IniciarEleicao()
	} else {
		broker.mu.Lock()
		coordAtual := broker.Coordenador
		broker.mu.Unlock()
		fmt.Printf("[Broker %d] Entrei na rede, coordenador ativo: %d\n", broker.ID, coordAtual)
	}
	select {}
}

// TCP loop aceitando conexões de outros brokers, drones ou sensores
func (b *Broker) Start() {
	_, porta, err := net.SplitHostPort(b.Endereco)
	if err != nil {
		fmt.Printf("[Broker %d] Erro ao extrair porta de %s: %v\n", b.ID, b.Endereco, err)
		os.Exit(1)
	}
	ln, err := net.Listen("tcp", "0.0.0.0:"+porta)
	if err != nil {
		fmt.Printf("[Broker %d] Erro ao iniciar na porta %s: %v\n", b.ID, porta, err)
		os.Exit(1)
	}
	fmt.Printf("[Broker %d] Escutando na porta %s\n", b.ID, porta)
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go b.LidarComMensagem(conn)
	}
}

// Checa periodicamente se o coordenador está vivo
func (b *Broker) verificarCoordenador() {
	b.mu.Lock()
	coordID := b.Coordenador
	b.mu.Unlock()
	if coordID == -1 || b.ID == coordID {
		return
	}
	conn, err := net.DialTimeout("tcp", b.OutrosBrokers[coordID], 2*time.Second)
	if err != nil {
		fmt.Printf("[Broker %d] Líder %d offline! Convocando nova eleição.\n", b.ID, coordID)
		go b.IniciarEleicao()
	} else {
		conn.Close()
	}
}

// Garante consenso de estado entre os brokers
func (b *Broker) sincronizarEstado() {
	b.mu.Lock()
	payload, _ := json.Marshal(b.Estado)
	b.mu.Unlock()
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoSyncEstado,
		IDOrigem:  b.ID,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}
	for _, endereco := range b.OutrosBrokers {
		go b.enviarMensagem(endereco, msg)
	}
}

// Trata TODOS os tipos de mensagem do protocolo
func (b *Broker) LidarComMensagem(conn net.Conn) {
	defer conn.Close()
	var msg protocol.Mensagem
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		return
	}
	switch msg.Tipo {
	case protocol.TipoEleicao:
		if msg.IDOrigem < b.ID {
			fmt.Printf("[Broker %d] ID %d quer liderar, mas sou maior. Assumindo eleição!\n", b.ID, msg.IDOrigem)
			msgOk := protocol.Mensagem{Tipo: protocol.TipoOkEleicao, IDOrigem: b.ID}
			b.enviarMensagem(b.OutrosBrokers[msg.IDOrigem], msgOk)
			go b.IniciarEleicao()
		}
	case protocol.TipoOkEleicao:
		// O algoritmo só depende de receber OK. Aqui marcamos log, mas poderíamos usar a info para abortar auto-votação (opcional).
		fmt.Printf("[Broker %d] Recebi OK de broker %d (existe alguém maior vivo).\n", b.ID, msg.IDOrigem)
	case protocol.TipoPedidoEstado:
		b.mu.Lock()
		isCoord := (b.ID == b.Coordenador)
		b.mu.Unlock()
		if isCoord {
			if endereco, ok := b.OutrosBrokers[msg.IDOrigem]; ok {
				b.mu.Lock()
				payload, _ := json.Marshal(b.Estado)
				b.mu.Unlock()
				resp := protocol.Mensagem{
					Tipo:      protocol.TipoSyncEstado,
					IDOrigem:  b.ID,
					Timestamp: time.Now(),
					Payload:   string(payload),
				}
				b.enviarMensagem(endereco, resp)
			}
		}
	case protocol.TipoSyncEstado:
		if msg.IDOrigem < b.ID {
			fmt.Printf("\n[SPLIT-BRAIN] Líder menor BROKER%d detectado. Vou sincronizar com ele antes de retomar a liderança.\n", msg.IDOrigem)
			var novoEstado state.GlobalState
			if err := json.Unmarshal([]byte(msg.Payload), &novoEstado); err == nil {
				b.mu.Lock()
				b.Estado = &novoEstado
				heap.Init(&b.Estado.FilaEspera)
				b.Coordenador = msg.IDOrigem
				b.UltimaSync = time.Now()
				b.mu.Unlock()
				fmt.Printf("[Broker %d] Estado sincronizado com o Coordenador %d\n", b.ID, msg.IDOrigem)
			}
			go b.IniciarEleicao()
			return
		}
		if msg.IDOrigem > b.ID {
			b.mu.Lock()
			if b.Coordenador != msg.IDOrigem {
				fmt.Printf("\n[SPLIT-BRAIN] Reconhecendo o retorno do líder superior: %d\n", msg.IDOrigem)
				b.Coordenador = msg.IDOrigem
			}
			b.mu.Unlock()
		}
		var novoEstado state.GlobalState
		if err := json.Unmarshal([]byte(msg.Payload), &novoEstado); err == nil {
			b.mu.Lock()
			b.Estado = &novoEstado
			heap.Init(&b.Estado.FilaEspera)
			b.UltimaSync = time.Now()
			b.mu.Unlock()
			fmt.Printf("[Broker %d] Estado sincronizado com o Coordenador %d\n", b.ID, msg.IDOrigem)
		}
	case protocol.TipoHandoff:
		fmt.Printf("[Broker %d] O antigo coordenador morreu. Assumindo a liderança\n", b.ID)
		b.mu.Lock()
		b.Coordenador = b.ID
		b.mu.Unlock()
		msgVitoria := protocol.Mensagem{
			Tipo:      protocol.TipoVitoria,
			IDOrigem:  b.ID,
			Timestamp: time.Now(),
		}
		for id, endereco := range b.OutrosBrokers {
			if id < b.ID {
				go b.enviarMensagem(endereco, msgVitoria)
			}
		}
		go b.tentarDespacharDrone()
	case protocol.TipoVitoria:
		b.mu.Lock()
		b.Coordenador = msg.IDOrigem
		pendentes := b.MensagensPendentes
		b.MensagensPendentes = nil
		b.mu.Unlock()
		fmt.Printf("[Broker %d] Novo Coordenador eleito: %d\n", b.ID, msg.IDOrigem)
		if msg.IDOrigem == b.ID {
			for _, pendente := range pendentes {
				go b.enviarMensagem(b.Endereco, pendente)
			}
			go b.tentarDespacharDrone()
		} else {
			for _, pendente := range pendentes {
				mensagemPendente := pendente
				go func(reenviarMsg protocol.Mensagem) {
					fmt.Printf("[Broker %d] Tentando reenviar pendência para o coordenador %d\n", b.ID, msg.IDOrigem)
					if !b.enviarMensagemComAck(b.OutrosBrokers[msg.IDOrigem], reenviarMsg) {
						fmt.Printf("[Broker %d] LÍDER %d FALHOU no reenvio! Vai para buffer local.\n", b.ID, msg.IDOrigem)
						b.mu.Lock()
						b.MensagensPendentes = append(b.MensagensPendentes, reenviarMsg)
						b.mu.Unlock()
						go b.verificarCoordenador()
					}
				}(mensagemPendente)
			}
		}
	case protocol.TipoOcorrencia:
		var ocorrencia protocol.Ocorrencia
		b.mu.Lock()
		euSouCoord := b.Coordenador == b.ID
		b.mu.Unlock()
		if euSouCoord {
			json.Unmarshal([]byte(msg.Payload), &ocorrencia)
			if ocorrencia.Timestamp.IsZero() {
				ocorrencia.Timestamp = time.Now()
			}
			b.mu.Lock()
			heap.Push(&b.Estado.FilaEspera, &ocorrencia)
			b.mu.Unlock()
			b.sincronizarEstado()
			ack := protocol.Mensagem{Tipo: protocol.TipoACK, IDOrigem: b.ID}
			json.NewEncoder(conn).Encode(ack)
			fmt.Printf("[Broker %d] Ocorrência %s enfileirada (Prioridade %d). Fila: %d item(s)\n",
				b.ID, ocorrencia.ID, ocorrencia.Prioridade, b.Estado.FilaEspera.Len())
			go b.tentarDespacharDrone()
		} else {
			b.mu.Lock()
			temCoord := b.Coordenador != -1
			coord := b.Coordenador
			b.mu.Unlock()
			if temCoord {
				fmt.Printf("[Broker %d] Repassando ocorrência ao Coordenador %d\n", b.ID, coord)
				if !b.enviarMensagemComAck(b.OutrosBrokers[coord], msg) {
					fmt.Printf("[Broker %d] O Coordenador %d parou de responder, iniciar nova eleição!\n", b.ID, coord)
					b.mu.Lock()
					b.MensagensPendentes = append(b.MensagensPendentes, msg)
					b.mu.Unlock()
					go b.IniciarEleicao()
				}
			} else {
				fmt.Printf("[Broker %d] Sem coordenador no momento. Ocorrência %s ficará temporariamente em espera.\n",
					b.ID, ocorrencia.ID)
				b.mu.Lock()
				b.MensagensPendentes = append(b.MensagensPendentes, msg)
				b.mu.Unlock()
			}
		}
	case protocol.TipoRegistroDrone:
		b.mu.Lock()
		euSouCoord := b.Coordenador == b.ID
		b.mu.Unlock()
		if euSouCoord {
			var droneInfo protocol.Drone
			json.Unmarshal([]byte(msg.Payload), &droneInfo)
			b.mu.Lock()
			b.Estado.Drones[droneInfo.ID] = &droneInfo
			b.mu.Unlock()
			b.sincronizarEstado()
			fmt.Printf("[Broker %d] Drone %s registrado (Status: %s, Bateria: %d%%)\n",
				b.ID, droneInfo.ID, droneInfo.Status, droneInfo.Bateria)
			go b.tentarDespacharDrone()
		} else {
			b.mu.Lock()
			temCoord := b.Coordenador != -1
			coord := b.Coordenador
			b.mu.Unlock()
			if temCoord {
				if !b.enviarMensagem(b.OutrosBrokers[coord], msg) {
					fmt.Printf("[Broker %d] O Broker Coordenador %d parou de responder, iniciar nova eleição!\n", b.ID, coord)
					go b.IniciarEleicao()
				}
			}
		}
	case protocol.TipoStatusDrone:
		b.mu.Lock()
		euSouCoord := b.Coordenador == b.ID
		b.mu.Unlock()
		if euSouCoord {
			var droneInfo protocol.Drone
			json.Unmarshal([]byte(msg.Payload), &droneInfo)
			b.mu.Lock()
			if drone, existe := b.Estado.Drones[droneInfo.ID]; existe {
				drone.Status = droneInfo.Status
				drone.Bateria = droneInfo.Bateria
				drone.MissaoID = ""
			}
			b.mu.Unlock()
			b.sincronizarEstado()
			fmt.Printf("[Broker %d] Drone %s retornou. Status: %s | Bateria: %d%%\n",
				b.ID, droneInfo.ID, droneInfo.Status, droneInfo.Bateria)
			go b.tentarDespacharDrone()
		} else {
			b.mu.Lock()
			temCoord := b.Coordenador != -1
			coord := b.Coordenador
			b.mu.Unlock()
			if temCoord {
				if !b.enviarMensagem(b.OutrosBrokers[coord], msg) {
					fmt.Printf("[Broker %d] O Broker Coordenador %d parou de responder, iniciar nova eleição!\n", b.ID, coord)
					go b.IniciarEleicao()
				}
			}
		}
	}
}

// Despacha drones enquanto existirem drones e ocorrências na fila (NÃO RECURSIVO!)
func (b *Broker) tentarDespacharDrone() {
	for {
		b.mu.Lock()
		if b.Coordenador != b.ID {
			b.mu.Unlock()
			return
		}
		if b.Estado.FilaEspera.Len() == 0 {
			b.mu.Unlock()
			return
		}
		var droneEscolhido *protocol.Drone
		for _, drone := range b.Estado.Drones {
			if drone.Status == "disponivel" && drone.Bateria > 10 {
				if droneEscolhido == nil || drone.Bateria > droneEscolhido.Bateria {
					droneEscolhido = drone
				}
			}
		}
		if droneEscolhido == nil {
			fmt.Printf("[Broker %d] Fila com %d item(s), mas nenhum drone disponível no momento.\n",
				b.ID, b.Estado.FilaEspera.Len())
			b.mu.Unlock()
			return
		}
		ocorrencia := heap.Pop(&b.Estado.FilaEspera).(*protocol.Ocorrencia)
		droneEscolhido.Status = "em_missao"
		droneEscolhido.MissaoID = ocorrencia.ID
		b.mu.Unlock()
		b.sincronizarEstado()
		fmt.Printf("[Broker %d] ✈ Despachando Drone %s para ocorrência %s (Prioridade %d)\n",
			b.ID, droneEscolhido.ID, ocorrencia.ID, ocorrencia.Prioridade)
		go b.enviarComandoAoDrone(droneEscolhido.Posicao, droneEscolhido.ID, ocorrencia)
		// Enquanto houver drones e ocorrências, permanece no loop!
	}
}

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
	sucesso := false
	conn, err := net.DialTimeout("tcp", enderecoDrone, 3*time.Second)
	if err == nil {
		defer conn.Close()
		if err := json.NewEncoder(conn).Encode(msg); err == nil {
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			var resposta protocol.Mensagem
			if err := json.NewDecoder(conn).Decode(&resposta); err == nil {
				if resposta.Tipo == protocol.TipoACK {
					sucesso = true
				}
			}
		}
	}
	if sucesso {
		go b.monitorarMissao(droneID, ocorrencia)
	} else {
		fmt.Printf("[Broker %d] Drone %s REJEITOU ou está OFFLINE! Devolvendo ocorrência %s para a fila.\n",
			b.ID, droneID, ocorrencia.ID)
		b.mu.Lock()
		heap.Push(&b.Estado.FilaEspera, ocorrencia)
		if drone, ok := b.Estado.Drones[droneID]; ok {
			drone.Status = "indisponivel"
			drone.MissaoID = ""
		}
		b.mu.Unlock()
		b.sincronizarEstado()
		go b.tentarDespacharDrone()
	}
}

// Se o drone não responder após 20s, consideramos perdido e reenfileiramos a ocorrência
func (b *Broker) monitorarMissao(droneID string, ocorrencia *protocol.Ocorrencia) {
	time.Sleep(20 * time.Second)
	b.mu.Lock()
	drone, existe := b.Estado.Drones[droneID]
	if existe && drone.MissaoID == ocorrencia.ID {
		fmt.Printf("\n[ALERTA CRÍTICO] Broker %d perdeu sinal do Drone %s no ar!\n", b.ID, droneID)
		fmt.Printf("[AÇÃO] Removendo drone %s da frota e reenfileirando a ocorrência %s...\n", droneID, ocorrencia.ID)
		delete(b.Estado.Drones, droneID)
		heap.Push(&b.Estado.FilaEspera, ocorrencia)
		b.mu.Unlock()
		b.sincronizarEstado()
		go b.tentarDespacharDrone()
		return
	}
	b.mu.Unlock()
}

// Implementação robusta do algoritmo do Valentão
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
		if idVizinho > b.ID {
			if b.enviarMensagem(endereco, msgEleicao) {
				temMaior = true
				fmt.Printf("[Broker %d] Eleição enviada ao ID %d\n", b.ID, idVizinho)
			}
		}
	}
	if !temMaior {
		b.mu.Lock()
		b.Coordenador = b.ID
		b.mu.Unlock()
		fmt.Printf("[Broker %d] Sou o novo Coordenador!\n", b.ID)
		msgVitoria := protocol.Mensagem{
			Tipo:      protocol.TipoVitoria,
			IDOrigem:  b.ID,
			Timestamp: time.Now(),
		}
		for idVizinho, endereco := range b.OutrosBrokers {
			if idVizinho < b.ID {
				b.enviarMensagem(endereco, msgVitoria)
			}
		}
		go b.tentarDespacharDrone()
	}
}

// Comunicação TCP básica
func (b *Broker) enviarMensagem(ipDestino string, msg protocol.Mensagem) bool {
	conn, err := net.DialTimeout("tcp", ipDestino, 1*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	return json.NewEncoder(conn).Encode(msg) == nil
}

// Envia mensagem e espera ACK de confirmação
func (b *Broker) enviarMensagemComAck(ipDestino string, msg protocol.Mensagem) bool {
	conn, err := net.DialTimeout("tcp", ipDestino, 2*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		return false
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var resposta protocol.Mensagem
	if err := json.NewDecoder(conn).Decode(&resposta); err != nil || resposta.Tipo != protocol.TipoACK {
		return false
	}
	return true
}
