package main

import (
	protocol "Desbloqueio-do-Estreito-de-Ormuz/Protocol"
	state "Desbloqueio-do-Estreito-de-Ormuz/State"
	"container/heap"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type Broker struct {
	ID            int            // Guarda o ID do Broker
	Endereco      string         // Guarda o endereço do Broker
	OutrosBrokers map[int]string //Guarda o ID e o IP dos outros Brokers

	Coordenador int                //Guarda o ID do coordenador, se for -1 não existe coordenador
	Estado      *state.GlobalState //
	mu          sync.Mutex
}

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
		fmt.Println("Uso: go run main.go [ID]")
		return
	}
	id, _ := strconv.Atoi(os.Args[1])

	mapaRede, err := carregarConfiguracao("../config.json")
	if err != nil {
		fmt.Println("Erro ao ler config.json. O arquivo existe na raiz?", err)
		return
	}

	meuEndereco, existe := mapaRede[id]
	if !existe {
		fmt.Printf("ID %d não encontrado no config.json\n", id)
		return
	}

	outros := make(map[int]string)
	for k, v := range mapaRede {
		if k != id {
			outros[k] = v
		}
	}

	// Inicializa o estado zerado
	estadoInicial := &state.GlobalState{
		Drones:       make(map[string]*protocol.Drone),
		FilaEspera:   make(state.FilaPrioridade, 0),
		UltimoUpdate: time.Now().Unix(),
	}
	heap.Init(&estadoInicial.FilaEspera)

	broker := &Broker{
		ID:            id,
		Endereco:      meuEndereco,
		OutrosBrokers: outros,
		Coordenador:   -1,
		Estado:        estadoInicial,
	}

	// Tenta carregar o estado salvo no disco (Recuperação de Falhas)
	nomeArquivoState := fmt.Sprintf("state_%d.json", id)
	if err := state.CarregarEstado(nomeArquivoState, broker.Estado); err == nil {
		fmt.Printf("[Broker %d] Estado anterior recuperado com sucesso do disco!\n", id)
		// Re-inicializa o heap caso a ordem no JSON estivesse bagunçada
		heap.Init(&broker.Estado.FilaEspera)
	}

	// Inicia o "ouvido" do servidor em uma Goroutine separada
	go broker.Start()

	// Dá 2 segundos para todos os brokers subirem antes de começar a briga
	time.Sleep(2 * time.Second)
	broker.IniciarEleicao()

	// Mantém o servidor rodando para sempre
	select {}
}

func (b *Broker) Start() {
	ln, err := net.Listen("tcp", b.Endereco)
	if err != nil {
		fmt.Printf("Erro ao iniciar o Broker %d: %v\n", b.ID, err)
		os.Exit(1)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			go b.LidarComMensagem(conn)
		}
	}()
}

func (b *Broker) LidarComMensagem(conn net.Conn) {
	defer conn.Close()

	var msg protocol.Mensagem

	err := json.NewDecoder(conn).Decode(&msg)
	if err != nil {
		return
	}
	fmt.Printf("[BROKER %d] Recebeu OK %s do Broker %d\n", b.ID, msg.Payload, msg.IDOrigem)

	switch msg.Tipo {
	case protocol.TipoEleicao:
		// REGRA DO VALENTÃO: Se alguém com ID MENOR que o meu pedir eleição...
		if msg.IDOrigem < b.ID {
			fmt.Printf("[Broker %d] ID %d quer ser líder, mas eu sou maior. Assumindo a eleição!\n", b.ID, msg.IDOrigem)
			// Eu calo ele, e inicio a minha própria eleição para mostrar quem manda.
			msgOk := protocol.Mensagem{
				Tipo:     protocol.TipoOkEleicao,
				IDOrigem: b.ID,
			}
			// Envio o OK para falar que sou maior
			b.enviarMensagem(b.OutrosBrokers[msg.IDOrigem], msgOk)

			//Inicio uma nova eleição me candidatando a ser o novo coordenador
			go b.IniciarEleicao()
		}

	// Se recebi um OK de alguém maior, apenas aguardo ele anunciar a vitória
	case protocol.TipoOkEleicao:
		fmt.Printf("[BROKER %d] Recebi um OK do ID %d. Ele é maior, vou aguardar a decisão.\n", b.ID, msg.IDOrigem)

	//Significa que alguém venceu a eleição
	case protocol.TipoVitoria:
		b.mu.Lock()                  // Tranco a memória
		b.Coordenador = msg.IDOrigem // Atualiza quem é o coordenador
		b.mu.Unlock()                // Destranco a memória
		fmt.Printf("[Broker %d] O Broker %d é o novo Coordenador!\n", b.ID, msg.IDOrigem)

	case protocol.TipoOcorrencia:
		if b.ID == b.Coordenador {
			var ocorrencia protocol.Ocorrencia
			json.Unmarshal([]byte(msg.Payload), &ocorrencia)

			// Proteção: Se a ocorrência veio sem horário, preenchemos com o horário atual para não bugar o Heap
			if ocorrencia.Timestamp.IsZero() {
				ocorrencia.Timestamp = time.Now()
			}

			b.mu.Lock()
			heap.Push(&b.Estado.FilaEspera, &ocorrencia)
			b.Estado.UltimoUpdate = time.Now().Unix()

			state.SalvarEstado(fmt.Sprintf("arquivo_%d", b.ID), b.Estado)
			b.mu.Unlock()
			fmt.Printf("[Broker %d] Ocorrência %s recebida! Prioridade: %d. Tamanho da Fila: %d\n",
				b.ID, ocorrencia.ID, ocorrencia.Prioridade, b.Estado.FilaEspera.Len())

		} else {
			// Se não sou o chefe, repasso o problema para ele
			if b.Coordenador != -1 {
				fmt.Printf("[Broker %d] Repassando ocorrência para o Coordenador %d...\n", b.ID, b.Coordenador)
				b.enviarMensagem(b.OutrosBrokers[b.Coordenador], msg)
			}
		}
	}
}

func (b *Broker) IniciarEleicao() {
	b.mu.Lock()
	b.Coordenador = -1
	b.mu.Unlock()

	fmt.Printf("\n[BROKER %d] O coordenador está morto! Iniciando nova eleição\n", b.ID)
	temMaior := false

	msgEleicao := protocol.Mensagem{
		Tipo:      protocol.TipoEleicao,
		IDOrigem:  b.ID,
		Timestamp: time.Now(),
	}

	//Percorre o mapa que contém os outros Brokers
	for idVizinho, ipPorta := range b.OutrosBrokers {
		if idVizinho > b.ID {
			if b.enviarMensagem(ipPorta, msgEleicao) {
				temMaior = true
				fmt.Printf("[BROKER %d] Enviei ELEICAO para vizinho maior (ID %d)\n", b.ID, idVizinho)
			}
		}
	}

	//Se não teve ninguém com ID menor, agora eu sou o coordenador
	if !temMaior {
		b.mu.Lock()
		b.Coordenador = b.ID
		b.mu.Unlock()

		fmt.Printf("[BROKER %d] Agora eu sou o novo coordenador!\n", b.ID)

		//Cria a mensagem de vitória
		msgVitoria := protocol.Mensagem{
			Tipo:      protocol.TipoVitoria,
			IDOrigem:  b.ID,
			Timestamp: time.Now(),
		}
		//Comunica a vitória para todos os nós MENORES que eu
		for idVizinho, ipVizinho := range b.OutrosBrokers {
			if idVizinho < b.ID {
				b.enviarMensagem(ipVizinho, msgVitoria)
				fmt.Printf("[BROKER %d] Enviei uma mensagem de vitória ao Broker %d informando que sou o novo coordenador\n", b.ID, idVizinho)
			}
		}
	}
}

func (b *Broker) enviarMensagem(ipDestino string, msg protocol.Mensagem) bool {
	//Estabelece conexão com o servidor destino
	conn, err := net.DialTimeout("tcp", ipDestino, 1*time.Second)
	if err != nil {
		fmt.Printf("[BROKER %d] Não foi possível estabelecer conexão com o servidor: %s\n\n", b.ID, ipDestino)
		return false
	}
	err = json.NewEncoder(conn).Encode(msg)
	return err == nil
}
