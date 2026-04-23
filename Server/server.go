package main

import (
	protocol "Desbloqueio-do-Estreito-de-Ormuz/Protocol"
	state "Desbloqueio-do-Estreito-de-Ormuz/State"
	"container/heap"
	"encoding/json"
	"fmt"
	"net"
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

func carregarConfiguracao(arquivo string) (map[int]string, error) {
	
}

func (b *Broker) Start() {
	ln, err := net.Listen("tcp", b.Endereco)
	if err != nil {
		fmt.Printf("Erro ao iniciar o Broker %d: %v\n", b.ID, err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			b.LidarComMensagem(conn)
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
	fmt.Printf("[BROKER %d] Recebeu %s do Broker %d\n", b.ID, msg.Payload, msg.IDOrigem)

	switch msg.Tipo {
	case protocol.TipoEleicao:
		// REGRA DO VALENTÃO: Se alguém com ID MENOR que o meu pedir eleição...
		if msg.IDOrigem < b.ID {
			fmt.Printf("[Broker %d] ID %d quer ser líder, mas eu sou maior. Assumindo a eleição!\n", b.ID, msg.IDOrigem)
			// Eu calo ele, e inicio a minha própria eleição para mostrar quem manda.
			b.IniciarEleicao()
		}
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
	b.mu.Lock()

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

		fmt.Printf("[BROKER %d] Agora eu sou o novo coordenador!")

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
				fmt.Printf("[BROKER %d] Enviei uma mensagem de vitória ao Broker %d informando que sou o novo coordenador", b.ID, idVizinho)
			}
		}
	}
}

func (b *Broker) enviarMensagem(ipDestino string, msg protocol.Mensagem) bool {
	//Estabelece conexão com o servidor destino
	conn, err := net.DialTimeout("tcp", ipDestino, 1*time.Second)
	if err != nil {
		fmt.Printf("[BROKER %d] Não foi possível estabelecer conexão com o servidor: %s", ipDestino)
		return false
	}
	err := json.NewEncoder(conn).Encode(msg)
	return err == nil
}

func main() {}
