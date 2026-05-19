package main

import (
	protocol "Desbloqueio-do-Estreito-de-Ormuz/Protocol"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

// Drone representa um drone físico autônomo de monitoramento.
// Escuta comandos TCP, executa missões simuladas e reporta conclusão ao broker.
type Drone struct {
	ID             string
	Endereco       string // Endereço TCP onde este drone escuta (ex: "drone1:9091")
	EnderecoBroker string // Broker local para registro e reports
	Status         string // "disponivel", "em_missao", "recarregando"
	Bateria        int
	mu             sync.Mutex
	Brokers        []string // lista de brokers para responder
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Uso: drone [ID] [ENDERECO_PROPRIO] [ENDERECO_BROKER]")
		fmt.Println("Exemplo: drone drone1 0.0.0.0:9091 broker1:9081")
		return
	}

	droneID := os.Args[1]
	enderecoProprio := os.Args[2]
	enderecoBroker := os.Args[3]

	drone := &Drone{
		ID:             droneID,
		Endereco:       enderecoProprio,
		EnderecoBroker: enderecoBroker,
		Status:         "disponivel",
		Bateria:        100,
	}

	// Carrega lista de brokers para recuperação se CONFIG_PATH estiver disponível
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "../config.json"
	}
	if brokers, err := carregarBrokers(configPath); err == nil {
		drone.Brokers = brokers
	}

	rand.Seed(time.Now().UnixNano())

	// Começa a ouvir comandos em paralelo
	go drone.escutar()

	// Aguarda o sistema subir antes de se registrar
	time.Sleep(3 * time.Second)
	drone.registrarNoBroker()

	// Re-registra periodicamente para o caso de o coordenador ter mudado
	// (nova eleição após falha de um broker)
	go drone.reregistroPeriodico()

	select {} // Mantém o processo vivo
}

// ============================================================
// SERVIDOR TCP DO DRONE
// ============================================================

func (d *Drone) escutar() {
	// 1. Extrai APENAS a porta do endereço que veio do docker-compose
	// Exemplo: de "192.168.15.13:9091", ele pega só o "9091"
	_, porta, err := net.SplitHostPort(d.Endereco)
	if err != nil {
		fmt.Printf("[Drone %s] Erro ao extrair porta de %s: %v\n", d.ID, d.Endereco, err)
		os.Exit(1)
	}

	// 2. Força o servidor TCP a escutar internamente em 0.0.0.0 (Obrigatório para Docker)
	ln, err := net.Listen("tcp", "0.0.0.0:"+porta)
	if err != nil {
		fmt.Printf("[Drone %s] Erro ao abrir porta %s: %v\n", d.ID, porta, err)
		os.Exit(1)
	}
	fmt.Printf("[Drone %s] Pronto em 0.0.0.0:%s (Registrado externamente como %s)\n", d.ID, porta, d.Endereco)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go d.processarComando(conn)
	}
}

func (d *Drone) processarComando(conn net.Conn) {
	defer conn.Close()

	var msg protocol.Mensagem
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		return
	}

	switch msg.Tipo {

	case protocol.TipoComandoDrone:
		// Garantia de exclusão mútua: não aceita missão se já estiver ocupado.
		// Isso satisfaz o requisito de "nunca despachar o mesmo drone duas vezes".
		d.mu.Lock()
		if d.Status != "disponivel" {
			fmt.Printf("[Drone %s] Recusei missão — já estou em %s\n", d.ID, d.Status)
			d.mu.Unlock()
			return
		}

		//Reserva o drone e coloca ele em missão
		d.Status = "em_missao"
		d.mu.Unlock()

		ack := protocol.Mensagem{
			Tipo:      protocol.TipoACK,
			IDOrigem:  msg.IDOrigem,
			Timestamp: msg.Timestamp,
		}

		if err := json.NewEncoder(conn).Encode(ack); err != nil {
			fmt.Printf("[Drone %s] Erro ao enviar resposta ao Broker: %v\n", d.ID, err)
		}

		//Processa os dados da missão
		var comando protocol.ComandoMissao
		json.Unmarshal([]byte(msg.Payload), &comando)

		fmt.Printf("[Drone %s] ✈ Missão aceita: %s (Prioridade %d)\n",
			d.ID, comando.OcorrenciaID, comando.Prioridade)

		// Executa em goroutine para liberar o handler imediatamente
		go d.executarMissao(comando)

	case protocol.TipoRegistroDrone:
		// Coordenador pedindo re-registro (após nova eleição)
		d.registrarNoBroker()
	}
}

// ============================================================
// SIMULAÇÃO DE MISSÃO
// ============================================================

// executarMissao simula o voo, inspeção e retorno do drone.
// Ao terminar, reporta status ao broker para liberar a fila.
func (d *Drone) executarMissao(comando protocol.ComandoMissao) {
	// Tempo de missão varia com a prioridade (missões críticas recebem atenção mais longa)
	baseSegundos := map[int]int{1: 5, 2: 8, 3: 12}
	base := baseSegundos[comando.Prioridade]
	if base == 0 {
		base = 7
	}
	duracaoMissao := time.Duration(base+rand.Intn(8)) * time.Second

	fmt.Printf("[Drone %s] Voando para %s. Duração estimada: %v\n",
		d.ID, comando.OcorrenciaID, duracaoMissao)

	time.Sleep(duracaoMissao)

	// Consome bateria proporcionalmente ao tempo de missão
	d.mu.Lock()
	consumo := 8 + rand.Intn(12) // 8–20% por missão
	d.Bateria -= consumo
	if d.Bateria < 0 {
		d.Bateria = 0
	}
	batAtual := d.Bateria
	d.mu.Unlock()

	fmt.Printf("[Drone %s] Missão %s concluída! Bateria restante: %d%%\n",
		d.ID, comando.OcorrenciaID, batAtual)

	// Bateria crítica: drone precisa recarregar antes de aceitar nova missão
	if batAtual < 20 {
		d.recarregar()
	} else {
		d.mu.Lock()
		d.Status = "disponivel"
		d.mu.Unlock()
	}

	// Reporta ao broker que está livre
	d.reportarStatus()
}

// recarregar simula o processo de recarga da bateria.
// Durante a recarga, o drone está indisponível para missões.
func (d *Drone) recarregar() {
	fmt.Printf("[Drone %s] ⚡ Bateria baixa. Recarregando (60s)...\n", d.ID)

	d.mu.Lock()
	d.Status = "recarregando"
	d.mu.Unlock()

	// Reporta status de recarga ao broker para não ficar "fantasma" na fila
	d.reportarStatus()

	time.Sleep(60 * time.Second) // Simula tempo de recarga

	d.mu.Lock()
	d.Bateria = 100
	d.Status = "disponivel"
	d.mu.Unlock()

	fmt.Printf("[Drone %s] ✅ Recarga completa. Pronto para novas missões!\n", d.ID)

	// Re-registra após recarga para garantir que o coordenador saiba que voltou
	d.registrarNoBroker()
}

// ============================================================
// COMUNICAÇÃO COM O BROKER
// ============================================================

// registrarNoBroker anuncia o drone ao broker local.
// O broker (ou o coordenador, se for diferente) inclui o drone no pool gerenciado.
func (d *Drone) registrarNoBroker() {
	d.mu.Lock()
	info := protocol.Drone{
		ID:      d.ID,
		Posicao: d.Endereco, // Endereço TCP para o coordenador enviar comandos
		Status:  d.Status,
		Bateria: d.Bateria,
	}
	d.mu.Unlock()

	payload, _ := json.Marshal(info)
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoRegistroDrone,
		IDOrigem:  0,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}

	if d.enviarParaBroker(msg) {
		fmt.Printf("[Drone %s] Registrado no broker %s\n", d.ID, d.EnderecoBroker)
	}
}

// reportarStatus envia o estado atual do drone ao broker.
// Usado após conclusão de missão ou recarga.
func (d *Drone) reportarStatus() {
	d.mu.Lock()
	info := protocol.Drone{
		ID:      d.ID,
		Posicao: d.Endereco,
		Status:  d.Status,
		Bateria: d.Bateria,
	}
	d.mu.Unlock()

	payload, _ := json.Marshal(info)
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoStatusDrone,
		IDOrigem:  0,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}

	d.enviarParaBroker(msg)
}

// reregistroPeriodico garante que o drone se reapresente após mudanças de coordenador.
// A cada 30 segundos, re-envia o registro — se o coordenador mudou, ele recebe e atualiza o pool.
func (d *Drone) reregistroPeriodico() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		d.mu.Lock()
		status := d.Status
		d.mu.Unlock()

		if status == "disponivel" {
			d.registrarNoBroker()
		}
	}
}

func (d *Drone) enviarParaBroker(msg protocol.Mensagem) bool {
	conn, err := net.DialTimeout("tcp", d.EnderecoBroker, 2*time.Second)
	if err != nil {
		fmt.Printf("[Drone %s] Broker %s indisponível: %v\n", d.ID, d.EnderecoBroker, err)
		return false
	}
	defer conn.Close()
	return json.NewEncoder(conn).Encode(msg) == nil
}
