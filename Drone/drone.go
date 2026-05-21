package main

import (
	protocol "Desbloqueio-do-Estreito-de-Ormuz/Protocol"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Drone representa um drone físico de monitoramento.
// Escuta comandos TCP, executa missões simuladas e reporta conclusão ao broker.
type Drone struct {
	ID             string     // Identificador único do drone (ex: "drone1")
	Endereco       string     // Endereço TCP onde este drone recebe comandos (ex: "0.0.0.0:9091")
	EnderecoBroker string     // Endereço do broker do seu setor atual (usado para reportes)
	Status         string     // Estado atual: "disponivel", "em_missao", "recarregando"
	Bateria        int        // Nível de bateria atual (0 a 100%)
	mu             sync.Mutex // Exclusão mútua para evitar race conditions
	Brokers        []string   // Lista de fallback com o endereço de todos os brokers conhecidos
}

func main() {
	// Valida os argumentos passados via linha de comando ou docker-compose
	if len(os.Args) < 4 {
		fmt.Println("Uso: drone [ID] [ENDERECO_PROPRIO] [ENDERECO_BROKER]")
		fmt.Println("Exemplo: drone drone1 0.0.0.0:9091 broker1:9081")
		return
	}

	droneID := os.Args[1]
	enderecoProprio := os.Args[2]
	enderecoBroker := os.Args[3]

	// Instancia o estado inicial do Drone
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

	// Aguarda o sistema subir antes de se registrar no Broker
	time.Sleep(3 * time.Second)
	drone.registrarNoBroker()

	// Re-registra periodicamente para o caso de o coordenador ter mudado
	// Ex: nova eleição após falha de um broker
	go drone.reregistroPeriodico()

	select {} // Mantém o processo vivo
}

// ============================================================
// SERVIDOR TCP DO DRONE
// ============================================================

func (d *Drone) escutar() {
	// Extrai apenas a porta do endereço que veio do docker-compose
	// Exemplo: de "192.168.15.13:9091", ele pega só o "9091"
	_, porta, err := net.SplitHostPort(d.Endereco)
	if err != nil {
		fmt.Printf("[Drone %s] Erro ao extrair porta de %s: %v\n", d.ID, d.Endereco, err)
		os.Exit(1)
	}

	// Força o servidor TCP a escutar internamente em 0.0.0.0 para receber mensagens de qualquer IP
	ln, err := net.Listen("tcp", "0.0.0.0:"+porta)
	if err != nil {
		fmt.Printf("[Drone %s] Erro ao abrir porta %s: %v\n", d.ID, porta, err)
		os.Exit(1)
	}
	fmt.Printf("[Drone %s] Pronto em 0.0.0.0:%s (Registrado externamente como %s)\n", d.ID, porta, d.Endereco)

	// É onde recebe e aceita as conexões
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		// É quem faz o processamento da mensagem recebida
		go d.processarComando(conn)
	}
}

// processarComando traduz a requisição TCP recebida do Coordenador.
func (d *Drone) processarComando(conn net.Conn) {
	defer conn.Close()

	var msg protocol.Mensagem
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		return
	}

	switch msg.Tipo {
	// O Coordenador enviou uma missão diretamente para este drone
	case protocol.TipoComandoDrone:
		// Garantia de exclusão mútua: não aceita missão se já estiver ocupado.
		// Garantindo nunca despachar o mesmo drone duas vezes;
		d.mu.Lock()
		if d.Status != "disponivel" {
			fmt.Printf("[Drone %s] Recusei missão — já estou em %s\n", d.ID, d.Status)
			d.mu.Unlock()
			return
		}

		//Reserva o drone e coloca ele em missão
		d.Status = "em_missao"
		d.mu.Unlock()

		// Responde com um ACK imediatamente para o Coordenador confirmar o despacho
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

	// Coordenador pedindo re-registro (após nova eleição)
	case protocol.TipoRegistroDrone:
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
	baseSegundos := map[int]int{1: 5, 2: 8, 3: 12} // 5s para prioridade 1, 8s para prioridade 2 e 12s para prioridade 3
	base := baseSegundos[comando.Prioridade]
	if base == 0 {
		base = 7
	}
	// Cria um tempo aleatório
	duracaoMissao := time.Duration(base+rand.Intn(8)) * time.Second

	fmt.Printf("[Drone %s] Voando para %s. Duração estimada: %v\n",
		d.ID, comando.OcorrenciaID, duracaoMissao)

	// Simula o tempo de voo
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

	// Se a bateria ficar crítica (< 20%), o drone força uma recarga antes de aceitar novos trabalhos
	if batAtual < 20 {
		d.recarregar()
	} else {
		// Se estiver com bateria ok, fica disponível novamente
		d.mu.Lock()
		d.Status = "disponivel"
		d.mu.Unlock()
	}

	// Avisa a rede que terminou o trabalho ou que entrou em modo de recarga
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
// O broker (ou o coordenador) inclui o drone na frota de drones.
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
		IDOrigem:  0, // Sensores e Drones não têm ID de Broker
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
// A cada 30 segundos, reenvia o registro, se o coordenador mudou, ele recebe e atualiza a frota.
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

// enviarParaBroker implementa resiliência de rede.
// Tenta enviar para o broker principal. Se ele estiver offline,
// percorre a lista de brokers conhecidos até achar um vivo.
func (d *Drone) enviarParaBroker(msg protocol.Mensagem) bool {
	// Tenta broker principal
	if d.tentarEnvio(d.EnderecoBroker, msg) {
		return true
	}

	// Tenta os outros brokers disponíveis
	for _, addr := range d.Brokers {
		if addr == d.EnderecoBroker {
			continue // Pula o que já falhou
		}
		if d.tentarEnvio(addr, msg) {
			// Atualiza o broker primário para o que respondeu
			d.EnderecoBroker = addr
			return true
		}
	}
	return false
}

// tentarEnvio vai tentar uma conexão TCP com timeout para não travar o atuador. Caso ele não consiga, aponta erro.
func (d *Drone) tentarEnvio(addr string, msg protocol.Mensagem) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		fmt.Printf("[Drone %s] Broker %s indisponível: %v\n", d.ID, addr, err)
		return false
	}
	defer conn.Close()
	return json.NewEncoder(conn).Encode(msg) == nil
}

// carregarBrokers lê o config.json para que o drone
// saiba para quem tentar contato caso o seu broker de setor caia
func carregarBrokers(caminho string) ([]string, error) {
	arquivo, err := os.ReadFile(caminho)
	if err != nil {
		return nil, err
	}
	mapaString := make(map[string]string)
	json.Unmarshal(arquivo, &mapaString)

	ids := make([]int, 0, len(mapaString))
	for k := range mapaString {
		id, _ := strconv.Atoi(k)
		ids = append(ids, id)
	}
	sort.Ints(ids)

	brokers := make([]string, 0, len(ids))
	for _, id := range ids {
		brokers = append(brokers, mapaString[strconv.Itoa(id)])
	}
	return brokers, nil
}
