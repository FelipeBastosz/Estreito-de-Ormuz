package main

import (
	protocol "Desbloqueio-do-Estreito-de-Ormuz/Protocol"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"
)

// Tipos de ocorrências reais do domínio do Estreito de Ormuz
var tiposOcorrencia = []string{
	"Suspeita de bloqueio parcial de rota",
	"Falha de sinalização marítima",
	"Embarcação civil à deriva",
	"Congestionamento em corredor marítimo",
	"Detecção de objeto não identificado submerso",
	"Inspeção visual urgente de embarcação suspeita",
	"Replanejamento de tráfego — risco ambiental detectado",
	"Embarcação sem transponder AIS ativo",
	"Possível mina à deriva na rota principal",
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Uso: sensor [ID_SENSOR] [ID_SETOR] [ENDERECO_BROKER]")
		fmt.Println("Exemplo: sensor S1 setor-1 localhost:9081")
		return
	}

	sensorID := os.Args[1]
	setorID := os.Args[2]
	enderecoBroker := os.Args[3]

	// Permite sobrescrever intervalo mínimo via env var (útil para testes de carga)
	intervaloMin := 3
	if v := os.Getenv("SENSOR_INTERVALO_MIN"); v != "" {
		fmt.Sscan(v, &intervaloMin)
	}
	intervaloMax := 8
	if v := os.Getenv("SENSOR_INTERVALO_MAX"); v != "" {
		fmt.Sscan(v, &intervaloMax)
	}

	fmt.Printf("[Sensor %s | Setor %s] Iniciado. Enviando para broker %s\n", sensorID, setorID, enderecoBroker)
	fmt.Printf("[Sensor %s] Intervalo de geração: %d–%ds\n", sensorID, intervaloMin, intervaloMax)

	rand.Seed(time.Now().UnixNano())
	contador := 0

	for {
		// Intervalo aleatório entre as geração de eventos
		espera := time.Duration(intervaloMin+rand.Intn(intervaloMax-intervaloMin+1)) * time.Second
		time.Sleep(espera)

		contador++

		// Distribui prioridades de forma realista:
		// ~60% nível 1 (Aviso), ~30% nível 2 (Alerta), ~10% nível 3 (Crítico)
		prioridade := gerarPrioridade()
		descricao := tiposOcorrencia[rand.Intn(len(tiposOcorrencia))]

		ocorrencia := protocol.Ocorrencia{
			ID:         fmt.Sprintf("%s-OC%04d", sensorID, contador),
			Prioridade: prioridade,
			Timestamp:  time.Now(),
			Descricao:  descricao,
			Setor:      setorID,
		}

		enviarOcorrencia(enderecoBroker, ocorrencia, sensorID)
	}
}

// gerarPrioridade retorna uma prioridade com distribuição realista.
func gerarPrioridade() int {
	n := rand.Intn(100)
	if n < 10 {
		return 3 // Crítico — 10%
	} else if n < 40 {
		return 2 // Alerta — 30%
	}
	return 1 // Aviso — 60%
}

// enviarOcorrencia empacota a ocorrência e envia ao broker local.
// Em caso de falha, apenas loga — o sensor não tem memória de eventos não entregues
// (tolerância a perdas aceitável neste nível).
func enviarOcorrencia(enderecoBroker string, ocorrencia protocol.Ocorrencia, sensorID string) {
	payload, err := json.Marshal(ocorrencia)
	if err != nil {
		fmt.Printf("[Sensor %s] Erro ao serializar ocorrência: %v\n", sensorID, err)
		return
	}

	msg := protocol.Mensagem{
		Tipo:      protocol.TipoOcorrencia,
		IDOrigem:  0, // Sensores não têm ID numérico de broker
		Timestamp: time.Now(),
		Payload:   string(payload),
	}

	conn, err := net.DialTimeout("tcp", enderecoBroker, 2*time.Second)
	if err != nil {
		fmt.Printf("[Sensor %s] Broker indisponível (%s): %v\n", sensorID, enderecoBroker, err)
		return
	}
	defer conn.Close()

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		fmt.Printf("[Sensor %s] Erro ao enviar: %v\n", sensorID, err)
		return
	}

	prioLabel := map[int]string{1: "Aviso", 2: "Alerta", 3: "CRÍTICO"}
	fmt.Printf("[Sensor %s | Setor %s] ▶ Ocorrência %s enviada — %s [P%d]\n",
		sensorID, ocorrencia.Setor, ocorrencia.ID, prioLabel[ocorrencia.Prioridade], ocorrencia.Prioridade)
}
