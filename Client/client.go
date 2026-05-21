package main

import (
	protocol "Desbloqueio-do-Estreito-de-Ormuz/Protocol"
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// ============================================================
// CLIENTE TCP
// ============================================================
// Este programa funciona como a interface humana do sistema.
// Permite que um usuário injete ocorrências manualmente em qualquer
// broker da rede, simulando detecções que não vieram dos sensores automatizados.
func main() {
	// Valida se o usuário passou o endereço do broker ao iniciar o programa
	if len(os.Args) < 2 {
		fmt.Println("Uso: client [ENDERECO_BROKER]")
		fmt.Println("Exemplo: client 192.168.1.50:9081")
		return
	}

	enderecoBroker := os.Args[1]
	reader := bufio.NewReader(os.Stdin)

	menu()
	fmt.Printf("Conectado ao Broker: %s\n\n", enderecoBroker)

	contador := 1
	// Mantém o terminal aberto aguardando comandos do usuário
	for {
		menu()
		fmt.Print("Digite a descrição da Ocorrência (ou 'sair'): ")
		descricao, _ := reader.ReadString('\n')
		descricao = strings.TrimSpace(descricao)

		// Condição de saída do terminal
		if strings.ToLower(descricao) == "sair" {
			break
		}

		fmt.Print("Insira a prioridade da requisição (1-Aviso, 2-Alerta, 3-Crítico): ")
		prioStr, _ := reader.ReadString('\n')
		prioridade, err := strconv.Atoi(strings.TrimSpace(prioStr))

		// Validação do input para não enviar lixo para a Fila de Prioridade
		if err != nil || prioridade < 1 || prioridade > 3 {
			fmt.Println("❌ Prioridade inválida. Use 1, 2 ou 3.\n")
			continue
		}

		// Constrói a Ocorrencia usando a estrutura padronizada
		ocorrencia := protocol.Ocorrencia{
			// Gera um ID único e rastreável. Ex: "USER-meupc-OC0001"
			ID:         fmt.Sprintf("USER-%s-OC%04d", os.Getenv("HOSTNAME"), contador),
			Prioridade: prioridade,
			Timestamp:  time.Now(),
			Descricao:  descricao,
			Setor:      "manual-input", // Identifica que foi injetado por um humano, não por um setor fixo
		}

		// Envia a requisição para o broker
		enviarOcorrencia(enderecoBroker, ocorrencia)
		contador++
		fmt.Println()
	}
}

func enviarOcorrencia(ipDestino string, ocorrencia protocol.Ocorrencia) {
	payload, _ := json.Marshal(ocorrencia)
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoOcorrencia,
		IDOrigem:  0,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}

	conn, err := net.DialTimeout("tcp", ipDestino, 2*time.Second)
	if err != nil {
		fmt.Printf("❌ Falha na conexão com %s: %v\n", ipDestino, err)
		return
	}
	defer conn.Close()

	json.NewEncoder(conn).Encode(msg)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var resposta protocol.Mensagem
	if err := json.NewDecoder(conn).Decode(&resposta); err == nil && resposta.Tipo == protocol.TipoACK {
		fmt.Printf("✅ Confirmado: Ocorrência %s aceita pelo líder!\n", ocorrencia.ID)
	} else {
		fmt.Printf("⚠️  Mensagem enviada, aguardando processamento...\n")
	}
}

func menu() {
	fmt.Println("==================================================")
	fmt.Println("   Terminal de Comando - Estreito de Ormuz        ")
	fmt.Println("==================================================")
}
