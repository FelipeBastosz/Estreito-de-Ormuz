Iniciando README
# 🛳️ Estreito de Ormuz: Sistema Distribuído de Monitoramento IoT

![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=for-the-badge&logo=go)
![Docker](https://img.shields.io/badge/Docker-Pronto-2496ED?style=for-the-badge&logo=docker)
![Status](https://img.shields.io/badge/Status-Conclu%C3%ADdo-success?style=for-the-badge)
![Brokers](https://img.shields.io/badge/Brokers-4%20Setores-orange?style=for-the-badge)
![Persistência](https://img.shields.io/badge/Persist%C3%AAncia-Docker%20Volumes-blueviolet?style=for-the-badge)

---

## 📌 Sobre o Projeto

Este projeto implementa um **sistema distribuído de monitoramento IoT em Golang**, simulando o controle de drones e sensores espalhados por quatro setores geográficos do **Estreito de Ormuz**. A arquitetura conta com múltiplos brokers independentes, persistência de estado e suporte a implantação em máquinas físicas separadas.

Projeto desenvolvido para a disciplina de Redes e Sistemas Distribuídos (PBL).

---

## 🎯 Objetivo do Projeto

O sistema simula um **ambiente de vigilância distribuída**, onde sensores coletam dados ambientais por setor e drones recebem comandos de missão via um broker local. O objetivo é explorar:

- Arquiteturas multi-broker com roteamento entre setores
- Eleição de líder distribuída com o Algoritmo Bully
- Fila de prioridade com heap para despacho de ocorrências críticas
- Persistência de estado com recuperação após falhas
- Comunicação via sockets TCP em Go
- Containerização e orquestração com Docker Compose
- Implantação distribuída em múltiplos computadores físicos

---

## 🚀 Principais Funcionalidades (Features)

* **Federação de Brokers:** 4 brokers independentes, um por setor geográfico, cada um escutando em sua própria porta (9081–9084). O roteamento entre setores é resolvido via `config.json`, permitindo que o cliente se conecte a qualquer broker da rede.
* **Eleição de Líder (Bully Algorithm):** Os brokers executam o **Algoritmo Bully** para eleger um coordenador entre os nós ativos. Quando o líder atual cai ou se torna inalcançável, uma nova eleição é disparada automaticamente — o broker com o maior ID que responder ao desafio assume a coordenação do cluster.
* **Fila de Prioridade (Max-Heap):** As ocorrências recebidas pelo broker líder são enfileiradas em uma **heap de prioridade máxima**. Isso garante que incidentes `Crítico` (prioridade 3) sejam sempre despachados aos drones antes de `Alertas` (2) ou `Avisos` (1), independentemente da ordem de chegada.
* **Persistência de Estado:** Cada broker grava seu estado em um **volume Docker dedicado**. Se um container cair e subir novamente, o estado é recuperado automaticamente — sensores, drones e últimas leituras são restaurados sem intervenção manual.
* **Frota de Drones (Atuadores):** Cada drone se registra com um nome, endereço próprio e o endereço do broker ao qual pertence, permitindo rastreamento e controle individualizados por setor.
* **Sensores por Setor:** Cada setor conta com 2 sensores independentes, com intervalos de envio configuráveis via variáveis de ambiente (`SENSOR_INTERVALO_MIN` / `SENSOR_INTERVALO_MAX`).
* **Protocolo Compartilhado:** Pacote `Protocol` centraliza as definições de mensagem usadas por todos os componentes, garantindo consistência e facilitando extensões futuras.
* **Implantação Multi-Máquina com Makefile:** Atalhos prontos (`make pc1`, `make pc2`) dividem a infraestrutura entre dois computadores físicos, tornando os testes de rede distribuída mais rápidos e reproduzíveis.

---

## 📡 Especificação do Protocolo

Todos os componentes se comunicam via **TCP**, usando mensagens encapsuladas em JSON e prefixos de identificação no handshake:

* **Sensores:** Registram-se no broker passando `[ID]`, `[setor]` e `[endereço_do_broker]` como argumentos. Enviam leituras periódicas de telemetria.
* **Drones:** Registram-se com `[nome]`, `[endereço_próprio]` e `[endereço_do_broker]`. Aguardam comandos de missão e respondem com confirmação de execução.
* **Clientes:** Conectam-se a um broker específico informando o IP:porta. A partir daí, usam comandos textuais para listar, monitorar e acionar os dispositivos de qualquer setor.

---

## 🏗️ Arquitetura do Sistema

O sistema é dividido em 6 componentes principais:

1. **Broker (×4):** Um broker por setor, cada um responsável pelos dispositivos do seu setor. Escuta na porta `908X` (TCP).
2. **Drone (×4):** Atuador vinculado a um broker. Recebe comandos e reporta seu estado de volta ao broker.
3. **Sensor (×8):** Dois sensores por setor, enviando telemetria periódica ao seu broker local.
4. **Client:** Interface interativa TCP que permite ao operador se conectar a qualquer broker e operar toda a frota.
5. **Protocol:** Pacote compartilhado com as definições de mensagem e protocolo de comunicação.
6. **State:** Pacote compartilhado responsável pela leitura, escrita e recuperação do estado persistido em disco.

---

## 📂 Estrutura do Projeto

```
Estreito-de-Ormuz
│
├── Broker/          # Lógica do broker por setor
│   └── broker.go
│
├── Client/          # Cliente interativo TCP
│   └── client.go
│
├── Drone/           # Atuadores vinculados a cada setor
│   └── drone.go
│
├── Protocol/        # Definições de protocolo compartilhadas
│   └── protocol.go
│
├── Sensor/          # Simuladores de telemetria por setor
│   └── sensor.go
│
├── State/           # Persistência e recuperação de estado
│   └── persistence.go
│   └── priorityQueue.go
│
├── config.json      # Mapa de broker ID → endereço
├── docker-compose.yml
├── Dockerfile.broker
├── Dockerfile.client
├── Dockerfile.drone
├── Dockerfile.sensor
├── go.mod
└── LICENSE
├── Makefile
├── README.md
```

---

## ⚙️ Configuração dos Setores (`config.json`)

O arquivo `config.json` mapeia o ID de cada setor ao endereço TCP do seu broker. Ele é montado como volume em todos os containers que precisam conhecer a topologia da rede:

```json
{
   "1": "broker1:9081",
   "2": "broker2:9082",
   "3": "broker3:9083",
   "4": "broker4:9084"
}
```

Para implantação em múltiplos computadores físicos, basta alterar os endereços para os IPs reais das máquinas.

---

## 🐳 Executando com Docker (Recomendado)

A orquestração de todos os serviços é feita via Docker Compose.

### 1. Subir a infraestrutura completa

Na raiz do projeto, onde está o `docker-compose.yml`, execute:

```bash
docker-compose up --build
```

Isso iniciará automaticamente os 4 brokers, 4 drones, 8 sensores (2 por setor) e a rede Docker isolada.

### 2. Encerrar o sistema

```bash
docker-compose down
```

Para encerrar **e apagar os volumes de persistência** (estado dos brokers):

```bash
docker-compose down -v
```

---

### 3. Comandos Úteis de Monitoramento e Interação

#### Listar todos os containers ativos:

```bash
docker ps
```

#### Acompanhar logs de toda a rede em tempo real:

```bash
docker-compose logs -f
```

#### Acompanhar logs de um container específico:

```bash
docker logs -f <nome_do_container>
```

#### Executar um cliente interativo:

O cliente recebe como argumento o endereço do broker ao qual deseja se conectar:

```bash
docker-compose run --rm client <IP_DO_BROKER>:<PORTA>
```

Exemplo para conectar ao broker do setor 1:

```bash
docker-compose run --rm client broker1:9081
```

#### Adicionar um novo drone ou sensor manualmente:

Novo sensor no setor 1:

```bash
docker-compose run -d --rm --no-deps sensor-s1a ./sensor <NOME> setor-1 broker1:9081
```

Novo drone no setor 2:

```bash
docker-compose run -d --rm --no-deps drone1 ./drone <NOME> <DRONE_ADDR> broker2:9082
```

#### Testar tolerância a falhas (derrubar um broker):

```bash
docker stop <nome_do_container_broker>
```

O drone e os sensores do setor tentarão reconexão. Ao reiniciar o broker, o estado é recuperado do volume:

```bash
docker restart <nome_do_container_broker>
```

---

## 🖥️ Executando em Múltiplos Computadores (Rede Distribuída)

O `Makefile` possui atalhos para dividir a infraestrutura entre dois computadores físicos na mesma rede local:

### Computador 1 — Setores 1 e 2:

```bash
make pc1
```

Sobe `broker1`, `broker2`, `drone1`, `drone2`, `sensor-s1a`, `sensor-s1b`, `sensor-s2a`, `sensor-s2b`.

### Computador 2 — Setores 3 e 4:

```bash
make pc2
```

Sobe `broker3`, `broker4`, `drone3`, `drone4`, `sensor-s3a`, `sensor-s3b`, `sensor-s4a`, `sensor-s4b`.

### Cliente de qualquer máquina:

```bash
make client IP=<IP_DO_COMPUTADOR_1>
```

Exemplo:

```bash
make client IP=172.16.201.9
```

> **Atenção:** Antes de rodar nos computadores remotos, atualize os endereços dos brokers no `config.json` para os IPs físicos reais das máquinas.

### Limpeza total (volumes incluídos):

```bash
make clean
```

---

## 🛠️ Como Executar Localmente (sem Docker)

Caso prefira rodar os programas Go diretamente:

1. Inicie o Broker do setor 1:

   ```bash
   go run ./Broker/broker.go 1
   ```

2. Conecte um Drone ao setor 1:

   ```bash
   go run ./Drone/drone.go drone1 localhost:9091 localhost:9081
   ```

3. Conecte um Sensor ao setor 1:

   ```bash
   go run ./Sensor/sensor.go S1A setor-1 localhost:9081
   ```

4. Conecte um Cliente:

   ```bash
   go run ./Client/client.go localhost:9081
   ```

> Repita os passos 1–3 com os IDs e portas corretos para subir os demais setores (2, 3 e 4).

---

## 💻 Terminal de Comando (Interface do Cliente)

O cliente é um **terminal de injeção manual de ocorrências**. Após conectar-se a um broker, o operador descreve o incidente e define sua prioridade. A ocorrência é encaminhada via TCP e processada pelo broker líder eleito, que confirma o recebimento com um ACK.

```
==================================================
   Terminal de Comando - Estreito de Ormuz
==================================================
Digite a descrição da Ocorrência (ou 'sair'): Embarcação suspeita no canal norte
Insira a prioridade da requisição (1-Aviso, 2-Alerta, 3-Crítico): 3
✅ Confirmado: Ocorrência USER-client-01-OC0001 aceita pelo líder!
```

### Campos de uma Ocorrência

| Campo        | Descrição                                                     | Exemplo                        |
| ------------ | ------------------------------------------------------------- | ------------------------------ |
| `ID`         | Identificador único gerado automaticamente pelo cliente       | `USER-client-01-OC0001`        |
| `Descricao`  | Texto livre descrevendo o incidente reportado                 | `Embarcação suspeita no canal` |
| `Prioridade` | Nível de urgência: `1`-Aviso, `2`-Alerta, `3`-Crítico        | `3`                            |
| `Setor`      | Definido como `manual-input` para entradas do operador        | `manual-input`                 |
| `Timestamp`  | Momento exato do envio, preenchido automaticamente            | `2025-07-10T14:32:01Z`         |

### Níveis de Prioridade

As ocorrências são enfileiradas no broker usando uma **heap de prioridade máxima**, garantindo que incidentes críticos sejam despachados para os drones antes dos avisos, independentemente da ordem de chegada.

| Nível | Nome      | Descrição                                         |
| ----- | --------- | ------------------------------------------------- |
| `1`   | ⚠️ Aviso  | Situação de baixo risco, monitoramento recomendado|
| `2`   | 🔶 Alerta | Situação que exige atenção e possível intervenção |
| `3`   | 🔴 Crítico| Ameaça confirmada, resposta imediata necessária   |

### Comando de saída

Para encerrar o terminal:

```
Digite a descrição da Ocorrência (ou 'sair'): sair
```

---

## 💾 Persistência de Estado

Cada broker possui um volume Docker dedicado para persistir seu estado local:

| Volume         | Broker   | Setor   |
| -------------- | -------- | ------- |
| `state-broker1`| broker1  | Setor 1 |
| `state-broker2`| broker2  | Setor 2 |
| `state-broker3`| broker3  | Setor 3 |
| `state-broker4`| broker4  | Setor 4 |

Em caso de queda e reinício do container, o broker relê o arquivo de estado e restaura automaticamente o registro de drones e sensores, sem necessidade de reconexão manual dos dispositivos.

---

## 📚 Referências e Links Úteis

Para a construção da arquitetura e tomada de decisões deste projeto, os seguintes materiais foram consultados:

**Golang & Infraestrutura**
* [Documentação Oficial do Go (Golang)](https://go.dev/doc/) — Base para Goroutines, canais e gerenciamento de memória concorrente.
* [Go by Example: Mutexes](https://gobyexample.com/mutexes) — Referência para implementação de Thread-Safety e prevenção de Race Conditions.
* [Pacote `net` do Go](https://pkg.go.dev/net) — Documentação oficial para conexões TCP, `DialTimeout` e `SetReadDeadline`.
* [Pacote `encoding/json` do Go](https://pkg.go.dev/encoding/json) — Serialização e desserialização das mensagens do protocolo distribuído.
* [Pacote `container/heap` do Go](https://pkg.go.dev/container/heap) — Interface nativa usada para implementar a fila de prioridade de ocorrências.

**Estruturas de Dados**
* [Golang: Heap Data Structure — YuminLee (Medium)](https://yuminlee2.medium.com/golang-heap-data-structure-45760f9562dc) — Referência principal para a implementação da **heap de prioridade máxima** que garante o despacho de ocorrências críticas antes das de menor urgência.

**Algoritmos Distribuídos**
* [The Bully Algorithm — GeeksforGeeks](https://www.geeksforgeeks.org/election-algorithm-and-distributed-processing/) — Embasamento teórico para o **Algoritmo Bully de Eleição de Líder**, utilizado para eleger o broker coordenador entre os nós ativos do cluster quando um líder cai ou se torna inalcançável.
* [Leader Election in Distributed Systems — Martin Kleppmann](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) — Discussão sobre as garantias e armadilhas de eleição de líder em sistemas distribuídos, incluindo split-brain e timeouts de eleição.
* [Distributed Systems: Principles and Paradigms — Tanenbaum & Van Steen](https://www.distributed-systems.net/index.php/books/ds3/) — Referência acadêmica para os conceitos de eleição de líder, comunicação entre processos e tolerância a falhas abordados no projeto.

**Redes e Protocolos**
* [Diferenças entre TCP e UDP (Cloudflare)](https://www.cloudflare.com/pt-br/learning/ddos/glossary/tcp-ip/) — Embasamento para a escolha do TCP como protocolo de transporte, dada a criticidade das ocorrências e a necessidade de entrega garantida.

---

## 👨‍💻 Autor

Felipe Bastos — Desenvolvedor Backend & Estudante de Engenharia de Computação — UEFS

---

## ⚖️ Licença

Este projeto está sob a licença MIT. Consulte o arquivo [LICENSE](LICENSE) para mais detalhes.