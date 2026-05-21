package state

import "Desbloqueio-do-Estreito-de-Ormuz/Protocol"

// FilaPrioridade é um Array de ponteiros para ocorrêcias.
type FilaPrioridade []*protocol.Ocorrencia

// Len ensina ao Heap como saber o tamanho da fila.
func (pq FilaPrioridade) Len() int { return len(pq) }

// Less é o motor de decisão. Ele ensina o Go a comparar dois itens (posição i e posição j).
func (pq FilaPrioridade) Less(i, j int) bool {
	// Regra 1: Criticidade. Se a prioridade for diferente, a MAIOR ganha (3 ganha de 2).
	if pq[i].Prioridade != pq[j].Prioridade {
		return pq[i].Prioridade > pq[j].Prioridade
	}

	// Regra 2: Desempate. Se ambos são nível 3, usamos o tempo (Relógios Lógicos).
	// O método Before retorna 'true' se a ocorrência 'i' aconteceu no mundo real antes da 'j'.
	return pq[i].Timestamp.Before(pq[j].Timestamp)
}

// Swap ensina o Heap a trocar dois itens de lugar no Array quando ele estiver se organizando.
func (pq FilaPrioridade) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push é chamado quando uma nova ocorrência chega.
// Note o *pq (Ponteiro para a Fila). Precisamos do ponteiro pois o append altera
// o tamanho original do Array na memória.
func (pq *FilaPrioridade) Push(x interface{}) {
	item := x.(*protocol.Ocorrencia) // Converte a interface genérica de volta para Ocorrencia
	*pq = append(*pq, item)          // Adiciona no final (o Heap se encarrega de subir ele para o topo depois)
}

// Pop é chamado quando o Coordenador diz: "Me dê a tarefa mais urgente para o drone!"
func (pq *FilaPrioridade) Pop() interface{} {
	old := *pq
	n := len(old)

	// O item mais importante sempre é colocado na última posição pelo algoritmo interno do Go antes do Pop ser acionado.
	item := old[n-1]

	// Cortamos a última posição do array (esquecemos ela, pois já entregamos o item)
	*pq = old[0 : n-1]

	return item // Entregamos a ocorrência para o Coordenador
}
