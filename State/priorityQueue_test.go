package state

import (
	protocol "Desbloqueio-do-Estreito-de-Ormuz/Protocol"
	"container/heap"
	"testing"
	"time"
)

func TestFilaPrioridade_Criticidade(t *testing.T) {
	fila := make(FilaPrioridade, 0)
	heap.Init(&fila)

	heap.Push(&fila, &protocol.Ocorrencia{ID: "OC-1", Prioridade: 1, Timestamp: time.Now()})
	heap.Push(&fila, &protocol.Ocorrencia{ID: "OC-2", Prioridade: 3, Timestamp: time.Now()})
	heap.Push(&fila, &protocol.Ocorrencia{ID: "OC-3", Prioridade: 2, Timestamp: time.Now()})

	item1 := heap.Pop(&fila).(*protocol.Ocorrencia)
	if item1.Prioridade != 3 {
		t.Errorf("Esperado prioridade 3 no primeiro item, obteve %d", item1.Prioridade)
	}
}

func TestFilaPrioridade_DesempatePorTempo(t *testing.T) {
	fila := make(FilaPrioridade, 0)
	heap.Init(&fila)

	agora := time.Now()
	heap.Push(&fila, &protocol.Ocorrencia{ID: "OC-A", Prioridade: 3, Timestamp: agora})
	heap.Push(&fila, &protocol.Ocorrencia{ID: "OC-B", Prioridade: 3, Timestamp: agora.Add(5 * time.Second)})

	primeiroItem := heap.Pop(&fila).(*protocol.Ocorrencia)
	if primeiroItem.ID != "OC-A" {
		t.Errorf("O desempate falhou. Esperado OC-A, obteve %s", primeiroItem.ID)
	}
}
