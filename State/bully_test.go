package state

import (
	"testing"
)

func TestDecisaoEleicao(t *testing.T) {
	meuID := 2
	outrosBrokers := map[int]string{1: "broker1:9081", 3: "broker3:9083", 4: "broker4:9084"}

	temMaioresVivos := false
	for id := range outrosBrokers {
		if id > meuID {
			temMaioresVivos = true
		}
	}

	if !temMaioresVivos {
		t.Errorf("Erro: O Broker 2 deveria detectar que existem IDs maiores.")
	}

	meuIDMax := 4
	alguemMaior := false
	for id := range outrosBrokers {
		if id > meuIDMax {
			alguemMaior = true
		}
	}

	if alguemMaior {
		t.Errorf("Erro: O Broker 4 não deveria encontrar ninguém maior que ele.")
	}
}
