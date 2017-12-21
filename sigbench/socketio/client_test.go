package socketio

import (
	"testing"
)

func TestGenerateId(t *testing.T) {
	t.Run("Geneate", func(t *testing.T) {
		yeast := Yeast{}
		t.Log(yeast.Generate())
	})
}
