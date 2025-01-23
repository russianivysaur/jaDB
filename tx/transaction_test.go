package tx

import (
	assertPkg "github.com/stretchr/testify/assert"
	"testing"
)

func TestTransactions(t *testing.T) {
	assert := assertPkg.New(t)

	t.Run("idk", func(t *testing.T) {
		assert.Equal(2, 2)
	})
}
