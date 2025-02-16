package scan_types

import (
	assertPkg "github.com/stretchr/testify/assert"
	"testing"
)

func TestProductScan(t *testing.T) {
	assert := assertPkg.New(t)
	env := initEnv(assert)

	clearEnv(t, env)
}
