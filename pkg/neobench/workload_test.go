package neobench

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func TestChooseWeightedWorkload(t *testing.T) {
	// This is a fuzz test, effectively.
	// It defines three scripts; a, b and c. a always has a weight of 1, the other two have random weights.
	// We ask for a large number of scripts to execute, and compare the distribution we get to the expected one,
	// based on normalizing by the number of "a"s we get, since that is "1"; eg. if we divide the b and c count
	// by the count of a, it should be roughly the weights requested.

	seed := time.Now().UnixNano()
	r := rand.New(rand.NewSource(seed))
	a := Script{
		Weight:   1,
		Commands: []Command{SetCommand{VarName: "a"}},
	}
	b := Script{
		Weight:   float64(r.Intn(100)),
		Commands: []Command{SetCommand{VarName: "b"}},
	}
	c := Script{
		Weight:   float64(r.Intn(100)),
		Commands: []Command{SetCommand{VarName: "c"}},
	}
	scripts := NewScripts(a, b, c)
	distribution := make(map[string]int64)

	for i := 0; i < 1000000; i++ {
		choice := scripts.Choose(r)
		distribution[choice.Commands[0].(SetCommand).VarName] += 1
	}

	// Normalize the results to a
	baseline := float64(distribution["a"])
	bNorm := float64(distribution["b"]) / baseline
	cNorm := float64(distribution["c"]) / baseline

	// Expect values to be within 10%
	maxDiffOnB := b.Weight * 0.1
	maxDiffOnC := c.Weight * 0.1

	assert.InDelta(t, b.Weight, bNorm, maxDiffOnB, "seed=%d", seed)
	assert.InDelta(t, c.Weight, cNorm, maxDiffOnC, "seed=%d", seed)
}
