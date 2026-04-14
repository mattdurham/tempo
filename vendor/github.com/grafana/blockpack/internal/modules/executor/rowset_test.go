package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRowSet_BasicOps(t *testing.T) {
	rs := newRowSet()
	assert.True(t, rs.IsEmpty())
	assert.Equal(t, 0, rs.Size())

	rs.Add(2)
	rs.Add(5)
	rs.Add(9)

	assert.False(t, rs.IsEmpty())
	assert.Equal(t, 3, rs.Size())

	assert.True(t, rs.Contains(2))
	assert.True(t, rs.Contains(5))
	assert.True(t, rs.Contains(9))
	assert.False(t, rs.Contains(0))
	assert.False(t, rs.Contains(3))
	assert.False(t, rs.Contains(10))
}

func TestRowSet_ToSlice(t *testing.T) {
	rs := newRowSet()
	rs.Add(1)
	rs.Add(3)
	rs.Add(7)

	slice := rs.ToSlice()
	assert.Equal(t, []int{1, 3, 7}, slice)
}

func TestRowSet_EmptyContains(t *testing.T) {
	rs := newRowSet()
	assert.False(t, rs.Contains(0))
	assert.False(t, rs.Contains(100))
}

func TestRowSetWithCap_ZeroHint(t *testing.T) {
	rs := newRowSetWithCap(0)
	assert.True(t, rs.IsEmpty())
	assert.Equal(t, 0, cap(rs.rows))
}

func TestRowSetWithCap_NegativeHint(t *testing.T) {
	rs := newRowSetWithCap(-5)
	assert.True(t, rs.IsEmpty())
	assert.Equal(t, 0, cap(rs.rows))
}

func TestRowSetWithCap_PositiveHint(t *testing.T) {
	rs := newRowSetWithCap(10)
	assert.True(t, rs.IsEmpty())
	assert.Equal(t, 10, cap(rs.rows))

	rs.Add(1)
	rs.Add(3)
	assert.Equal(t, 2, rs.Size())
	assert.True(t, rs.Contains(1))
	assert.True(t, rs.Contains(3))
}
