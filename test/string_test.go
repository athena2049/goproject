package test

import (
	"strings"
	"testing"
)

func TestString(t *testing.T) {
	var builder strings.Builder

	for i := 0; i < 10; i++ {
		builder.WriteString("data ")
	}
	builder.WriteByte('a')
	builder.WriteRune('你')
	t.Log(builder.String())
}
