package storage

import (
	"math/rand"
	"testing"
)

func TestName(t *testing.T) {
	t.Run("name has fixed length", func(t *testing.T) {
		s := "hello world"
		n := NewNameFromGoString(s)

		if Size(len(n)) != SizeOfName {
			t.Fatalf("expected %d, got %d", SizeOfName, len(n))
		}

		if got := string(n[1 : len(s)+1]); got != s {
			t.Fatalf("expected %s, got %s", s, got)
		}
	})

	t.Run("name is truncated at 64 bytes", func(t *testing.T) {
		s := make([]byte, 72)

		for i := 0; i < len(s); i++ {
			s[i] = byte('a' + rand.Intn('z'-'a'))
		}

		n := NewNameFromGoString(string(s))

		if got := string(n[1:]); got != string(s[:NameMaxLen]) {
			t.Fatalf("expected %s, got %s", s[:NameMaxLen], got)
		}
	})

	t.Run("name as go string", func(t *testing.T) {
		s := "hello world"
		n := NewNameFromGoString(s)

		if got := n.UnsafeAsGoString(); got != s {
			t.Fatalf("expected %s, got %s", s, got)
		}
	})
}
