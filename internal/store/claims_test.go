package store

import (
	"context"
	"testing"
	"time"
)

func TestClaimRepositoryBounds(t *testing.T) {
	if _, err := NewClaimRepository(nil, time.Second, time.Second); err != ErrInvalidInput {
		t.Fatal("nil pool accepted")
	}
	r := &ClaimRepository{lease: time.Second, timeout: time.Second}
	for _, slots := range []int{-1, MaxClaimSlots + 1} {
		if claims, err := r.ClaimDue(context.Background(), slots); err != ErrInvalidInput || claims != nil {
			t.Fatal("invalid slot bound accepted")
		}
	}
	claims, err := r.ClaimDue(context.Background(), 0)
	if err != nil || len(claims) != 0 {
		t.Fatal("zero slots accessed database")
	}
}
