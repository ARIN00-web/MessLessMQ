package topic

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRegistryCreateAndGetTopic(t *testing.T) {
	registry := NewRegistry()

	created, err := registry.Create("orders")
	if err != nil {
		t.Fatalf("Create(orders) error: %v", err)
	}

	if created.Name() != "orders" {
		t.Fatalf(
			"created topic name = %q, want %q",
			created.Name(),
			"orders",
		)
	}

	got, err := registry.Get("orders")
	if err != nil {
		t.Fatalf("Get(orders) error: %v", err)
	}

	if got != created {
		t.Fatal("Get(orders) returned a different topic instance")
	}
}

func TestRegistryRejectsDuplicateTopic(t *testing.T) {
	registry := NewRegistry()

	if _, err := registry.Create("orders"); err != nil {
		t.Fatalf("first Create(orders) error: %v", err)
	}

	if _, err := registry.Create("orders"); !errors.Is(err, ErrTopicAlreadyExists) {
		t.Fatalf(
			"second Create(orders) error = %v, want ErrTopicAlreadyExists",
			err,
		)
	}
}

func TestRegistryRejectsInvalidTopicNames(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: "empty", want: ""},
		{name: "single dot", want: "."},
		{name: "double dot", want: ".."},
		{name: "forward slash", want: "orders/payments"},
		{name: "backslash", want: `orders\payments`},
		{name: "space", want: "order events"},
		{name: "colon", want: "orders:payments"},
		{name: "non ASCII", want: "café"},
		{
			name: "too long",
			want: strings.Repeat("a", MaxTopicNameLength+1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := NewRegistry()

			if _, err := registry.Create(tt.want); !errors.Is(err, ErrInvalidTopicName) {
				t.Fatalf(
					"Create(%q) error = %v, want ErrInvalidTopicName",
					tt.want,
					err,
				)
			}
		})
	}
}

func TestRegistryGetMissingTopic(t *testing.T) {
	registry := NewRegistry()

	if _, err := registry.Get("orders"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf(
			"Get(orders) error = %v, want ErrTopicNotFound",
			err,
		)
	}
}

func TestRegistryListIsSorted(t *testing.T) {
	registry := NewRegistry()

	for _, name := range []string{
		"payments",
		"orders",
		"notifications",
	} {
		if _, err := registry.Create(name); err != nil {
			t.Fatalf("Create(%q) error: %v", name, err)
		}
	}

	got := registry.List()

	want := []string{
		"notifications",
		"orders",
		"payments",
	}

	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("List() = %v, want %v", got, want)
	}
}

func TestRegistryNamesAreCaseSensitive(t *testing.T) {
	registry := NewRegistry()

	for _, name := range []string{
		"orders",
		"Orders",
	} {
		if _, err := registry.Create(name); err != nil {
			t.Fatalf("Create(%q) error: %v", name, err)
		}
	}

	if got := len(registry.List()); got != 2 {
		t.Fatalf("topic count = %d, want 2", got)
	}
}

func TestRegistryConcurrentCreateSameName(t *testing.T) {
	registry := NewRegistry()

	const workers = 32

	var wg sync.WaitGroup
	var created atomic.Int32
	var alreadyExists atomic.Int32

	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start

			_, err := registry.Create("concurrent-topic")

			switch {
			case err == nil:
				created.Add(1)

			case errors.Is(err, ErrTopicAlreadyExists):
				alreadyExists.Add(1)

			default:
				t.Errorf("Create() unexpected error: %v", err)
			}
		}()
	}

	close(start)
	wg.Wait()

	if got := created.Load(); got != 1 {
		t.Fatalf("successful creations = %d, want 1", got)
	}

	if got := alreadyExists.Load(); got != workers-1 {
		t.Fatalf(
			"duplicate results = %d, want %d",
			got,
			workers-1,
		)
	}
}

func TestZeroValueRegistry(t *testing.T) {
	var registry Registry

	if _, err := registry.Create("events"); err != nil {
		t.Fatalf("Create() on zero-value registry error: %v", err)
	}

	if _, err := registry.Get("events"); err != nil {
		t.Fatalf("Get() on zero-value registry error: %v", err)
	}
}
