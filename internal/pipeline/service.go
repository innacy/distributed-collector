package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

type Service struct {
	name     string
	addr     string
	listener net.Listener
	server   *http.Server
	mu       sync.Mutex
	healthy  bool
}

func NewService(name, addr string) *Service {
	return &Service{
		name:    name,
		addr:    addr,
		healthy: true,
	}
}

func (s *Service) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

func (s *Service) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)

	var err error
	s.mu.Lock()
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("listen: %w", err)
	}
	s.server = &http.Server{Handler: mux}
	s.mu.Unlock()

	slog.Info("service started", "name", s.name, "addr", s.Addr())

	errCh := make(chan error, 1)
	go func() { errCh <- s.server.Serve(s.listener) }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(shutdownCtx)
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

func (s *Service) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	healthy := s.healthy
	s.mu.Unlock()

	if healthy {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"healthy"}`)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"status":"unhealthy"}`)
	}
}

func (s *Service) SetHealthy(healthy bool) {
	s.mu.Lock()
	s.healthy = healthy
	s.mu.Unlock()
}
