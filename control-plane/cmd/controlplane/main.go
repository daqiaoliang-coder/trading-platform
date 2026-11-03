// Command controlplane is the Go control plane for shard migration
// orchestration. It validates operator plans, calls the Rust node admin API,
// and persists an append-only audit trail of every attempt.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"trading-platform/control-plane/internal/api"
	"trading-platform/control-plane/internal/node"
	"trading-platform/control-plane/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "control plane listen address")
	nodeURL := flag.String("node-url", "http://127.0.0.1:8081", "Rust node admin base URL")
	stateDir := flag.String("state-dir", "control-plane-state", "audit log directory")
	flag.Parse()

	audit, err := store.Open(*stateDir)
	if err != nil {
		log.Fatalf("control plane state: %v", err)
	}
	defer audit.Close()

	client := node.New(*nodeURL)
	healthCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := client.Health(healthCtx); err != nil {
		log.Printf("warning: node %s is not healthy yet: %v", *nodeURL, err)
	}
	cancel()

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	server := &http.Server{
		Handler:           api.New(client, audit).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()
	fmt.Printf("control-plane listening on %s node=%s state=%s\n", listener.Addr(), *nodeURL, *stateDir)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Printf("control plane shutting down")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
