package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"agrisync/internal/api"
	"agrisync/internal/config"
	"agrisync/internal/db"
	"agrisync/internal/service"
	"agrisync/web"
)

func healthcheck() int {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	host := "127.0.0.1"
	port := "8080"
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		port = addr[i+1:]
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s:%s/healthz", host, port))
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func main() {
	// Container healthcheck: probe our own /healthz and exit 0/1. Distroless
	// images have no shell or curl, so the binary probes itself.
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		os.Exit(healthcheck())
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, err := config.Load()
	if err != nil {
		log.Error("config", "error", err)
		os.Exit(2)
	}

	openCtx, cancelOpen := context.WithTimeout(context.Background(), 60*time.Second)
	store, err := db.Open(openCtx, cfg)
	cancelOpen()
	if err != nil {
		log.Error("database", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	seedCtx, cancelSeed := context.WithTimeout(context.Background(), 15*time.Second)
	for _, code := range store.RegionCodes() {
		if err := store.SeedRegionState(seedCtx, code); err != nil {
			cancelSeed()
			log.Error("region state", "region", code, "error", err)
			os.Exit(1)
		}
	}
	cancelSeed()

	svc := service.New(cfg, store)
	ruleCtx, cancelRules := context.WithTimeout(context.Background(), 15*time.Second)
	for _, code := range store.RegionCodes() {
		if err := svc.SeedDefaultRules(ruleCtx, code); err != nil {
			cancelRules()
			log.Error("sequence rules", "region", code, "error", err)
			os.Exit(1)
		}
	}
	cancelRules()
	srv := api.New(cfg, store, svc, log)
	srv.UseStatic(web.Assets())

	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()
	if cfg.ReplicationInterval > 0 {
		go api.NewReplicator(svc, log, cfg.ReplicationInterval).Run(runCtx)
		log.Info("background replication enabled", "interval", cfg.ReplicationInterval.String())
	}

	httpServer := &http.Server{
		Addr:              cfg.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Info("listening", "addr", cfg.Addr, "regions", store.RegionCodes(),
			"docs", "http://localhost"+cfg.Addr+"/docs")
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("listen", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	stopRun()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	log.Info("stopped")
}
