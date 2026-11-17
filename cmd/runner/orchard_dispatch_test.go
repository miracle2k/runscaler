package main

import (
	"context"
	"log/slog"
	"testing"

	"github.com/ysya/runscaler/internal/config"
	"github.com/ysya/runscaler/internal/provider"
)

func TestOrchardDispatchDoesNotTouchLocalBackends(t *testing.T) {
	ss := validScaleSet("orchard", 0)
	ss.Provider = "orchard"
	// Inherited Docker/Tart settings must not activate any local backend.
	ss.Docker.SharedVolume = "/shared"
	ss.Docker.CacheVolumes = []string{"cache:/cache"}
	ss.Tart.Home = "/unused"
	runtimes := newScaleSetRuntimes([]config.ScaleSetConfig{ss}, runtimeBackends{})
	if len(runtimes) != 1 || runtimes[0].docker != nil || runtimes[0].tart != nil {
		t.Fatal("Orchard acquired a local backend")
	}
	runtimes[0].logger = slog.New(slog.DiscardHandler)
	if _, ok := runtimes[0].newProvider(context.Background(), managerDeps{}).(*provider.OrchardProvider); !ok {
		t.Fatal("Orchard provider not selected")
	}
	if len(groupDockerScaleSets([]config.ScaleSetConfig{ss})) != 0 {
		t.Fatal("Orchard grouped with Docker")
	}
	if len(tartCacheStores([]config.ScaleSetConfig{ss}, runtimes[0].logger)) != 0 {
		t.Fatal("Orchard activated Tart cache management")
	}
	if len(cacheVolumeStoresFor([]config.ScaleSetConfig{ss}, nil, "")) != 0 {
		t.Fatal("Orchard activated Docker cache management")
	}
	err := prepareRunnerImages(context.Background(), []config.ScaleSetConfig{ss}, func(context.Context, config.ScaleSetConfig) error { t.Fatal("Orchard pulled Docker image"); return nil }, func(context.Context, config.ScaleSetConfig) error {
		t.Fatal("Orchard prepared local Tart image")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
