package main

import (
	"github.com/virtual-kubelet/virtual-kubelet/cmd/virtual-kubelet/internal/provider"
	"github.com/virtual-kubelet/virtual-kubelet/cmd/virtual-kubelet/internal/provider/modelarts"
)

func registerModelarts(s *provider.Store) {
	s.Register("modelarts", func(cfg provider.InitConfig) (provider.Provider, error) {
		return modelarts.NewModelartsProvider(
			cfg.ConfigPath,
			cfg.NodeName,
			cfg.OperatingSystem,
			cfg.InternalIP,
			cfg.DaemonPort,
		)
	})
}
