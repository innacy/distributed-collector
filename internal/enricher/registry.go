package enricher

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type ServiceInfo struct {
	Team string `yaml:"team"`
	Tier string `yaml:"tier"`
	SLA  string `yaml:"sla"`
}

type Registry struct {
	services map[string]*ServiceInfo
}

type registryFile struct {
	Services map[string]*ServiceInfo `yaml:"services"`
}

func LoadRegistry(path string) (*Registry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read registry file: %w", err)
	}

	var rf registryFile
	if err := yaml.Unmarshal(data, &rf); err != nil {
		return nil, fmt.Errorf("parse registry yaml: %w", err)
	}

	return &Registry{services: rf.Services}, nil
}

func (r *Registry) Lookup(service string) (*ServiceInfo, bool) {
	info, ok := r.services[service]
	return info, ok
}
