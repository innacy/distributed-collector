package enricher

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadRegistry_ParsesYAML(t *testing.T) {
	content := `services:
  payment-service:
    team: payments
    tier: critical
    sla: "99.99"
  auth-service:
    team: platform
    tier: standard
    sla: "99.9"
`
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "registry.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))

	reg, err := LoadRegistry(path)
	require.NoError(t, err)

	info, ok := reg.Lookup("payment-service")
	require.True(t, ok)
	assert.Equal(t, "payments", info.Team)
	assert.Equal(t, "critical", info.Tier)

	info, ok = reg.Lookup("auth-service")
	require.True(t, ok)
	assert.Equal(t, "platform", info.Team)
	assert.Equal(t, "standard", info.Tier)
}

func TestLoadRegistry_UnknownService(t *testing.T) {
	content := `services:
  payment-service:
    team: payments
    tier: critical
    sla: "99.99"
`
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "registry.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))

	reg, err := LoadRegistry(path)
	require.NoError(t, err)

	_, ok := reg.Lookup("unknown-service")
	assert.False(t, ok)
}
