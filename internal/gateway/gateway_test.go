package gateway

import (
	"testing"

	"github.com/frkr-io/frkr-common/db"
	"github.com/frkr-io/frkr-common/plugins"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStreamingGateway(t *testing.T) {
	testDB, _ := db.SetupTestDB(t, "../../../frkr-common/migrations")
	defer testDB.Close()

	secretPlugin, _ := plugins.NewDatabaseSecretPlugin(testDB)
	authPlugin := plugins.NewBasicAuthPlugin(testDB)

	t.Run("successful creation with valid plugins", func(t *testing.T) {
		gw, err := NewStreamingGateway(authPlugin, secretPlugin)
		require.NoError(t, err)
		require.NotNil(t, gw)
		assert.NotNil(t, gw.authPlugin)
		assert.NotNil(t, gw.secretPlugin)
	})

	t.Run("error when authPlugin is nil", func(t *testing.T) {
		gw, err := NewStreamingGateway(nil, secretPlugin)
		require.Error(t, err)
		assert.Nil(t, gw)
		assert.Contains(t, err.Error(), "authPlugin cannot be nil")
	})

	t.Run("error when secretPlugin is nil", func(t *testing.T) {
		gw, err := NewStreamingGateway(authPlugin, nil)
		require.Error(t, err)
		assert.Nil(t, gw)
		assert.Contains(t, err.Error(), "secretPlugin cannot be nil")
	})
}
