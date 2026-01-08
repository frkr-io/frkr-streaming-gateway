package main

import (
	"log"

	gwcommon "github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/plugins"
	"github.com/frkr-io/frkr-streaming-gateway/internal/gateway"
)

func main() {
	cfg, err := gwcommon.LoadConfigFromFlags()
	if err != nil {
		log.Fatal(err)
	}

	db, err := gwcommon.NewDB(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	secretPlugin, err := plugins.NewDatabaseSecretPlugin(db)
	if err != nil {
		log.Fatal(err)
	}

	authPlugin := plugins.NewBasicAuthPlugin(db)

	gw, err := gateway.NewStreamingGateway(authPlugin, secretPlugin)
	if err != nil {
		log.Fatal(err)
	}

	if err := gw.Start(cfg, db); err != nil {
		log.Fatalf("Gateway failed: %v", err)
	}
}
