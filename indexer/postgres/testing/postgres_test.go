package testing

import (
	"context"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/hashicorp/consul/sdk/freeport"
	"github.com/stretchr/testify/require"

	indexertesting "cosmossdk.io/schema/testing"

	"cosmossdk.io/indexer/postgres"
	listenertest "cosmossdk.io/schema/testing/listener"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresIndexer(t *testing.T) {
	dbPort := freeport.GetOne(t)
	pgConfig := embeddedpostgres.DefaultConfig().Port(uint32(dbPort))
	dbUrl := pgConfig.GetConnectionURL()
	pg := embeddedpostgres.NewDatabase(pgConfig)
	require.NoError(t, pg.Start())

	ctx, cancel := context.WithCancel(context.Background())

	t.Cleanup(func() {
		cancel()
		require.NoError(t, pg.Stop())
	})

	indexer, err := postgres.NewIndexer(ctx, postgres.Options{
		Driver:        "pgx",
		ConnectionURL: dbUrl,
	})
	require.NoError(t, err)

	fixture := listenertest.NewAppSimulator(listenertest.AppSimulatorOptions{
		Listener:  indexer.Listener(),
		AppSchema: indexertesting.ExampleAppSchema,
	})

	require.NoError(t, fixture.Initialize())

	blockDataGen := fixture.BlockDataGen()
	for i := 0; i < 10; i++ {
		blockData := blockDataGen.Example(i + 1)
		require.NoError(t, fixture.ProcessBlockData(blockData))
	}
}