package main

import (
	"errors"
	"fmt"
	"os"

	clientv2helpers "cosmossdk.io/client/v2/helpers"
	"cosmossdk.io/core/transaction"
	serverv2 "cosmossdk.io/server/v2"
	"cosmossdk.io/simapp/v2"
	"cosmossdk.io/simapp/v2/simdv2/cmd"
)

func main() {
	// see: https://github.com/spf13/cobra/blob/e94f6d0dd9a5e5738dca6bce03c4b1207ffbc0ec/command.go#L1082
	args := os.Args[1:]
	rootCmd, err := cmd.NewRootCmd[transaction.Tx](args)
	if err != nil {
		if _, pErr := fmt.Fprintln(os.Stderr, err); pErr != nil {
			panic(errors.Join(err, pErr))
		}
		os.Exit(1)
	}
	if err := serverv2.Execute(rootCmd, clientv2helpers.EnvPrefix, simapp.DefaultNodeHome); err != nil {
		fmt.Fprintln(rootCmd.OutOrStderr(), err)
		os.Exit(1)
	}
}
