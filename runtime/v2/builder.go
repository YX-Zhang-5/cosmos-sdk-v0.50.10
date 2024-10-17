package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"cosmossdk.io/core/appmodule"
	appmodulev2 "cosmossdk.io/core/appmodule/v2"
	"cosmossdk.io/core/store"
	"cosmossdk.io/core/transaction"
	"cosmossdk.io/server/v2/appmanager"
	"cosmossdk.io/server/v2/stf"
	"cosmossdk.io/server/v2/stf/branch"
	"cosmossdk.io/store/v2/root"
)

// AppBuilder is a type that is injected into a container by the runtime/v2 module
// (as *AppBuilder) which can be used to create an app which is compatible with
// the existing app.go initialization conventions.
type AppBuilder[T transaction.Tx] struct {
	app          *App[T]
	storeBuilder root.Builder

	// the following fields are used to overwrite the default
	txValidator func(ctx context.Context, tx T) error
	postTxExec  func(ctx context.Context, tx T, success bool) error
}

// DefaultGenesis returns a default genesis from the registered AppModule's.
func (a *AppBuilder[T]) DefaultGenesis() map[string]json.RawMessage {
	return a.app.moduleManager.DefaultGenesis()
}

// RegisterModules registers the provided modules with the module manager.
// This is the primary hook for integrating with modules which are not registered using the app config.
func (a *AppBuilder[T]) RegisterModules(modules map[string]appmodulev2.AppModule) error {
	for name, appModule := range modules {
		// if a (legacy) module implements the HasName interface, check that the name matches
		if mod, ok := appModule.(interface{ Name() string }); ok {
			if name != mod.Name() {
				a.app.logger.Warn(fmt.Sprintf("module name %q does not match name returned by HasName: %q", name, mod.Name()))
			}
		}

		if _, ok := a.app.moduleManager.modules[name]; ok {
			return fmt.Errorf("module named %q already exists", name)
		}
		a.app.moduleManager.modules[name] = appModule

		if mod, ok := appModule.(appmodulev2.HasRegisterInterfaces); ok {
			mod.RegisterInterfaces(a.app.interfaceRegistrar)
		}

		if mod, ok := appModule.(appmodule.HasAminoCodec); ok {
			mod.RegisterLegacyAminoCodec(a.app.amino)
		}
	}

	return nil
}

// Build builds an *App instance.
func (a *AppBuilder[T]) Build(opts ...AppBuilderOption[T]) (*App[T], error) {
	for _, opt := range opts {
		opt(a)
	}

	// default branch
	if a.app.branch == nil {
		a.app.branch = branch.DefaultNewWriterMap
	}

	// default tx validator
	if a.txValidator == nil {
		a.txValidator = a.app.moduleManager.TxValidators()
	}

	// default post tx exec
	if a.postTxExec == nil {
		a.postTxExec = func(ctx context.Context, tx T, success bool) error {
			return nil
		}
	}

	a.app.db = a.storeBuilder.Get()
	if a.app.db == nil {
		return nil, fmt.Errorf("storeBuilder did not return a db")
	}

	if err := a.app.moduleManager.RegisterServices(a.app); err != nil {
		return nil, err
	}

	endBlocker, valUpdate := a.app.moduleManager.EndBlock()

	stf, err := stf.NewSTF[T](
		a.app.logger.With("module", "stf"),
		a.app.msgRouterBuilder,
		a.app.queryRouterBuilder,
		a.app.moduleManager.PreBlocker(),
		a.app.moduleManager.BeginBlock(),
		endBlocker,
		a.txValidator,
		valUpdate,
		a.postTxExec,
		a.app.branch,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create STF: %w", err)
	}
	a.app.stf = stf

	appManager, err := appmanager.NewAppManager[T](
		appmanager.Config{
			ValidateTxGasLimit: a.app.config.GasConfig.ValidateTxGasLimit,
			QueryGasLimit:      a.app.config.GasConfig.QueryGasLimit,
			SimulationGasLimit: a.app.config.GasConfig.SimulationGasLimit,
		},
		a.app.db,
		a.app.stf,
		a.app.initGenesis,
		a.app.exportGenesis,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create AppManager: %w", err)
	}
	a.app.appm = appManager

	return a.app, nil
}

// AppBuilderOption is a function that can be passed to AppBuilder.Build to customize the resulting app.
type AppBuilderOption[T transaction.Tx] func(*AppBuilder[T])

// AppBuilderWithBranch sets a custom branch implementation for the app.
func AppBuilderWithBranch[T transaction.Tx](branch func(state store.ReaderMap) store.WriterMap) AppBuilderOption[T] {
	return func(a *AppBuilder[T]) {
		a.app.branch = branch
	}
}

// AppBuilderWithTxValidator sets the tx validator for the app.
// It overrides all default tx validators defined by modules.
func AppBuilderWithTxValidator[T transaction.Tx](
	txValidators func(
		ctx context.Context, tx T,
	) error,
) AppBuilderOption[T] {
	return func(a *AppBuilder[T]) {
		a.txValidator = txValidators
	}
}

// AppBuilderWithPostTxExec sets logic that will be executed after each transaction.
// When not provided, a no-op function will be used.
func AppBuilderWithPostTxExec[T transaction.Tx](
	postTxExec func(
		ctx context.Context, tx T, success bool,
	) error,
) AppBuilderOption[T] {
	return func(a *AppBuilder[T]) {
		a.postTxExec = postTxExec
	}
}
