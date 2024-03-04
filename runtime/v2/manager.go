package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"golang.org/x/exp/maps"
	"google.golang.org/grpc"
	protobuf "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/runtime/protoiface"

	runtimev2 "cosmossdk.io/api/cosmos/app/runtime/v2"
	cosmosmsg "cosmossdk.io/api/cosmos/msg/v1"
	"cosmossdk.io/core/appmodule"
	appmodulev2 "cosmossdk.io/core/appmodule/v2"
	"cosmossdk.io/core/transaction"
	"cosmossdk.io/log"
	"cosmossdk.io/runtime/v2/protocompat"
	"cosmossdk.io/server/v2/stf"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	sdkmodule "github.com/cosmos/cosmos-sdk/types/module"
)

type MM struct {
	logger             log.Logger
	cdc                codec.Codec
	config             *runtimev2.Module
	modules            map[string]appmodulev2.AppModule
	migrationRegistrar *migrationRegistrar
}

// NewModuleManager is the constructor for the module manager
// It handles all the interactions between the modules and the application
func NewModuleManager(
	logger log.Logger,
	cdc codec.Codec,
	config *runtimev2.Module,
	modules map[string]appmodulev2.AppModule,
) *MM {
	// good defaults for the module manager order
	modulesName := maps.Keys(modules)
	if len(config.PreBlockers) == 0 {
		config.PreBlockers = modulesName
	}
	if len(config.BeginBlockers) == 0 {
		config.BeginBlockers = modulesName
	}
	if len(config.EndBlockers) == 0 {
		config.EndBlockers = modulesName
	}
	if len(config.TxValidation) == 0 {
		config.TxValidation = modulesName
	}
	if len(config.InitGenesis) == 0 {
		config.InitGenesis = modulesName
	}
	if len(config.ExportGenesis) == 0 {
		config.ExportGenesis = modulesName
	}
	if len(config.OrderMigrations) == 0 {
		config.OrderMigrations = sdkmodule.DefaultMigrationsOrder(modulesName)
	}

	mm := &MM{
		logger:             logger,
		cdc:                cdc,
		config:             config,
		modules:            modules,
		migrationRegistrar: newMigrationRegistrar(),
	}

	if err := mm.validateConfig(); err != nil {
		panic(err)

	}

	return mm
}

// Modules returns the modules registered in the module manager
func (m *MM) Modules() map[string]appmodulev2.AppModule {
	return m.modules
}

// RegisterLegacyAminoCodec registers all module codecs
func (m *MM) RegisterLegacyAminoCodec(cdc *codec.LegacyAmino) {
	for _, b := range m.modules {
		if mod, ok := b.(sdkmodule.HasAminoCodec); ok {
			mod.RegisterLegacyAminoCodec(cdc)
		}
	}
}

// RegisterInterfaces registers all module interface types
func (m *MM) RegisterInterfaces(registry codectypes.InterfaceRegistry) {
	for _, b := range m.modules {
		if mod, ok := b.(sdkmodule.HasRegisterInterfaces); ok {
			mod.RegisterInterfaces(registry)
		} else if mod, ok := b.(sdkmodule.HasRegisterInterfacesV2); ok {
			mod.RegisterInterfaces(registry)
		}
	}
}

// DefaultGenesis provides default genesis information for all modules
func (m *MM) DefaultGenesis(cdc codec.JSONCodec) map[string]json.RawMessage {
	genesisData := make(map[string]json.RawMessage)
	for _, b := range m.modules {
		if mod, ok := b.(appmodulev2.HasGenesis); ok {
			_ = mod // TODO, support appmodulev2 genesis
		} else if mod, ok := b.(sdkmodule.HasGenesisBasics); ok {
			genesisData[mod.Name()] = mod.DefaultGenesis(cdc)
		} else if mod, ok := b.(sdkmodule.HasName); ok {
			genesisData[mod.Name()] = []byte("{}")
		}
	}

	return genesisData
}

// ValidateGenesis performs genesis state validation for all modules
func (m *MM) ValidateGenesis(cdc codec.JSONCodec, txEncCfg client.TxEncodingConfig, genesisData map[string]json.RawMessage) error {
	for _, b := range m.modules {
		// first check if the module is an adapted Core API Module
		if mod, ok := b.(appmodulev2.HasGenesis); ok {
			_ = mod // TODO, support appmodulev2 genesis
		} else if mod, ok := b.(sdkmodule.HasGenesisBasics); ok {
			if err := mod.ValidateGenesis(cdc, txEncCfg, genesisData[mod.Name()]); err != nil {
				return err
			}
		}
	}

	return nil
}

// InitGenesis performs init genesis functionality for modules.
func (m *MM) InitGenesis() {
	panic("implement me")
}

// ExportGenesis performs export genesis functionality for modules
func (m *MM) ExportGenesis() {
	panic("implement me")
}

// BeginBlock runs the begin-block logic of all modules
func (m *MM) BeginBlock() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		for _, moduleName := range m.config.BeginBlockers {
			if module, ok := m.modules[moduleName].(appmodulev2.HasBeginBlocker); ok {
				if err := module.BeginBlock(ctx); err != nil {
					return fmt.Errorf("failed to run beginblocker for %s: %w", moduleName, err)
				}
			}
		}

		return nil
	}
}

// EndBlock runs the end-block logic of all modules and tx validator updates
func (m *MM) EndBlock() (endBlockFunc func(ctx context.Context) error, valUpdateFunc func(ctx context.Context) ([]appmodulev2.ValidatorUpdate, error)) {
	validatorUpdates := []appmodulev2.ValidatorUpdate{}
	endBlockFunc = func(ctx context.Context) error {
		for _, moduleName := range m.config.EndBlockers {
			if module, ok := m.modules[moduleName].(appmodulev2.HasEndBlocker); ok {
				err := module.EndBlock(ctx)
				if err != nil {
					return fmt.Errorf("failed to run endblock for %s: %w", moduleName, err)
				}
			} else if module, ok := m.modules[moduleName].(sdkmodule.HasABCIEndBlock); ok { // we need to keep this for our module compatibility promise
				moduleValUpdates, err := module.EndBlock(ctx)
				if err != nil {
					return fmt.Errorf("failed to run enblock for %s: %w", moduleName, err)
				}
				// use these validator updates if provided, the module manager assumes
				// only one module will update the validator set
				if len(moduleValUpdates) > 0 {
					if len(validatorUpdates) > 0 {
						return errors.New("validator end block updates already set by a previous module")
					}

					validatorUpdates = append(validatorUpdates, moduleValUpdates...)
				}
			}
		}

		return nil
	}

	valUpdateFunc = func(ctx context.Context) ([]appmodulev2.ValidatorUpdate, error) {
		// get validator updates of modules implementing directly the new HasUpdateValidators interface
		for _, v := range m.modules {
			if module, ok := v.(appmodulev2.HasUpdateValidators); ok {
				moduleValUpdates, err := module.UpdateValidators(ctx)
				if err != nil {
					return nil, err
				}

				if len(moduleValUpdates) > 0 {
					if len(validatorUpdates) > 0 {
						return nil, errors.New("validator end block updates already set by a previous module")
					}

					validatorUpdates = append(validatorUpdates, moduleValUpdates...)
				}
			}
		}

		return validatorUpdates, nil
	}

	return endBlockFunc, valUpdateFunc
}

// PreBlocker runs the pre-block logic of all modules
func (m *MM) PreBlocker() func(ctx context.Context, txs []transaction.Tx) error {
	return func(ctx context.Context, txs []transaction.Tx) error {
		for _, moduleName := range m.config.PreBlockers {
			if module, ok := m.modules[moduleName].(appmodule.HasPreBlocker); ok {
				if _, err := module.PreBlock(ctx); err != nil {
					return fmt.Errorf("failed to run preblock for %s: %w", moduleName, err)
				}
			}
		}

		return nil
	}
}

// TxValidators validates incoming transactions
func (m *MM) TxValidation() func(ctx context.Context, tx transaction.Tx) error {
	return func(ctx context.Context, tx transaction.Tx) error {
		for _, moduleName := range m.config.TxValidation {
			if module, ok := m.modules[moduleName].(appmodulev2.HasTxValidation[transaction.Tx]); ok {
				if err := module.TxValidator(ctx, tx); err != nil {
					return fmt.Errorf("failed to run txvalidator for %s: %w", moduleName, err)
				}
			}
		}

		return nil
	}
}

// TODO write as descriptive godoc as module manager v1.
func (m *MM) RunMigrations(ctx context.Context, fromVM appmodulev2.VersionMap) (appmodulev2.VersionMap, error) {
	updatedVM := appmodulev2.VersionMap{}
	for _, moduleName := range m.config.OrderMigrations {
		module := m.modules[moduleName]
		fromVersion, exists := fromVM[moduleName]
		toVersion := uint64(0)
		if module, ok := module.(appmodulev2.HasConsensusVersion); ok {
			toVersion = module.ConsensusVersion()
		}

		// We run migration if the module is specified in `fromVM`.
		// Otherwise we run InitGenesis.
		//
		// The module won't exist in the fromVM in two cases:
		// 1. A new module is added. In this case we run InitGenesis with an
		// empty genesis state.
		// 2. An existing chain is upgrading from version < 0.43 to v0.43+ for the first time.
		// In this case, all modules have yet to be added to x/upgrade's VersionMap store.
		if exists {
			m.logger.Info(fmt.Sprintf("migrating module %s from version %d to version %d", moduleName, fromVersion, toVersion))
			if err := m.migrationRegistrar.RunModuleMigrations(ctx, moduleName, fromVersion, toVersion); err != nil {
				return nil, err
			}
		} else {
			m.logger.Info(fmt.Sprintf("adding a new module: %s", moduleName))
			if mod, ok := m.modules[moduleName].(appmodulev2.HasGenesis); ok {
				if err := mod.InitGenesis(ctx, mod.DefaultGenesis()); err != nil {
					return nil, fmt.Errorf("failed to run InitGenesis for %s: %w", moduleName, err)
				}
			}
			if mod, ok := m.modules[moduleName].(sdkmodule.HasGenesis); ok {
				mod.InitGenesis(ctx, m.cdc, mod.DefaultGenesis(m.cdc))
			}
			if mod, ok := m.modules[moduleName].(sdkmodule.HasABCIGenesis); ok {
				moduleValUpdates := mod.InitGenesis(ctx, m.cdc, mod.DefaultGenesis(m.cdc))
				// The module manager assumes only one module will update the validator set, and it can't be a new module.
				if len(moduleValUpdates) > 0 {
					return nil, fmt.Errorf("validator InitGenesis update is already set by another module")
				}
			}
		}

		updatedVM[moduleName] = toVersion
	}

	return updatedVM, nil
}

// RegisterServices registers all module services.
func (m *MM) RegisterServices(app *App) error {
	for _, module := range m.modules {
		// register msg + query
		if services, ok := module.(appmodule.HasServices); ok {
			if err := registerServices(services, app, protoregistry.GlobalFiles); err != nil {
				return err
			}
		}

		// register migrations
		if module, ok := module.(appmodulev2.HasMigrations); ok {
			if err := module.RegisterMigrations(m.migrationRegistrar); err != nil {
				return err
			}
		}

		// TODO: register pre and post msg
	}

	return nil
}

// validateConfig validates the module manager configuration
// it asserts that all modules are defined in the configuration and that no modules are forgotten
func (m *MM) validateConfig() error {
	if err := m.assertNoForgottenModules("PreBlockers", m.config.PreBlockers, func(moduleName string) bool {
		module := m.modules[moduleName]
		_, hasBlock := module.(appmodule.HasPreBlocker)
		return !hasBlock
	}); err != nil {
		return err
	}

	if err := m.assertNoForgottenModules("BeginBlockers", m.config.BeginBlockers, func(moduleName string) bool {
		module := m.modules[moduleName]
		_, hasBeginBlock := module.(appmodulev2.HasBeginBlocker)
		return !hasBeginBlock
	}); err != nil {
		return err
	}

	if err := m.assertNoForgottenModules("EndBlockers", m.config.EndBlockers, func(moduleName string) bool {
		module := m.modules[moduleName]
		if _, hasEndBlock := module.(appmodulev2.HasEndBlocker); hasEndBlock {
			return !hasEndBlock
		}

		_, hasABCIEndBlock := module.(sdkmodule.HasABCIEndBlock)
		return !hasABCIEndBlock
	}); err != nil {
		return err
	}

	if err := m.assertNoForgottenModules("TxValidation", m.config.TxValidation, func(moduleName string) bool {
		module := m.modules[moduleName]
		_, hasTxValidation := module.(appmodulev2.HasTxValidation[transaction.Tx])
		return !hasTxValidation
	}); err != nil {
		return err
	}

	if err := m.assertNoForgottenModules("InitGenesis", m.config.InitGenesis, func(moduleName string) bool {
		module := m.modules[moduleName]
		if _, hasGenesis := module.(appmodulev2.HasGenesis); hasGenesis {
			return !hasGenesis
		}

		// TODO, if we actually don't support old genesis, let's panic here saying this module isn't server/v2 compatible
		if _, hasABCIGenesis := module.(sdkmodule.HasABCIGenesis); hasABCIGenesis {
			return !hasABCIGenesis
		}

		_, hasGenesis := module.(sdkmodule.HasGenesis)
		return !hasGenesis
	}); err != nil {
		return err
	}

	if err := m.assertNoForgottenModules("ExportGenesis", m.config.ExportGenesis, func(moduleName string) bool {
		module := m.modules[moduleName]
		if _, hasGenesis := module.(appmodulev2.HasGenesis); hasGenesis {
			return !hasGenesis
		}

		// TODO, if we actually don't support old genesis, let's panic here saying this module isn't server/v2 compatible
		if _, hasABCIGenesis := module.(sdkmodule.HasABCIGenesis); hasABCIGenesis {
			return !hasABCIGenesis
		}

		_, hasGenesis := module.(sdkmodule.HasGenesis)
		return !hasGenesis
	}); err != nil {
		return err
	}

	if err := m.assertNoForgottenModules("OrderMigrations", m.config.OrderMigrations, nil); err != nil {
		return err
	}

	return nil
}

// assertNoForgottenModules checks that we didn't forget any modules in the *runtimev2.Module config.
// `pass` is a closure which allows one to omit modules from `moduleNames`.
// If you provide non-nil `pass` and it returns true, the module would not be subject of the assertion.
func (m *MM) assertNoForgottenModules(setOrderFnName string, moduleNames []string, pass func(moduleName string) bool) error {
	ms := make(map[string]bool)
	for _, m := range moduleNames {
		ms[m] = true
	}
	var missing []string
	for m := range m.modules {
		m := m
		if pass != nil && pass(m) {
			continue
		}

		if !ms[m] {
			missing = append(missing, m)
		}
	}

	if len(missing) != 0 {
		sort.Strings(missing)
		return fmt.Errorf("all modules must be defined when setting %s, missing: %v", setOrderFnName, missing)
	}

	return nil
}

func registerServices(s appmodule.HasServices, app *App, registry *protoregistry.Files) error {
	c := &configurator{
		cdc:            app.cdc,
		stfQueryRouter: app.queryRouterBuilder,
		stfMsgRouter:   app.msgRouterBuilder,
		registry:       registry,
		err:            nil,
	}
	return s.RegisterServices(c)
}

var _ grpc.ServiceRegistrar = (*configurator)(nil)

type configurator struct {
	cdc            codec.BinaryCodec
	stfQueryRouter *stf.MsgRouterBuilder
	stfMsgRouter   *stf.MsgRouterBuilder
	registry       *protoregistry.Files
	err            error
}

func (c *configurator) RegisterService(sd *grpc.ServiceDesc, ss interface{}) {
	// first we check if it's a msg server
	prefSd, err := c.registry.FindDescriptorByName(protoreflect.FullName(sd.ServiceName))
	if err != nil {
		c.err = fmt.Errorf("register service: unable to find protov2 service descriptor: please make sure protov2 API counterparty is imported: %s", sd.ServiceName)
		return
	}

	if !protobuf.HasExtension(prefSd.(protoreflect.ServiceDescriptor).Options(), cosmosmsg.E_Service) {
		err = c.registerQueryHandlers(sd, ss)
		if err != nil {
			c.err = err
		}
	} else {
		err = c.registerMsgHandlers(sd, ss)
		if err != nil {
			c.err = err
		}
	}
}

func (c *configurator) registerQueryHandlers(sd *grpc.ServiceDesc, ss interface{}) error {
	for _, md := range sd.Methods {
		// TODO(tip): what if a query is not deterministic?
		err := registerMethod(c.cdc, c.stfQueryRouter, sd, md, ss)
		if err != nil {
			return fmt.Errorf("unable to register query handler %s: %w", md.MethodName, err)
		}
	}
	return nil
}

func (c *configurator) registerMsgHandlers(sd *grpc.ServiceDesc, ss interface{}) error {
	for _, md := range sd.Methods {
		err := registerMethod(c.cdc, c.stfMsgRouter, sd, md, ss)
		if err != nil {
			return fmt.Errorf("unable to register msg handler %s: %w", md.MethodName, err)
		}
	}
	return nil
}

func registerMethod(cdc codec.BinaryCodec, stfRouter *stf.MsgRouterBuilder, sd *grpc.ServiceDesc, md grpc.MethodDesc, ss interface{}) error {
	requestName, err := protocompat.RequestFullNameFromMethodDesc(sd, md)
	if err != nil {
		return err
	}

	responseName, err := protocompat.ResponseFullNameFromMethodDesc(sd, md)
	if err != nil {
		return err
	}

	// now we create the hybrid handler
	hybridHandler, err := protocompat.MakeHybridHandler(cdc, sd, md, ss)
	if err != nil {
		return err
	}

	responseV2Type, err := protoregistry.GlobalTypes.FindMessageByName(responseName)
	if err != nil {
		return err
	}

	return stfRouter.RegisterHandler(string(requestName), func(ctx context.Context, msg transaction.Type) (resp transaction.Type, err error) {
		resp = responseV2Type.New().Interface()
		return resp, hybridHandler(ctx, msg.(protoiface.MessageV1), resp.(protoiface.MessageV1))
	})
}