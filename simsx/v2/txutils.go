package v2

import (
	"context"
	"errors"
	"math/rand"

	"cosmossdk.io/core/transaction"

	"github.com/cosmos/cosmos-sdk/client"
	types2 "github.com/cosmos/cosmos-sdk/crypto/types"
	"github.com/cosmos/cosmos-sdk/simsx"
	"github.com/cosmos/cosmos-sdk/testutil/sims"
	"github.com/cosmos/cosmos-sdk/types"
)

type TXBuilder[T Tx] interface {
	Build(ctx context.Context,
		ak simsx.AccountSource,
		senders []simsx.SimAccount,
		msg types.Msg,
		r *rand.Rand,
		chainID string,
	) (T, error)
}

var _ TXBuilder[Tx] = TXBuilderFn[Tx](nil)

type TXBuilderFn[T Tx] func(ctx context.Context, ak simsx.AccountSource, senders []simsx.SimAccount, msg types.Msg, r *rand.Rand, chainID string) (T, error)

func (b TXBuilderFn[T]) Build(ctx context.Context, ak simsx.AccountSource, senders []simsx.SimAccount, msg types.Msg, r *rand.Rand, chainID string) (T, error) {
	return b(ctx, ak, senders, msg, r, chainID)
}

func NewGenericTXBuilder[T Tx](txConfig client.TxConfig) TXBuilder[T] {
	return TXBuilderFn[T](func(ctx context.Context, ak simsx.AccountSource, senders []simsx.SimAccount, msg types.Msg, r *rand.Rand, chainID string) (tx T, err error) {
		sdkTx, err := BuildTestTX(ctx, ak, senders, msg, r, txConfig, chainID)
		if err != nil {
			return tx, err
		}
		out, ok := sdkTx.(T)
		if !ok {
			return out, errors.New("unexpected Tx type")
		}
		return out, nil
	})
}

func BuildTestTX(
	ctx context.Context,
	ak simsx.AccountSource,
	senders []simsx.SimAccount,
	msg types.Msg,
	r *rand.Rand,
	txConfig client.TxConfig,
	chainID string,
) (types.Tx, error) {
	accountNumbers := make([]uint64, len(senders))
	sequenceNumbers := make([]uint64, len(senders))
	for i := 0; i < len(senders); i++ {
		acc := ak.GetAccount(ctx, senders[i].Address)
		accountNumbers[i] = acc.GetAccountNumber()
		sequenceNumbers[i] = acc.GetSequence()
	}
	fees := senders[0].LiquidBalance().RandFees()
	return sims.GenSignedMockTx( // todo: inline code from testutil
		r,
		txConfig,
		[]types.Msg{msg},
		fees,
		sims.DefaultGenTxGas,
		chainID,
		accountNumbers,
		sequenceNumbers,
		simsx.Collect(senders, func(a simsx.SimAccount) types2.PrivKey { return a.PrivKey })...,
	)
}

type Tx = transaction.Tx

var _ transaction.Codec[Tx] = &GenericTxDecoder[Tx]{}

// todo: this is the same as in commands
type GenericTxDecoder[T Tx] struct {
	txConfig client.TxConfig
}

// NewGenericTxDecoder constructor
func NewGenericTxDecoder[T Tx](txConfig client.TxConfig) *GenericTxDecoder[T] {
	return &GenericTxDecoder[T]{txConfig: txConfig}
}

// Decode implements transaction.Codec.
func (t *GenericTxDecoder[T]) Decode(bz []byte) (T, error) {
	var out T
	tx, err := t.txConfig.TxDecoder()(bz)
	if err != nil {
		return out, err
	}

	var ok bool
	out, ok = tx.(T)
	if !ok {
		return out, errors.New("unexpected Tx type")
	}

	return out, nil
}

// DecodeJSON implements transaction.Codec.
func (t *GenericTxDecoder[T]) DecodeJSON(bz []byte) (T, error) {
	var out T
	tx, err := t.txConfig.TxJSONDecoder()(bz)
	if err != nil {
		return out, err
	}

	var ok bool
	out, ok = tx.(T)
	if !ok {
		return out, errors.New("unexpected Tx type")
	}

	return out, nil
}
