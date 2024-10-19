package client

import (
	"fmt"

	clienttx "github.com/cosmos/cosmos-sdk/client/tx"
	"github.com/cosmos/cosmos-sdk/testutil/testdata"
	"github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/cosmos/cosmos-sdk/types/tx/signing"
	authclient "github.com/cosmos/cosmos-sdk/x/auth/client"

	"cosmossdk.io/math"

	"github.com/cosmos/cosmos-sdk/testutil"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/bank/types"
)

// go test -v -run TestSendTransaction ./e2e/bank/grpc.go
func (s *E2ETestSuite) TestSendTransaction() {
	val := s.network.Validators[0]
	s.Require().NoError(s.network.WaitForNextBlock())
	// prepare txBuilder with msg
	txBuilder := val.ClientCtx.TxConfig.NewTxBuilder()
	feeAmount := sdk.Coins{sdk.NewInt64Coin(s.cfg.BondDenom, 10)}
	gasLimit := testdata.NewTestGasLimit()

	baseURL := val.APIAddress
	sender := val.Address
	// send to recipient
	recipient := sdk.AccAddress("cosmos1v4nxzt6s4xnn68v7zxkwssyj32nxlgpzrdtxx9")

	testCases := []struct {
		name     string
		msg      *types.MsgSend
		expErr   bool
		expected string
	}{
		// 1. Normal transaction
		{
			"Send normal transaction",
			&types.MsgSend{
				FromAddress: sender.String(),
				ToAddress:   recipient.String(),
				Amount:      sdk.Coins{sdk.NewInt64Coin(s.cfg.BondDenom, 100)},
			},
			false,
			"success",
		},
		// 2. Unsufficient balance
		{
			"Insufficient balance",
			&types.MsgSend{
				FromAddress: sender.String(),
				ToAddress:   recipient.String(),
				Amount:      sdk.Coins{sdk.NewInt64Coin(s.cfg.BondDenom, 100000000000)}, // 超过账户余额
			},
			true,
			"account sequence mismatch",
		},
		// 3. Send negative amount
		{
			"Send negative amount",
			&types.MsgSend{
				FromAddress: sender.String(),
				ToAddress:   recipient.String(),
				Amount: sdk.Coins{
					sdk.Coin{
						Denom:  s.cfg.BondDenom,
						Amount: math.NewInt(-100),
					},
				},
			},
			true,
			"account sequence mismatch",
		},
		// 4. Maximum balance sent
		{
			"Send maximum balance",
			&types.MsgSend{
				FromAddress: sender.String(),
				ToAddress:   recipient.String(),
				Amount:      sdk.Coins{sdk.NewCoin(s.cfg.BondDenom, s.cfg.StakingTokens.Sub(s.cfg.BondedTokens))},
			},
			true,
			"account sequence mismatch",
		},
		// 5. Send to invalid address
		{
			"Send to invalid address",
			&types.MsgSend{
				FromAddress: sender.String(),
				ToAddress:   sdk.AccAddress("invalidaddress").String(),
				Amount:      sdk.Coins{sdk.NewInt64Coin(s.cfg.BondDenom, 100)},
			},
			true,
			"account sequence mismatch",
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.Run(tc.name, func() {
			s.Require().NoError(
				txBuilder.SetMsgs(tc.msg),
			)
			txBuilder.SetFeeAmount(feeAmount)
			txBuilder.SetGasLimit(gasLimit)
			txBuilder.SetMemo("foobar")
			signers, err := txBuilder.GetTx().GetSigners()

			s.Require().NoError(err)
			s.Require().Equal([][]byte{val.Address}, signers)

			// setup txFactory
			txFactory := clienttx.Factory{}.
				WithChainID(val.ClientCtx.ChainID).
				WithKeybase(val.ClientCtx.Keyring).
				WithTxConfig(val.ClientCtx.TxConfig).
				WithSignMode(signing.SignMode_SIGN_MODE_DIRECT)

			// Sign Tx.
			err = authclient.SignTx(txFactory, val.ClientCtx, val.Moniker, txBuilder, false, true)

			s.Require().NoError(err)
			// Build and broadcast.
			txbz, err := s.cfg.TxConfig.TxEncoder()(txBuilder.GetTx())

			s.Require().NoError(err)

			req := &tx.BroadcastTxRequest{
				Mode:    tx.BroadcastMode_BROADCAST_MODE_SYNC,
				TxBytes: txbz,
			}
			req1, err := val.ClientCtx.Codec.MarshalJSON(req)

			s.Require().NoError(err)
			// broadcast tx
			resp, err := testutil.PostRequest(fmt.Sprintf("%s/cosmos/tx/v1beta1/txs", baseURL), "application/json", req1)

			// check response
			s.Require().NoError(err)
			if tc.expErr {
				fmt.Println(string(resp))
				s.Require().Contains(string(resp), tc.expected)
			} else {
				var result tx.BroadcastTxResponse
				err = val.ClientCtx.Codec.UnmarshalJSON(resp, &result)
				fmt.Println(result)
				s.Require().NoError(err)
				s.Require().Equal(uint32(0), result.TxResponse.Code, "rawlog", result.TxResponse.RawLog)
			}
		})
	}
}
