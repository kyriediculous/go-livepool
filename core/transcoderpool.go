package core

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/lpms/ffmpeg"
)

const payoutTicker = 1 * time.Hour

var errPixelMismatch = errors.New("pixel mismatch")

type PublicTranscoderPool struct {
	node *LivepeerNode

	commission *big.Int // precentage points (eg. 2% -> 200)
	roundSub   func(sink chan<- types.Log) event.Subscription
	quit       chan struct{}
}

func NewPublicTranscoderPool(n *LivepeerNode, roundSub func(sink chan<- types.Log) event.Subscription, commission *big.Int) *PublicTranscoderPool {
	return &PublicTranscoderPool{
		node:       n,
		commission: commission,
		roundSub:   roundSub,
		quit:       make(chan struct{}),
	}
}

func (pool *PublicTranscoderPool) Commission() *big.Int {
	return pool.commission
}

func (pool *PublicTranscoderPool) TotalPayouts() (*big.Int, error) {
	return pool.node.Database.GetPoolPayout()
}

// StartPayoutLoop starts the PublicTranscoderPool payout loop
func (pool *PublicTranscoderPool) StartPayoutLoop() {
	roundEvents := make(chan types.Log, 10)
	sub := pool.roundSub(roundEvents)
	defer sub.Unsubscribe()

	ticker := time.NewTicker(payoutTicker)

	for {
		select {
		case <-pool.quit:
			return
		case <-ticker.C:
			pool.payout()
		}
	}
}

// StopPayoutLoop stops the PublicTranscoderPool payout loop
func (pool *PublicTranscoderPool) StopPayoutLoop() {
	close(pool.quit)
}

func (pool *PublicTranscoderPool) payout() {
	transcoders, err := pool.node.Database.RemoteTranscoders()
	if err != nil {
		glog.Error(err)
		return
	}

	for _, t := range transcoders {
		go func(t *common.DBRemoteT) {
			if err := pool.payoutTranscoder(t.Address); err != nil {
				glog.Errorf("error paying out transcoder transcoder=%v err=%v", t.Address.Hex(), err)
			}
		}(t)
	}
}

func (pool *PublicTranscoderPool) payoutTranscoder(transcoder ethcommon.Address) error {
	rt, err := pool.node.Database.SelectRemoteTranscoder(transcoder)
	if err != nil {
		return err
	}
	bal := rt.Pending
	if bal == nil || bal.Cmp(big.NewInt(0)) <= 0 {
		return nil
	}

	// check transaction cost overhead
	gasLimit := big.NewInt(21000)
	b, err := pool.node.Eth.Backend()
	if err != nil {
		return err
	}
	timeOut := 6 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()

	gasPrice, err := b.SuggestGasPrice(ctx)
	txCost := new(big.Int).Mul(gasPrice, gasLimit)

	multiplier := big.NewInt(50)
	if bal.Cmp(new(big.Int).Mul(txCost, multiplier)) <= 0 {
		glog.V(6).Infof("Transcoder does not have enough balance to pay out transcoder=%v balance=%v txCost=%v", rt.Address.Hex(), bal, txCost)
		return nil
	}

	payout := bal.Sub(bal, txCost)
	err = pool.node.Eth.SendEth(payout, transcoder)
	if err != nil {
		return err
	}

	if err := pool.node.Database.UpdateRemoteTranscoder(&common.DBRemoteT{
		Address: transcoder,
		Pending: big.NewInt(0),
		Payout:  new(big.Int).Add(rt.Payout, payout),
	}); err != nil {
		glog.Error(err)
		return err
	}

	glog.Infof("Payed out %v to transcoder %v", payout, transcoder.Hex())

	return pool.node.Database.IncreasePoolPayout(payout)
}

func (pool *PublicTranscoderPool) Reward(transcoder *RemoteTranscoder, td *TranscodeData) error {
	if err := verifyPixels(td); err != nil {
		glog.Errorf("pixel verification failed for transcoder=%v", transcoder.ethereumAddr.Hex())
		return err
	}
	t, err := pool.node.Database.SelectRemoteTranscoder(transcoder.ethereumAddr)
	if err != nil {
		return err
	}
	price := pool.node.GetBasePrice()
	fees := new(big.Rat).Mul(price, big.NewRat(td.Pixels, 1))
	commission := new(big.Rat).Mul(fees, big.NewRat(pool.commission.Int64(), 10000))
	feesInt, ok := new(big.Int).SetString(fees.Sub(fees, commission).FloatString(0), 10)
	if !ok {
		return errors.New("error calculating fees")
	}
	return pool.node.Database.UpdateRemoteTranscoder(&common.DBRemoteT{
		Address: transcoder.ethereumAddr,
		Pending: new(big.Int).Add(t.Pending, feesInt),
	})
}

func verifyPixels(td *TranscodeData) error {
	count := int64(0)
	for i := 0; i < len(td.Segments); i++ {
		pxls, err := countPixels(td.Segments[i].Data)
		if err != nil {
			return err
		}
		if pxls != td.Segments[i].Pixels {
			glog.Errorf("Pixel mismatch count=%v actual=%v", count, td.Pixels)
			return errPixelMismatch
		}
	}

	return nil
}

func countPixels(data []byte) (int64, error) {
	tempfile, err := ioutil.TempFile("", common.RandName())
	if err != nil {
		return 0, fmt.Errorf("error creating temp file for pixels verification: %w", err)
	}
	defer os.Remove(tempfile.Name())

	if _, err := tempfile.Write(data); err != nil {
		tempfile.Close()
		return 0, fmt.Errorf("error writing temp file for pixels verification: %w", err)
	}

	if err = tempfile.Close(); err != nil {
		return 0, fmt.Errorf("error closing temp file for pixels verification: %w", err)
	}

	fname := tempfile.Name()
	p, err := pixels(fname)
	if err != nil {
		return 0, err
	}

	return p, nil
}

func pixels(fname string) (int64, error) {
	in := &ffmpeg.TranscodeOptionsIn{Fname: fname}
	res, err := ffmpeg.Transcode3(in, nil)
	if err != nil {
		return 0, err
	}

	return res.Decoded.Pixels, nil
}
