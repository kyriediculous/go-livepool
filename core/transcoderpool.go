package core

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"sort"
	"sync"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/go-livepeer/eth"
	"github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/go-livepeer/net"
	"github.com/livepeer/lpms/ffmpeg"
)

var errPixelMismatch = errors.New("pixel mismatch")

type TranscoderManager interface {
	RegisteredTranscodersCount() int
	RegisteredTranscodersInfo() []net.RemoteTranscoderInfo
	Manage(stream net.Transcoder_RegisterTranscoderServer, capacity int, ethereumAddr ethcommon.Address)
	Transcode(job string, fname string, profiles []ffmpeg.VideoProfile) (*TranscodeData, error)

	selectTranscoder() *RemoteTranscoder
	completeTranscoders(trans *RemoteTranscoder)

	getTaskChan(taskID int64) (TranscoderChan, error)
	addTaskChan() (int64, TranscoderChan)
	removeTaskChan(taskID int64)

	transcoderResults(tcID int64, res *RemoteTranscoderResult)
}

type PublicTranscoderPool struct {
	remoteTranscoders []*RemoteTranscoder
	liveTranscoders   map[net.Transcoder_RegisterTranscoderServer]*RemoteTranscoder
	RTmutex           *sync.Mutex

	// For tracking tasks assigned to remote transcoders
	taskMutex *sync.RWMutex
	taskChans map[int64]TranscoderChan
	taskCount int64

	commission *big.Int // precentage points (eg. 2% -> 200)

	getBasePrice func() *big.Rat

	roundSub func(sink chan<- types.Log) event.Subscription
	eth      eth.LivepeerEthClient

	quit chan struct{}
}

func NewPublicTranscoderPool(n *LivepeerNode, roundSub func(sink chan<- types.Log) event.Subscription, commission *big.Int) *PublicTranscoderPool {
	return &PublicTranscoderPool{
		remoteTranscoders: []*RemoteTranscoder{},
		liveTranscoders:   map[net.Transcoder_RegisterTranscoderServer]*RemoteTranscoder{},
		RTmutex:           &sync.Mutex{},

		taskMutex:    &sync.RWMutex{},
		taskChans:    make(map[int64]TranscoderChan),
		getBasePrice: n.GetBasePrice,

		roundSub: roundSub,
		eth:      n.Eth,

		commission: commission,

		quit: make(chan struct{}),
	}
}

func (rtm *PublicTranscoderPool) Commission() string {
	return big.NewRat(rtm.commission.Int64(), 100).FloatString(2)
}

// StartPayoutLoop starts the PublicTranscoderPool payout loop
func (rtm *PublicTranscoderPool) StartPayoutLoop() {
	roundEvents := make(chan types.Log, 10)
	sub := rtm.roundSub(roundEvents)
	defer sub.Unsubscribe()

	for {
		select {
		case <-rtm.quit:
			return
		case err := <-sub.Err():
			glog.Error(err)
		case <-roundEvents:
			rtm.payout()
		}
	}
}

// StopPayoutLoop stops the PublicTranscoderPool payout loop
func (rtm *PublicTranscoderPool) StopPayoutLoop() {
	close(rtm.quit)
}

func (rtm *PublicTranscoderPool) payout() {
	for _, t := range rtm.liveTranscoders {
		go func(t *RemoteTranscoder) {
			if err := rtm.payoutTranscoder(t); err != nil {
				glog.Errorf("error paying out transcoder transcoder=%v err=%v", t.ethereumAddr, err)
			}
		}(t)
	}
}

func (rtm *PublicTranscoderPool) payoutTranscoder(t *RemoteTranscoder) error {
	bal := t.Balance()
	if bal == nil || bal.Cmp(big.NewInt(0)) <= 0 {
		return nil
	}
	var balAfterFees *big.Int
	if rtm.commission == nil || rtm.commission.Int64() <= 0 { // this will never be nil if we properly set it on node startup
		balAfterFees = bal
	} else {
		balAfterFees = new(big.Int).Div(new(big.Int).Mul(bal, rtm.commission), big.NewInt(100))
	}
	err := rtm.eth.SendEth(balAfterFees, t.ethereumAddr)
	if err != nil {
		return err
	}
	t.Debit(bal)
	return nil
}

// RegisteredTranscodersCount returns number of registered transcoders
func (rtm *PublicTranscoderPool) RegisteredTranscodersCount() int {
	rtm.RTmutex.Lock()
	defer rtm.RTmutex.Unlock()
	return len(rtm.liveTranscoders)
}

// RegisteredTranscodersInfo returns list of restered transcoder's information
func (rtm *PublicTranscoderPool) RegisteredTranscodersInfo() []net.RemoteTranscoderInfo {
	rtm.RTmutex.Lock()
	res := make([]net.RemoteTranscoderInfo, 0, len(rtm.liveTranscoders))
	for _, transcoder := range rtm.liveTranscoders {
		res = append(res, net.RemoteTranscoderInfo{Address: transcoder.addr, Capacity: transcoder.capacity, Load: transcoder.load, EthereumAddress: transcoder.ethereumAddr, Balance: transcoder.Balance()})
	}
	rtm.RTmutex.Unlock()
	return res
}

// Manage adds transcoder to list of live transcoders. Doesn't return untill transcoder disconnects
func (rtm *PublicTranscoderPool) Manage(stream net.Transcoder_RegisterTranscoderServer, capacity int, ethereumAddr ethcommon.Address) {
	from := common.GetConnectionAddr(stream.Context())
	transcoder := NewRemoteTranscoder(rtm, stream, capacity, ethereumAddr)
	go func() {
		ctx := stream.Context()
		<-ctx.Done()
		err := ctx.Err()
		glog.Errorf("Stream closed for transcoder=%s, err=%v", from, err)
		transcoder.done()
	}()

	rtm.RTmutex.Lock()
	rtm.liveTranscoders[transcoder.stream] = transcoder
	rtm.remoteTranscoders = append(rtm.remoteTranscoders, transcoder)
	sort.Sort(byLoadFactor(rtm.remoteTranscoders))
	var totalLoad, totalCapacity, liveTranscodersNum int
	if monitor.Enabled {
		totalLoad, totalCapacity, liveTranscodersNum = rtm.totalLoadAndCapacity()
	}
	rtm.RTmutex.Unlock()
	if monitor.Enabled {
		monitor.SetTranscodersNumberAndLoad(totalLoad, totalCapacity, liveTranscodersNum)
	}

	<-transcoder.eof
	glog.Infof("Got transcoder=%s eof, removing from live transcoders map", from)

	rtm.RTmutex.Lock()
	delete(rtm.liveTranscoders, transcoder.stream)
	if monitor.Enabled {
		totalLoad, totalCapacity, liveTranscodersNum = rtm.totalLoadAndCapacity()
	}
	rtm.RTmutex.Unlock()
	if monitor.Enabled {
		monitor.SetTranscodersNumberAndLoad(totalLoad, totalCapacity, liveTranscodersNum)
	}
}

func (rtm *PublicTranscoderPool) selectTranscoder() *RemoteTranscoder {
	rtm.RTmutex.Lock()
	defer rtm.RTmutex.Unlock()

	checkTranscoders := func(rtm *PublicTranscoderPool) bool {
		return len(rtm.remoteTranscoders) > 0
	}

	for checkTranscoders(rtm) {
		last := len(rtm.remoteTranscoders) - 1
		currentTranscoder := rtm.remoteTranscoders[last]
		if _, ok := rtm.liveTranscoders[currentTranscoder.stream]; !ok {
			// transcoder does not exist in table; remove and retry
			rtm.remoteTranscoders = rtm.remoteTranscoders[:last]
			continue
		}
		if currentTranscoder.load == currentTranscoder.capacity {
			// Head of queue is at capacity, so the rest must be too. Exit early
			return nil
		}
		currentTranscoder.load++
		sort.Sort(byLoadFactor(rtm.remoteTranscoders))
		return currentTranscoder
	}

	return nil
}

func (rtm *PublicTranscoderPool) completeTranscoders(trans *RemoteTranscoder) {
	rtm.RTmutex.Lock()
	defer rtm.RTmutex.Unlock()

	t, ok := rtm.liveTranscoders[trans.stream]
	if !ok {
		return
	}
	t.load--
	sort.Sort(byLoadFactor(rtm.remoteTranscoders))
}

// Caller of this function should hold RTmutex lock
func (rtm *PublicTranscoderPool) totalLoadAndCapacity() (int, int, int) {
	var load, capacity int
	for _, t := range rtm.liveTranscoders {
		load += t.load
		capacity += t.capacity
	}
	return load, capacity, len(rtm.liveTranscoders)
}

// Transcode does actual transcoding using remote transcoder from the pool
func (rtm *PublicTranscoderPool) Transcode(job string, fname string, profiles []ffmpeg.VideoProfile) (*TranscodeData, error) {
	currentTranscoder := rtm.selectTranscoder()
	if currentTranscoder == nil {
		return nil, errors.New("No transcoders available")
	}
	res, err := currentTranscoder.Transcode(job, fname, profiles)
	_, fatal := err.(RemoteTranscoderFatalError)
	if fatal {
		// Don't retry if we've timed out; broadcaster likely to have moved on
		// XXX problematic for VOD when we *should* retry
		if err.(RemoteTranscoderFatalError).error == ErrRemoteTranscoderTimeout {
			return res, err
		}
		return rtm.Transcode(job, fname, profiles)
	}

	if err == nil && rtm.getBasePrice != nil {
		go rtm.reward(currentTranscoder, res)
	}

	rtm.completeTranscoders(currentTranscoder)
	return res, err
}

func (rtm *PublicTranscoderPool) getTaskChan(taskID int64) (TranscoderChan, error) {
	rtm.taskMutex.RLock()
	defer rtm.taskMutex.RUnlock()
	if tc, ok := rtm.taskChans[taskID]; ok {
		return tc, nil
	}
	return nil, fmt.Errorf("No transcoder channel")
}

func (rtm *PublicTranscoderPool) addTaskChan() (int64, TranscoderChan) {
	rtm.taskMutex.Lock()
	defer rtm.taskMutex.Unlock()
	taskID := rtm.taskCount
	rtm.taskCount++
	if tc, ok := rtm.taskChans[taskID]; ok {
		// should really never happen
		glog.V(common.DEBUG).Info("Transcoder channel already exists for ", taskID)
		return taskID, tc
	}
	rtm.taskChans[taskID] = make(TranscoderChan, 1)
	return taskID, rtm.taskChans[taskID]
}

func (rtm *PublicTranscoderPool) removeTaskChan(taskID int64) {
	rtm.taskMutex.Lock()
	defer rtm.taskMutex.Unlock()
	if _, ok := rtm.taskChans[taskID]; !ok {
		glog.V(common.DEBUG).Info("Transcoder channel nonexistent for job ", taskID)
		return
	}
	delete(rtm.taskChans, taskID)
}

func (rtm *PublicTranscoderPool) transcoderResults(tcID int64, res *RemoteTranscoderResult) {
	remoteChan, err := rtm.getTaskChan(tcID)
	if err != nil {
		return // do we need to return anything?
	}
	remoteChan <- res
}

func (rtm *PublicTranscoderPool) reward(transcoder *RemoteTranscoder, td *TranscodeData) {
	if err := verifyPixels(td); err != nil {
		glog.Errorf("pixel verification failed for transcoder=%v", transcoder.ethereumAddr.Hex())
		return
	}
	price := rtm.getBasePrice()
	if price != nil {
		fees := new(big.Rat).Mul(price, big.NewRat(td.Pixels, 1))
		commission := new(big.Rat).Mul(fees, big.NewRat(rtm.commission.Int64(), 10000))
		feesInt, ok := new(big.Int).SetString(fees.Sub(fees, commission).FloatString(0), 10)
		if ok {
			transcoder.Credit(feesInt)
		}
	}
}

func verifyPixels(td *TranscodeData) error {
	count := int64(0)
	for i := 0; i < len(td.Segments); i++ {
		pxls, err := countPixels(td.Segments[i].Data)
		if err != nil {
			return err
		}
		count += pxls
	}
	if count != td.Pixels {
		return errPixelMismatch
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
