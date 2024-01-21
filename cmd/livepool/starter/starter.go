package starter

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/go-livepeer/core"
	lpmon "github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/go-livepeer/server"
	"github.com/livepeer/lpms/ffmpeg"
)

var (
	// The timeout for ETH RPC calls
	ethRPCTimeout = 20 * time.Second
	// The maximum blocks for the block watcher to retain
	blockWatcherRetentionLimit = 20

	// Estimate of the gas required to redeem a PM ticket on L1 Ethereum
	redeemGasL1 = 350000
	// Estimate of the gas required to redeem a PM ticket on L2 Arbitrum
	redeemGasL2 = 1200000
	// The multiplier on the transaction cost to use for PM ticket faceValue
	txCostMultiplier = 100

	// The interval at which to clean up cached max float values for PM senders and balances per stream
	cleanupInterval = 10 * time.Minute
	// The time to live for cached max float values for PM senders (else they will be cleaned up) in seconds
	smTTL = 172800 // 2 days
)

const (
	BroadcasterRpcPort  = "9935"
	BroadcasterCliPort  = "5935"
	BroadcasterRtmpPort = "1935"
	OrchestratorRpcPort = "8935"
	OrchestratorCliPort = "7935"
	TranscoderCliPort   = "6935"

	RefreshPerfScoreInterval = 10 * time.Minute
)

type LivepeerConfig struct {
	HttpAddr               *string
	OrchAddr               *string
	HttpIngest             *bool
	TestTranscoder         *bool
	Transcoder             *bool
	OrchSecret             *string
	TranscodingOptions     *string
	MaxAttempts            *int
	Region                 *string
	MaxSessions            *string
	CurrentManifest        *bool
	Nvidia                 *string
	Netint                 *string
	EthAcctAddr            *string
	Monitor                *bool
	MetricsPerStream       *bool
	MetricsExposeClientIP  *bool
	MetadataQueueUri       *string
	MetadataAmqpExchange   *string
	MetadataPublishTimeout *time.Duration
	Datadir                *string
	Objectstore            *string
	Recordstore            *string
	FVfailGsBucket         *string
	FVfailGsKey            *string
}

// DefaultLivepeerConfig creates LivepeerConfig exactly the same as when no flags are passed to the livepeer process.
func DefaultLivepeerConfig() LivepeerConfig {
	// Network & Addresses:
	defaultHttpAddr := ""
	defaultOrchAddr := "161.35.157.107:8935,143.110.211.203:8935,165.232.166.255:8935,143.110.233.92:8935"

	// Transcoding:
	defaultTestTranscoder := true
	defaultTranscoder := true
	defaultOrchSecret := "livepoolio-arb"
	defaultTranscodingOptions := "P240p30fps16x9,P360p30fps16x9"
	defaultMaxAttempts := 3

	defaultMaxSessions := strconv.Itoa(10)
	defaultRegion := ""
	defaultCurrentManifest := false
	defaultNvidia := ""
	defaultNetint := ""

	// Onchain:
	defaultEthAcctAddr := ""

	defaultMonitor := false
	defaultMetricsPerStream := false
	defaultMetricsExposeClientIP := false

	// Ingest:
	defaultHttpIngest := true

	return LivepeerConfig{

		HttpAddr: &defaultHttpAddr,
		OrchAddr: &defaultOrchAddr,

		// Transcoding:
		TestTranscoder:     &defaultTestTranscoder,
		Transcoder:         &defaultTranscoder,
		OrchSecret:         &defaultOrchSecret,
		TranscodingOptions: &defaultTranscodingOptions,
		MaxAttempts:        &defaultMaxAttempts,
		MaxSessions:        &defaultMaxSessions,
		Region:             &defaultRegion,
		CurrentManifest:    &defaultCurrentManifest,
		Nvidia:             &defaultNvidia,
		Netint:             &defaultNetint,

		// Onchain:
		EthAcctAddr:           &defaultEthAcctAddr,
		Monitor:               &defaultMonitor,
		MetricsPerStream:      &defaultMetricsPerStream,
		MetricsExposeClientIP: &defaultMetricsExposeClientIP,

		// Ingest:
		HttpIngest: &defaultHttpIngest,
	}
}

func StartLivepeer(ctx context.Context, cfg LivepeerConfig) {

	intMaxSessions, err := strconv.Atoi(*cfg.MaxSessions)
	if err != nil || intMaxSessions <= 0 {
		glog.Exit("-maxSessions must be 'auto' or greater than zero")
	}

	core.MaxSessions = intMaxSessions

	if *cfg.Netint != "" && *cfg.Nvidia != "" {
		glog.Exit("both -netint and -nvidia arguments specified, this is not supported")
	}

	// If multiple orchAddr specified, ensure other necessary flags present and clean up list
	orchURLs := parseOrchAddrs(*cfg.OrchAddr)

	n, err := core.NewLivepeerNode(nil, *cfg.Datadir, nil)
	if err != nil {
		glog.Errorf("Error creating livepeer node: %v", err)
	}

	if *cfg.OrchSecret != "" {
		n.OrchSecret, _ = common.ReadFromFile(*cfg.OrchSecret)
	}

	var transcoderCaps []core.Capability
	if *cfg.Transcoder {
		core.WorkDir = *cfg.Datadir
		accel := ffmpeg.Software
		var devicesStr string
		if *cfg.Nvidia != "" {
			accel = ffmpeg.Nvidia
			devicesStr = *cfg.Nvidia
		}
		if *cfg.Netint != "" {
			accel = ffmpeg.Netint
			devicesStr = *cfg.Netint
		}
		if accel != ffmpeg.Software {
			accelName := ffmpeg.AccelerationNameLookup[accel]
			tf, err := core.GetTranscoderFactoryByAccel(accel)
			if err != nil {
				exit("Error unsupported acceleration: %v", err)
			}
			// Get a list of device ids
			devices, err := common.ParseAccelDevices(devicesStr, accel)
			glog.Infof("%v devices: %v", accelName, devices)
			if err != nil {
				exit("Error while parsing '-%v %v' flag: %v", strings.ToLower(accelName), devices, err)
			}
			glog.Infof("Transcoding on these %v devices: %v", accelName, devices)
			// Test transcoding with specified device
			if *cfg.TestTranscoder {
				transcoderCaps, err = core.TestTranscoderCapabilities(devices, tf)
				if err != nil {
					glog.Exit(err)
				}
			} else {
				// no capability test was run, assume default capabilities
				transcoderCaps = append(transcoderCaps, core.DefaultCapabilities()...)
			}
			// Initialize LB transcoder
			n.Transcoder = core.NewLoadBalancingTranscoder(devices, tf)
		} else {
			// for local software mode, enable all capabilities
			transcoderCaps = append(core.DefaultCapabilities(), core.OptionalCapabilities()...)
			n.Transcoder = core.NewLocalTranscoder(*cfg.Datadir)
		}
	}

	n.NodeType = core.TranscoderNode

	lpmon.NodeID = *cfg.EthAcctAddr
	if lpmon.NodeID != "" {
		lpmon.NodeID += "-"
	}
	hn, _ := os.Hostname()
	lpmon.NodeID += hn

	if *cfg.Monitor {
		if *cfg.MetricsExposeClientIP {
			*cfg.MetricsPerStream = true
		}
		lpmon.Enabled = true
		lpmon.PerStreamMetrics = *cfg.MetricsPerStream
		lpmon.ExposeClientIP = *cfg.MetricsExposeClientIP
		nodeType := lpmon.Transcoder
		lpmon.InitCensus(nodeType, core.LivepeerVersion)
	}

	if lpmon.Enabled {
		lpmon.MaxSessions(core.MaxSessions)
	}

	if n.NodeType == core.TranscoderNode {
		if n.OrchSecret == "" {
			glog.Exit("Missing -orchSecret")
		}
		if len(orchURLs) <= 0 {
			glog.Exit("Missing -orchAddr")
		}

		ethereumAddr := ethcommon.HexToAddress(*cfg.EthAcctAddr)
		go server.RunTranscoder(n, orchURLs, core.MaxSessions, transcoderCaps, ethereumAddr)
	}

	glog.Infof("**Liveepeer Running in Transcoder Mode***")

	glog.Infof("Livepeer Node version: %v", core.LivepeerVersion)
}

func parseOrchAddrs(addrs string) []*url.URL {
	var res []*url.URL
	if len(addrs) > 0 {
		for _, addr := range strings.Split(addrs, ",") {
			addr = strings.TrimSpace(addr)
			addr = defaultAddr(addr, "127.0.0.1", OrchestratorRpcPort)
			if !strings.HasPrefix(addr, "http") {
				addr = "https://" + addr
			}
			uri, err := url.ParseRequestURI(addr)
			if err != nil {
				glog.Error("Could not parse orchestrator URI: ", err)
				continue
			}
			res = append(res, uri)
		}
	}
	return res
}

func validateURL(u string) (*url.URL, error) {
	if u == "" {
		return nil, nil
	}
	p, err := url.ParseRequestURI(u)
	if err != nil {
		return nil, err
	}
	if p.Scheme != "http" && p.Scheme != "https" {
		return nil, errors.New("URL should be HTTP or HTTPS")
	}
	return p, nil
}

func isLocalURL(u string) (bool, error) {
	uri, err := url.ParseRequestURI(u)
	if err != nil {
		return false, err
	}

	hostname := uri.Hostname()
	if net.ParseIP(hostname).IsLoopback() || hostname == "localhost" {
		return true, nil
	}

	return false, nil
}

func defaultAddr(addr, defaultHost, defaultPort string) string {
	if addr == "" {
		return defaultHost + ":" + defaultPort
	}

	if addr[0] == ':' {
		return defaultHost + addr
	}
	// not IPv6 safe
	if !strings.Contains(addr, ":") {
		return addr + ":" + defaultPort
	}
	return addr
}

func exit(msg string, args ...any) {
	glog.Errorf(msg, args...)
	os.Exit(2)
}
