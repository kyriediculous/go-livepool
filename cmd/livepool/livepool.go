/*
Livepeer is a peer-to-peer global video live streaming network.  The Golp project is a go implementation of the Livepeer protocol.  For more information, visit the project wiki.
*/

// 	orchAddr := flag.String("orchAddr", "161.35.157.107:8935,143.110.211.203:8935,165.232.166.255:8935,143.110.233.92:8935", "Orchestrator to connect to as a standalone transcoder")
// 	orchSecret := flag.String("orchSecret", "livepoolio", "Shared secret with the orchestrator as a standalone transcoder")
// 	transcoder := flag.Bool("transcoder", true, "Set to true to be a transcoder")
// 	maxSessions := flag.Int("maxSessions", 5, "Maximum number of concurrent transcoding sessions for Orchestrator, maximum number or RTMP streams for Broadcaster, or maximum capacity for transcoder")

// publicTPool := flag.Bool("transcoderPool", false, "Set to true to enable a public transcoder pool")
// poolCommission := flag.Int("poolCommission", 1, "Commision for the public transcoder pool in percentage points")
/*
Livepeer is a peer-to-peer global video live streaming network.  The Golp project is a go implementation of the Livepeer protocol.  For more information, visit the project wiki.
*/
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"time"

	"github.com/olekukonko/tablewriter"

	"github.com/livepeer/go-livepeer/cmd/livepool/starter"
	"github.com/livepeer/livepeer-data/pkg/mistconnector"
	"github.com/peterbourgon/ff/v3"

	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/core"
)

func main() {
	// Override the default flag set since there are dependencies that
	// incorrectly add their own flags (specifically, due to the 'testing'
	// package being linked)
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")
	//We preserve this flag before resetting all the flags.  Not a scalable approach, but it'll do for now.  More discussions here - https://github.com/livepeer/go-livepeer/pull/617
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flag.CommandLine.SetOutput(os.Stdout)

	// Help & Log
	mistJSON := flag.Bool("j", false, "Print application info as json")
	version := flag.Bool("version", false, "Print out the version")
	verbosity := flag.String("v", "3", "Log verbosity.  {4|5|6}")

	cfg := parseLivepeerConfig()

	// Config file
	_ = flag.String("config", "", "Config file in the format 'key value', flags and env vars take precedence over the config file")
	err := ff.Parse(flag.CommandLine, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithEnvVarPrefix("LP"),
		ff.WithConfigFileParser(ff.PlainParser),
	)
	if err != nil {
		glog.Exit("Error parsing config: ", err)
	}

	vFlag.Value.Set(*verbosity)

	if *mistJSON {
		mistconnector.PrintMistConfigJson(
			"livepeer",
			"Official implementation of the Livepeer video processing protocol. Can play all roles in the network.",
			"Livepeer",
			core.LivepeerVersion,
			flag.CommandLine,
		)
		return
	}

	cfg = updateNilsForUnsetFlags(cfg)

	// compare current settings with default values, and print the difference
	defCfg := starter.DefaultLivepeerConfig()
	vDefCfg := reflect.ValueOf(defCfg)
	vCfg := reflect.ValueOf(cfg)
	cfgType := vCfg.Type()
	paramTable := tablewriter.NewWriter(os.Stdout)
	for i := 0; i < cfgType.NumField(); i++ {
		if !vDefCfg.Field(i).IsNil() && !vCfg.Field(i).IsNil() && vCfg.Field(i).Elem().Interface() != vDefCfg.Field(i).Elem().Interface() {
			paramTable.Append([]string{cfgType.Field(i).Name, fmt.Sprintf("%v", vCfg.Field(i).Elem())})
		}
	}
	paramTable.SetAlignment(tablewriter.ALIGN_LEFT)
	paramTable.SetCenterSeparator("*")
	paramTable.SetColumnSeparator("|")
	paramTable.Render()

	if *version {
		fmt.Println("Livepeer Node Version: " + core.LivepeerVersion)
		fmt.Printf("Golang runtime version: %s %s\n", runtime.Compiler, runtime.Version())
		fmt.Printf("Architecture: %s\n", runtime.GOARCH)
		fmt.Printf("Operating system: %s\n", runtime.GOOS)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	lc := make(chan struct{})

	go func() {
		starter.StartLivepeer(ctx, cfg)
		lc <- struct{}{}
	}()

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)
	select {
	case sig := <-c:
		glog.Infof("Exiting Livepeer: %v", sig)
		cancel()
		time.Sleep(time.Millisecond * 500) //Give time for other processes to shut down completely
	case <-lc:
	}
}

func parseLivepeerConfig() starter.LivepeerConfig {
	cfg := starter.DefaultLivepeerConfig()

	// Network & Addresses:
	cfg.HttpAddr = flag.String("httpAddr", *cfg.HttpAddr, "Address to bind for HTTP commands")

	cfg.HttpIngest = flag.Bool("httpIngest", *cfg.HttpIngest, "Set to true to enable HTTP ingest")

	// Broadcaster's Selection Algorithm
	cfg.OrchAddr = flag.String("orchAddr", *cfg.OrchAddr, "Comma-separated list of orchestrators to connect to")
	cfg.Region = flag.String("region", *cfg.Region, "Region in which a broadcaster is deployed; used to select the region while using the orchestrator's performance stats")

	// Transcoding:
	cfg.Transcoder = flag.Bool("transcoder", *cfg.Transcoder, "Set to true to be a transcoder")
	cfg.OrchSecret = flag.String("orchSecret", *cfg.OrchSecret, "Shared secret with the orchestrator as a standalone transcoder or path to file")
	cfg.TranscodingOptions = flag.String("transcodingOptions", *cfg.TranscodingOptions, "Transcoding options for broadcast job, or path to json config")
	cfg.MaxAttempts = flag.Int("maxAttempts", *cfg.MaxAttempts, "Maximum transcode attempts")
	cfg.MaxSessions = flag.String("maxSessions", *cfg.MaxSessions, "Maximum number of concurrent transcoding sessions for Orchestrator or 'auto' for dynamic limit, maximum number of RTMP streams for Broadcaster, or maximum capacity for transcoder.")
	cfg.CurrentManifest = flag.Bool("currentManifest", *cfg.CurrentManifest, "Expose the currently active ManifestID as \"/stream/current.m3u8\"")
	cfg.Nvidia = flag.String("nvidia", *cfg.Nvidia, "Comma-separated list of Nvidia GPU device IDs (or \"all\" for all available devices)")
	cfg.Netint = flag.String("netint", *cfg.Netint, "Comma-separated list of NetInt device GUIDs (or \"all\" for all available devices)")
	cfg.TestTranscoder = flag.Bool("testTranscoder", *cfg.TestTranscoder, "Test Nvidia GPU transcoding at startup")

	// Onchain:
	cfg.EthAcctAddr = flag.String("ethAcctAddr", *cfg.EthAcctAddr, "Existing Eth account address. For use when multiple ETH accounts exist in the keystore directory")

	// Metrics & logging:
	cfg.Monitor = flag.Bool("monitor", *cfg.Monitor, "Set to true to send performance metrics")
	cfg.MetricsPerStream = flag.Bool("metricsPerStream", *cfg.MetricsPerStream, "Set to true to group performance metrics per stream")
	cfg.MetricsExposeClientIP = flag.Bool("metricsClientIP", *cfg.MetricsExposeClientIP, "Set to true to expose client's IP in metrics")
	cfg.MetadataQueueUri = flag.String("metadataQueueUri", *cfg.MetadataQueueUri, "URI for message broker to send operation metadata")
	cfg.MetadataAmqpExchange = flag.String("metadataAmqpExchange", *cfg.MetadataAmqpExchange, "Name of AMQP exchange to send operation metadata")
	cfg.MetadataPublishTimeout = flag.Duration("metadataPublishTimeout", *cfg.MetadataPublishTimeout, "Max time to wait in background for publishing operation metadata events")

	return cfg
}

// updateNilsForUnsetFlags changes some cfg fields to nil if they were not explicitly set with flags.
// For some flags, the behavior is different whether the value is default or not set by the user at all.
func updateNilsForUnsetFlags(cfg starter.LivepeerConfig) starter.LivepeerConfig {
	res := cfg

	isFlagSet := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { isFlagSet[f.Name] = true })

	if !isFlagSet["httpIngest"] {
		res.HttpIngest = nil
	}

	return res
}
