package net

import (
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/m3u8"
)

type RemoteTranscoderInfo struct {
	Address         string
	Capacity        int
	Load            int
	EthereumAddress ethcommon.Address
}

type NodeStatus struct {
	Manifests                   map[string]*m3u8.MasterPlaylist
	OrchestratorPool            []string
	Version                     string
	GolangRuntimeVersion        string
	GOArch                      string
	GOOS                        string
	RegisteredTranscodersNumber int
	RegisteredTranscoders       []*RemoteTranscoderInfo
	LocalTranscoding            bool // Indicates orchestrator that is also transcoder
	// xxx add transcoder's version here
}
