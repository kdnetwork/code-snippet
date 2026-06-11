package utils

import (
	"runtime"
	"runtime/debug"
)

type DebugInfo struct {
	GoVersion string `json:"go_version"`
	MainPath  string `json:"path"`
	Version   string `json:"version"`

	// vcs
	CommitHash string `json:"commit_hash"`
	CommitTime string `json:"commit_time"`
	Modified   bool   `json:"modified"`

	OS   string `json:"os"`
	Arch string `json:"arch"`
	CGO  bool   `json:"cgo"`
}

func VersionInfo() *DebugInfo {
	info := &DebugInfo{
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
	}
	buildInfo, ok := debug.ReadBuildInfo()
	if ok {
		info.MainPath = buildInfo.Main.Path
		info.Version = buildInfo.Main.Version

		kv := make(map[string]string)
		for _, s := range buildInfo.Settings {
			kv[s.Key] = s.Value
		}

		info.CommitHash = kv["vcs.revision"]
		info.Modified = kv["vcs.modified"] == "true"
		info.CommitTime = kv["vcs.time"]
		info.CGO = kv["CGO_ENABLED"] == "1"
	}

	return info
}
