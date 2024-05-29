package hystrix

import (
	"sync"
	"time"
)

var (
	// DefaultTimeout is how long to wait for command to complete, in milliseconds
	DefaultTimeout = 1000
	// DefaultMaxConcurrent is how many commands of the same type can run at the same time
	DefaultMaxConcurrent = 10
	// DefaultVolumeThreshold is the minimum number of requests needed before a circuit can be tripped due to health
	DefaultVolumeThreshold = 20
	// DefaultSleepWindow is how long, in milliseconds, to wait after a circuit opens before testing for recovery
	DefaultSleepWindow = 5000
	// DefaultErrorPercentThreshold causes circuits to open once the rolling measure of errors exceeds this percent of requests
	DefaultErrorPercentThreshold = 50
	// DefaultLogger is the default logger that will be used in the Hystrix package. By default prints nothing.
	DefaultLogger = NoopLogger{}
)

type Settings struct {
	Timeout                time.Duration
	MaxConcurrentRequests  int
	RequestVolumeThreshold uint64
	SleepWindow            time.Duration
	ErrorPercentThreshold  int
}

// CommandConfig is used to tune circuit settings at runtime
type CommandConfig struct {
	// 等待command完成的时间，单位毫秒，默认1000
	Timeout int `json:"timeout"`
	// 同一个command支持的的并发量，默认10
	MaxConcurrentRequests int `json:"max_concurrent_requests"`
	// 触发开启熔断的最小请求数，默认20
	RequestVolumeThreshold int `json:"request_volume_threshold"`
	// 熔断开启后，多长时间后开始检测是否恢复，单位毫秒，默认5000
	SleepWindow int `json:"sleep_window"`
	// 触发熔断的错误百分比，当失败比率超过该值时，开启熔断， 默认50(%)
	ErrorPercentThreshold int `json:"error_percent_threshold"`
}

var circuitSettings map[string]*Settings
var settingsMutex *sync.RWMutex
var log logger

func init() {
	circuitSettings = make(map[string]*Settings)
	settingsMutex = &sync.RWMutex{}
	log = DefaultLogger
}

// Configure applies settings for a set of circuits
// 添加批量配置
func Configure(cmds map[string]CommandConfig) {
	for k, v := range cmds {
		ConfigureCommand(k, v)
	}
}

// ConfigureCommand applies settings for a circuit
// 添加单个配置
func ConfigureCommand(name string, config CommandConfig) {
	settingsMutex.Lock()
	defer settingsMutex.Unlock()

	timeout := DefaultTimeout
	if config.Timeout != 0 {
		timeout = config.Timeout
	}

	max := DefaultMaxConcurrent
	if config.MaxConcurrentRequests != 0 {
		max = config.MaxConcurrentRequests
	}

	volume := DefaultVolumeThreshold
	if config.RequestVolumeThreshold != 0 {
		volume = config.RequestVolumeThreshold
	}

	sleep := DefaultSleepWindow
	if config.SleepWindow != 0 {
		sleep = config.SleepWindow
	}

	errorPercent := DefaultErrorPercentThreshold
	if config.ErrorPercentThreshold != 0 {
		errorPercent = config.ErrorPercentThreshold
	}

	circuitSettings[name] = &Settings{
		Timeout:                time.Duration(timeout) * time.Millisecond,
		MaxConcurrentRequests:  max,
		RequestVolumeThreshold: uint64(volume),
		SleepWindow:            time.Duration(sleep) * time.Millisecond,
		ErrorPercentThreshold:  errorPercent,
	}
}

func getSettings(name string) *Settings {
	settingsMutex.RLock()
	s, exists := circuitSettings[name]
	settingsMutex.RUnlock()

	if !exists {
		ConfigureCommand(name, CommandConfig{})
		s = getSettings(name)
	}

	return s
}

func GetCircuitSettings() map[string]*Settings {
	copy := make(map[string]*Settings)

	settingsMutex.RLock()
	for key, val := range circuitSettings {
		copy[key] = val
	}
	settingsMutex.RUnlock()

	return copy
}

// SetLogger configures the logger that will be used. This only applies to the hystrix package.
func SetLogger(l logger) {
	log = l
}
