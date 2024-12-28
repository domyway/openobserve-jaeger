package config

type Config struct {
	OpenObserve OpenObserveConfig `yaml:"openobserve"`
}

// OpenObserveConfig holds the configuration for OpenObserve
type OpenObserveConfig struct {
	Addr                          string `yaml:"addr"`
	Auth                          string `yaml:"auth"`
	DefaultTraceDetailSearchRange int    `yaml:"default_trace_detail_search_range_time"`
	DefaultQueryUIMaxSearchRange  int    `yaml:"default_queryui_max_search_range_time"`
	DefaultServiceNameSize        int64  `yaml:"default_servicename_size"`
	DefaultOperationNameSize      int64  `yaml:"default_operationname_size"`
	DefaultSpanSize               int    `yaml:"default_span_size"`
}

var Cfg Config
