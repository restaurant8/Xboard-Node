package report

type TrafficStat struct {
	Category   string  `json:"category"`
	MainDomain *string `json:"main_domain,omitempty"`
	U          int64   `json:"u"`
	D          int64   `json:"d"`
	RecordAt   int64   `json:"record_at"`
}
