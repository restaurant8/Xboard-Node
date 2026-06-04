package report

type TrafficStat struct {
	UserID          int     `json:"user_id,omitempty"`
	SourceIP        string  `json:"source_ip,omitempty"`
	DestinationIP   string  `json:"destination_ip,omitempty"`
	Destination     string  `json:"destination,omitempty"`
	DestinationPort uint16  `json:"destination_port,omitempty"`
	Network         string  `json:"network,omitempty"`
	Category        string  `json:"category"`
	MainDomain      *string `json:"main_domain,omitempty"`
	U               int64   `json:"u"`
	D               int64   `json:"d"`
	RecordAt        int64   `json:"record_at"`
}
