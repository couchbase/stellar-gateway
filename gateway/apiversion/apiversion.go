package apiversion

type ApiVersion uint64

const Latest ApiVersion = VectorSearch

const (
	QueryDurabilityLevel ApiVersion = 20240510
	CollectionNoExpiry   ApiVersion = 20240514
	VectorSearch         ApiVersion = 20240514
)
