package apiversion

type ApiVersion uint64

const Latest ApiVersion = CollectionNoExpiry

const (
	QueryDurabilityLevel ApiVersion = 20240510
	CollectionNoExpiry   ApiVersion = 20240514
)
