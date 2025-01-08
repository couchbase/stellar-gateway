package cbclientnames

import "strings"

func FromUserAgent(userAgent string) string {
	clientName, _, _ := strings.Cut(userAgent, " ")
	if len(clientName) > 32 {
		clientName = clientName[:32]
	}
	return clientName
}
