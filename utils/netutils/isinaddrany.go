package netutils

func IsInAddrAny(addr string) bool {
	return addr == "" || addr == "::/0" || addr == "0.0.0.0"
}
