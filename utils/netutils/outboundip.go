package netutils

import (
	"net"
)

func GetOutboundIP() (net.IP, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return nil, err
	}

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	_ = conn.Close()

	return localAddr.IP, nil
}
