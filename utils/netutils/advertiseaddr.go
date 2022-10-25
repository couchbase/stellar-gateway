package netutils

func GetAdvertiseAddress(bindAddress string) (string, error) {
	// if no advertise port was explicitly provided, use the bind address if it
	// was not an inaddr_any bind.
	if !IsInAddrAny(bindAddress) {
		return bindAddress, nil
	}

	// if the bind address was also not provided, try to get it from the system.
	outboundIP, err := GetOutboundIP()
	if err != nil {
		return "", err
	}

	return outboundIP.String(), nil
}
