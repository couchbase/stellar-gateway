package servers

import (
	"fmt"

	"github.com/couchbase/gocbcore/v10/memd"
)

// TODO(brett19): This file is copy-pasted from direct-nebula, it should be shared...

func bytesToHexAsciiString(bytes []byte) string {
	out := ""
	var ascii [16]byte
	n := (len(bytes) + 15) &^ 15
	for i := 0; i < n; i++ {
		// include the line numbering at beginning of every line
		if i%16 == 0 {
			out += fmt.Sprintf("%4d", i)
		}

		// extra space between blocks of 8 bytes
		if i%8 == 0 {
			out += " "
		}

		// if we have bytes left, print the hex
		if i < len(bytes) {
			out += fmt.Sprintf(" %02X", bytes[i])
		} else {
			out += "   "
		}

		// build the ascii
		if i >= len(bytes) {
			ascii[i%16] = ' '
		} else if bytes[i] < 32 || bytes[i] > 126 {
			ascii[i%16] = '.'
		} else {
			ascii[i%16] = bytes[i]
		}

		// at the end of the line, print the newline.
		if i%16 == 15 {
			out += fmt.Sprintf("  %s\n", string(ascii[:]))
		}
	}
	return out
}

func memdMagicToString(magic memd.CmdMagic) string {
	switch magic {
	case memd.CmdMagicReq:
		return "CmdMagicReq"
	case memd.CmdMagicRes:
		return "CmdMagicRes"
	}
	return fmt.Sprintf("CmdMagicUnk(%d)", magic)
}

func memdPacketToString(pak *memd.Packet) string {
	return fmt.Sprintf(
		"memd.Packet{Magic:%x(%s), Command:%x(%s), Datatype:%x, Status:%x(%s), Vbucket:%d, Opaque:%08x, Cas: %08x, CollectionID:%d\nKey:\n%sValue:\n%sExtras:\n%s}",
		pak.Magic,
		memdMagicToString(pak.Magic),
		pak.Command,
		pak.Command.Name(),
		pak.Datatype,
		pak.Status,
		pak.Status.String(),
		pak.Vbucket,
		pak.Opaque,
		pak.Cas,
		pak.CollectionID,
		bytesToHexAsciiString(pak.Key),
		bytesToHexAsciiString(pak.Value),
		bytesToHexAsciiString(pak.Extras))

	/*
		BarrierFrame           *BarrierFrame
		DurabilityLevelFrame   *DurabilityLevelFrame
		DurabilityTimeoutFrame *DurabilityTimeoutFrame
		StreamIDFrame          *StreamIDFrame
		OpenTracingFrame       *OpenTracingFrame
		ServerDurationFrame    *ServerDurationFrame
		UserImpersonationFrame *UserImpersonationFrame
		PreserveExpiryFrame    *PreserveExpiryFrame
		UnsupportedFrames      []UnsupportedFrame
	*/
}

type memdPacketStringer struct {
	Packet *memd.Packet
}

func (p memdPacketStringer) String() string {
	return memdPacketToString(p.Packet)
}
