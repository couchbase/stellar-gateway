package server_v1

import (
	"net/http"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/stellar-gateway/dataapiv1"
	"github.com/golang/snappy"
)

type CompressHandler struct {
}

func (h CompressHandler) CompressContent(in []byte, datatype memdx.DatatypeFlag) ([]byte, *Status) {
	if datatype&memdx.DatatypeFlagCompressed != 0 {
		return in, nil
	}

	compressLen := snappy.MaxEncodedLen(len(in))
	out := make([]byte, compressLen)
	out = snappy.Encode(out, in)
	return out, nil
}

func (h CompressHandler) UncompressContent(in []byte, datatype memdx.DatatypeFlag) ([]byte, *Status) {
	if datatype&memdx.DatatypeFlagCompressed == 0 {
		return in, nil
	}

	out := make([]byte, len(in))
	out, err := snappy.Decode(out, in)
	if err != nil {
		return nil, &Status{
			StatusCode: http.StatusInternalServerError,
			Message:    "Compressed content could not be decompressed.",
		}
	}

	return out, nil
}

func (h CompressHandler) MaybeCompressContent(
	in []byte,
	datatype memdx.DatatypeFlag,
	acceptedEncoding *[]dataapiv1.DocumentEncoding,
) (dataapiv1.DocumentEncoding, []byte, *Status) {
	canAcceptSnappy := true
	if acceptedEncoding != nil {
		for _, encoding := range *acceptedEncoding {
			if encoding == dataapiv1.DocumentEncodingSnappy {
				canAcceptSnappy = true
			}
		}
	}

	if canAcceptSnappy {
		if datatype&memdx.DatatypeFlagCompressed != 0 {
			return dataapiv1.DocumentEncodingSnappy, in, nil
		} else {
			return dataapiv1.DocumentEncoding(""), in, nil
		}
	} else {
		uncompressedValue, errSt := h.UncompressContent(in, datatype)
		if errSt != nil {
			return dataapiv1.DocumentEncoding(""), nil, errSt
		}

		return dataapiv1.DocumentEncoding(""), uncompressedValue, nil
	}
}
