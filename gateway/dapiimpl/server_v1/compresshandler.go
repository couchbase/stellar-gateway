/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package server_v1

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

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
	acceptedEncoding *string,
) (dataapiv1.DocumentEncoding, []byte, *Status) {
	identityQ := 0.001
	snappyQ := 0.0

	if acceptedEncoding != nil {
		encodings := strings.Split(*acceptedEncoding, ",")

		for encodingIdx, encoding := range encodings {
			encodingParts := strings.Split(encoding, ";")
			if len(encodingParts) >= 3 {
				return dataapiv1.DocumentEncoding(""), nil, &Status{
					StatusCode: http.StatusBadRequest,
					Code:       dataapiv1.ErrorCodeInvalidArgument,
					Message: fmt.Sprintf("Invalid Accept-Encoding format at index %d",
						encodingIdx),
				}
			}

			encodingName := strings.TrimSpace(encodingParts[0])
			encodingQ := 1.0

			if len(encodingParts) >= 2 {
				qValue := strings.TrimSpace(encodingParts[1])
				if !strings.HasPrefix(qValue, "q=") {
					return dataapiv1.DocumentEncoding(""), nil, &Status{
						StatusCode: http.StatusBadRequest,
						Code:       dataapiv1.ErrorCodeInvalidArgument,
						Message: fmt.Sprintf("Invalid Accept-Encoding format at index %d, expected q=",
							encodingIdx),
					}
				}

				parsedQ, err := strconv.ParseFloat(qValue[2:], 64)
				if err != nil {
					return dataapiv1.DocumentEncoding(""), nil, &Status{
						StatusCode: http.StatusBadRequest,
						Code:       dataapiv1.ErrorCodeInvalidArgument,
						Message: fmt.Sprintf("Invalid Accept-Encoding format at index %d, expected floating-point q",
							encodingIdx),
					}
				}

				encodingQ = parsedQ
			}

			if encodingName == "identity" {
				identityQ = encodingQ
			} else if encodingName == "snappy" {
				snappyQ = encodingQ
			} else {
				// we intentionally ignore unknown encodings
				continue
			}
		}
	}

	if snappyQ > identityQ {
		if datatype&memdx.DatatypeFlagCompressed != 0 {
			return dataapiv1.DocumentEncodingSnappy, in, nil
		} else {
			if identityQ > 0 {
				return dataapiv1.DocumentEncoding(""), in, nil
			} else {
				compressedValue, errSt := h.CompressContent(in, datatype)
				if errSt != nil {
					return dataapiv1.DocumentEncoding(""), nil, errSt
				}

				return dataapiv1.DocumentEncodingSnappy, compressedValue, nil
			}
		}
	} else {
		uncompressedValue, errSt := h.UncompressContent(in, datatype)
		if errSt != nil {
			return dataapiv1.DocumentEncoding(""), nil, errSt
		}

		return dataapiv1.DocumentEncoding(""), uncompressedValue, nil
	}
}
