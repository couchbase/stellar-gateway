/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package server_v1

import (
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/golang/snappy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CompressHandler struct {
}

func (h CompressHandler) CompressContent(in []byte, datatype memdx.DatatypeFlag) ([]byte, *status.Status) {
	if datatype&memdx.DatatypeFlagCompressed != 0 {
		return in, nil
	}

	compressLen := snappy.MaxEncodedLen(len(in))
	out := make([]byte, compressLen)
	out = snappy.Encode(out, in)
	return out, nil
}

func (h CompressHandler) UncompressContent(in []byte, datatype memdx.DatatypeFlag) ([]byte, *status.Status) {
	if datatype&memdx.DatatypeFlagCompressed == 0 {
		return in, nil
	}

	out := make([]byte, len(in))
	out, err := snappy.Decode(out, in)
	if err != nil {
		return nil, status.New(codes.InvalidArgument, "Compressed content could not be decompressed.")
	}

	return out, nil
}

func (h CompressHandler) MaybeCompressContent(
	in []byte,
	datatype memdx.DatatypeFlag,
	compressMode *kv_v1.CompressionEnabled,
) (bool, []byte, *status.Status) {
	if compressMode != nil {
		switch *compressMode {
		case kv_v1.CompressionEnabled_COMPRESSION_ENABLED_OPTIONAL:
			if datatype&memdx.DatatypeFlagCompressed != 0 {
				return true, in, nil
			} else {
				return false, in, nil
			}
		case kv_v1.CompressionEnabled_COMPRESSION_ENABLED_ALWAYS:
			compressedValue, errSt := h.CompressContent(in, datatype)
			if errSt != nil {
				return false, nil, errSt
			}

			return true, compressedValue, nil
		default:
			return false, nil, status.New(codes.InvalidArgument, "Unexpected compression mode.")
		}
	} else {
		uncompressedValue, errSt := h.UncompressContent(in, datatype)
		if errSt != nil {
			return false, nil, errSt
		}

		return false, uncompressedValue, nil
	}
}
