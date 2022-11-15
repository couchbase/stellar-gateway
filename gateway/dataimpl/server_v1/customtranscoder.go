package server_v1

import (
	"errors"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/stellar-nebula/genproto/kv_v1"
)

type psTranscodeData struct {
	ContentBytes []byte
	ContentType  kv_v1.DocumentContentType
}

type customTranscoder struct {
}

// Decode applies legacy transcoding behaviour to decode into a Go type.
func (t customTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	valueType, compression := gocbcore.DecodeCommonFlags(flags)

	// Make sure compression is disabled
	if compression != gocbcore.NoCompression {
		return errors.New("unexpected value compression")
	}

	typedOut, ok := out.(*psTranscodeData)
	if !ok {
		return errors.New("expected psTranscodeData pointer as output structure")
	}

	// Normal types of decoding
	if valueType == gocbcore.BinaryType {
		typedOut.ContentBytes = bytes
		typedOut.ContentType = kv_v1.DocumentContentType_BINARY
		return nil
	} else if valueType == gocbcore.StringType {
		// we don't support string data types in this case and instead just
		// handle them as raw binary data instead...
		// TODO(brett19): Decide how to handle string transcoding better.
		typedOut.ContentBytes = bytes
		typedOut.ContentType = kv_v1.DocumentContentType_BINARY
		return nil
	} else if valueType == gocbcore.JSONType {
		typedOut.ContentBytes = bytes
		typedOut.ContentType = kv_v1.DocumentContentType_JSON
		return nil
	}

	return errors.New("unexpected expectedFlags value")
}

// Encode applies legacy transcoding behavior to encode a Go type.
func (t customTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	typedValue, ok := value.(psTranscodeData)
	if !ok {
		return nil, 0, errors.New("expected psTranscodeData pointer as input structure")
	}

	if typedValue.ContentType == kv_v1.DocumentContentType_BINARY {
		return typedValue.ContentBytes, gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression), nil
	} else if typedValue.ContentType == kv_v1.DocumentContentType_JSON {
		return typedValue.ContentBytes, gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression), nil
	}

	return nil, 0, errors.New("unexpected document content type when marshalling")
}
