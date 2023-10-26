package server_v1

import (
	"testing"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
)

func TestCompressBasic(t *testing.T) {
	compressContent := func(t *testing.T, in []byte) []byte {
		out := make([]byte, snappy.MaxEncodedLen(len(in)))
		out = snappy.Encode(out, in)
		return out
	}

	uncompressContent := func(t *testing.T, in []byte) []byte {
		out := make([]byte, len(in))
		out, err := snappy.Decode(out, in)
		require.NoError(t, err)
		return out
	}

	var testContent = []byte("hello world")
	var testCompressed = compressContent(t, testContent)

	t.Run("NoDatatype", func(t *testing.T) {
		t.Run("NoCompression", func(t *testing.T) {
			isCompressed, out, err :=
				CompressHandler{}.MaybeCompressContent(testContent, 0, nil)
			require.NoError(t, err.Err())
			require.Equal(t, false, isCompressed)
			require.Equal(t, testContent, out)
		})

		t.Run("OptionalCompression", func(t *testing.T) {
			isCompressed, out, err :=
				CompressHandler{}.MaybeCompressContent(testContent, 0,
					kv_v1.CompressionEnabled_COMPRESSION_ENABLED_OPTIONAL.Enum())
			require.NoError(t, err.Err())
			require.Equal(t, false, isCompressed)
			require.Equal(t, testContent, out)
		})

		t.Run("AlwaysCompression", func(t *testing.T) {
			isCompressed, out, err :=
				CompressHandler{}.MaybeCompressContent(testContent, 0,
					kv_v1.CompressionEnabled_COMPRESSION_ENABLED_ALWAYS.Enum())
			require.NoError(t, err.Err())
			require.Equal(t, true, isCompressed)
			require.Equal(t, testContent, uncompressContent(t, out))
		})
	})

	t.Run("CompressedDatatype", func(t *testing.T) {
		t.Run("NoCompression", func(t *testing.T) {
			isCompressed, out, err :=
				CompressHandler{}.MaybeCompressContent(testCompressed, memdx.DatatypeFlagCompressed, nil)
			require.NoError(t, err.Err())
			require.Equal(t, false, isCompressed)
			require.Equal(t, testContent, out)
		})

		t.Run("OptionalCompression", func(t *testing.T) {
			isCompressed, out, err :=
				CompressHandler{}.MaybeCompressContent(testCompressed, memdx.DatatypeFlagCompressed,
					kv_v1.CompressionEnabled_COMPRESSION_ENABLED_OPTIONAL.Enum())
			require.NoError(t, err.Err())
			require.Equal(t, true, isCompressed)
			require.Equal(t, testContent, uncompressContent(t, out))
		})

		t.Run("AlwaysCompression", func(t *testing.T) {
			isCompressed, out, err :=
				CompressHandler{}.MaybeCompressContent(testCompressed, memdx.DatatypeFlagCompressed,
					kv_v1.CompressionEnabled_COMPRESSION_ENABLED_ALWAYS.Enum())
			require.NoError(t, err.Err())
			require.Equal(t, true, isCompressed)
			require.Equal(t, testContent, uncompressContent(t, out))
		})
	})
}
