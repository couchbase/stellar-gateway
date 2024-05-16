package dapiimpl

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/couchbase/stellar-gateway/gateway/dapiimpl/server_v1"
)

func StatusErrorHttpHandler(w http.ResponseWriter, r *http.Request, err error) {
	var errSt *server_v1.StatusError
	if errors.As(err, &errSt) {
		errBytes, _ := json.Marshal(errSt.S)
		w.WriteHeader(errSt.S.StatusCode)
		_, _ = w.Write(errBytes)
		return
	}

	http.Error(w, err.Error(), http.StatusInternalServerError)
}
