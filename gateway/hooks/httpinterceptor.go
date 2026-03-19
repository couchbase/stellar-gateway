package hooks

import (
	"net/http"

	"go.uber.org/zap"
)

func makeHTTPMiddleware(manager *HooksManager, log *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hooksID := r.Header.Get("X-Hooks-ID")
			if hooksID == "" {
				next.ServeHTTP(w, r)
				return
			}

			hooksContext := manager.GetHooksContext(hooksID)
			if hooksContext == nil {
				next.ServeHTTP(w, r)
				return
			}

			barrierID := r.Header.Get("X-Barrier-ID")
			if barrierID == "" {
				next.ServeHTTP(w, r)
				return
			}

			log.Info("http request waiting on barrier",
				zap.String("hooks-id", hooksID),
				zap.String("barrier-id", barrierID))

			barrier := hooksContext.GetBarrier(barrierID)
			barrier.Wait(r.Context(), hooksID+":"+barrierID, nil)

			log.Info("http request done waiting on barrier",
				zap.String("hooks-id", hooksID),
				zap.String("barrier-id", barrierID))

			next.ServeHTTP(w, r)
		})
	}
}
