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

			log.Info(
				"calling registered hooks context",
				zap.String("hooks-id", hooksID),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.String("remote_addr", r.RemoteAddr),
			)
			hooksContext.HandleHTTPRequest(w, r, next)
		})
	}
}
