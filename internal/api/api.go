package api

import (
	"encoding/json"
	"net/http"

	"github.com/VictoriaMetrics/metrics"
	"github.com/cosmo-local-credit/eth-tracker/internal/pub"
	"github.com/cosmo-local-credit/eth-tracker/internal/stats"
	"github.com/uptrace/bunrouter"
)

func New(statsProvider *stats.Stats, pub pub.Pub, enablePprof bool) *bunrouter.Router {
	router := bunrouter.New()

	router.GET("/metrics", metricsHandler())
	router.GET("/health", healthHandler(statsProvider, pub))
	router.GET("/stats", statsHandler(statsProvider))

	if enablePprof {
		pprofHandler := bunrouter.HTTPHandler(http.DefaultServeMux)
		router.GET("/debug/pprof/*path", pprofHandler)
		router.POST("/debug/pprof/*path", pprofHandler)
	}

	return router
}

func metricsHandler() bunrouter.HandlerFunc {
	return func(w http.ResponseWriter, _ bunrouter.Request) error {
		metrics.WritePrometheus(w, true)
		return nil
	}
}

func healthHandler(s *stats.Stats, p pub.Pub) bunrouter.HandlerFunc {
	return func(w http.ResponseWriter, _ bunrouter.Request) error {
		healthy := true
		checks := map[string]bool{
			"nats": p.Healthy(),
		}

		latestBlock := s.GetLatestBlock()
		checks["syncer"] = latestBlock > 0

		for _, v := range checks {
			if !v {
				healthy = false
				break
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if !healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(checks)
		return nil
	}
}

func statsHandler(s *stats.Stats) bunrouter.HandlerFunc {
	return func(w http.ResponseWriter, r bunrouter.Request) error {
		w.Header().Set("Content-Type", "application/json")
		resp, err := s.APIStatsResponse(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return err
		}
		json.NewEncoder(w).Encode(resp)
		return nil
	}
}
