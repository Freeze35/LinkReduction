package initprometheus

import "github.com/prometheus/client_golang/prometheus"

// PrometheusMetrics - структура для хранения метрик Prometheus
type PrometheusMetrics struct {
	CreateShortLinkTotal   *prometheus.CounterVec
	DbInsertTotal          *prometheus.CounterVec
	RedirectTotal          *prometheus.CounterVec
	CreateShortLinkLatency *prometheus.HistogramVec
	CleanupTotal           *prometheus.CounterVec // Новая метрика для удалений
}

// InitPrometheus инициализирует метрики Prometheus
func InitPrometheus() *PrometheusMetrics {
	metrics := &PrometheusMetrics{
		CreateShortLinkTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "shortener_create_short_link_total",
				Help: "Total number of short link creation requests",
			},
			[]string{"status"},
		),
		DbInsertTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "shortener_db_insert_total",
				Help: "Total number of database insert operations",
			},
			[]string{"status"},
		),
		RedirectTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "shortener_redirect_total",
				Help: "Total number of redirect requests",
			},
			[]string{"status"},
		),
		CreateShortLinkLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "shortener_create_short_link_latency_seconds",
				Help:    "Latency of short link creation requests in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"status"},
		),
		CleanupTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "shortener_cleanup_total",
				Help: "Total number of old links removed from the database",
			},
			[]string{"status"},
		),
	}

	// Регистрация метрик в Prometheus
	prometheus.MustRegister(metrics.CreateShortLinkTotal)
	prometheus.MustRegister(metrics.DbInsertTotal)
	prometheus.MustRegister(metrics.RedirectTotal)
	prometheus.MustRegister(metrics.CreateShortLinkLatency)
	prometheus.MustRegister(metrics.CleanupTotal)

	return metrics
}
