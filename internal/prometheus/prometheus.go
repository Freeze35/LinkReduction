package initprometheus

import "github.com/prometheus/client_golang/prometheus"

// prometheusMetrics - структура для хранения метрик Prometheus
type PrometheusMetrics struct {
	CreateShortLinkTotal   *prometheus.CounterVec
	DbInsertTotal          *prometheus.CounterVec
	RedirectTotal          *prometheus.CounterVec
	CreateShortLinkLatency *prometheus.HistogramVec
}

// initPrometheus инициализирует метрики Prometheus
func InitPrometheus() *PrometheusMetrics {

	/*shortener_create_short_link_total:
	  	Общее количество запросов на создание коротких ссылок через эндпоинт /createShortLink, разделённых по статусу (success или error).
	  shortener_db_insert_total:
	  	Общее количество операций вставки коротких ссылок в базу данных PostgreSQL, выполняемых функцией ConsumeClaim через Kafka, разделённых по статусу (success или error).
	  shortener_redirect_total:
	  	Общее количество запросов на редирект по коротким ссылкам через эндпоинт /:key, разделённых по статусу (success или error).
	  shortener_create_short_link_latency_seconds:
	  	Латентность (время выполнения) запросов на создание коротких ссылок через эндпоинт /createShortLink, измеряемая в секундах, разделённая по статусу (success).*/

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
	}

	// Регистрация метрик в Prometheus
	prometheus.MustRegister(metrics.CreateShortLinkTotal)
	prometheus.MustRegister(metrics.DbInsertTotal)
	prometheus.MustRegister(metrics.RedirectTotal)
	prometheus.MustRegister(metrics.CreateShortLinkLatency)

	return metrics
}
