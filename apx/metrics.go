package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	incomingPackets      prometheus.CounterVec
	bouncePackets        prometheus.CounterVec
	bounceResultPackets  prometheus.CounterVec
	connectedClients     prometheus.GaugeVec
	connectedSlots       prometheus.GaugeVec
	bytesReceived        prometheus.CounterVec
	bytesSent            prometheus.CounterVec
	datapackageBytesSent prometheus.CounterVec
	droppedLogs          prometheus.CounterVec
	totalRooms           prometheus.Gauge
	activeRooms          prometheus.Gauge
	totalDatapackages    prometheus.Gauge
	cachedDatapackages   prometheus.Gauge
}

func initMetrics() (*prometheus.Registry, *metrics) {
	reg := prometheus.NewRegistry()

	m := &metrics{
		incomingPackets: *promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "apx_incoming_packets_total",
				Help: "Total number of incoming packets per slot and command type",
			},
			[]string{"room", "slot", "game", "cmd"},
		),
		bouncePackets: *promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "apx_bounce_packets_total",
				Help: "Total number of bounce packets per slot and tag",
			},
			[]string{"room", "slot", "game", "tag"},
		),
		bounceResultPackets: *promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "apx_bounce_packets_resulting_total",
				Help: "Total number of bounce packets sent on to other clients per slot",
			},
			[]string{"room", "slot", "game"},
		),
		connectedClients: *promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "apx_connections",
				Help: "Number of connections to room",
			},
			[]string{"room"},
		),
		connectedSlots: *promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "apx_slots_connected",
				Help: "Number of slots connected to room",
			},
			[]string{"room"},
		),
		bytesReceived: *promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "apx_bytes_received_total",
				Help: "Total number of bytes received per slot",
			},
			[]string{"room", "slot", "game"},
		),
		bytesSent: *promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "apx_bytes_sent_total",
				Help: "Total number of bytes sent per slot",
			},
			[]string{"room", "slot", "game"},
		),
		datapackageBytesSent: *promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "apx_datapackage_bytes_sent_total",
				Help: "Total number of bytes sent for datapackage requests",
			},
			[]string{"room"},
		),
		droppedLogs: *promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "apx_dropped_logs_total",
				Help: "Total number of log entries dropped due to full channel",
			},
			[]string{"source"},
		),
		totalRooms: promauto.With(reg).NewGauge(
			prometheus.GaugeOpts{
				Name: "apx_total_rooms",
				Help: "Total number of total rooms including idle",
			},
		),
		activeRooms: promauto.With(reg).NewGauge(
			prometheus.GaugeOpts{
				Name: "apx_active_rooms",
				Help: "Total number of active rooms",
			},
		),
		totalDatapackages: promauto.With(reg).NewGauge(
			prometheus.GaugeOpts{
				Name: "apx_total_datapackages",
				Help: "Total number of saved datapackages",
			},
		),
		cachedDatapackages: promauto.With(reg).NewGauge(
			prometheus.GaugeOpts{
				Name: "apx_cached_datapackages",
				Help: "Total number of cached datapackages",
			},
		),
	}

	return reg, m
}
