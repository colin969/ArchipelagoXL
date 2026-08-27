package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"strconv"
	"strings"
)

type SubdomainRouter struct {
	registry *RoomRegistry
	tlsCfg   *tls.Config
}

func newSubdomainRouter(registry *RoomRegistry, tlsCfg *tls.Config) *SubdomainRouter {
	return &SubdomainRouter{registry: registry, tlsCfg: tlsCfg}
}

func (sr *SubdomainRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host := r.Host

	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	// ap12345.domain -> ap12345
	subdomain, _, _ := strings.Cut(host, ".")

	// ap12345 -> 12345
	idStr, found := strings.CutPrefix(subdomain, "ap")
	if !found {
		http.Error(w, "invalid subdomain", http.StatusBadRequest)
		return
	}

	id, err := strconv.Atoi(idStr)
	if err != nil || id < 10000 || id > 65535 {
		http.Error(w, "invalid id in subdomain", http.StatusBadRequest)
		return
	}

	handler, ok := sr.registry.GetHandlerById(id)
	if !ok {
		http.Error(w, "room not found", http.StatusNotFound)
		return
	}

	handler.ServeHTTP(w, r)
}
