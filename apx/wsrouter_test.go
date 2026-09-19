package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Helpers

func registryWithIDs(ids ...int) *RoomRegistry {
	r := &RoomRegistry{
		idToHandler: make(map[int]*apxHandler),
	}
	for _, id := range ids {
		r.idToHandler[id] = &apxHandler{
			id:      id,
			reduced: false,
		}
	}
	return r
}

func makeRequest(host, path string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Host = host
	return req
}

func serve(sr *SubdomainRouter, host string) int {
	w := httptest.NewRecorder()
	sr.ServeHTTP(w, makeRequest(host, "/"))
	return w.Code
}

// Tests

func TestSubdomainRouter(t *testing.T) {
	cases := []struct {
		name     string
		registry *RoomRegistry
		host     string
		want     int
	}{
		// Valid routing
		{"valid ID routes to handler", registryWithIDs(12345), "ap12345.example.com", http.StatusOK},
		{"min boundary ID", registryWithIDs(10000), "ap10000.example.com", http.StatusOK},
		{"max boundary ID", registryWithIDs(65535), "ap65535.example.com", http.StatusOK},
		{"host with port", registryWithIDs(12345), "ap12345.example.com:443", http.StatusOK},

		// Invalid subdomain prefix
		{"missing ap prefix", registryWithIDs(12345), "12345.example.com", http.StatusBadRequest},
		{"wrong prefix", registryWithIDs(12345), "xy12345.example.com", http.StatusBadRequest},

		// Invalid ID values
		{"non-numeric ID", registryWithIDs(12345), "apabc.example.com", http.StatusBadRequest},
		{"ID too low", registryWithIDs(12345), "ap9999.example.com", http.StatusBadRequest},
		{"ID too high", registryWithIDs(12345), "ap65536.example.com", http.StatusBadRequest},
		{"ID zero", registryWithIDs(12345), "ap0.example.com", http.StatusBadRequest},

		// Room not found
		{"room not found", registryWithIDs(), "ap12345.example.com", http.StatusNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sr := newSubdomainRouter(tc.registry, nil)
			if code := serve(sr, tc.host); code != tc.want {
				t.Errorf("got %d, want %d", code, tc.want)
			}
		})
	}
}
