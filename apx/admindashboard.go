package main

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

type roomsPageData struct {
	ActiveNav string
	Error     string
	Success   string
	ApiKey    string
	Rooms     []ApxRoomInfo
}

type statsPageData struct {
	ActiveNav          string
	ApiKey             string
	RoomCountTotal     int
	RoomCount          int
	TotalConnections   int
	CachedDataPackages int
}

func (rm *RoomManager) handleAdminRooms(w http.ResponseWriter, r *http.Request) {
	rooms, err := rm.store.LoadAllSlim()
	if err != nil {
		renderTemplate(w, "base", roomsPageData{
			ActiveNav: "rooms",
			Error:     "failed to load rooms from store",
			ApiKey:    rm.config.ApiKey,
		}, "templates/base_admin.gohtml", "templates/rooms.gohtml")
		return
	}

	result := make([]ApxRoomInfo, 0, len(rooms))
	for _, room := range rooms {
		info := ApxRoomInfo{
			LobbyRoomId:       room.LobbyRoomId,
			NormalId:          room.NormalId,
			ReducedId:         room.ReducedId,
			Disabled:          room.Disabled,
			PerSlotPasswords:  room.PerSlotPasswords,
			DeathlinkDisabled: room.DeathlinkDisabled,
			ReducedAccess:     room.ReducedAccess,
			CreatedAt:         room.CreatedAt.Format(time.RFC3339),
		}
		if liveRoom, ok := rm.registry.Get(room.LobbyRoomId); ok {
			info.Disabled = false
			info.LastActivity = time.Unix(liveRoom.lastActivity.Load(), 0).UTC().Format(time.RFC3339)
		}
		result = append(result, info)
	}

	renderTemplate(w, "base", roomsPageData{
		ActiveNav: "rooms",
		Rooms:     result,
		ApiKey:    rm.config.ApiKey,
	}, "templates/base_admin.gohtml", "templates/rooms.gohtml")
}

func (rm *RoomManager) handleAdminStats(w http.ResponseWriter, r *http.Request) {
	totalRooms, _ := rm.store.CountRooms()
	totalConnections := rm.registry.CountConnections()
	cachedDataPackages := rm.registry.datapackages.Count()

	renderTemplate(w, "base", statsPageData{
		ActiveNav:          "stats",
		TotalConnections:   totalConnections,
		RoomCountTotal:     totalRooms,
		RoomCount:          rm.registry.Count(),
		CachedDataPackages: cachedDataPackages,
		ApiKey:             rm.config.ApiKey,
	}, "templates/base_admin.gohtml", "templates/stats.gohtml")
}

// renderTemplate is a small helper to keep handlers tidy
func renderTemplate(w http.ResponseWriter, name string, data any, files ...string) {
	tmpl, err := template.ParseFiles(files...)
	if err != nil {
		http.Error(w, fmt.Sprintf("template error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, name, data); err != nil {
		log.Printf("template execute error: %v", err)
	}
}

func (rm *RoomManager) registerAdminRoutes(r *mux.Router) {
	admin := r.PathPrefix("/admin").Subrouter()
	admin.Use(rm.adminAuthMiddleware)
	admin.HandleFunc("/rooms", rm.handleAdminRooms).Methods(http.MethodGet)
	admin.HandleFunc("/stats", rm.handleAdminStats).Methods(http.MethodGet)
}
func (rm *RoomManager) adminAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || rm.config.ApiKey == "" || user != "admin" || pass != rm.config.ApiKey {
			w.Header().Set("WWW-Authenticate", `Basic realm="APX Admin"`)
			w.Header().Set("Content-Type", "text/plain")
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
