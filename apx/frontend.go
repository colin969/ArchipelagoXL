package main

import (
	"apx/multidata"
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

type uploadPageData struct {
	ActiveNav  string
	Error      string
	Success    string
	ApiKey     string
	FormValues uploadRoomRequest
}

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

func (rm *RoomManager) handleAdminUpload(w http.ResponseWriter, r *http.Request) {
	data := uploadPageData{
		ActiveNav: "upload",
		ApiKey:    rm.config.ApiKey,
	}

	if r.Method == http.MethodPost {
		if err := r.ParseMultipartForm(100 << 20); err != nil {
			data.Error = "failed to parse form"
			renderTemplate(w, "base", data, "templates/base.gohtml", "templates/upload.gohtml")
			return
		}

		uploadReq := uploadRoomRequestFromForm(r)
		if uploadReq.LobbyRoomId == "" {
			uploadReq.LobbyRoomId = randomId(24)
		}

		if !isValidLobbyRoomId(uploadReq.LobbyRoomId) {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "lobby room id must be alphanumeric"})
			return
		}

		data.FormValues = uploadReq

		file, header, err := r.FormFile("file")
		if err != nil {
			data.Error = "no file provided"
			renderTemplate(w, "base", data, "templates/base.gohtml", "templates/upload.gohtml")
			return
		}
		defer file.Close()

		fileBytes, err := io.ReadAll(file)
		if err != nil {
			data.Error = "failed to read file"
			renderTemplate(w, "base", data, "templates/base.gohtml", "templates/upload.gohtml")
			return
		}

		if strings.HasSuffix(strings.ToLower(header.Filename), ".zip") {
			zr, err := zip.NewReader(bytes.NewReader(fileBytes), int64(len(fileBytes)))
			if err != nil {
				data.Error = fmt.Sprintf("invalid zip: %v", err)
				renderTemplate(w, "base", data, "templates/base.gohtml", "templates/upload.gohtml")
				return
			}
			fileBytes, err = multidata.ReadArchipelagoFromZip(zr)
			if err != nil {
				data.Error = fmt.Sprintf("reading zip: %v", err)
				renderTemplate(w, "base", data, "templates/base.gohtml", "templates/upload.gohtml")
				return
			}
		}

		md, err := multidata.ParseMultidata(fileBytes)
		if err != nil {
			data.Error = fmt.Sprintf("invalid multidata: %v", err)
			renderTemplate(w, "base", data, "templates/base.gohtml", "templates/upload.gohtml")
			return
		}

		lobbyRoomId := uploadReq.LobbyRoomId

		_, err = rm.startRoomFromMultiData(lobbyRoomId, md, nil, nil,
			uploadReq.PerSlotPasswords, uploadReq.DeathlinkDisabled, uploadReq.ReducedAccess, 1, true)
		if err != nil {
			data.Error = fmt.Sprintf("failed to start room: %v", err)
			renderTemplate(w, "base", data, "templates/base.gohtml", "templates/upload.gohtml")
			return
		}

		if rm.metrics != nil {
			rm.metrics.activeRooms.Inc()
		}

		http.Redirect(w, r, "/admin/rooms", http.StatusSeeOther)
		return
	}

	renderTemplate(w, "base", data, "templates/base.gohtml", "templates/upload.gohtml")
}

func (rm *RoomManager) handleAdminRooms(w http.ResponseWriter, r *http.Request) {
	rooms, err := rm.store.LoadAllSlim()
	if err != nil {
		renderTemplate(w, "base", roomsPageData{
			ActiveNav: "rooms",
			Error:     "failed to load rooms from store",
			ApiKey:    rm.config.ApiKey,
		}, "templates/base.gohtml", "templates/rooms.gohtml")
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
	}, "templates/base.gohtml", "templates/rooms.gohtml")
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
	}, "templates/base.gohtml", "templates/stats.gohtml")
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
	admin.HandleFunc("/upload", rm.handleAdminUpload).Methods(http.MethodGet, http.MethodPost)
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
