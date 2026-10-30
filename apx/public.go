package main

import (
	"net/http"
	"sort"
	"strconv"

	"github.com/gorilla/mux"
)

type dashboardBase struct {
	PublicUrl         string
	WsUrl             string
	RoomId            string
	RoomNormalId      int
	RoomReducedId     int
	RoomReducedAccess bool
	Error             string
	Session           Session
}

type RoomPlayer struct {
	Team int
	Slot int
	Name string
	Game string
}

type dashboardRoomData struct {
	dashboardBase
	Players []RoomPlayer
}

type dashboardSlotData struct {
	dashboardBase
	Team                int
	Slot                int
	Name                string
	Game                string
	DataPackageChecksum string
}

func (rm *RoomManager) getDashboardBase(r *http.Request, room *HostedRoom) dashboardBase {
	session := rm.getSession(r)
	roomId := ""
	roomNormalId := 0
	roomReducedId := 0
	roomReducedAccess := false
	if room != nil {
		roomId = room.lobbyRoomId
		roomNormalId = room.normalHandler.id
		roomReducedId = room.reducedHandler.id
		roomReducedAccess = room.reducedAccess
	}
	return dashboardBase{
		PublicUrl:         rm.config.PublicUrl,
		WsUrl:             rm.config.WsUrl,
		RoomId:            roomId,
		RoomNormalId:      roomNormalId,
		RoomReducedId:     roomReducedId,
		RoomReducedAccess: roomReducedAccess,
		Session:           session,
	}
}

func (rm *RoomManager) getDashboardBaseWithError(err string) dashboardBase {
	return dashboardBase{
		PublicUrl: rm.config.PublicUrl,
		Error:     err,
	}
}

func (rm *RoomManager) handleFrontendDashboardTracker(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(r)

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		renderTemplate(w, "base", rm.getDashboardBaseWithError("room not found"), "templates/base.gohtml", "templates/dashboard_error.gohtml")
		return
	}

	base := rm.getDashboardBase(r, room)
	renderTemplate(w, "base", base,
		"templates/base.gohtml", "templates/dashboard.gohtml")
}

func (rm *RoomManager) handleFrontendDashboardRoom(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(r)

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		renderTemplate(w, "base", rm.getDashboardBaseWithError("room not found"), "templates/base.gohtml", "templates/dashboard_error.gohtml")
		return
	}

	base := rm.getDashboardBase(r, room)

	players := make([]RoomPlayer, 0, len(room.apx.state.SlotInfo))
	for _, slotInfo := range room.apx.state.SlotInfo {
		players = append(players, RoomPlayer{
			Slot: slotInfo.Slot,
			Team: slotInfo.Team,
			Name: slotInfo.Name,
			Game: slotInfo.Game,
		})
	}

	sort.Slice(players, func(i, j int) bool {
		return players[i].Slot < players[j].Slot
	})

	data := dashboardRoomData{
		dashboardBase: base,
		Players:       players,
	}

	renderTemplate(w, "base", data,
		"templates/base.gohtml", "templates/dashboard_room.gohtml")
}

func (rm *RoomManager) handleFrontendDashboardSlotTracker(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(r)

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		renderTemplate(w, "base", rm.getDashboardBaseWithError("room not found"), "templates/base.gohtml", "templates/dashboard_error.gohtml")
		return
	}

	vars := mux.Vars(r)
	teamId, err := strconv.Atoi(vars["teamId"])
	if err != nil {
		http.Error(w, "invalid teamId", http.StatusBadRequest)
		return
	}
	slotId, err := strconv.Atoi(vars["slotId"])
	if err != nil {
		http.Error(w, "invalid slotId", http.StatusBadRequest)
		return
	}

	slotInfo, ok := room.apx.state.SlotInfo[TeamSlot{teamId, slotId}]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		renderTemplate(w, "base", rm.getDashboardBaseWithError("team slot not found in room"), "templates/base.gohtml", "templates/dashboard_error.gohtml")
		return
	}

	base := rm.getDashboardBase(r, room)

	data := dashboardSlotData{
		dashboardBase:       base,
		Team:                teamId,
		Slot:                slotId,
		Name:                slotInfo.Name,
		Game:                slotInfo.Game,
		DataPackageChecksum: room.apx.roomInfo.DatapackageChecksums[slotInfo.Game],
	}

	renderTemplate(w, "base", data,
		"templates/base.gohtml", "templates/dashboard_slot.gohtml")
}

func (rm *RoomManager) registerDashboardRoutes(r *mux.Router) {
	r.HandleFunc("/upload", rm.withAuth(rm.handleFrontendUpload)).Methods(http.MethodGet)

	dashboard := r.PathPrefix("/dashboard/{roomId}").Subrouter()
	dashboard.HandleFunc("", rm.handleFrontendDashboardRoom).Methods(http.MethodGet)
	dashboard.HandleFunc("/tracker", rm.handleFrontendDashboardTracker).Methods(http.MethodGet)
	dashboard.HandleFunc("/tracker/{teamId}/{slotId}", rm.handleFrontendDashboardSlotTracker).Methods(http.MethodGet)
}

type publicUploadPageData struct {
	dashboardBase
	Error string
}

func (rm *RoomManager) handleFrontendUpload(w http.ResponseWriter, r *http.Request) {
	base := rm.getDashboardBase(r, nil)

	renderTemplate(w, "base", publicUploadPageData{
		dashboardBase: base,
	},
		"templates/base.gohtml", "templates/upload.gohtml")
}
