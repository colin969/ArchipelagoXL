package main

import (
	"net/http"
	"sort"
	"strconv"

	"github.com/gorilla/mux"
)

type dashboardBase struct {
	RoomId string
	Error  string
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

func (rm *RoomManager) handleFrontendDashboardTracker(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(r)

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		renderTemplate(w, "base", dashboardBase{
			Error: "room not found",
		}, "templates/base_dashboard.gohtml", "templates/dashboard_error.gohtml")
		return
	}

	base := getDashboardBase(room)
	renderTemplate(w, "base", base,
		"templates/base_dashboard.gohtml", "templates/dashboard.gohtml")
}

func (rm *RoomManager) handleFrontendDashboardRoom(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(r)

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		renderTemplate(w, "base", dashboardBase{
			Error: "room not found",
		}, "templates/base_dashboard.gohtml", "templates/dashboard_error.gohtml")
		return
	}

	base := getDashboardBase(room)

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
		"templates/base_dashboard.gohtml", "templates/dashboard_room.gohtml")
}

func (rm *RoomManager) handleFrontendDashboardSlotTracker(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(r)

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		renderTemplate(w, "base", dashboardBase{
			Error: "room not found",
		}, "templates/base_dashboard.gohtml", "templates/dashboard_error.gohtml")
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
		renderTemplate(w, "base", dashboardBase{
			Error: "team slot not found in room",
		}, "templates/base_dashboard.gohtml", "templates/dashboard_error.gohtml")
		return
	}

	base := getDashboardBase(room)

	data := dashboardSlotData{
		dashboardBase:       base,
		Team:                teamId,
		Slot:                slotId,
		Name:                slotInfo.Name,
		Game:                slotInfo.Game,
		DataPackageChecksum: room.apx.roomInfo.DatapackageChecksums[slotInfo.Game],
	}

	renderTemplate(w, "base", data,
		"templates/base_dashboard.gohtml", "templates/dashboard_slot.gohtml")
}

func (rm *RoomManager) registerDashboardRoutes(r *mux.Router) {
	dashboard := r.PathPrefix("/dashboard/{roomId}").Subrouter()
	dashboard.HandleFunc("", rm.handleFrontendDashboardRoom).Methods(http.MethodGet)
	dashboard.HandleFunc("/tracker", rm.handleFrontendDashboardTracker).Methods(http.MethodGet)
	dashboard.HandleFunc("/tracker/{teamId}/{slotId}", rm.handleFrontendDashboardSlotTracker).Methods(http.MethodGet)
}

func getDashboardBase(room *HostedRoom) dashboardBase {
	return dashboardBase{
		RoomId: room.lobbyRoomId,
	}
}
