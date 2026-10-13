package main

import (
	"embed"
	"html/template"
	"net/http"
)

//go:embed templates/upload.gohtml
var templateFS embed.FS

var uploadTemplate = template.Must(
	template.ParseFS(templateFS, "templates/upload.gohtml"),
)

type uploadPageData struct {
	ApiKey string
}

func (rm *RoomManager) handleFrontendUpload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := uploadTemplate.Execute(w, uploadPageData{ApiKey: rm.config.ApiKey}); err != nil {
		http.Error(w, "template error", http.StatusInternalServerError)
	}
}
