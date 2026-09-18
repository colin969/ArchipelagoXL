package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
)

func runProfiler() {
	go func() {
		log.Println("pprof listening on :6060")
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Printf("pprof server error: %v", err)
		}
	}()
}
