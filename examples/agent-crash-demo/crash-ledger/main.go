// Copyright 2026 checkpointd authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command crash-ledger remembers which turns chat-agent has already crashed on, because a crashed actor is rewound and cannot remember it itself.
package main

import (
	"flag"
	"log"
	"net/http"
	"sync"
)

var addr = flag.String("addr", "0.0.0.0:8080", "address to listen on")

func main() {
	flag.Parse()
	log.Printf("crash-ledger listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, newHandler()))
}

// newHandler serves POST /crash?turn=ID, which answers 200 the first time it sees ID and 409 every time after, and GET /health.
func newHandler() http.Handler {
	var mu sync.Mutex
	seen := map[string]bool{}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /crash", func(w http.ResponseWriter, r *http.Request) {
		turn := r.URL.Query().Get("turn")
		if turn == "" {
			http.Error(w, "missing turn", http.StatusBadRequest)
			return
		}
		mu.Lock()
		already := seen[turn]
		seen[turn] = true
		mu.Unlock()
		if already {
			http.Error(w, "already crashed on this turn", http.StatusConflict)
			return
		}
		log.Printf("turn %s: first crash granted", turn)
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
