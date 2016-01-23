package drystonedb

import (
	"log"
	_"fmt"	
	"net/http"
	_"net/url"

)

type Drystone struct {
	URL string
	data map[string][]byte
}

func NewDrystone(url *string,hosts *string)(stone *Drystone){
	log.Println("NewDrystone")
	stone = &Drystone{
		URL: *url,
		data:	make(map[string][]byte),
	}

	go stone.run()

	return stone
}


type DrystoneHttp struct {
	stone *Drystone
}

func (stone *Drystone) run() {
	log.Printf("Drystone run %s",stone.URL)

	go stone.serveStoneHTTP()
}


func (e *DrystoneHttp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("WorkerHttp.ServeHTTP: %s %s %s", r.RemoteAddr, r.Method, r.URL)

	http.Error(w, "bad reguest", http.StatusBadRequest)
}

func (stone *Drystone) serveStoneHTTP() {
	err := http.ListenAndServe(stone.URL, &DrystoneHttp{stone: stone})
	if err != nil {
		log.Fatal("node.serveStoneHTTP error", err)
	}
}
