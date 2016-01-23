package drystonedb

import (
	"log"
	_"fmt"	
	"net/http"
	_"net/url"

)

type DrystoneNode struct {
	URL string
	data map[string][]byte
}

func NewDrystoneNode()(node *DrystoneNode){
	log.Println("NewDrystoneNode")
	node =&DrystoneNode{
		data:	make(map[string][]byte),
	}

	return node
}


type DrystoneHttp struct {
	node *DrystoneNode
}

func (e *DrystoneHttp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("WorkerHttp.ServeHTTP: %s %s %s", r.RemoteAddr, r.Method, r.URL)

	http.Error(w, "bad reguest", http.StatusBadRequest)
}

func (node *DrystoneNode) serveWorkerResultsHTTP() {
	err := http.ListenAndServe(node.URL, &DrystoneHttp{node: node})
	if err != nil {
		log.Fatal("node.serveWorkerResultsHTTP error", err)
	}
}
