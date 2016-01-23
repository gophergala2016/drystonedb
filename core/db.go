package drystonedb

import (
	"fmt"
	"log"
	"net/http"
	_ "net/url"
	"strings"
)

type Drystone struct {
	CURL string
	SURL string
	nm   string
	data map[string][]byte
}

func NewDrystone(curl *string, surl *string, hosts *string) (stone *Drystone) {
	stone = &Drystone{
		CURL: *curl,
		SURL: *surl,
		data: make(map[string][]byte),
	}

	stone.name()

	go stone.run()

	return stone
}

func (stone *Drystone) name() string {
	cs := strings.Split(stone.CURL, ":")
	if len(cs) != 2 {
		panic(fmt.Sprintf("wrong client url %s", stone.CURL))
	}
	ss := strings.Split(stone.CURL, ":")
	if len(ss) != 2 {
		panic(fmt.Sprintf("wrong stone url %s", stone.SURL))
	}
	stone.nm = fmt.Sprintf("%s:[%s,%s]", cs[0], cs[1], ss[1])

	return stone.nm
}

func (stone *Drystone) run() {
	log.Printf("Drystone run [%s,%s]", stone.CURL, stone.SURL)

	go stone.serveClientHTTP()
	go stone.serveStoneHTTP()

}

func (stone *Drystone) processClientPostRequest(w *http.ResponseWriter, r *http.Request) {
	log.Printf("Drystone processClientPostRequest %s", stone.nm)
}

func (stone *Drystone) processClientGetRequest(w *http.ResponseWriter, r *http.Request) {
	log.Printf("Drystone processClientGetRequest %s", stone.nm)
}

func (stone *Drystone) processStonePostRequest(w *http.ResponseWriter, r *http.Request) {
	log.Printf("Drystone processStonePostRequest %s", stone.nm)
}

func (stone *Drystone) processStoneGetRequest(w *http.ResponseWriter, r *http.Request) {
	log.Printf("Drystone processStoneGetRequest %s", stone.nm)
}

// serve client http requests

type ClientHttp struct {
	stone *Drystone
}

func (c *ClientHttp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("ClientHttp.ServeHTTP: %s %s %s", r.RemoteAddr, r.Method, r.URL)

	if r.Method == "POST" {
		if r.URL.Path == "/data" {
			c.stone.processClientPostRequest(&w, r)
			return
		}

	}

	if r.Method == "GET" {
		if r.URL.Path == "/data" {
			c.stone.processClientGetRequest(&w, r)
			return
		}

	}

	http.Error(w, "bad reguest", http.StatusBadRequest)
}

func (stone *Drystone) serveClientHTTP() {
	err := http.ListenAndServe(stone.CURL, &ClientHttp{stone: stone})
	if err != nil {
		log.Fatal("stone.serveClientHTTP error", err)
	}
}

// boom
// serve stone nodes http requests
// http2 is cool, but later

type DrystoneHttp struct {
	stone *Drystone
}

func (e *DrystoneHttp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("ClientHttp.ServeHTTP: %s %s %s", r.RemoteAddr, r.Method, r.URL)

	if r.Method == "POST" {
		if r.URL.Path == "/stone" {
			e.stone.processStonePostRequest(&w, r)
			return
		}

	}

	if r.Method == "GET" {
		if r.URL.Path == "/stone" {
			e.stone.processStoneGetRequest(&w, r)
			return
		}

	}

	http.Error(w, "bad reguest", http.StatusBadRequest)
}

func (stone *Drystone) serveStoneHTTP() {
	err := http.ListenAndServe(stone.SURL, &DrystoneHttp{stone: stone})
	if err != nil {
		log.Fatal("stone.serveStoneHTTP error", err)
	}
}
