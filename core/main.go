package main

import(
	"flag"
	dsdb "github.com/gophergala2016/drystonedb/core"
)

func main() {

	StoneHosts := flag.String("stones", "http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379", "hosts list host0:port0,host1:port1,..")

	flag.Parse()

    stone := dsdb.NewDrystone()	

}