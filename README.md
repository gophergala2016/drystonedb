![header](https://raw.githubusercontent.com/beyondns/gotips/master/gophers.jpg)

# drystonedb

Distributed in memory key/value storage with custom consensus algorithm  

Solve [CAP](https://en.wikipedia.org/wiki/CAP_theorem) theorem issues  

### Consistency

Custom consensus algorithm with version control

### Availability

Data available if at least one node (stone) available

### Partion tolerance

All nodes (stones) equal, no single point of failure (SPF), client talk with different partitioned nodes

### http api

##### add data
curl -XPOST -v 'http://127.0.0.1:12379/data?g=boom&k=cambala' -d "cobra"
##### get data
curl -XGET -v 'http://127.0.0.1:12379/data?g=boom&k=cambala'
##### del data
curl -XDELETE -v 'http://127.0.0.1:12379/data?g=boom&k=cambala'

### build
./build.sh

### start local
foreman start


