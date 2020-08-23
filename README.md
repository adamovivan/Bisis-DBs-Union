### Bisis-DBs-Union

Application is used to merge multiple databases of library information system into a single database. Currently application works with BGB, GBNS, BS and BMB databases.

### Usage

Application operates with multiple Mongo databases and one Redis database. Redis is used for merge speed improvements. In `unil/Constants` are specified Mongo connection url, Redis host and port.

Application works in two merge modes: 
* Full mode
* Incremental mode

Merge mode is **required**, and it has to be passed as an argument to main method.

To start the application in a _full mode_ pass `full` argument.  
To start the application in a _incremental mode_ pass `incremental` argument.
