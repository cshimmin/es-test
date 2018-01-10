Setup
====

1) Copy `crayfis_data_pb2.py` to `./src/`

2) Put some data in `./test-data/` (should have the same `YYYY/MM/DD/HOST/HH.tar.gz` format as on the server).

3) Bring up services:

```
docker-compose up [-d] kibana elasticsearch [elasticsearch2]
```

4) Build & start the test container:

```
docker-compose up --build es-test
```

5) Check out kibana at `http://localhost:5601`
