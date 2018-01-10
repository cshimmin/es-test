Setup
====

1) Start an elastic server:

```
docker run -p 9200:9200 -p 9300:9300 --name es-server -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.1.1
```

2) Get some `.msg` files and put them in `./test-data.tgz`.

3) Copy `crayfis_data_pb2.py` to `./src/`

4) Build & start the test container:

```
docker build -t es-test .
```

```
docker run --link es-server:elastic es-test
```
