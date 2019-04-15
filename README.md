## carrybasket sync tool

carrybasket is a simple tool that synchronizes data from client to server
directory over IP.

### Assignment description

Build an application to synchronise a source folder and a destination folder
over IP:

1. a simple command line client which takes one directory as argument and keeps
   monitoring changes in that directory and uploads any change to its server
2. a simple server which takes one empty directory as argument and receives any
   change from its client

Bonus 1. optimise data transfer by avoiding uploading multiple times the same
file

Bonus 2. optimise data transfer by avoiding uploading multiple times the same
partial files (files are sharing partially the same content)

### Running tests

```bash
$ go test . -coverprofile=cover.out && go tool cover -html=cover.out -o cover.html
ok      github.com/balta2ar/carrybasket 0.401s  coverage: 90.0% of statements
```

### Running in host OS

The tool was created using Go v1.12.3:

```bash
git clone https://github.com/balta2ar/carrybasket

# prepare directories
mkdir -p data/server
mkdir -p data/client

go run server/main.go data/server
# in a separate terminal
go run client/main.go data/client

# put some files into data/client to check...
```

### Running in Docker

```bash
# prepare directories
mkdir -p data/server
mkdir -p data/client

# build and run server
docker build -f server.Dockerfile -t carrybasket_server:0.1 .
docker run -it --rm -v $(pwd)/data/server:/data --network=host carrybasket_server:0.1 /data

# build and run client
docker build -f client.Dockerfile -t carrybasket_client:0.1 .
docker run -it --rm -v $(pwd)/data/client:/data --network=host carrybasket_client:0.1 /data
```
