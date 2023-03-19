REM docker build -t general-worker-acceleration:0.0.1 -t general-worker-acceleration:latest .
docker rmi -f general-worker-acceleration:latest
docker build -t general-worker-acceleration:latest .

REM if it runs on localhost
REM docker run -it -p 8080:8080 general-worker-acceleration:latest
REM if it runs on 192.168.1.100
REM docker run --env DATAPLATFORM_IP=192.168.1.100 -it -p 8080:8080 general-worker-acceleration:latest