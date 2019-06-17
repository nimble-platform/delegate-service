# Delegate Service

This service communicates with other **Delegate** services running in multiple Nimble clusters, collects the responses from the other services and returns back a single response which consists of an array of per-cluster response.

On startup, the service registers with the [Eureka server](https://github.com/Netflix/eureka) and then it uses the Eureka client to dynamically get the list of the other Delegate services running in multiple Nimble clusters.

On shutdown, the service un-registers from the Eureka server.

## APIs

The following list specifies the APIs which are supported by the service:

* `/serve?local=0` returns a single response which is a collection of all the responses from all the Nimble clusters.

* `/serve?local=1` returns a local response which is a response of the local Delegate service ONLY (without calling the Delegate services in the other Nimble clusters).

* `/eureka` returns a list of all the Delegate services registered in the Eureka server (used for debug).

## Running the service on a single machine using Docker

### Preqrequisites

* [Apache Maven](https://maven.apache.org/) 3.6.0
* [Oracle JAVA](https://www.oracle.com/java/) 1.8
* [Docker](https://www.docker.com/)

### Build

```shell
$ git clone git@github.com:nimble-platform/delegate-service.git
$ cd delegate-service
$ mvn compile
$ mvn package
$ docker build -t nimble-delegate .
```

### Configuration

* The Eureka server URL is defined by the `eureka.serviceUrl.default` parameter in the [eureka-client.properties](../master/src/main/resources/eureka-client.properties
) file. The default value is `http://eureka:8080/eureka/v2/`.
* The Eureka `vipAddress` of the Delegate Service is defined by the `eureka.vipAddress` parameter in the [eureka-client.properties](../master/src/main/resources/eureka-client.properties
) file and by the `context-param` parameter in the [web.xml](../blob/master/WEB-INF/web.xml) file. The value must be the **same**.
* The URL path of the main service API (`/serve`) is defined by the `context-param` parameter int the [web.xml](../master/WEB-INF/web.xml) file. The default value is `/serve`. 

### Create a docker network

```shell
$ docker network create nimble
```

This **nimble** network is used to connect all the docker containers

### Run the Eureka server

```shell
$ docker run -d --name eureka --net nimble -p 8080:8080 netflixoss/eureka:1.3.1
```

Once the Eureka server is running, you can access the Dashboard using the [Dashboard URL](http://localhost:8080/eureka)

### Run the Delegate Services

```shell
$ docker run -d -p 8881:8080 --net nimble --name delegate-1 nimble-delegate 
$ docker run -d -p 8882:8080 --net nimble --name delegate-2 nimble-delegate 
$ docker run -d -p 8883:8080 --net nimble --name delegate-3 nimble-delegate 
```

### Access the Delegate service

```shell
$ curl localhost:8881/serve?local=0
```

You should receive a single JSON response which consists of an array of multiple responses (one from each Delegate service)

### Stop the Delegate Services

```shell
$ docker stop delegate-1 delegate-2 delegate-3
```

Note: it is better to stop the services before removing them, to let them un-register **gracefully** from the Eureka server. If you remove the service by using (`docker rm -f`) it will take about 30 seconds (hearbeat timeout period) until the service is removed from the Eureka Server.

### Remove the Delegate Services

```shell
$ docker rm delegate-1 delegate-2 delegate-3
```

### deploy the delegate service on Staging
follow these steps:
1. git clone delegate service project
2. go to the file `src/main/resources/eureka-client.properties` and change `eureka.name=Staging`
3. build the project using mvn and create a docker image locally with the following command: `mvn compile ; mvn package ; docker build -t nimbleplatform/delegate-service:staging .`
4. need to update docker-compose.yml and add these lines (under services):
```
  ###############################
  ###### Delegate Service #######
  ###############################

  delegate-service:
    image: nimbleplatform/delegate-service:staging
    env_file:
      - env_vars
    environment:
      - FRONTEND_URL=http://nimble-staging.salzburgresearch.at/frontend/
      - INDEXING_SERVICE_URL=nimble-staging.salzburgresearch.at/indexing-service
      - INDEXING_SERVICE_PORT=-1
    ports:
      - "9265:8080"
    networks:
      - infra
```
(you need to update the ips and port for the staging instance, port = -1 if you're using no port in case like nginx)
5. then in dev I ran the docker using `./run-dev.sh restart-single delegate-service`, not sure how you do it in staging


