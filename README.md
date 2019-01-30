# Delegate Service

This service communicates with other **Delegate** services running in multiple Nimble clusters, collects the responses from the other services and returns back a single response which consists of an array of per-cluster response.

On startup, the service registers with the [Eureka server](https://github.com/Netflix/eureka) and then it uses the Eureka client to dynamically get the list of the other Delegate services running in multiple Nimble clusters.

On shutdown, the service un-registers from the Eureka server.

## APIs

The following list specifies the APIs which are supported by the service:

* `/serve?local=0` returns a single response which is a collection of all the responses from all the Nimble clusters.

* `/serve?local=1` returns a local response which is a response of the local Delegate service ONLY (without calling the Delegate services in the other Nimble clusters).

* `/eureka` returns a list of all the Delegate services registered in the Eureka server (used for debug).

## Running the service

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

* The Eureka server URL is defined by the `eureka.serviceUrl.default` parameter in the [eureka-client.properties](../blob/master/src/main/resources/eureka-client.properties
) file. The default value is `http://eureka:8080/eureka/v2/`.
* The Eureka `vipAddress` of the Delegate Service is defined by the `eureka.vipAddress` parameter in the [eureka-client.properties](../blob/master/src/main/resources/eureka-client.properties
) file and by the `context-param` parameter in the [web.xml](../blob/master/WEB-INF/web.xml) file. The value must be the **same**.
* The URL path of the main service API (`/serve`) is defined by the `context-param` parameter int the [web.xml](../blob/master/WEB-INF/web.xml) file. The default value is `/serve`. 

### Run the Eureka server

```shell
$ docker run -d --name eureka -p 8080:8080 netflixoss/eureka:1.3.1
```

### Run the Delegate Service

```shell
$ docker run -d -p 8888:8080 --link eureka --name nimble-delegate nimble-delegate 
```

### Access the Eureka Server Dashboard

You should see the Delegate service in the [Dashboard](http://localhost:8080/eureka) after a few seconds.

### Access the Delegate service

```shell
$ curl localhost:8888/serve?local=0
```
You should receive a single JSON response which consists of an array of multiple responses

### Stop the Delegate Service

```shell
$ docker stop nimble-delegate
```

Note: it is better to stop the service before removing it, to let it un-registers gracefully from the Eureka server. If you remove it by using (`docker rm -f`) it will take about 30 seconds (hearbeat timeout period) until the service is removed from the Eureka Server.

### Remove the Delegate Service

```shell
$ docker rm nimble-delegate
```
