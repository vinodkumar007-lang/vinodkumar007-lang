C:\Users\CC437236\Documents\file-manager>docker build -t file-manager-app .
[+] Building 32.9s (2/2) FINISHED                                                                  docker:desktop-linux
 => [internal] load build definition from Dockerfile                                                               0.2s
 => => transferring dockerfile: 185B                                                                               0.0s
 => ERROR [internal] load metadata for docker.io/library/openjdk:17-jdk-slim                                      32.3s
------
 > [internal] load metadata for docker.io/library/openjdk:17-jdk-slim:
------
Dockerfile:1
--------------------
   1 | >>> FROM openjdk:17-jdk-slim
   2 |     WORKDIR /app
   3 |     COPY target/file-manager-1.0-SNAPSHOT.jar app.jar
--------------------
ERROR: failed to solve: DeadlineExceeded: DeadlineExceeded: DeadlineExceeded: openjdk:17-jdk-slim: failed to resolve source metadata for docker.io/library/openjdk:17-jdk-slim: failed to authorize: DeadlineExceeded: failed to fetch anonymous token: Get "https://auth.docker.io/token?scope=repository%3Alibrary%2Fopenjdk%3Apull&service=registry.docker.io": dial tcp 98.85.153.80:443: i/o timeout
