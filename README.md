C:\Users\CC437236\Docker image creation\file-manager>docker build -t file-manager-app .
[+] Building 0.4s (7/7) FINISHED                                                                                                                       docker:desktop-linux
 => [internal] load build definition from Dockerfile                                                                                                                   0.1s
 => => transferring dockerfile: 372B                                                                                                                                   0.0s
 => [internal] load metadata for docker.io/library/openjdk:17-jdk-slim                                                                                                 0.0s
 => [internal] load .dockerignore                                                                                                                                      0.0s
 => => transferring context: 2B                                                                                                                                        0.0s
 => [1/3] FROM docker.io/library/openjdk:17-jdk-slim                                                                                                                   0.0s
 => [internal] load build context                                                                                                                                      0.0s
 => => transferring context: 2B                                                                                                                                        0.0s
 => CACHED [2/3] WORKDIR /app                                                                                                                                          0.0s
 => ERROR [3/3] COPY app.jar .                                                                                                                                         0.0s
------
 > [3/3] COPY app.jar .:
------
Dockerfile:8
--------------------
   6 |
   7 |     # Copy the JAR file into the container
   8 | >>> COPY app.jar .
   9 |
  10 |     # Expose port if your app runs on a port (optional)
--------------------
ERROR: failed to solve: failed to compute cache key: failed to calculate checksum of ref a3221534-bfbd-466b-8fea-770f5dc34ba1::m7hes8orfskycbkevh1kvkqqy: "/app.jar": not found
