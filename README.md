C:\Users\CC437236\Documents\file-manager>docker build -t file-manager-app .
[+] Building 1.8s (8/8) FINISHED                                                                   docker:desktop-linux
 => [internal] load build definition from Dockerfile                                                               0.1s
 => => transferring dockerfile: 185B                                                                               0.1s
 => [internal] load metadata for docker.io/library/openjdk:17-jdk-slim                                             0.0s
 => [internal] load .dockerignore                                                                                  0.1s
 => => transferring context: 2B                                                                                    0.0s
 => [1/3] FROM docker.io/library/openjdk:17-jdk-slim                                                               0.3s
 => [internal] load build context                                                                                  0.8s
 => => transferring context: 34.54kB                                                                               0.5s
 => [2/3] WORKDIR /app                                                                                             0.2s
 => [3/3] COPY target/file-manager-1.0-SNAPSHOT.jar app.jar                                                        0.2s
 => exporting to image                                                                                             0.2s
 => => exporting layers                                                                                            0.1s
 => => writing image sha256:21369e43051411dfe9bcff0ec66198fc1b2c007278e56d20817f43dde36d1095                       0.0s
 => => naming to docker.io/library/file-manager-app                                                                0.0s

What's next:
    View a summary of image vulnerabilities and recommendations → docker scout quickview
