Error starting ApplicationContext. To display the condition evaluation report re-run your application with 'debug' enabled.
2025-05-28T12:28:35.380Z ERROR 1 --- [           main] o.s.boot.SpringApplication               : Application run failed
 
org.springframework.context.ApplicationContextException: Unable to start web server
 at org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext.onRefresh(ServletWebServerApplicationContext.java:164) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.context.support.AbstractApplicationContext.refresh(AbstractApplicationContext.java:578) ~[spring-context-6.0.2.jar!/:6.0.2]
 at org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext.refresh(ServletWebServerApplicationContext.java:146) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.SpringApplication.refresh(SpringApplication.java:730) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.SpringApplication.refreshContext(SpringApplication.java:432) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.SpringApplication.run(SpringApplication.java:308) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.SpringApplication.run(SpringApplication.java:1302) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.SpringApplication.run(SpringApplication.java:1291) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at com.nedbank.kafka.filemanage.Application.main(Application.java:10) ~[classes!/:na]
 at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
 at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
 at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
 at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
 at org.springframework.boot.loader.MainMethodRunner.run(MainMethodRunner.java:49) ~[file-manager-1.0-SNAPSHOT.jar:na]
 at org.springframework.boot.loader.Launcher.launch(Launcher.java:95) ~[file-manager-1.0-SNAPSHOT.jar:na]
 at org.springframework.boot.loader.Launcher.launch(Launcher.java:58) ~[file-manager-1.0-SNAPSHOT.jar:na]
 at org.springframework.boot.loader.JarLauncher.main(JarLauncher.java:65) ~[file-manager-1.0-SNAPSHOT.jar:na]
Caused by: org.springframework.boot.web.server.WebServerException: Unable to create tempDir. java.io.tmpdir is set to /tmp
 at org.springframework.boot.web.server.AbstractConfigurableWebServerFactory.createTempDir(AbstractConfigurableWebServerFactory.java:208) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory.getWebServer(TomcatServletWebServerFactory.java:194) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext.createWebServer(ServletWebServerApplicationContext.java:183) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext.onRefresh(ServletWebServerApplicationContext.java:161) ~[spring-boot-3.0.0.jar!/:3.0.0]
 ... 16 common frames omitted
Caused by: java.nio.file.FileSystemException: /tmp/tomcat.8080.15177094959256078919: Read-only file system
 at java.base/sun.nio.fs.UnixException.translateToIOException(UnixException.java:100) ~[na:na]
 at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:106) ~[na:na]
 at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:111) ~[na:na]
 at java.base/sun.nio.fs.UnixFileSystemProvider.createDirectory(UnixFileSystemProvider.java:397) ~[na:na]
 at java.base/java.nio.file.Files.createDirectory(Files.java:700) ~[na:na]
 at java.base/java.nio.file.TempFileHelper.create(TempFileHelper.java:134) ~[na:na]
 at java.base/java.nio.file.TempFileHelper.createTempDirectory(TempFileHelper.java:171) ~[na:na]
 at java.base/java.nio.file.Files.createTempDirectory(Files.java:1017) ~[na:na]
 at org.springframework.boot.web.server.AbstractConfigurableWebServerFactory.createTempDir(AbstractConfigurableWebServerFactory.java:202) ~[spring-boot-3.0.0.jar!/:3.0.0]
 ... 19 common frames omitted
 
