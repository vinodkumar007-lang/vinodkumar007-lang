C:\Users\CC437236\Docker image creation\file-manager>docker run -p 8080:8080 --name file-manager-container file-manager-app
Exception in thread "main" java.lang.ClassNotFoundException: com.org.filemanager.Application
        at java.base/java.net.URLClassLoader.findClass(URLClassLoader.java:445)
        at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:587)
        at org.springframework.boot.loader.LaunchedURLClassLoader.loadClass(LaunchedURLClassLoader.java:149)
        at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:520)
        at java.base/java.lang.Class.forName0(Native Method)
        at java.base/java.lang.Class.forName(Class.java:467)
        at org.springframework.boot.loader.MainMethodRunner.run(MainMethodRunner.java:46)
        at org.springframework.boot.loader.Launcher.launch(Launcher.java:95)
        at org.springframework.boot.loader.Launcher.launch(Launcher.java:58)
        at org.springframework.boot.loader.JarLauncher.main(JarLauncher.java:65)
