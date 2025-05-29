2025-05-29T05:38:53.0987823Z Downloading from nexus: https://nexus.devops.nednet.co.za/repository/maven-group/org/slf4j/slf4j-parent/2.0.9/slf4j-parent-2.0.9.pom
2025-05-29T05:38:53.1112025Z [ERROR] [ERROR] Some problems were encountered while processing the POMs:
2025-05-29T05:38:53.1113609Z [ERROR] Non-resolvable import POM: The following artifacts could not be resolved: org.springframework.boot:spring-boot-dependencies:pom:3.0.0 (absent): Could not transfer artifact org.springframework.boot:spring-boot-dependencies:pom:3.0.0 from/to nexus (https://nexus.devops.nednet.co.za/repository/maven-group/): nexus.devops.nednet.co.za: Name or service not known @ line 30, column 25
2025-05-29T05:38:53.1114749Z [ERROR] Non-resolvable import POM: The following artifacts could not be resolved: org.slf4j:slf4j-parent:pom:2.0.9 (absent): Could not transfer artifact org.slf4j:slf4j-parent:pom:2.0.9 from/to nexus (https://nexus.devops.nednet.co.za/repository/maven-group/): nexus.devops.nednet.co.za @ line 38, column 25
2025-05-29T05:38:53.1115544Z [ERROR] 'dependencies.dependency.version' for org.springframework.boot:spring-boot-starter-web:jar is missing. @ line 87, column 21
2025-05-29T05:38:53.1122392Z [ERROR] 'dependencies.dependency.version' for com.fasterxml.jackson.core:jackson-databind:jar is missing. @ line 106, column 21
2025-05-29T05:38:53.1123251Z [ERROR] 'dependencies.dependency.version' for org.projectlombok:lombok:jar is missing. @ line 112, column 21
2025-05-29T05:38:53.1123909Z  @ 
2025-05-29T05:38:53.1124370Z [ERROR] The build could not read 1 project -> [Help 1]
2025-05-29T05:38:53.1124907Z [ERROR]   
2025-05-29T05:38:53.1125410Z [ERROR]   The project com.org.filemanager:file-manager:1.0-SNAPSHOT (/home/vsts/work/1/s/file-manager/pom.xml) has 5 errors
2025-05-29T05:38:53.1126702Z [ERROR]     Non-resolvable import POM: The following artifacts could not be resolved: org.springframework.boot:spring-boot-dependencies:pom:3.0.0 (absent): Could not transfer artifact org.springframework.boot:spring-boot-dependencies:pom:3.0.0 from/to nexus (https://nexus.devops.nednet.co.za/repository/maven-group/): nexus.devops.nednet.co.za: Name or service not known @ line 30, column 25: Unknown host nexus.devops.nednet.co.za: Name or service not known -> [Help 2]
2025-05-29T05:38:53.1128423Z [ERROR]     Non-resolvable import POM: The following artifacts could not be resolved: org.slf4j:slf4j-parent:pom:2.0.9 (absent): Could not transfer artifact org.slf4j:slf4j-parent:pom:2.0.9 from/to nexus (https://nexus.devops.nednet.co.za/repository/maven-group/): nexus.devops.nednet.co.za @ line 38, column 25: Unknown host nexus.devops.nednet.co.za -> [Help 2]
2025-05-29T05:38:53.1129594Z [ERROR]     'dependencies.dependency.version' for org.springframework.boot:spring-boot-starter-web:jar is missing. @ line 87, column 21
2025-05-29T05:38:53.1130244Z [ERROR]     'dependencies.dependency.version' for com.fasterxml.jackson.core:jackson-databind:jar is missing. @ line 106, column 21
2025-05-29T05:38:53.1130873Z [ERROR]     'dependencies.dependency.version' for org.projectlombok:lombok:jar is missing. @ line 112, column 21
2025-05-29T05:38:53.1131312Z [ERROR] 
2025-05-29T05:38:53.1132079Z [ERROR] To see the full stack trace of the errors, re-run Maven with the -e switch.
2025-05-29T05:38:53.1132590Z [ERROR] Re-run Maven using the -X switch to enable full debug logging.
2025-05-29T05:38:53.1133204Z [ERROR] 
2025-05-29T05:38:53.1133967Z [ERROR] For more information about the errors and possible solutions, please read the following articles:
2025-05-29T05:38:53.1134821Z [ERROR] [Help 1] http://cwiki.apache.org/confluence/display/MAVEN/ProjectBuildingException
2025-05-29T05:38:53.1135353Z [ERROR] [Help 2] http://cwiki.apache.org/confluence/display/MAVEN/UnresolvableModelException
2025-05-29T05:38:53.1253538Z 
2025-05-29T05:38:53.1306797Z The process '/usr/bin/mvn' failed with exit code 1
2025-05-29T05:38:53.1342096Z ##[error]Build failed.
2025-05-29T05:38:53.1358995Z Could not retrieve code analysis results - Maven run failed.
2025-05-29T05:38:53.1443443Z ##[section]Finishing: Maven
 
