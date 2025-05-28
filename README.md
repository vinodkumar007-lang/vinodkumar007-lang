This XML file does not appear to have any style information associated with it. The document tree is shown below.
<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 -->
<settings xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://maven.apache.org/SETTINGS/1.0.0" xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
<!--  localRepository
     | The path to the local repository maven will use to store artifacts.
     |
     | Default: ~/.m2/repository
    <localRepository>/path/to/local/repo</localRepository>
     -->
<!--  interactiveMode
     | This will determine whether maven prompts you when it needs input. If set to false,
     | maven will use a sensible default value, perhaps based on some other setting, for
     | the parameter in question.
     |
     | Default: true
    <interactiveMode>true</interactiveMode>
     -->
<!--  offline
     | Determines whether maven should attempt to connect to the network when executing a build.
     | This will have an effect on artifact downloads, artifact deployment, and others.
     |
     | Default: false
    <offline>false</offline>
     -->
<!--  pluginGroups
     | This is a list of additional group identifiers that will be searched when resolving plugins by their prefix, i.e.
     | when invoking a command line like "mvn prefix:goal". Maven will automatically add the group identifiers
     | "org.apache.maven.plugins" and "org.codehaus.mojo" if these are not already contained in the list.
     | -->
<pluginGroups>
<!--  pluginGroup
         | Specifies a further group identifier to use for plugin lookup.
        <pluginGroup>com.your.plugins</pluginGroup>
         -->
</pluginGroups>
<!--  proxies
     | This is a list of proxies which can be used on this machine to connect to the network.
     | Unless otherwise specified (by system property or command-line switch), the first proxy
     | specification in this list marked as active will be used.
     | -->
<proxies>
<!--  proxy
         | Specification for one proxy, to be used in connecting to the network.
         |
        <proxy>
          <id>optional</id>
          <active>true</active>
          <protocol>http</protocol>
          <username>proxyuser</username>
          <password>proxypass</password>
          <host>proxy.host.net</host>
          <port>80</port>
          <nonProxyHosts>local.net|some.host.com</nonProxyHosts>
        </proxy>
         -->
</proxies>
<!--  servers
     | This is a list of authentication profiles, keyed by the server-id used within the system.
     | Authentication profiles can be used whenever maven must make a connection to a remote server.
     | -->
<servers>
<server>
<id>nexus-releases</id>
<username>CC437236</username>
<password>23dYaB@jEh45</password>
</server>
<server>
<id>nexus-snapshots</id>
<username>CC437236</username>
<password>34dYaB@jEh56</password>
</server>
</servers>
<!--  mirrors
     | This is a list of mirrors to be used in downloading artifacts from remote repositories.
     |
     | It works like this: a POM may declare a repository to use in resolving certain artifacts.
     | However, this repository may have problems with heavy traffic at times, so people have mirrored
     | it to several places.
     |
     | That repository definition will have a unique id, so we can create a mirror reference for that
     | repository, to be used as an alternate download site. The mirror site will be the preferred
     | server for that repository.
     |  -->
<mirrors>
<mirror>
<id>nexus</id>
<mirrorOf>*</mirrorOf>
<name>Mirror for all cr repos</name>
<url>https://nexus.devops.nednet.co.za/repository/maven-group/</url>
</mirror>
</mirrors>
<profiles>
<profile>
<id>sonar</id>
<activation>
<activeByDefault>false</activeByDefault>
</activation>
<properties>
<sonar.host.url> https://sonarqube.nednet.co.za </sonar.host.url>
</properties>
</profile>
<profile>
<id>nexus</id>
<activation>
<activeByDefault>true</activeByDefault>
</activation>
<properties>
<!--  <ci.server.host.ip>10.59.9.45</ci.server.host.ip>  -->
<!-- <nexus.host.name>nexus:nexus@nexus.nednet.co.za</nexus.host.name> -->
<nexus.host.name>nexus.devops.nednet.co.za</nexus.host.name>
<nexus.host.url>https://${nexus.host.name}</nexus.host.url>
<sonar.host.name>sonarqube.nednet.co.za</sonar.host.name>
<sonar.host.url>https://${sonar.host.name}</sonar.host.url>
<!--  Used for Reveal JS slides  -->
<project.slides.directory>${project.build.directory}/generated-slides</project.slides.directory>
<!--  This is the document source folder structure  -->
<nedjci.src.main.dir>${project.build.directory}</nedjci.src.main.dir>
<nedjci.src.asciidocs.dir>${nedjci.src.main.dir}/generated-docs</nedjci.src.asciidocs.dir>
<nedjci.src.site.dir>${nedjci.src.main.dir}/site</nedjci.src.site.dir>
<!--  This is the document destination folder structure  -->
<nedjci.dest.main.dir>${project.build.directory}/projects/</nedjci.dest.main.dir>
<nedjci.dest.training.dir>${nedjci.dest.main.dir}uploads/training/content/${project.artifactId}/</nedjci.dest.training.dir>
<project.has.traing.docs>false</project.has.traing.docs>
<!-- Urbancode Properties -->
<!--  The default is set to false. This property should be added to the templates & existing projects and set to true  -->
<!-- UC.ActivateProfile>false</UC.ActivateProfile -->
</properties>
<repositories>
<repository>
<id>nexus</id>
<url>https://nexus.devops.nednet.co.za/repository/maven-group</url>
<releases>
<enabled>true</enabled>
</releases>
<snapshots>
<enabled>true</enabled>
</snapshots>
</repository>
</repositories>
<pluginRepositories>
<pluginRepository>
<id>nexus</id>
<url>https://nexus.devops.nednet.co.za/repository/maven-group</url>
<releases>
<enabled>true</enabled>
</releases>
<snapshots>
<enabled>true</enabled>
</snapshots>
</pluginRepository>
</pluginRepositories>
</profile>
</profiles>
<activeProfiles>
<!-- make the profile active all the time  -->
<activeProfile>nexus</activeProfile>
</activeProfiles>
</settings>
