trigger:
  branches:
    include:
      - main
 
pool:
  name: 'Rancher-prod-azure'
 
variables:
  imageName: file-manager
  dockerRegistryServiceConnection: nsnakscontregecm001
  containerRegistry: nsnakscontregecm001.azurecr.io
 
stages:
  - stage: Build
    jobs:
      - job: BuildWithMaven
        steps:
          - task: DownloadSecureFile@1
            name: DownloadSettings
            inputs:
              secureFile: maven-settings.xml
 
          - task: Maven@3
            inputs:
              mavenPomFile: './file-manager/pom.xml'
              goals: 'dependency:go-offline'
              options: '-s $(DownloadSettings.secureFilePath)'
              publishJUnitResults: false
 
          - task: Maven@3
            inputs:
              mavenPomFile: './file-manager/pom.xml'
              goals: 'package'
              options: '-s $(DownloadSettings.secureFilePath) -DskipTests'
              publishJUnitResults: false
          - task: Maven@3
            inputs:
              mavenPomFile: './file-manager/pom.xml'
              goals: 'clean install'
              options: '-X'  # enables detailed debug logging
              publishJUnitResults: true
              testResultsFiles: '**/surefire-reports/TEST-*.xml'
          - task: Docker@2
            inputs:
              containerRegistry: $(dockerRegistryServiceConnection)            
              # repository: $(containerRegistry)
              command: buildAndPush
              Dockerfile: '**/Dockerfile'
              tags: latest
