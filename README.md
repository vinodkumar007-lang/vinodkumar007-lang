git config --global http.proxy http://CC437236:67dYaB%40jEh89@proxy.company.com:8080
git config --global https.proxy http://CC437236:67dYaB%40jEh89@proxy.company.com:8080
git config --global http.sslVerify false
git clone https://CC437236:<PAT>@dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager
cd ECM_comp_Filemanager
git branch -r                # list all remote branches
git checkout -b develop origin/develop   # example for "develop"
git pull origin develop
