git clone https://CC437236:67dYaB%40jEh89@dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager
cd ECM_comp_Filemanager
git checkout -b develop origin/develop   # example for "develop" branch


$ git clone https://CC437236:67dYaB%40jEh89@dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager
Cloning into 'ECM_comp_Filemanager'...
git: 'credential-manager-core' is not a git command. See 'git --help'.
fatal: Authentication failed for 'https://dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager/'

git config --global http.sslVerify false
