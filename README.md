git config --global http.proxy http://CC437236:67dYaB%40jEh89@proxy.company.com:8080
git config --global https.proxy http://CC437236:67dYaB%40jEh89@proxy.company.com:8080

git clone git@ssh.dev.azure.com:v3/Nedbank-Limited/ECM_Exstream/ECM_comp_Filemanager
cd ECM_comp_Filemanager
git checkout -b develop origin/develop

$ git clone git@ssh.dev.azure.com:v3/Nedbank-Limited/ECM_Exstream/ECM_comp_Filemanager
Cloning into 'ECM_comp_Filemanager'...
ssh: connect to host ssh.dev.azure.com port 22: Connection timed out
fatal: Could not read from remote repository.

Please make sure you have the correct access rights
and the repository exists.

proxy.host=proxyprod.africa.nedcor.net
proxy.port=80

git config --global http.proxy http://proxyprod.africa.nedcor.net:80
git config --global https.proxy http://proxyprod.africa.nedcor.net:80

$ git clone https://dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager
Cloning into 'ECM_comp_Filemanager'...
fatal: unable to access 'https://dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager/': CONNECT tunnel failed, response 407

=========

CC437236@V105P11PRA2683 MINGW64 ~/08-09-2025
$ git config --global http.proxy http://CC437236:67dYaB%40jEh89@proxy.company.com:8080

CC437236@V105P11PRA2683 MINGW64 ~/08-09-2025
$ git config --global https.proxy http://CC437236:67dYaB%40jEh89@proxy.company.com:8080

CC437236@V105P11PRA2683 MINGW64 ~/08-09-2025
$ git clone https://dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager
Cloning into 'ECM_comp_Filemanager'...
fatal: unable to access 'https://dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager/': Could not resolve proxy: proxy.company.com

CC437236@V105P11PRA2683 MINGW64 ~/08-09-2025
$ git config --global http.proxy http://proxyprod.africa.nedcor.net:80

CC437236@V105P11PRA2683 MINGW64 ~/08-09-2025
$ git config --global https.proxy http://proxyprod.africa.nedcor.net:80

CC437236@V105P11PRA2683 MINGW64 ~/08-09-2025
$ git clone https://dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager
Cloning into 'ECM_comp_Filemanager'...
fatal: unable to access 'https://dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager/': CONNECT tunnel failed, response 407




