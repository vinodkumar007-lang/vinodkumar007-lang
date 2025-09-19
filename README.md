kubectl run -it --rm debug --image=quay.io/centos/centos:stream8 -- bash
yum install -y nc
nc -vz nbpigelpdev02.africa.nedcor.net 9093
