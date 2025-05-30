Unpacking objects: 100% (4/4), 368 bytes | 12.00 KiB/s, done.
From https://dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager
   0182559..57de1bd  main       -> origin/main
Merge made by the 'ort' strategy.
 Kafka_AKS/deployment.yaml | 2 +-
 1 file changed, 1 insertion(+), 1 deletion(-)

C:\Users\CC437236\ECM_comp_Filemanager>git push origin main
git: 'credential-manager-core' is not a git command. See 'git --help'.
fatal: unable to access 'https://dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager/': Recv failure: Connection was reset

C:\Users\CC437236\ECM_comp_Filemanager>git status
On branch main
Your branch is ahead of 'origin/main' by 2 commits.
  (use "git push" to publish your local commits)

Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
        modified:   file-manager/Dockerfile
        modified:   file-manager/src/main/resources/application.properties

no changes added to commit (use "git add" and/or "git commit -a")

C:\Users\CC437236\ECM_comp_Filemanager>git push
git: 'credential-manager-core' is not a git command. See 'git --help'.
git: 'credential-manager-core' is not a git command. See 'git --help'.
Enumerating objects: 51, done.
Counting objects: 100% (36/36), done.
Delta compression using up to 4 threads
Compressing objects: 100% (14/14), done.
Writing objects: 100% (19/19), 4.11 KiB | 468.00 KiB/s, done.
Total 19 (delta 10), reused 0 (delta 0), pack-reused 0 (from 0)
remote: Analyzing objects... (19/19) (18 ms)
remote: Validating commits... (2/2) done (1 ms)
remote: Storing packfile... done (557 ms)
remote: Storing index... done (243 ms)
remote: Updating refs... done (143 ms)
To https://dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager
   57de1bd..1b40d8c  main -> main

C:\Users\CC437236\ECM_comp_Filemanager>git status
On branch main
Your branch is up to date with 'origin/main'.

Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
        modified:   file-manager/Dockerfile
        modified:   file-manager/src/main/resources/application.properties

no changes added to commit (use "git add" and/or "git commit -a")
