CC437236@V105P10PRA0496 MINGW64 ~/dev/ECM_comp_Filemanager (devops)
$ git add .

CC437236@V105P10PRA0496 MINGW64 ~/dev/ECM_comp_Filemanager (devops)
$ git commit -m "key vault"
[devops 1ab2ffd] key vault
 Committer: Chekkirathi <VinodChe@nedbank.co.za>
Your name and email address were configured automatically based
on your username and hostname. Please check that they are accurate.
You can suppress this message by setting them explicitly:

    git config --global user.name "Your Name"
    git config --global user.email you@example.com

After doing this, you may fix the identity used for this commit with:

    git commit --amend --reset-author

 2 files changed, 26 insertions(+), 26 deletions(-)

CC437236@V105P10PRA0496 MINGW64 ~/dev/ECM_comp_Filemanager (devops)
$ git status
On branch devops
Your branch and 'origin/devops' have diverged,
and have 3 and 9 different commits each, respectively.
  (use "git pull" if you want to integrate the remote branch with yours)

nothing to commit, working tree clean

CC437236@V105P10PRA0496 MINGW64 ~/dev/ECM_comp_Filemanager (devops)
$ git pull origin devops --rebase
git: 'credential-manager-core' is not a git command. See 'git --help'.
git: 'credential-manager-core' is not a git command. See 'git --help'.
From https://dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Filemanager
 * branch            devops     -> FETCH_HEAD
Auto-merging src/main/resources/application.properties
CONFLICT (content): Merge conflict in src/main/resources/application.properties
error: could not apply 1ab2ffd... key vault
hint: Resolve all conflicts manually, mark them as resolved with
hint: "git add/rm <conflicted_files>", then run "git rebase --continue".
hint: You can instead skip this commit: run "git rebase --skip".
hint: To abort and get back to the state before "git rebase", run "git rebase --abort".
hint: Disable this message with "git config advice.mergeConflict false"
Could not apply 1ab2ffd... key vault

CC437236@V105P10PRA0496 MINGW64 ~/dev/ECM_comp_Filemanager (devops|REBASE 3/3)
