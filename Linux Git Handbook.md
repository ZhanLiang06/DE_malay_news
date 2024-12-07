# GitHub Linux Handbook
Note: Everytime you working on the project, make sure you are on your branch, use `git branch` to see which branch are you currently in
## 1.0 Git Installation on Linux
currently, you at linux cli as student 
### 1. install git
- `(type -p wget >/dev/null || (sudo apt update && sudo apt-get install wget -y)) \
	&& sudo mkdir -p -m 755 /etc/apt/keyrings \
	&& wget -qO- https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo tee /etc/apt/keyrings/githubcli-archive-keyring.gpg > /dev/null \
	&& sudo chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
	&& echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
	&& sudo apt update \
	&& sudo apt install gh -y`

### 2. Login git
- `gh auth login`

### 3. Clone repo
- `gh repo clone ZhanLiang06/DE_malay_news`

### 4. create branch
- `git checkout -b {insert any name here}`
- `git push -u origin {branch_name}`

### 5. coding
here you make changes on the files like jupyter notebook

**Note: Everytime you working on the project, make sure you are on your branch, use `git branch` to see which branch are you currently in**

### 6. Now you wish to update the contents in your branch (which is commit changes)
- `git add {files that you have made changes}`
- `git commit`

Tips: you can execute git commit first to see what files have you changed

### 7. Go to github
### 8. Navigate to your branch
### 9. Make pull request (this is to update the main repository)

### 10. then wait for review and merge

## 2.0 To get updated version into your main and branch
### 1. switch to the main branch first
- `git switch main`
### 2. fetch and pull all the commit to update your local main branch
- `git fetch`

- `git pull`
### 3. switch back to your branch
- `git switch {your branch name}`

### 4. merge your branch with origin/main, and update the remote branch
- `git merge origin/main`

- `git push`

### Please do note that we have local and remote repository
In general sense, we can say that:
- local repo (the folders inside your computer linux)
- remote repo (the folders you see on github website)
Your linux main and branch in your PC will not automatically update to the most recent commit from this website (if you no perform fetch and pull at your linux environment)

### Note: Everytime you working on the project, make sure you are on your branch, use `git branch` to see which branch are you currently in


