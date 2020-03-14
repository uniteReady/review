# git

## git 关联到远程仓库
```
git init

git add .

git commit -m "这里填写本地修改的注释内容"

git remote add origin [这里是git仓库地址]

git pull origin master --allow-unrelated-histories

git branch --set-upstream-to=origin/master master

git push -u origin master -f
    
```

## git 去除不需要提交的文件目录
```
git rm -r --cached .idea
rm -r .idea
git add .
git status
git commit -m 'other:remove .idea'
git push
```
