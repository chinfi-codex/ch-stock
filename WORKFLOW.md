# ch-stock 协作与上线流程（极简版）

适用场景：一人公司（老板 + 助手）协作开发。目标是**简单、可控、可回滚**。

## 流程

1. 助手在服务器从 `main` 拉新分支开发：`task/xxx`
2. 助手提交并 push 到 GitHub（不直接改 `main`）
3. 老板在本地拉该分支测试并确认是否可上线
4. 老板确认后，助手将分支合并到 `main`
5. 服务器只从 `main` 部署，生产环境永远只运行 `main`

## 三条硬规则

1. 不在 `main` 直接开发
2. 未经老板确认，不合并到 `main`
3. 生产部署只认 `main`

## 常用命令

### 助手开发
```bash
git checkout main
git pull origin main
git checkout -b task/<name>
# coding...
git add .
git commit -m "feat: ..."
git push -u origin task/<name>
```

### 老板本地测试
```bash
git fetch origin
git checkout task/<name>
# test...
```

### 确认后上线
```bash
git checkout main
git pull origin main
git merge --no-ff task/<name>
git push origin main
# deploy from main
```

---
最后更新：2026-02-14
