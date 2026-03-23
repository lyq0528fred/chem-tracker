# 化工利润历史分位追踪

基于钢联（隆众资讯）数据，追踪 100+ 化工品种的利润历史分位，自动生成可视化网站。

## 🚀 部署步骤（一次性设置，约 10 分钟）

### 第一步：创建 GitHub 仓库

1. 登录 [github.com](https://github.com)，点击右上角 **+** → **New repository**
2. 仓库名填 `chem-tracker`（或你喜欢的名字）
3. 选 **Private**（数据不公开）或 **Public**
4. 点击 **Create repository**

### 第二步：上传本项目文件

方法A（网页上传，最简单）：
1. 在新建的仓库页面，点击 **uploading an existing file**
2. 把本 ZIP 包解压后，将所有文件拖入上传
3. 点击 **Commit changes**

方法B（命令行）：
```bash
git clone https://github.com/你的用户名/chem-tracker.git
cd chem-tracker
# 复制本项目所有文件进来
git add .
git commit -m "初始化"
git push
```

### 第三步：开启 GitHub Pages

1. 进入仓库 → **Settings** → 左侧 **Pages**
2. Source 选 **GitHub Actions**
3. 保存

### 第四步：上传 Excel 数据（触发首次生成）

1. 进入仓库的 `data/` 目录
2. 点击 **Add file** → **Upload files**
3. 上传你的 Excel 文件（文件名需包含"化工"或"能源"）：
   - `化工利润.xlsx`
   - `能源毛利.xlsx`
4. 点击 **Commit changes**

⏳ 等待约 3-5 分钟，GitHub Actions 自动处理数据并部署。

### 第五步：访问网站

部署完成后，在 **Settings → Pages** 页面可以看到网站地址，格式为：
```
https://你的用户名.github.io/chem-tracker/
```

---

## 📅 日常更新数据

每次更新只需：

1. 进入仓库 `data/` 目录
2. 上传新的 Excel 文件（**同名覆盖**旧文件）
3. Commit → 等 3-5 分钟 → 网站自动更新

也可以在 **Actions** 标签页手动触发（点击 **Run workflow**）。

---

## 📁 文件结构

```
chem-tracker/
├── .github/
│   └── workflows/
│       └── build.yml          # 自动化流程配置
├── data/
│   ├── 化工利润.xlsx           # ← 放你的数据文件
│   └── 能源毛利.xlsx
├── scripts/
│   ├── update_tracker.py      # 数据处理脚本
│   └── template.html          # 网站模板
└── README.md
```

---

## ❓ 常见问题

**Q: Actions 报错怎么办？**
进入 **Actions** 标签页 → 点击失败的任务 → 查看日志，把错误信息发给 Claude 即可。

**Q: 可以设置私密访问吗？**
GitHub Pages Public 仓库任何人可访问；Private 仓库需要付费 GitHub Pro 才能部署 Pages。如果需要私密，推荐改用 Cloudflare Pages（同样免费，支持访问控制）。

**Q: 怎么添加新品种？**
新的 Excel 文件里如果有新品种，会自动出现在网站上。品类分类需要手动在 `scripts/update_tracker.py` 的 CAT 字典里添加。
