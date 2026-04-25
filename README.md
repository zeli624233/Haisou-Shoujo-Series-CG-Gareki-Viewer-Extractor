# Haison Shoujo Series CG & Gareki Viewer & Extractor
 ![image](https://github.com/zeli624233/Haison-Shoujo-Series-CG-Gareki-Viewer-Extractor/blob/main/%E4%BD%BF%E7%94%A8%E8%AF%B4%E6%98%8E2.png)
## 简介

这是一个用于《廃村少女》系列 CG/立绘资源查看与导出的项目，支持读取 LSF 与 PNG 资源，组合预览并导出当前组合或批量导出组合图。

## 目前支持

- 廃村少女 ～妖し惑ひの籠の郷～（原版）
- 廃村少女 外伝 ～嬌絡夢現～
- 廃村少女 番外 ～籠愛拾遺～
- 廃村少女［弐］ ～陰り誘う秘姫の匣～

> 说明：废村1的 Extract 内容未做完整测试，不保证支持。
## 目录支持
- bg
- ev
- gfx/face/adv
- st
## 主要功能

- LSF + PNG 组合预览。
- 导出当前 PNG。
- 批量导出当前组合。
- 批量导出多线程支持：2、4、6、8、12、16，默认 4。
- 批量导出进度条、百分比、总数量、预计剩余时间。
- 主窗口启动自动居中。
- 二级窗口与主程序左上角对齐。
- 程序多线程运行支持。
- 优化了软件的内存使用量，会自动释放空闲内存。
- 支持自动化测试，快速检查软件对CG，人物立绘的识别是否正确。

## 运行源码版

### 环境要求

- Python 3.10 或更新版本
- Windows 10/11 推荐

### 安装依赖

```bash
pip install -r requirements.txt
```

### 启动

```bash
python main.py
```

也可以在 Windows 下双击：

```text
run_windows.bat
```

## 打包为 Windows EXE

源码包已包含 PyInstaller 配置。

### 方法一：本地打包

```bash
pip install -r requirements.txt
pip install -r requirements-build.txt
build_windows.bat
```

生成结果位于：

```text
dist/HaisonShoujoViewerExtractor/
```

其中主程序为：

```text
HaisonShoujoViewerExtractor.exe
```

### 方法二：GitHub Actions 自动打包

把源码提交到 GitHub 后，进入仓库的 **Actions** 页面，运行 `Build Windows EXE` 工作流，完成后可在 Actions Artifacts 中下载 Windows 版程序包。

## 许可证

本项目使用 MIT License。详见 [LICENSE](LICENSE)。
# 感谢：
该项目的解包离不开Chenx221大佬（ https://github.com/Chenx221 ）的EscudeTools工具( https://github.com/Chenx221/EscudeTools ),解包该游戏十分好用，感谢大佬的开源！。

