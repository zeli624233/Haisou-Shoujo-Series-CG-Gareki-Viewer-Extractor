# Windows EXE 打包说明

本项目的 EXE 必须在 Windows 环境打包。PyInstaller 不支持在 Linux 中交叉编译 Windows `.exe`。

## 本地打包

1. 安装 Python 3.10+，推荐 3.11。
2. 解压源码包。
3. 双击 `build_windows_release.bat`。
4. 打包完成后查看：

```text
release/HaisouShoujoViewerExtractor_Ver1.0_Windows/HaisouShoujoViewerExtractor.exe
release/HaisouShoujoViewerExtractor_Ver1.0_Windows.zip
```

## GitHub Actions 打包

1. 把源码提交到 GitHub。
2. 打开仓库 Actions。
3. 运行 `Build Windows EXE`。
4. 在 Artifacts 下载 `HaisouShoujoViewerExtractor_Ver1.0_Windows.zip`。
