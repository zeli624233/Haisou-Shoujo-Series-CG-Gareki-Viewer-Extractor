
 ![image](https://github.com/zeli624233/Haison-Shoujo-Series-CG-Gareki-Viewer-Extractor/blob/main/Logo3.png)
## 简介

- 这是一个用于小e社《廃村少女》系列 CG/立绘资源查看与导出的开源项目，支持读取 LSF 与 PNG 资源，组合预览并导出当前组合或批量导出组合图。

## 目前支持

- 1.廃村少女 ～妖し惑ひの籠の郷～（原版）
- 2.廃村少女 外伝 ～嬌絡夢現～
- 3.廃村少女 番外 ～籠愛拾遺～
- 4.廃村少女［弐］ ～陰り誘う秘姫の匣～

> 说明：废村1的 Extract 内容未做完整测试，不保证支持。
>
> 如果不出意外，这个工具支持的系列我会继续更新下去，只要你小e社继续做好废村系列！。
> 当然，有一天我跑路了，弃坑了，或者小e社发脑瘫，搞臭了本系列名声，Github源代码我也发布了，有能力的人可以接手本项目，继续更新下去。

## 额外（基本）支持
- 1.悠刻のファムファタル
- 2.戦巫〈センナギ〉―穢れた契りと神ころも―
- 3.姫と艶欲のインペリウム
- 4.姫と婬欲のテスタメント
- 5.姫と穢欲のサクリファイス
> 这里的游戏我玩的不多，我只能保证：软件对bg和ev目录的识别和准确率基本没问题。
>
> gfx/face/adv和st目录，功能上没问题，但逻辑上有点乱，只要你花点时间，还是能得到你想要的结果。
## 目录支持
- bg
- ev
- gfx/face/adv
- st
## 主要功能

- LSF + PNG 组合预览。
- 导出当前 PNG。
- 批量导出当前组合。
- 批量导出多线程支持：2、4、6、8、12、16....
- 批量导出进度条、百分比、总数量、预计剩余时间。
- 主窗口启动自动居中。
- 二级窗口与主程序左上角对齐。
- 程序多线程运行支持。
- 优化了软件的内存使用量，会自动释放空闲内存。
- 支持自动化测试，快速检查软件对CG，人物立绘的识别是否正确。
- 组合选项新增“一键清空”功能，鼠标指针在该位置，右击可设置清空选项。
## 使用说明
 ![image](https://github.com/zeli624233/Haison-Shoujo-Series-CG-Gareki-Viewer-Extractor/blob/main/%E4%BD%BF%E7%94%A8%E8%AF%B4%E6%98%8E2.png)
  

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

## 许可证

本项目使用 MIT License。详见 [LICENSE](LICENSE)。
# 感谢：
该项目的解包离不开Chenx221大佬（ https://github.com/Chenx221 ）的EscudeTools工具( https://github.com/Chenx221/EscudeTools ),解包该游戏十分好用，感谢大佬的开源！。

