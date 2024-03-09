# TigerBot-mnbvc

## 项目描述

- 本项目目的是将TigerBot开源数据集清洗为MNBVC的标准文本格式，数据源链接：[TigerBot](https://github.com/TigerResearch/TigerBot?tab=readme-ov-file#%E5%BC%80%E6%BA%90%E6%95%B0%E6%8D%AE%E9%9B%86)

## 环境

1. 下载本项目
```
git clone TigerBot-mnbvc
```
2. 进入目录并安装依赖
```
cd TigerBot-mnbvc
pip install -r requirements.txt
```

## 用法

通过以下命令将文件夹中文件清洗到目标文件夹中：

```shell
 python .\tiger_bot_extract.py [源文件夹] [目标文件夹] -m [模式]
```

- `源文件夹`：TigerBot数据集文件夹
- `目标文件夹`：清洗后的文件夹
- `模式`：可选，`-m`后面可以跟`domain`或`fine-tuning`或`pretraining`。`domain`模式用于清洗领域数据，`fine-tuning`模式用于清洗微调数据，`pretraining`模式用于清洗预训练数据。

## 相关项目

[MNBVC](https://github.com/esbatmop/MNBVC)
[WikiHowQAExtractor-mnbvc](https://github.com/wanicca/WikiHowQAExtractor-mnbvc)
[ShareGPTQAExtractor-mnbvc](https://github.com/pany8125/ShareGPTQAExtractor-mnbvc)
[deduplication_mnbvc](https://github.com/aplmikex/deduplication_mnbvc)
