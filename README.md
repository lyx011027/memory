# 测试代码使用

# 测试流程

1. 修改 `config.py` 中的 `DATA_SOURCE_PATH` 为原始数据路径，
2. 运行 `bash run.sh`
3. 若 `split.py` 运行过程中发生OOM，修改 `config.py` 中的 `BATCH_NUM` 为更大值