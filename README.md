# 模型用量统计 astrbot_plugin_model_usage

统计每次对话调用的**模型、调用次数、Token 用量、耗时**，按模型 / 按天汇总。
数据全部落在本地 SQLite，不依赖任何中转站或供应商的查询接口。

## 安装

1. 把整个 `astrbot_plugin_model_usage` 目录放进 `data/plugins/`。
2. 重启 AstrBot，或在 WebUI「插件管理」里重载。
3. WebUI → 插件页 → **模型用量统计**，即可看到面板。

## 使用

聊天里：

- `/用量` — 最近 1 天
- `/用量 7` — 最近 7 天
- `/mu`、`/modelusage` — 同义指令

面板里可切换 今天 / 7 天 / 30 天 / 90 天，查看按模型汇总与最近 50 次调用明细。

## 数据

- 位置：`data/plugin_data/astrbot_plugin_model_usage/usage.db`
- 表 `usage`：`ts, day, provider_id, model, role, input_tokens, cached_tokens, output_tokens, total_tokens, latency_ms, umo`
- 想清零直接停服删掉这个 db 文件即可。

## 说明

- 统计口径是「一次对话调用」，走的是 AstrBot 的 `on_llm_response` 钩子；工具循环内部的多步请求会合并为一次记录。
- 若供应商不回传 usage，Token 会记 0，但调用次数仍然准确。
- 写入走后台队列，不会拖慢消息响应。
