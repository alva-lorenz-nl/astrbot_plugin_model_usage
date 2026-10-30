"""模型用量统计插件。

每次对话调用 LLM 后记一笔（模型、Token、耗时），落到本地 SQLite；
既可以在聊天里用 /用量 查询，也可以在 WebUI 插件页看图表。

设计要点：
- 热路径只往 asyncio 队列里塞一行，落盘交给后台 writer，绝不阻塞事件循环；
- 读操作全部丢到线程里执行，避免 sqlite 阻塞；
- 只依赖 AstrBot 官方接口（on_llm_response 钩子 + register_web_api）。
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from quart import request

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools, register

PLUGIN_NAME = "astrbot_plugin_model_usage"

SCHEMA = """
CREATE TABLE IF NOT EXISTS usage (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts            REAL    NOT NULL,
    day           TEXT    NOT NULL,
    provider_id   TEXT    DEFAULT '',
    model         TEXT    DEFAULT '',
    role          TEXT    DEFAULT '',
    input_tokens  INTEGER DEFAULT 0,
    cached_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    total_tokens  INTEGER DEFAULT 0,
    latency_ms    INTEGER DEFAULT 0,
    umo           TEXT    DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_usage_ts  ON usage(ts);
CREATE INDEX IF NOT EXISTS idx_usage_day ON usage(day);
CREATE INDEX IF NOT EXISTS idx_usage_model ON usage(model);
"""


@register(
    PLUGIN_NAME,
    "阿尔瓦·洛伦兹",
    "统计每个模型的调用次数与 Token 用量，带网页统计面板",
    "1.0.0",
)
class ModelUsagePlugin(Star):
    def __init__(self, context: Context, config: dict | None = None) -> None:
        super().__init__(context, config)
        self.config = config or {}

        try:
            self.data_dir = Path(StarTools.get_data_dir(PLUGIN_NAME))
        except Exception:  # 兜底：插件目录下自建
            self.data_dir = Path(__file__).resolve().parent / "data"
            self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "usage.db"

        self._queue: asyncio.Queue = asyncio.Queue()
        self._writer_task: asyncio.Task | None = None
        self._stopping = False

        self._init_db()
        self._register_web_apis()

    # ------------------------------------------------------------------ 生命周期

    async def initialize(self) -> None:
        self._writer_task = asyncio.create_task(self._writer_loop())
        logger.info(f"[model_usage] 已就绪，数据库：{self.db_path}")

    async def terminate(self) -> None:
        self._stopping = True
        await self._queue.put(None)
        if self._writer_task:
            try:
                await asyncio.wait_for(self._writer_task, timeout=5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._writer_task.cancel()

    # ------------------------------------------------------------------ 存储

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    def _insert_rows(self, rows: list[tuple]) -> None:
        conn = self._connect()
        try:
            conn.executemany(
                "INSERT INTO usage (ts, day, provider_id, model, role, input_tokens,"
                " cached_tokens, output_tokens, total_tokens, latency_ms, umo)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
            conn.commit()
        finally:
            conn.close()

    async def _writer_loop(self) -> None:
        while True:
            row = await self._queue.get()
            if row is None:
                break
            batch = [row]
            while True:
                try:
                    nxt = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if nxt is None:
                    self._stopping = True
                    break
                batch.append(nxt)
            try:
                await asyncio.to_thread(self._insert_rows, batch)
            except Exception as exc:  # noqa: BLE001
                logger.error(f"[model_usage] 写入失败：{exc}")
            if self._stopping:
                break

    # ------------------------------------------------------------------ 记录

    @filter.on_llm_request(priority=1000)
    async def _mark_start(self, event: AstrMessageEvent, req: Any) -> None:
        event.set_extra("_mu_t0", time.time())

    @filter.on_llm_response()
    async def _record(self, event: AstrMessageEvent, response: Any) -> None:
        try:
            usage = getattr(response, "usage", None)
            input_other = int(getattr(usage, "input_other", 0) or 0)
            input_cached = int(getattr(usage, "input_cached", 0) or 0)
            output = int(getattr(usage, "output", 0) or 0)

            provider_id, model = "", ""
            try:
                prov = self.context.get_using_provider(event.unified_msg_origin)
                if prov is not None:
                    meta = prov.meta()
                    provider_id = str(getattr(meta, "id", "") or "")
                    model = str(getattr(meta, "model", "") or "")
            except Exception:  # noqa: BLE001
                pass

            if not model and usage is None:
                return

            t0 = event.get_extra("_mu_t0")
            latency_ms = int((time.time() - t0) * 1000) if t0 else 0
            ts = time.time()
            day = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")

            self._queue.put_nowait(
                (
                    ts,
                    day,
                    provider_id,
                    model or "未知模型",
                    str(getattr(response, "role", "") or ""),
                    input_other,
                    input_cached,
                    output,
                    input_other + input_cached + output,
                    latency_ms,
                    event.unified_msg_origin or "",
                )
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[model_usage] 记录失败：{exc}")

    # ------------------------------------------------------------------ 查询

    def _summary_sync(self, days: int) -> dict:
        since = time.time() - days * 86400
        conn = self._connect()
        try:
            models = conn.execute(
                "SELECT model, provider_id, COUNT(*) AS cnt,"
                " SUM(input_tokens), SUM(cached_tokens), SUM(output_tokens),"
                " SUM(total_tokens), MAX(ts)"
                " FROM usage WHERE ts >= ?"
                " GROUP BY model, provider_id ORDER BY cnt DESC",
                (since,),
            ).fetchall()

            series = conn.execute(
                "SELECT day, COUNT(*), SUM(total_tokens) FROM usage"
                " WHERE ts >= ? GROUP BY day ORDER BY day",
                (since,),
            ).fetchall()

            total = conn.execute(
                "SELECT COUNT(*), SUM(total_tokens), SUM(input_tokens),"
                " SUM(cached_tokens), SUM(output_tokens) FROM usage WHERE ts >= ?",
                (since,),
            ).fetchone()

            today = datetime.now().strftime("%Y-%m-%d")
            today_cnt = conn.execute(
                "SELECT COUNT(*), SUM(total_tokens) FROM usage WHERE day = ?",
                (today,),
            ).fetchone()

            all_cnt = conn.execute("SELECT COUNT(*) FROM usage").fetchone()[0]
        finally:
            conn.close()

        return {
            "days": days,
            "total": {
                "count": total[0] or 0,
                "tokens": total[1] or 0,
                "input": total[2] or 0,
                "cached": total[3] or 0,
                "output": total[4] or 0,
            },
            "today": {"count": today_cnt[0] or 0, "tokens": today_cnt[1] or 0},
            "all_count": all_cnt,
            "models": [
                {
                    "model": r[0],
                    "provider": r[1],
                    "count": r[2],
                    "input": r[3] or 0,
                    "cached": r[4] or 0,
                    "output": r[5] or 0,
                    "tokens": r[6] or 0,
                    "last": r[7],
                }
                for r in models
            ],
            "series": [
                {"day": r[0], "count": r[1], "tokens": r[2] or 0} for r in series
            ],
        }

    async def _summary(self, days: int) -> dict:
        return await asyncio.to_thread(self._summary_sync, days)

    # ------------------------------------------------------------------ 聊天指令

    @filter.command("用量", alias={"modelusage", "mu"})
    async def usage_cmd(self, event: AstrMessageEvent):
        """查看模型调用次数：/用量 [天数]"""
        days = 1
        text = (event.message_str or "").strip()
        parts = text.split()
        if len(parts) > 1 and parts[-1].isdigit():
            days = max(1, min(int(parts[-1]), 365))

        data = await self._summary(days)
        total = data["total"]
        if not total["count"]:
            yield event.plain_result(f"最近 {days} 天还没有调用记录呢。")
            return

        lines = [
            f"📊 最近 {days} 天：共 {total['count']} 次调用，"
            f"{total['tokens']:,} tokens"
        ]
        for m in data["models"][:8]:
            lines.append(
                f"· {m['model']}：{m['count']} 次 / {m['tokens']:,} tokens"
            )
        if len(data["models"]) > 8:
            lines.append(f"……另有 {len(data['models']) - 8} 个模型")
        lines.append(f"今天 {data['today']['count']} 次，累计 {data['all_count']} 次")
        yield event.plain_result("\n".join(lines))

    # ------------------------------------------------------------------ Web API

    def _register_web_apis(self) -> None:
        apis = (
            ("summary", self.api_summary, ["GET"], "用量汇总"),
            ("recent", self.api_recent, ["GET"], "最近调用记录"),
        )
        for endpoint, handler, methods, desc in apis:
            try:
                self.context.register_web_api(
                    f"/{PLUGIN_NAME}/{endpoint}", handler, methods, desc
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"[model_usage] 注册 API {endpoint} 失败：{exc}")

    @staticmethod
    def _ok(data: Any = None) -> dict:
        return {"status": "ok", "message": None, "data": data if data is not None else {}}

    async def api_summary(self):
        try:
            days = int(request.args.get("days", 7))
        except (TypeError, ValueError):
            days = 7
        days = max(1, min(days, 365))
        data = await self._summary(days)
        return self._ok(data)

    async def api_recent(self):
        try:
            limit = int(request.args.get("limit", 50))
        except (TypeError, ValueError):
            limit = 50
        limit = max(1, min(limit, 500))
        rows = await asyncio.to_thread(self._recent_sync, limit)
        return self._ok({"rows": rows})

    def _recent_sync(self, limit: int) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT ts, model, role, input_tokens, cached_tokens,"
                " output_tokens, total_tokens, latency_ms, umo"
                " FROM usage ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        finally:
            conn.close()
        return [
            {
                "ts": r[0],
                "model": r[1],
                "role": r[2],
                "input": r[3],
                "cached": r[4],
                "output": r[5],
                "tokens": r[6],
                "latency_ms": r[7],
                "umo": r[8],
            }
            for r in rows
        ]
