# -*- coding: utf-8 -*-
"""AstrBot 群聊冷场唤醒插件。

插件只负责在允许的轮次内判断什么时候主动开口，以及生成这一条唤醒消息；
后续是否回复、如何回复仍交给 AstrBot 原有插件链处理。
"""
from __future__ import annotations

import asyncio
import copy
import json
import random
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from astrbot.api import AstrBotConfig, ToolSet, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import At, Plain, Reply
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.platform.astrbot_message import AstrBotMessage, MessageMember
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.platform.platform_metadata import PlatformMetadata

STATE_FILE = "state.json"
MODE_TOOLS = "轻量工具调用"
MODE_TEXT = "纯文本生成"
MODE_PIPELINE = "标准消息链"
WAKE_EVENT_TEXT = "群聊唤醒"
WAKEUP_ECHO_SECONDS = 120


def _looks_like_upstream_llm_error(text: Any) -> bool:
    """保留超时、带模块名异常及群聊错误前缀。"""
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()[:2000]
    if not cleaned:
        return False
    lowered = cleaned.lower()
    normalized = re.sub(r"[^a-z0-9\u4e00-\u9fff_]+", " ", lowered).strip()
    compact = re.sub(r"[^a-z0-9\u4e00-\u9fff_]+", "", lowered)

    direct_markers = (
        "allchatmodelsfailed",
        "allllmprovidersfailed",
        "allavailablechatmodelsareunavailable",
        "promptcouldnotbesubmitted",
        "promptwasnotsubmitted",
        "promptcontainssensitivewords",
        "unabletosubmitrequest",
        "providerapierrorupstreamreturnedaninternalfailuremessage",
    )
    if any(marker in compact for marker in direct_markers):
        return True

    google_policy_features = (
        "generativeaiprohibitedusepolicy",
        "tryrephrasingtheprompt",
        "sensitivewords",
    )
    if sum(feature in compact for feature in google_policy_features) >= 2:
        return True

    if "functiondeclaration" in compact and any(
        marker in compact
        for marker in (
            "schemadidntspecify",
            "invalidrequest",
            "badrequest",
            "errorcode400",
        )
    ):
        return True

    if lowered.startswith("traceback (most recent call last):"):
        return True
    if re.match(
        r"^(?:(?:模型|api|provider|函数工具|工具)\s*调用失败|生成回复时发生错误)\s*[:：]",
        cleaned,
        flags=re.IGNORECASE,
    ):
        return True

    if re.match(
        r"^(?:api connection error|api status error|authentication error|"
        r"permission denied error|rate limit error|internal server error)\s*(?:[:：-]|$)",
        lowered,
    ):
        return True
    if lowered.startswith((
        "error occurred while processing agent request:",
        "error occurred during ai execution. error type:",
    )):
        return True

    error_classes = (
        "badrequesterror",
        "apiconnectionerror",
        "apistatuserror",
        "authenticationerror",
        "permissiondeniederror",
        "ratelimiterror",
        "notfounderror",
        "internalservererror",
        "apitimeouterror",
    )
    error_class = next((name for name in error_classes if name in compact), "")
    if error_class:
        structured_signal = any(
            marker in compact
            for marker in (
                "errorcode",
                "statuscode",
                "httpstatus",
                "requestid",
                "invalidrequest",
            )
        )
        leading_error_class = bool(
            re.match(
                rf"^(?:(?:llm|provider|api)\s+(?:response\s+)?error\s+|error\s+)?{error_class}\b",
                normalized,
            )
        )
        if structured_signal or (
            leading_error_class
            and (":" in cleaned[:100] or "：" in cleaned[:100] or len(cleaned) <= 48)
        ):
            return True
        if re.match(rf"^(?:[a-z_]+\.)+{error_class}\s*(?:[:：]|$)", lowered):
            return True

    if (
        lowered.startswith(("{", "["))
        and any(marker in lowered for marker in ('"error"', "'error'"))
        and any(
            marker in compact
            for marker in ("invalid_request", "badrequest", "permissiondenied", "ratelimit")
        )
    ):
        return True

    return False


class _SyntheticWakeEvent(AstrMessageEvent):
    """为后台唤醒提供群会话上下文，标准链子类另行投递。"""

    def __init__(
        self, *, context: Context, umo: str, message: str,
        platform_meta: PlatformMetadata | None = None,
        self_id: str = "", group_id: str = "", group_name: str = "",
        sender_id: str = "",
    ) -> None:
        session = MessageSession.from_str(umo)
        platform_meta = platform_meta or PlatformMetadata(
            name=session.platform_id,
            description="GroupWakeup",
            id=session.platform_id,
        )
        message_obj = AstrBotMessage()
        message_obj.type = session.message_type
        message_obj.self_id = self_id or session.session_id
        message_obj.session_id = session.session_id
        message_obj.group_id = group_id or session.session_id
        if message_obj.group is not None:
            message_obj.group.group_name = group_name
        # 不编号，避免后台工具回调拿去引用一条平台上不存在的消息。
        message_obj.message_id = ""
        message_obj.sender = MessageMember(
            user_id=sender_id or session.session_id,
            nickname="GroupWakeup",
        )
        message_obj.message = [Plain(message)]
        message_obj.message_str = message
        message_obj.raw_message = message
        message_obj.timestamp = int(time.time())

        self.umo = umo
        super().__init__(message, message_obj, platform_meta, session.session_id)
        self.context_obj = context
        self.is_at_or_wake_command = True
        self.is_wake = True
        self.set_extra("group_wakeup", True)

    async def send(self, message: MessageChain) -> None:
        if message is None:
            return
        sent = await self.context_obj.send_message(self.umo, message)
        if sent is False:
            raise RuntimeError("未找到可发送唤醒消息的平台实例")
        await super().send(message)


class _PipelineWakeEvent(_SyntheticWakeEvent):
    """标准处理链的后台事件；发送出口受控，生命周期结束由调度器通知。"""

    def __init__(self, *, plugin: GroupWakeupPlugin, group: dict, prompt: str, meta: PlatformMetadata) -> None:
        super().__init__(
            context=plugin.context, umo=group["umo"], message=WAKE_EVENT_TEXT,
            platform_meta=meta, self_id=group["self_id"],
            group_id=group["group_id"], group_name=group.get("group_name", ""),
            sender_id="0",
        )
        self.plugin = plugin
        self.group = group
        self.task_prompt = prompt
        self.expected_activity = float(group["last_activity"])
        self.finished = asyncio.get_running_loop().create_future()
        self.send_lock = asyncio.Lock()
        self.injected_requests: list[Any] = []
        self.error = ""
        self.closed = False
        self.delivered = False
        self.message_obj.message = [At(qq=self.get_self_id()), Plain(WAKE_EVENT_TEXT)]
        self.message_obj.raw_message = {"group_wakeup": True}
        self.set_extra("_api_key_allow_admin_role", False)
        self.set_extra("enable_streaming", False)
        self.set_extra("enable_reasoning", False)

    @property
    def unified_msg_origin(self) -> str:
        # 唤醒属于原群会话，不能被 unique_session 改成虚拟发送者的会话。
        return self.umo

    @property
    def role(self) -> str:
        return "member"

    @role.setter
    def role(self, value: str) -> None:
        # 旧版唤醒检查不认识权限隔离标记，后台事件始终保持普通成员权限。
        pass

    def close(self) -> None:
        self.closed = True
        self.stop_event()
        if not self.finished.done():
            self.finished.set_result(None)

    def cleanup_temporary_local_files(self) -> None:
        # AstrBot 4.22.0 起 PipelineScheduler.execute 在 finally 中调用此方法。
        failure = sys.exc_info()[1]
        if failure is not None and not isinstance(failure, asyncio.CancelledError):
            self.error = self.error or f"消息链异常：{failure}"
        try:
            super().cleanup_temporary_local_files()
        finally:
            self.close()

    async def send(self, message: MessageChain) -> None:
        if message is not None:
            await self.plugin._send_pipeline_chain(self, message)

    async def send_streaming(self, generator, use_fallback: bool = False) -> None:
        # 本模式关闭实时流式；其他插件自行产生的流也先收齐，再检查后发送。
        chain = []
        async for message in generator:
            if self.closed or self.error:
                break
            if message is not None and getattr(message, "type", "") not in {"break", "reasoning"}:
                chain.extend(message.chain)
        if chain:
            await self.send(MessageChain(chain))


class GroupWakeupPlugin(Star):
    """群聊冷场唤醒：只观察、只定时、只主动发送。"""

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context)
        self.config = config
        self.data_dir = Path(StarTools.get_data_dir())
        self.state_path = self.data_dir / STATE_FILE
        self._state: dict[str, Any] = {"groups": {}}
        self._task: asyncio.Task[None] | None = None
        self._pipeline_events: dict[str, _PipelineWakeEvent | None] = {}
        self._wake_tasks: dict[str, asyncio.Task[None]] = {}

    async def initialize(self) -> None:
        self._load_state()
        self._task = asyncio.create_task(self._patrol_loop())
        logger.info(
            "[群聊唤醒] 已启动，巡检间隔 %s 秒",
            self._cfg_int("check_interval_seconds", 60, lo=10, hi=3600),
        )

    async def terminate(self) -> None:
        for event in list(self._pipeline_events.values()):
            if event is not None:
                event.close()
        wake_tasks = list(self._wake_tasks.values())
        self._wake_tasks.clear()
        for task in wake_tasks:
            task.cancel()
        if wake_tasks:
            await asyncio.gather(*wake_tasks, return_exceptions=True)
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._save_state()

    def _cfg_int(
        self,
        key: str,
        default: int,
        *,
        lo: int | None = None,
        hi: int | None = None,
    ) -> int:
        try:
            value = int(self.config.get(key, default))
        except (TypeError, ValueError):
            value = default
        if lo is not None:
            value = max(lo, value)
        if hi is not None:
            value = min(hi, value)
        return value

    def _cfg_bool(self, key: str, default: bool = False) -> bool:
        value = self.config.get(key, default)
        return bool(value)

    def _cfg_str(self, key: str, default: str = "") -> str:
        value = self.config.get(key, default)
        return str(value or "").strip()

    def _cfg_list(self, key: str) -> list[str]:
        value = self.config.get(key, [])
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]

    def _generation_mode(self) -> str:
        mode = self._cfg_str("generation_mode", MODE_TOOLS)
        if mode not in {MODE_TOOLS, MODE_TEXT, MODE_PIPELINE}:
            raise RuntimeError("请选择中文生成模式：轻量工具调用、纯文本生成或标准消息链")
        return mode

    @staticmethod
    def _safe_str(func: Callable[[], Any], default: str = "") -> str:
        try:
            value = func()
        except Exception:
            return default
        return str(value or "").strip()

    def _load_state(self) -> None:
        try:
            if self.state_path.exists():
                data = json.loads(self.state_path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    self._state = data
        except Exception as exc:
            logger.warning("[群聊唤醒] 状态文件读取失败，使用空状态：%s", exc)
            self._state = {"groups": {}}
        if not isinstance(self._state.get("groups"), dict):
            self._state["groups"] = {}

    def _save_state(self) -> None:
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = self.state_path.with_suffix(".tmp")
            tmp_path.write_text(
                json.dumps(self._state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp_path.replace(self.state_path)
        except Exception as exc:
            logger.warning("[群聊唤醒] 状态保存失败：%s", exc)

    def _group(self, umo: str) -> dict[str, Any]:
        groups = self._state.setdefault("groups", {})
        group = groups.get(umo)
        if not isinstance(group, dict):
            group = {}
            groups[umo] = group
        group.setdefault("umo", umo)
        group.setdefault("group_id", "")
        group.setdefault("group_name", "")
        group.setdefault("self_id", "")
        group.setdefault("last_activity", 0.0)
        group.setdefault("last_attempt_at", 0.0)
        group.setdefault("topics_today", 0)
        group.setdefault("today", "")
        group.setdefault("recent_topics", [])
        group.setdefault("history", [])
        group.setdefault("last_inbound_message_id", "")
        group.setdefault("wake_after", 0.0)
        group.setdefault("wake_after_activity", 0.0)
        group.setdefault("wake_after_delay_max", 0)
        group.setdefault("wake_after_silence_minutes", 0)
        group.setdefault("quiet_resume_at", 0.0)
        group.setdefault("breaker_tripped", False)
        group.setdefault("breaker_reason", "")
        group.setdefault("breaker_tripped_at", 0.0)
        return group

    @staticmethod
    def _day_key(now: float) -> str:
        return datetime.fromtimestamp(now).strftime("%Y-%m-%d")

    def _roll_daily(self, group: dict[str, Any], now: float) -> None:
        day = self._day_key(now)
        if group.get("today") != day:
            group["today"] = day
            group["topics_today"] = 0

    @staticmethod
    def _session_id(umo: str) -> str:
        parts = umo.split(":", 2)
        return parts[2] if len(parts) == 3 else ""

    def _append_history(
        self,
        group: dict[str, Any],
        *,
        sender_id: str,
        sender_name: str,
        text: str,
        is_bot: bool,
        timestamp: float,
    ) -> None:
        if not self._cfg_bool("history_enabled", True):
            return
        size = self._cfg_int("history_size", 20, lo=0, hi=200)
        if size <= 0 or not text:
            return
        history = group.setdefault("history", [])
        if not isinstance(history, list):
            history = []
            group["history"] = history
        history.append(
            {
                "sender_id": sender_id,
                "sender_name": sender_name,
                "text": text,
                "is_bot": is_bot,
                "timestamp": timestamp,
            }
        )
        if len(history) > size:
            del history[:-size]

    @staticmethod
    def _safe_bool(func: Callable[[], Any], default: bool = False) -> bool:
        try:
            return bool(func())
        except Exception:
            return default

    @staticmethod
    def _event_group_name(event: AstrMessageEvent) -> str:
        group_obj = getattr(event.message_obj, "group", None)
        return str(getattr(group_obj, "group_name", "") or "").strip()

    @filter.event_message_type(
        filter.EventMessageType.GROUP_MESSAGE, priority=sys.maxsize
    )
    async def observe_group_message(self, event: AstrMessageEvent) -> None:
        if isinstance(event, _PipelineWakeEvent):
            if event.closed:
                event.stop_event()
            return
        if (
            self._safe_bool(lambda: event.get_extra("group_wakeup"))
            or self._safe_str(event.get_sender_name) == "GroupWakeup"
        ):
            return
        try:
            umo = str(event.unified_msg_origin)
            if not umo:
                return
            now = time.time()
            group = self._group(umo)
            self._roll_daily(group, now)

            group["group_id"] = self._safe_str(event.get_group_id)
            group["group_name"] = self._event_group_name(event)

            sender_id = self._safe_str(event.get_sender_id)
            self_id = self._safe_str(event.get_self_id)
            if self_id:
                group["self_id"] = self_id
            is_bot = bool(sender_id and sender_id == self_id)
            # 平台回显本插件刚发出的唤醒时，不能把它当成真实活动。
            last_attempt_at = float(group.get("last_attempt_at", 0.0) or 0.0)
            is_wakeup_echo = is_bot and 0 <= now - last_attempt_at <= WAKEUP_ECHO_SECONDS
            if not is_wakeup_echo:
                group["last_activity"] = now
                group["last_attempt_at"] = 0.0
                self._clear_wake_after(group)
                group["quiet_resume_at"] = 0.0


            message_id = str(getattr(event.message_obj, "message_id", "") or "")
            if message_id and message_id == group.get("last_inbound_message_id"):
                return
            group["last_inbound_message_id"] = message_id

            text = str(getattr(event, "message_str", "") or "").strip()
            if not text:
                text = self._safe_str(event.get_message_outline)
            self._append_history(
                group,
                sender_id=sender_id,
                sender_name=self._safe_str(event.get_sender_name),
                text=text,
                is_bot=is_bot,
                timestamp=now,
            )
            self._save_state()
        except Exception as exc:
            logger.warning("[群聊唤醒] 记录群消息失败：%s", exc)

    @staticmethod
    def _result_text(event: AstrMessageEvent) -> str:
        try:
            result = event.get_result()
            chain = getattr(result, "chain", None) or []
            return "".join(
                str(getattr(component, "text", "") or "")
                for component in chain
            ).strip()
        except Exception:
            return ""

    @filter.after_message_sent()
    async def after_message_sent(self, event: AstrMessageEvent) -> None:
        if (
            isinstance(event, _PipelineWakeEvent)
            or self._safe_bool(lambda: event.get_extra("group_wakeup"))
            or self._safe_str(event.get_sender_name) == "GroupWakeup"
        ):
            return
        try:
            if self._safe_bool(event.is_private_chat, True):
                return
            umo = str(event.unified_msg_origin)
            if not umo:
                return
            now = time.time()
            group = self._group(umo)
            last_attempt_at = float(group.get("last_attempt_at", 0.0) or 0.0)
            # 标准链路的人工事件已被识别；这里处理直连唤醒的平台发送回执/回显。
            if not 0 <= now - last_attempt_at <= WAKEUP_ECHO_SECONDS:
                group["last_activity"] = now
                group["last_attempt_at"] = 0.0
                self._clear_wake_after(group)
                group["quiet_resume_at"] = 0.0

            text = self._result_text(event)
            if _looks_like_upstream_llm_error(text):
                self._save_state()
                return

            if group.get("breaker_tripped"):
                group["breaker_tripped"] = False
                group["breaker_reason"] = ""
                group["breaker_tripped_at"] = 0.0
                logger.info("[群聊唤醒] 观察到正常回复，已恢复群 %s 的唤醒", umo)

            self._append_history(
                group,
                sender_id=self._safe_str(event.get_self_id),
                sender_name="Bot",
                text=text,
                is_bot=True,
                timestamp=now,
            )
            self._save_state()
        except Exception as exc:
            logger.warning("[群聊唤醒] 记录发送后状态失败：%s", exc)

    def _group_allowed(self, group: dict[str, Any]) -> bool:
        group_id = str(group.get("group_id", "") or "").strip()
        session_id = self._session_id(str(group.get("umo", "")))
        blacklist = self._cfg_list("blacklist")
        if group_id and group_id in blacklist:
            return False
        if session_id and session_id in blacklist:
            return False

        whitelist = self._cfg_list("whitelist")
        if not whitelist:
            return True
        return (bool(group_id and group_id in whitelist)) or (
            bool(session_id and session_id in whitelist)
        )

    @staticmethod
    def _parse_time(value: str, default: int) -> int:
        match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", value)
        if not match:
            return default
        return int(match.group(1)) * 60 + int(match.group(2))

    def _in_quiet_time(self, now: float) -> bool:
        if not self._cfg_bool("quiet_time_enabled", True):
            return False
        start = self._parse_time(self._cfg_str("quiet_time_start", "23:00"), 23 * 60)
        end = self._parse_time(self._cfg_str("quiet_time_end", "07:00"), 7 * 60)
        if start == end:
            return False
        now_dt = datetime.fromtimestamp(now)
        minute = now_dt.hour * 60 + now_dt.minute
        if start < end:
            return start <= minute < end
        return minute >= start or minute < end

    def _quiet_end_timestamp(self, now: float) -> float:
        end = self._parse_time(
            self._cfg_str("quiet_time_end", "07:00"), 7 * 60
        )
        current = datetime.fromtimestamp(now)
        candidate = current.replace(
            hour=end // 60,
            minute=end % 60,
            second=0,
            microsecond=0,
        )
        if candidate.timestamp() <= now:
            candidate += timedelta(days=1)
        return candidate.timestamp()

    def _can_wake(self, group: dict[str, Any], now: float) -> bool:
        umo = str(group.get("umo", "") or "")
        if umo in self._pipeline_events:
            return False
        wake_task = self._wake_tasks.get(umo)
        if wake_task is not None and not wake_task.done():
            return False
        if group.get("breaker_tripped"):
            return False
        if not self._group_allowed(group):
            return False
        if self._in_quiet_time(now):
            if not self._cfg_bool("quiet_time_enabled", True):
                return False
            quiet_resume_at = self._quiet_end_timestamp(now)
            if float(group.get("quiet_resume_at", 0.0) or 0.0) != quiet_resume_at:
                group["quiet_resume_at"] = quiet_resume_at
                self._clear_wake_after(group)
                self._save_state()
            return False

        quiet_resume_at = float(group.get("quiet_resume_at", 0.0) or 0.0)
        if quiet_resume_at > now:
            # Quiet configuration changed while the group was waiting.
            group["quiet_resume_at"] = 0.0
            self._clear_wake_after(group)
            self._save_state()
            quiet_resume_at = 0.0

        last_activity = float(group.get("last_activity", 0.0) or 0.0)
        if last_activity <= 0:
            return False
        activity_anchor = max(last_activity, quiet_resume_at)
        silence = now - activity_anchor
        last_attempt_at = float(group.get("last_attempt_at", 0.0) or 0.0)
        # 死群截止从最后真实活动起算；下一轮的冷场间隔从上次尝试起算。
        interval_anchor = max(activity_anchor, last_attempt_at)
        interval = now - interval_anchor
        silence_minutes = self._cfg_int("silence_minutes", 40, lo=1, hi=1440)
        dead_group_minutes = self._cfg_int(
            "dead_group_minutes", 360, lo=silence_minutes, hi=1440
        )
        if silence > dead_group_minutes * 60 or interval < silence_minutes * 60:
            return False

        daily_limit = self._cfg_int("daily_limit", 1, lo=0)
        if daily_limit > 0 and int(group.get("topics_today", 0)) >= daily_limit:
            return False

        delay_max = self._cfg_int("random_delay_minutes", 5, lo=0, hi=120)
        if delay_max <= 0:
            if group.get("wake_after"):
                self._clear_wake_after(group)
                self._save_state()
            return True

        drawn_activity = float(group.get("wake_after_activity", 0.0) or 0.0)
        drawn_max = int(group.get("wake_after_delay_max", 0) or 0)
        drawn_silence = int(group.get("wake_after_silence_minutes", 0) or 0)
        if (
            drawn_activity != interval_anchor
            or drawn_max != delay_max
            or drawn_silence != silence_minutes
            or not group.get("wake_after")
        ):
            group["wake_after_activity"] = interval_anchor
            group["wake_after_delay_max"] = delay_max
            group["wake_after_silence_minutes"] = silence_minutes
            group["wake_after"] = (
                interval_anchor
                + silence_minutes * 60
                + random.uniform(0.0, delay_max * 60.0)
            )
            self._save_state()
        return now >= float(group["wake_after"])

    @staticmethod
    def _clear_wake_after(group: dict[str, Any]) -> None:
        group["wake_after"] = 0.0
        group["wake_after_activity"] = 0.0
        group["wake_after_delay_max"] = 0
        group["wake_after_silence_minutes"] = 0

    @staticmethod
    def _persona_prompt(persona: Any) -> str:
        if isinstance(persona, dict):
            value = persona.get("prompt")
        else:
            value = getattr(persona, "prompt", None)
        return str(value or "").strip()

    def _history_prompt(self, group: dict[str, Any]) -> str:
        if not self._cfg_bool("history_enabled", True):
            return "（未启用历史记录）"
        size = self._cfg_int("history_size", 20, lo=0, hi=200)
        if size <= 0:
            return "（未启用历史记录）"
        history = group.get("history", [])
        if not isinstance(history, list) or not history:
            return "（暂无记录）"
        lines: list[str] = []
        for item in history[-size:]:
            if not isinstance(item, dict):
                continue
            sender = str(item.get("sender_name") or item.get("sender_id") or "").strip()
            sender = sender or ("Bot" if item.get("is_bot") else "群友")
            text = str(item.get("text") or "").strip()
            if text:
                lines.append(f"{sender}: {text}")
        return "\n".join(lines) if lines else "（暂无记录）"

    def _topics_prompt(self, group: dict[str, Any]) -> str:
        size = self._cfg_int("recent_topics_size", 5, lo=0, hi=50)
        topics = group.get("recent_topics", [])
        if size <= 0 or not isinstance(topics, list) or not topics:
            return "（暂无）"
        values = [str(item).strip() for item in topics[-size:] if str(item).strip()]
        if not values:
            return "（暂无）"
        return "\n".join(f"{index}. {item}" for index, item in enumerate(values, 1))

    def _topic_template(self) -> str:
        template = str(self.config.get("topic_prompt", "") or "").strip()
        if not template:
            raise RuntimeError("未配置唤醒提示词")
        return template

    def _render_prompt(
        self,
        group: dict[str, Any],
        persona_prompt: str,
        now: float,
    ) -> str:
        template = self._topic_template()
        replacements = {
            "{{persona}}": persona_prompt or "（未设置人格）",
            "{{time}}": datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S"),
            "{{group_id}}": str(group.get("group_id") or ""),
            "{{group_name}}": str(group.get("group_name") or group.get("group_id") or "未知群"),
            "{{recent_history}}": self._history_prompt(group),
            "{{recent_topics}}": self._topics_prompt(group),
        }
        for key, value in replacements.items():
            template = template.replace(key, value)
        return template.strip()

    async def _provider_id(self, umo: str) -> str:
        configured = self._cfg_str("provider_id")
        if configured:
            return configured
        return await self.context.get_current_chat_provider_id(umo)

    async def _persona(self, umo: str) -> Any:
        manager = getattr(self.context, "persona_manager", None)
        if manager is None:
            return None
        return await manager.get_default_persona_v3(umo)

    def _tools_for_persona(self, persona: Any) -> ToolSet:
        tools = self.context.get_llm_tool_manager().get_full_tool_set()
        if isinstance(persona, dict):
            allowed = persona.get("tools")
        else:
            allowed = getattr(persona, "tools", None)
        if allowed is None:
            return tools

        filtered = ToolSet()
        for name in allowed:
            tool = tools.get_tool(str(name))
            if tool is not None and getattr(tool, "active", True):
                filtered.add_tool(tool)
        return filtered

    @filter.on_llm_request(priority=sys.maxsize)
    async def inject_wakeup_prompt(self, event: AstrMessageEvent, req) -> None:
        if not isinstance(event, _PipelineWakeEvent) or event.plugin is not self:
            return
        if event.closed or float(event.group["last_activity"]) != event.expected_activity:
            event.close()
            return
        if any(previous is req for previous in event.injected_requests):
            return
        req.system_prompt = "\n\n".join(
            part for part in (str(req.system_prompt or ""), event.task_prompt) if part
        )
        event.injected_requests.append(req)

    @filter.on_llm_response(priority=sys.maxsize)
    async def check_wakeup_response(self, event: AstrMessageEvent, response) -> None:
        if not isinstance(event, _PipelineWakeEvent) or event.plugin is not self:
            return
        role = str(getattr(response, "role", "") or "").strip().lower()
        text = str(getattr(response, "completion_text", "") or "").strip()
        if role == "err" or _looks_like_upstream_llm_error(text):
            # 记录原始失败，不阻止后续响应钩子做清理、通知或追踪。
            event.error = text or "模型返回错误响应"

    @filter.on_decorating_result(priority=-sys.maxsize)
    async def check_wakeup_result(self, event: AstrMessageEvent) -> None:
        if not isinstance(event, _PipelineWakeEvent) or event.plugin is not self:
            return
        text = self._result_text(event)
        if _looks_like_upstream_llm_error(text):
            event.error = event.error or text
        if event.error or event.closed:
            event.clear_result()
            event.stop_event()

    async def _send_via_pipeline(self, umo: str, group: dict[str, Any], now: float) -> None:
        if umo in self._pipeline_events:
            return
        self._pipeline_events[umo] = None
        event = None
        try:
            if not group.get("self_id") or not group.get("group_id"):
                raise RuntimeError("标准消息链需要群和机器人身份，请先在该群发送一条消息")
            platform = self.context.get_platform_inst(MessageSession.from_str(umo).platform_id)
            if platform is None:
                raise RuntimeError("未找到可发送唤醒消息的平台实例")
            meta = copy.copy(platform.meta())
            if not meta.support_proactive_message:
                raise RuntimeError("当前平台不支持主动发送消息")
            meta.support_streaming_message = False
            activity_before = float(group["last_activity"])
            persona = await self._persona(umo)
            prompt = self._render_prompt(group, self._persona_prompt(persona), now)
            if float(group["last_activity"]) != activity_before:
                return
            event = _PipelineWakeEvent(plugin=self, group=group, prompt=prompt, meta=meta)
            if provider_id := self._cfg_str("provider_id"):
                event.set_extra("selected_provider", provider_id)
            self._pipeline_events[umo] = event
            self.context.get_event_queue().put_nowait(event)
            await asyncio.wait_for(
                asyncio.shield(event.finished),
                self._cfg_int("pipeline_timeout_seconds", 600, lo=30, hi=3600),
            )
            if event.error:
                raise RuntimeError(event.error)
            if not event.delivered:
                if float(group["last_activity"]) == activity_before:
                    group["last_attempt_at"] = time.time()
                    self._clear_wake_after(group)
                    self._save_state()
                logger.info("[群聊唤醒] 群 %s 的处理链结束，未发送，不计额度", umo)
        except asyncio.TimeoutError as exc:
            raise RuntimeError("标准消息链处理超时，已关闭该事件的发送出口") from exc
        finally:
            if event is not None:
                event.close()
            self._pipeline_events.pop(umo, None)

    async def _send_pipeline_chain(self, event: _PipelineWakeEvent, message: MessageChain) -> None:
        async with event.send_lock:
            group = event.group
            if event.closed or event.is_stopped():
                return
            if float(group["last_activity"]) != event.expected_activity:
                event.close()
                return
            text = "".join(str(getattr(comp, "text", "") or "") for comp in message.chain).strip()
            event.error = event.error or str(event.get_extra("_llm_error_message", "") or "")
            if _looks_like_upstream_llm_error(text):
                event.error = event.error or text
            if event.error:
                event.stop_event()
                return
            if getattr(message, "type", "") in {"tool_call", "reasoning", "agent_stats", "break"}:
                return
            # 不能引用不存在的平台消息，也不能 @ 虚拟发送者 0；保留其他工具内容。
            chain = [comp for comp in message.chain if not (
                isinstance(comp, Reply) and str(comp.id) == event.message_obj.message_id
                or isinstance(comp, At) and str(comp.qq) == event.get_sender_id()
                or isinstance(comp, Plain) and not comp.text.strip()
            )]
            if not chain:
                return
            if not event.injected_requests:
                event.error = "唤醒提示词未注入，已取消发送；请确认该会话启用了群聊唤醒插件"
                event.stop_event()
                return
            now = time.time()
            first_send = not event.delivered
            old_state = {key: group[key] for key in ("topics_today", "last_attempt_at")}
            if first_send:
                self._roll_daily(group, now)
                old_state["topics_today"] = group["topics_today"]
                group["topics_today"] += 1
                group["last_attempt_at"] = now
                self._save_state()
            try:
                outgoing = copy.copy(message)
                outgoing.chain = chain
                sent = await self.context.send_message(event.umo, outgoing)
                if sent is False:
                    raise RuntimeError("未找到可发送唤醒消息的平台实例")
            except BaseException as exc:
                if first_send:
                    group.update(old_state)
                event.error = f"发送失败：{exc}"
                self._save_state()
                raise
            event.delivered = True
            event._has_send_oper = True
            if text:
                self._remember_topic(group, text)
            if float(group["last_activity"]) != event.expected_activity:
                event.close()
            self._clear_wake_after(group)
            self._save_state()
            if first_send:
                logger.info("[群聊唤醒] 已通过标准消息链向群 %s 发送唤醒消息", event.umo)

    async def _generate_wakeup(
        self,
        umo: str,
        group: dict[str, Any],
        now: float,
    ) -> str:
        persona = await self._persona(umo)
        persona_prompt = self._persona_prompt(persona)
        raw_template = self._topic_template()
        prompt = self._render_prompt(group, persona_prompt, now)
        provider_id = await self._provider_id(umo)
        system_prompt = None if "{{persona}}" in raw_template else persona_prompt or None
        mode = self._generation_mode()

        if mode == MODE_TEXT:
            response = await self.context.llm_generate(
                chat_provider_id=provider_id,
                prompt=prompt,
                system_prompt=system_prompt,
            )
        elif mode == MODE_TOOLS:
            manager = getattr(self.context, "conversation_manager", None)
            if manager is not None:
                try:
                    if not await manager.get_curr_conversation_id(umo):
                        await manager.new_conversation(
                            umo, MessageSession.from_str(umo).platform_id
                        )
                except Exception as exc:
                    logger.warning(
                        "[群聊唤醒] 无法准备群 %s 的对话记录，继续执行工具：%s",
                        umo,
                        exc,
                    )
            event = _SyntheticWakeEvent(
                context=self.context,
                umo=umo,
                message=prompt,
                self_id=str(group.get("self_id") or ""),
                group_id=str(group.get("group_id") or ""),
                group_name=str(group.get("group_name") or ""),
            )
            try:
                response = await self.context.tool_loop_agent(
                    event=event,
                    chat_provider_id=provider_id,
                    prompt=prompt,
                    tools=self._tools_for_persona(persona),
                    system_prompt=system_prompt,
                    max_steps=self._cfg_int("agent_max_steps", 8, lo=1, hi=30),
                    tool_call_timeout=self._cfg_int(
                        "agent_tool_call_timeout", 120, lo=1, hi=600
                    ),
                )
            except Exception as exc:
                if "Agent did not produce a final LLM response" in str(exc):
                    return ""
                raise
        else:
            raise RuntimeError("标准消息链必须通过事件队列执行")

        return self._extract_response_text(response)

    @staticmethod
    def _extract_response_text(response: Any) -> str:
        role = str(getattr(response, "role", "") or "").strip().lower()
        text = str(getattr(response, "completion_text", "") or "").strip()
        if role == "err" or _looks_like_upstream_llm_error(text):
            raise RuntimeError(text or "模型返回错误响应")
        if response is None or not text:
            raise RuntimeError("模型未返回可发送内容")
        return text

    def _trip_breaker(self, group: dict[str, Any], reason: str) -> None:
        group["breaker_tripped"] = True
        group["breaker_reason"] = str(reason or "未知错误")[:500]
        group["breaker_tripped_at"] = time.time()
        self._save_state()
        logger.warning(
            "[群聊唤醒] 群 %s 触发熔断：%s",
            group.get("umo", ""),
            group.get("breaker_reason", ""),
        )

    async def _send_wakeup(
        self,
        umo: str,
        group: dict[str, Any],
        now: float,
    ) -> None:
        if self._generation_mode() == MODE_PIPELINE:
            await self._send_via_pipeline(umo, group, now)
            return
        activity_before = float(group.get("last_activity", 0.0) or 0.0)
        text = await self._generate_wakeup(umo, group, now)

        # 生成期间有人说话或 bot 已回复，就放弃本次唤醒。
        if float(
            group.get("last_activity", 0.0)
        ) != activity_before:
            logger.info("[群聊唤醒] 群 %s 在生成期间恢复活动，取消发送", umo)
            return

        # 工具把结果直接交给平台时，Agent 可能没有最终文本；本轮仍算完成。
        if not text:
            self._roll_daily(group, now)
            group["topics_today"] += 1
            group["last_attempt_at"] = now
            self._clear_wake_after(group)
            self._save_state()
            logger.info("[群聊唤醒] 群 %s 已由工具直接处理，本轮计次", umo)
            return

        old_topics_today = int(group.get("topics_today", 0))
        old_last_attempt_at = float(group.get("last_attempt_at", 0.0) or 0.0)

        # 先落盘再发送，避免发送成功后进程崩溃导致重启后重复发送。
        group["topics_today"] = old_topics_today + 1
        group["last_attempt_at"] = now
        self._save_state()

        try:
            sent = await self.context.send_message(
                umo, MessageChain([Plain(text)])
            )
            if sent is False:
                raise RuntimeError("未找到可发送唤醒消息的平台实例")
        except Exception:
            group["topics_today"] = old_topics_today
            group["last_attempt_at"] = old_last_attempt_at
            raise

        self._remember_topic(group, text)
        self._clear_wake_after(group)
        self._save_state()
        logger.info("[群聊唤醒] 已向群 %s 发送唤醒消息", umo)

    def _remember_topic(self, group: dict[str, Any], text: str) -> None:
        topics = group.setdefault("recent_topics", [])
        if not isinstance(topics, list):
            topics = []
            group["recent_topics"] = topics
        topics.append(text)
        topic_size = self._cfg_int("recent_topics_size", 5, lo=0, hi=50)
        if topic_size > 0 and len(topics) > topic_size:
            del topics[:-topic_size]
        elif topic_size == 0:
            topics.clear()

    async def _patrol_loop(self) -> None:
        while True:
            interval = self._cfg_int(
                "check_interval_seconds", 60, lo=10, hi=3600
            )
            await asyncio.sleep(interval)
            await self._patrol_once(time.time())

    async def _patrol_once(self, now: float) -> None:
        groups = list(self._state.get("groups", {}).items())
        for umo, group in groups:
            if not isinstance(group, dict):
                continue
            self._roll_daily(group, now)
            if self._can_wake(group, now):
                self._schedule_wake(umo, group, now)

    def _schedule_wake(self, umo: str, group: dict[str, Any], now: float) -> None:
        existing = self._wake_tasks.get(umo)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(
            self._run_wake(umo, group, now),
            name=f"group-wakeup:{umo}",
        )
        self._wake_tasks[umo] = task

        def clear_task(done: asyncio.Task[None]) -> None:
            if self._wake_tasks.get(umo) is done:
                self._wake_tasks.pop(umo, None)

        task.add_done_callback(clear_task)

    async def _run_wake(self, umo: str, group: dict[str, Any], now: float) -> None:
        try:
            await self._send_wakeup(umo, group, now)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._trip_breaker(group, f"生成或发送失败：{exc}")
