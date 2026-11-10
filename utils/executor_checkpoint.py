"""Save and restore the orders an API executor already owns.

A restart used to mark every RUNNING row SYSTEM_CLEANUP and forget the exchange.
Recreating the executor from its create-config places a second set of orders.
This module records the order ids (and, for a filled order, the order itself)
so the same executor can be started again and reattached.

Nothing here places or cancels an order. A grid is not refused because one
open order fits two levels: that order stays unattached and new orders at its
price are capped. An order id this checkpoint does not already own is never
adopted by price.
"""
import copy
import hashlib
import json
import logging
import os
import tempfile
import threading
import time
from contextvars import ContextVar
from decimal import Decimal
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

CHECKPOINT_VERSION = 1
# Not retained_orders: seat_known_opens reads that list and would attach by price.
CLOSE_HOLD_KEY = "close_holds"
# Blocks a second close. seat_known_opens and _saved_ids do not read this.
CLOSE_BAN_KEY = "close_bans"

# Relative price match for an order the checkpoint missed but the connector
# still tracks. Wider than one tick, narrower than a grid step we would confuse
# with the next level.
PRICE_MATCH = Decimal("0.001")

SUPPORTED_TYPES = frozenset({
    "position_executor",
    "grid_executor",
    "dca_executor",
    "twap_executor",
    "arbitrage_executor",
    "xemm_executor",
    "order_executor",
    "lp_executor",
})


def _enum_name(value: Any) -> Optional[str]:
    if value is None:
        return None
    name = getattr(value, "name", None)
    return name if isinstance(name, str) else str(value)


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def capture_tracked(order: Any) -> Optional[Dict[str, Any]]:
    """A TrackedOrder, or None when it does not own an exchange order yet."""
    if order is None:
        return None
    order_id = getattr(order, "order_id", None)
    if not order_id:
        return None
    payload: Dict[str, Any] = {"order_id": order_id}
    live = getattr(order, "order", None)
    if live is not None and hasattr(live, "to_json"):
        payload["order"] = live.to_json()
        payload["filled"] = bool(getattr(live, "is_filled", False))
        price = getattr(live, "price", None)
        if price not in (None, ""):
            payload["price"] = str(price)
        return payload
    saved = getattr(order, "_broken_snapshot", None)
    if isinstance(saved, dict) and saved:
        # Захват не должен записать исполненную покупку как неисполненную.
        payload["order"] = dict(saved)
        payload["filled"] = True
        return payload
    payload["filled"] = False
    return payload


def _capture_grid(executor: Any) -> Dict[str, Any]:
    levels = []
    for level in getattr(executor, "grid_levels", []) or []:
        levels.append({
            "id": level.id,
            "price": _text(level.price),
            "amount_quote": _text(level.amount_quote),
            "take_profit": _text(level.take_profit),
            "side": _enum_name(level.side),
            "open_order_type": _enum_name(level.open_order_type),
            "take_profit_order_type": _enum_name(level.take_profit_order_type),
            "open_order": capture_tracked(getattr(level, "active_open_order", None)),
            "close_order": capture_tracked(getattr(level, "active_close_order", None)),
        })
    close_order = capture_tracked(getattr(executor, "_close_order", None))
    bans = set(str(item) for item in (getattr(executor, "_column_close_ids", []) or []) if item)
    bans.update(
        str(item) for item in (getattr(executor, "_close_ban_ids", []) or []) if item
    )
    return {
        "levels": levels,
        "close_order": close_order,
        CLOSE_HOLD_KEY: list(getattr(executor, "_close_holds", []) or []),
        CLOSE_BAN_KEY: sorted(bans),
    }


def _capture_position(executor: Any) -> Dict[str, Any]:
    return {
        "open_order": capture_tracked(getattr(executor, "_open_order", None)),
        "close_order": capture_tracked(getattr(executor, "_close_order", None)),
        "take_profit_order": capture_tracked(getattr(executor, "_take_profit_limit_order", None)),
    }


def _capture_dca(executor: Any) -> Dict[str, Any]:
    return {
        "open_orders": [item for item in (
            capture_tracked(order) for order in getattr(executor, "_open_orders", []) or []
        ) if item],
        "close_orders": [item for item in (
            capture_tracked(order) for order in getattr(executor, "_close_orders", []) or []
        ) if item],
    }


def _capture_order(executor: Any) -> Dict[str, Any]:
    return {"order": capture_tracked(getattr(executor, "_order", None))}


def _capture_twap(executor: Any) -> Dict[str, Any]:
    plan = []
    for timestamp, order in (getattr(executor, "_order_plan", {}) or {}).items():
        plan.append({
            "timestamp": _text(timestamp),
            "order": capture_tracked(order),
        })
    return {
        "start_timestamp": _text(getattr(executor, "_start_timestamp", None)),
        "plan": plan,
    }


def _capture_arbitrage(executor: Any) -> Dict[str, Any]:
    return {
        "buy_order": capture_tracked(getattr(executor, "_buy_order", None)),
        "sell_order": capture_tracked(getattr(executor, "_sell_order", None)),
    }


def _capture_xemm(executor: Any) -> Dict[str, Any]:
    return {
        "maker_order": capture_tracked(getattr(executor, "maker_order", None)),
        "taker_order": capture_tracked(getattr(executor, "taker_order", None)),
    }


def _capture_lp(executor: Any) -> Dict[str, Any]:
    state = getattr(executor, "lp_position_state", None)
    dumped: Dict[str, Any] = {}
    if state is not None and hasattr(state, "model_dump"):
        dumped = state.model_dump(mode="json")
        # TrackedOrder is not a pydantic field we can round-trip blindly.
        for key in ("active_open_order", "active_close_order", "active_swap_order"):
            dumped.pop(key, None)
    position_address = dumped.get("position_address")
    state_name = _enum_name(dumped.get("state")) or _text(dumped.get("state"))
    opening = state_name == "OPENING" or bool(position_address)
    return {
        "position_address": position_address,
        "state": state_name,
        "saw_open_attempt": opening or bool(getattr(executor, "_position_seen_onchain", False)),
        "state_json": dumped,
        "open_order": capture_tracked(getattr(state, "active_open_order", None)) if state else None,
        "close_order": capture_tracked(getattr(state, "active_close_order", None)) if state else None,
    }


_CAPTURERS = {
    "grid_executor": _capture_grid,
    "position_executor": _capture_position,
    "dca_executor": _capture_dca,
    "order_executor": _capture_order,
    "twap_executor": _capture_twap,
    "arbitrage_executor": _capture_arbitrage,
    "xemm_executor": _capture_xemm,
    "lp_executor": _capture_lp,
}


def order_ledger(executor: Any) -> Dict[str, Dict[str, Any]]:
    """Ids this executor has placed, kept until the exchange confirms they are gone."""
    ledger = getattr(executor, "_order_ledger", None)
    if not isinstance(ledger, dict):
        ledger = {}
        try:
            executor._order_ledger = ledger
        except Exception:
            return ledger
    return ledger


# Written in the same moment the exchange accepts the order, before the
# database task gets a turn. Lives under bots/ so a container recreate keeps it:
# compose mounts that directory, not /hummingbot-api/data.
LEDGER_KEEP_SECONDS = 48 * 3600
LEDGER_LOCK = threading.Lock()


class _LedgerReadError(OSError):
    """The ledger file exists but could not be read. Not an empty ledger."""


def order_ledger_path() -> str:
    return os.environ.get(
        "EXECUTOR_ORDER_LEDGER",
        "/hummingbot-api/bots/data/order-ledger.jsonl",
    )


def _read_ledger() -> List[Dict[str, Any]]:
    path = order_ledger_path()
    if not os.path.exists(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
    except OSError as exc:
        logger.warning("order ledger read failed: %s", exc)
        raise _LedgerReadError(str(exc)) from exc
    return rows


def _fsync_directory(path: str) -> bool:
    """Fsync the parent of path. True only when that fsync succeeded."""
    directory = os.path.dirname(path)
    if not directory:
        return False
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError as exc:
        logger.warning("order ledger directory fsync failed: %s", exc)
        return False
    try:
        os.fsync(descriptor)
    except OSError as exc:
        logger.warning("order ledger directory fsync failed: %s", exc)
        return False
    finally:
        os.close(descriptor)
    return True


# Two durable sets beside the ledger, not inside it. "miss" is a number whose
# send never left this process; "sent" is a number whose place call was entered.
# A row leaves the hold/price view only in the first and not in the second.
MISS_LOCK = threading.Lock()
SENT_LOCK = threading.Lock()


def _miss_path() -> str:
    return os.environ.get(
        "EXECUTOR_ORDER_MISS",
        os.path.join(os.path.dirname(order_ledger_path()) or ".", "order-miss.json"),
    )


def _sent_path() -> str:
    return os.environ.get(
        "EXECUTOR_ORDER_SENT",
        os.path.join(os.path.dirname(order_ledger_path()) or ".", "order-sent.json"),
    )


def _load_set_file(path: str) -> Optional[set]:
    """An absent file over a readable directory is an empty set. Any read error is None.

    os.path.exists is not consulted: a broken stat must not read as an absent
    file, because an empty sent would drop a live order's journal line and
    free its price. ENOENT is an absent file only once the directory itself
    has been read; every other OSError stays an error.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            body = json.load(handle)
    except FileNotFoundError:
        try:
            os.listdir(os.path.dirname(path) or ".")
        except OSError as exc:
            logger.warning("order set directory read failed (%s): %s", path, exc)
            return None
        return set()
    except (OSError, ValueError) as exc:
        logger.warning("order set read failed (%s): %s", path, exc)
        return None
    if not isinstance(body, list):
        logger.warning("order set is not a list (%s)", path)
        return None
    return {str(item) for item in body if item}


def _durable_write_set(path: str, items) -> bool:
    """Whole-set replace: temp file, flush and fsync, os.replace, then directory fsync."""
    directory = os.path.dirname(path) or "."
    temporary = None
    descriptor = None
    try:
        os.makedirs(directory, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".order-set-", dir=directory)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = None
            json.dump(sorted(str(item) for item in items), handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
        return _fsync_directory(path)
    except OSError as exc:
        logger.warning("order set write failed (%s): %s", path, exc)
        return False
    finally:
        _bump_ledger_version()
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def _set_contains(path: str, order_id: str) -> Optional[bool]:
    """True or False from the live file; None when the read itself failed.

    A failed read must not answer "the number is absent": add_sent_order
    decides placement on this answer.
    """
    current = _load_set_file(path)
    if current is None:
        return None
    return str(order_id) in current


def add_miss_order(order_id: str) -> bool:
    """Durable before the journal line is visible. False: do not remember, release, return.

    A failed write rolls the previous set back the same durable way, so a
    half-published number does not linger. False is safe here even when the
    rollback also fails: no journal line exists yet, and the next attempt mints
    a fresh number.
    """
    if not order_id:
        return False
    with MISS_LOCK:
        current = _load_set_file(_miss_path())
        if current is None:
            return False
        if str(order_id) in current:
            return True
        previous = set(current)
        current.add(str(order_id))
        if _durable_write_set(_miss_path(), current):
            return True
        try:
            _durable_write_set(_miss_path(), previous)
        except Exception:
            logger.warning("order miss rollback failed", exc_info=True)
        return False


def add_sent_order(order_id: str) -> bool:
    """Durable before the original place call. False: do not place; stay in miss.

    True only after the live order-sent.json name carries the number and its
    directory fsync succeeded. A failed directory fsync after a published
    replace is retried on the same live name first. A still-undurable name is
    unpublished by rolling the previous set back the same durable way; False
    is honest only once the live file no longer carries the number. When the
    rollback cannot unpublish either, one more fsync is tried, and a visible
    but undurable number is reported as a warned failure: placing on it opens
    a crash window that drops the journal line beside a live exchange order,
    and a silent False would free the price to a second order.
    """
    if not order_id:
        return False
    with SENT_LOCK:
        current = _load_set_file(_sent_path())
        if current is None:
            return False
        previous = set(current)
        current.add(str(order_id))
        if _durable_write_set(_sent_path(), current):
            return True
        present = _set_contains(_sent_path(), order_id)
        if present is False:
            return False
        # True requires the live name verified to carry the number and a
        # successful directory fsync of that name. An unreadable file is
        # neither "absent" nor "present": it must not green-light a place.
        if present is True and _fsync_directory(_sent_path()):
            return True
        try:
            _durable_write_set(_sent_path(), previous)
        except Exception:
            logger.warning("order sent rollback failed", exc_info=True)
        present = _set_contains(_sent_path(), order_id)
        if present is False:
            return False
        if present is True and _fsync_directory(_sent_path()):
            return True
        logger.warning("order sent mark is not verifiably durable; not placing")
        return False


def remove_miss_order(order_id: str) -> bool:
    """Only after reached, with sent already durable. Failure is safe: sent keeps the row."""
    if not order_id:
        return False
    with MISS_LOCK:
        current = _load_set_file(_miss_path())
        if current is None or str(order_id) not in current:
            return True
        current.discard(str(order_id))
        return _durable_write_set(_miss_path(), current)


def remove_sent_order(order_id: str) -> bool:
    """Unmark a send that provably never reached the place call.

    False keeps the owner: an unreadable sent file, or a write that did not
    durably land, must not free the level. A live file that already stopped
    carrying the number answers True even when its directory fsync failed —
    the rows are already invisible to readers.
    """
    if not order_id:
        return False
    with SENT_LOCK:
        current = _load_set_file(_sent_path())
        if current is None:
            return False
        if str(order_id) not in current:
            return True
        current.discard(str(order_id))
        return _durable_write_set(_sent_path(), current)


def order_is_unsent(order_id: str) -> bool:
    """True only in durable miss and not in durable sent: the send never left."""
    if not order_id:
        return False
    miss = _load_set_file(_miss_path())
    if not miss or str(order_id) not in miss:
        return False
    sent = _load_set_file(_sent_path())
    if sent is None:
        return False
    return str(order_id) not in sent


def order_send_unknown(order_id: str) -> bool:
    """Номер в durable miss, а набор sent не читается: судьба отправки неизвестна.

    order_is_unsent здесь отвечает ложью (он держит только доказанный miss без
    sent). Для стопа нечитаемый набор — не разрешение пройти: отправка могла
    дойти до биржи.
    """
    if not order_id:
        return False
    miss = _load_set_file(_miss_path())
    if not miss or str(order_id) not in miss:
        return False
    return _load_set_file(_sent_path()) is None


def order_miss_unreadable() -> bool:
    """Набор miss не читается. Ошибка чтения — не пустой набор и не «номера там нет».

    order_send_unknown здесь ложь. Пока набор не читается, отправка любого номера
    неизвестна: стоп держит, шаг сборки конец не пишет.
    """
    return _load_set_file(_miss_path()) is None


def read_ledger_filtered() -> List[Dict[str, Any]]:
    """View for hold and price readers. Raw _read_ledger stays for debt recovery, prune, rewrite.

    A number in durable miss and not in durable sent never reached the exchange,
    so its line holds neither a level nor a tick. An unreadable sent keeps every
    line; an unreadable miss drops nothing.

    Served from the ledger snapshot: the file is read again only when its key
    changed (п.1 плана ledger-scan-cache). Rows are read-only for readers — a
    reader that edits a row takes its own copy first.
    """
    return _ledger_snapshot()["rows"]


# ── Снимок журнала: одно чтение файла на шаг (план ledger-scan-cache) ─────────
#
# Строки снимка для читателей неизменяемы: читатель, которому нужно поправить
# строку, берёт свою копию. Писатели журнала и наборов идут мимо кэша: в конце
# записи поднимают версию, и следующее чтение пересобирает снимок — только что
# записанная строка видна тому же шагу. Устаревший снимок при сбое чтения не
# подставляется: сбой чтения журнала так же кидает _LedgerReadError.

_LEDGER_CACHE_LOCK = threading.RLock()
_LEDGER_CACHE_VERSION = 0
_LEDGER_SNAPSHOT: Optional[Dict[str, Any]] = None
_LEDGER_CACHE_STATS = {"reads": 0, "lookups": 0}
_EMPTY_VIEW: Dict[str, Any] = {
    "orders": {},
    "holding_anon": set(),
    "holding_by_executor": {},
    "unowned_ids": set(),
    "close_price_ids": {},
    "min_ts": None,
}


def ledger_cache_stats() -> Dict[str, int]:
    return dict(_LEDGER_CACHE_STATS)


def ledger_cache_reset_stats() -> None:
    _LEDGER_CACHE_STATS["reads"] = 0
    _LEDGER_CACHE_STATS["lookups"] = 0


def _bump_ledger_version() -> None:
    global _LEDGER_CACHE_VERSION
    with _LEDGER_CACHE_LOCK:
        _LEDGER_CACHE_VERSION += 1


def _ledger_file_key(path: str) -> Optional[tuple]:
    """None means the key could not be built: rebuild by reading, never serve cache.

    A broken stat is not an absent file. Absence is confirmed only when the
    directory itself can be read — the same discipline as _load_set_file, so a
    broken stat can never free a live order's price by hiding its line.
    """
    try:
        stat = os.stat(path)
    except FileNotFoundError:
        try:
            os.listdir(os.path.dirname(path) or ".")
        except OSError:
            return None
        return ("absent", path)
    except OSError:
        return None
    return ("file", path, stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)


def _ledger_cache_key() -> Optional[tuple]:
    parts = []
    for path in (order_ledger_path(), _miss_path(), _sent_path()):
        part = _ledger_file_key(path)
        if part is None:
            return None
        parts.append(part)
    return (tuple(parts), _LEDGER_CACHE_VERSION)


def _build_ledger_snapshot(pre_key: Optional[tuple]) -> Dict[str, Any]:
    """One read of the file and the sets. pre_key is taken before that read.

    A write landing after this read bumps the version, so the next lookup sees
    a changed key and rebuilds: the just-written line is never hidden. A
    snapshot built while a set was unreadable is not cacheable — the filter
    kept every line only because the set could not be read.
    """
    raw = _read_ledger()
    miss = _load_set_file(_miss_path())
    sent = _load_set_file(_sent_path()) if miss else None
    if not miss:
        rows = raw
    elif sent is None:
        rows = raw
    else:
        rows = [
            row for row in raw
            if str(row.get("order_id") or "") not in miss
            or str(row.get("order_id") or "") in sent
        ]
    cacheable = pre_key is not None and miss is not None and (not miss or sent is not None)
    return {
        "key": pre_key if cacheable else None,
        "raw": raw,
        "rows": rows,
        "answers": _answers_for_rows(rows),
        "answers_raw": _answers_for_rows(raw),
    }


def _ledger_snapshot() -> Dict[str, Any]:
    global _LEDGER_SNAPSHOT
    with _LEDGER_CACHE_LOCK:
        pre_key = _ledger_cache_key()
        snap = _LEDGER_SNAPSHOT
        if snap is not None and pre_key is not None and snap["key"] == pre_key:
            return snap
        _LEDGER_CACHE_STATS["reads"] += 1
        try:
            snap = _build_ledger_snapshot(pre_key)
        except BaseException:
            _LEDGER_SNAPSHOT = None
            raise
        _LEDGER_SNAPSHOT = snap if snap["key"] is not None else None
        return snap


def _rows_answers(rows: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    """Per-order answers and ready holding sets for these rows. Snapshot's lists are cached."""
    if rows is None:
        return _EMPTY_VIEW
    snap = _LEDGER_SNAPSHOT
    if snap is not None and rows is snap["rows"]:
        _LEDGER_CACHE_STATS["lookups"] += 1
        return snap["answers"]
    if snap is not None and rows is snap["raw"]:
        _LEDGER_CACHE_STATS["lookups"] += 1
        return snap["answers_raw"]
    return _answers_for_rows(rows)


# ── Ответы прямых читателей журнала (план durable-open-hotspots) ─────────────
#
# Шесть читателей сервиса читали _read_ledger() напрямую — файл и разбор всех
# строк на каждое обращение, а _close_price_known звался на каждый номер
# закрытия на каждый уровень. Теперь они берут готовые ответы снимка: те же
# формулы над теми же сырыми строками (без фильтра наборов — читатели и раньше
# читали сырой журнал). Писатели идут мимо кэша как прежде: версия в finally.

def ledger_raw_rows() -> List[Dict[str, Any]]:
    """Сырой взгляд снимка ровно как _read_ledger(): строки только на чтение."""
    return _ledger_snapshot()["raw"]


def ledger_price_known(order_id: Any) -> bool:
    """Есть ли строка номера с ценой (та же формула, что петля _close_price_known)."""
    item = _rows_answers(ledger_raw_rows())["orders"].get(str(order_id)) or {}
    return bool(item.get("price_known"))


def ledger_journal_mark(order_id: Any) -> tuple:
    """owned/unowned/absent как петля _journal_mark: владелец — последняя
    строка номера с исполнителем, поздняя строка без исполнителя его не сбрасывает."""
    item = _rows_answers(ledger_raw_rows())["orders"].get(str(order_id))
    if item is None:
        return "absent", None
    owner = item.get("mark_owner")
    if owner:
        return "owned", owner
    return "unowned", None


def ledger_close_price_ids(price: Any) -> set:
    """Номера строк role=close/market_close на этой цене (правило номера: сам
    журнал решает, наш номер; живость решает finished)."""
    return set(_rows_answers(ledger_raw_rows())["close_price_ids"].get(str(price)) or set())


def ledger_close_price_taken(price: Any, ignore_id: Any = "") -> bool:
    """Занята ли цена строки role=close/market_close: та же вычитающая форма
    петли _ledger_close_price_taken (строка без номера и пустой ignore_id отсекают
    друг друга)."""
    return bool(ledger_close_price_ids(price) - {str(ignore_id or "")})


def ledger_unowned_ids() -> set:
    """Номера строк с order_id и без executor_id (как _raw_unowned_ids)."""
    return set(_rows_answers(ledger_raw_rows())["unowned_ids"])


def ledger_min_ts() -> Any:
    """Меньший ts строк журнала (как петля _oldest_ts над _read_ledger())."""
    return _rows_answers(ledger_raw_rows())["min_ts"]


# One ledger path owes a directory entry until that fsync is seen to succeed.
# A different path does not inherit the debt. The durable debt is the marker file.
_LEDGER_NAME_FSYNC_OWED: Optional[str] = None
_LEDGER_PARENT_FSYNC_OWED: Optional[str] = None
_MARKER_FD: Optional[int] = None
_MARKER_FD_PATH: Optional[str] = None
_MARKER_NAME_SYNCED = False
_MARKER_UNLINK_DIR_OWED = False
_BOUND_LEDGER_PATH: Optional[str] = None
OWED_FIELDS = (
    "order_id",
    "price",
    "executor_id",
    "account_name",
    "connector",
    "trading_pair",
    "ts",
)


def _write_ledger(rows: List[Dict[str, Any]]) -> None:
    """Replace only after owed bytes are durable and the live name points at them."""
    global _LEDGER_NAME_FSYNC_OWED
    path = order_ledger_path()
    temporary = None
    descriptor = None
    replaced = False
    try:
        kept = _rows_plus_owed(rows)
        if kept is None:
            return
        if not _marker_synced_before_name(kept):
            return
        directory = os.path.dirname(path) or "."
        created_directory = not os.path.isdir(directory)
        os.makedirs(directory, exist_ok=True)
        if created_directory:
            _LEDGER_PARENT_FSYNC_OWED = directory
            if not _fsync_new_directory(directory):
                return
        descriptor, temporary = tempfile.mkstemp(prefix=".order-ledger-", dir=directory)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = None
            for row in kept:
                handle.write(json.dumps(row) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        replaced = True
        temporary = None
        _LEDGER_NAME_FSYNC_OWED = path
        if not _fsync_live_file(path):
            return
        if not _fsync_directory(path):
            return
        _LEDGER_NAME_FSYNC_OWED = None
        _release_marker_if_journal_has_owed()
    except OSError as exc:
        logger.warning("order ledger write failed: %s", exc)
    finally:
        _bump_ledger_version()
        if descriptor is not None:
            os.close(descriptor)
        if temporary and not replaced:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def _append_ledger_line(row: Dict[str, Any], *, marker_ready: bool = False) -> bool:
    """Append one id. False means this line is not durable and must not be sent.

    True only when this call's file fsync and owed directory fsyncs succeeded.
    A name or parent debt left by this process is not a success.
    """
    global _LEDGER_NAME_FSYNC_OWED, _LEDGER_PARENT_FSYNC_OWED
    _bind_process_to_ledger_path()
    path = order_ledger_path()
    descriptor = None
    file_synced = False
    directory = os.path.dirname(path) or "."
    created_directory = not os.path.isdir(directory)
    created_file = created_directory or not os.path.exists(path)
    marker_existed = os.path.exists(ledger_debt_marker())
    if created_directory or created_file:
        if marker_ready:
            if not _marker_name_synced():
                return False
        elif not _marker_synced_before_name([row]):
            return False
    try:
        os.makedirs(directory, exist_ok=True)
        if created_directory:
            _LEDGER_PARENT_FSYNC_OWED = directory
        created_file = not os.path.exists(path)
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        if created_file:
            _LEDGER_NAME_FSYNC_OWED = path
        size = os.lseek(descriptor, 0, os.SEEK_END)
        if size:
            os.lseek(descriptor, size - 1, os.SEEK_SET)
            last = os.read(descriptor, 1)
            os.lseek(descriptor, 0, os.SEEK_END)
            if last != b"\n":
                os.write(descriptor, b"\n")
        os.write(descriptor, (json.dumps(row) + "\n").encode("utf-8"))
        os.fsync(descriptor)
        file_synced = True
    except (OSError, TypeError, ValueError) as exc:
        logger.warning("order ledger append failed: %s", exc)
    finally:
        _bump_ledger_version()
        if descriptor is not None:
            os.close(descriptor)
    if not file_synced:
        return False
    if _LEDGER_NAME_FSYNC_OWED == path and _fsync_directory(path):
        _LEDGER_NAME_FSYNC_OWED = None
    if _LEDGER_PARENT_FSYNC_OWED == directory and _LEDGER_NAME_FSYNC_OWED != path:
        if not _fsync_new_directory(directory):
            return False
    if (
        file_synced
        and created_file
        and not marker_existed
        and not _LEDGER_NAME_FSYNC_OWED
        and not _LEDGER_PARENT_FSYNC_OWED
    ):
        _release_marker_if_journal_has_owed()
    return file_synced and not _LEDGER_NAME_FSYNC_OWED and not _LEDGER_PARENT_FSYNC_OWED


def append_durable_row(row: Dict[str, Any]) -> bool:
    """Append one close line. False means do not send.

    Today's append return is not a signal. This flag is True only after the file
    fsync of this call and after any name or parent-directory debt is cleared.
    The lock stays outside _append_ledger_line: that function does not take it.
    """
    with LEDGER_LOCK:
        try:
            return _append_ledger_line(row)
        except (OSError, TypeError, ValueError):
            return False


def numeric_ts(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def merge_ledger_lines(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """One view per order_id. sent=true on any durable line wins. executor_id only if a line has it."""
    merged: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        order_id = row.get("order_id")
        if not order_id or numeric_ts(row.get("ts")) is None:
            continue
        item = merged.setdefault(str(order_id), {"order_id": str(order_id)})
        if row.get("executor_id"):
            item["executor_id"] = row["executor_id"]
        if row.get("price") not in (None, ""):
            item["price"] = row["price"]
        if row.get("role"):
            item["role"] = row["role"]
        if row.get("level_id"):
            item["level_id"] = row["level_id"]
        if row.get("sent") is True or row.get("sent") == "true":
            item["sent"] = True
        elif row.get("sent") is False and item.get("sent") is not True:
            item["sent"] = False
        item["ts"] = numeric_ts(row.get("ts"))
    return merged


def is_proven_close_row(item: Dict[str, Any]) -> bool:
    """A close we may keep out of entry seating. A role-less line is not this."""
    return item.get("role") in ("close", "market_close", "close_order")


def is_close_ledger_row(item: Dict[str, Any]) -> bool:
    """Whether a line may block a second close.

    A remember_order_now line has executor_id and no role. That is still an
    entry until the side is read as a close. It must stay seatable.
    """
    role = item.get("role")
    if role in ("open", "open_order"):
        return False
    if is_proven_close_row(item):
        return True
    if item.get("executor_id"):
        return False
    return bool(item.get("order_id")) and numeric_ts(item.get("ts")) is not None


def read_merged_ledger() -> Dict[str, Dict[str, Any]]:
    """Full lines. A read error is raised, not an empty book."""
    return merge_ledger_lines(read_ledger_filtered())


def _hold_role(checkpoint: Dict[str, Any], order_id: str) -> Optional[str]:
    for item in checkpoint.get(CLOSE_HOLD_KEY) or []:
        if isinstance(item, dict) and str(item.get("order_id")) == str(order_id):
            return item.get("role")
    return None


def close_hold_ids(checkpoint: Optional[Dict[str, Any]]) -> set:
    if not checkpoint:
        return set()
    found = set()
    for item in checkpoint.get(CLOSE_HOLD_KEY) or []:
        if isinstance(item, dict) and item.get("order_id"):
            found.add(str(item["order_id"]))
        elif item:
            found.add(str(item))
    return found


def strip_close_holds_from_saved(checkpoint: Dict[str, Any]) -> None:
    """A hold must not be an open the next resume seats by price."""
    held = close_hold_ids(checkpoint)
    if not held:
        return
    retained = []
    for saved in checkpoint.get("retained_orders") or []:
        if saved.get("order_id") not in held:
            retained.append(saved)
    checkpoint["retained_orders"] = retained
    # An entry already on a level stays. A close hold is not seated by clearing it.


def append_level_place_line(
    executor_id: str,
    order_id: str,
    level_id: str,
    price: Any,
    take_profit_price: Any = None,
 ) -> None:
    """Дописать строку постановки и сбросить файл, не отдавая цикл событий.

    Повтор сброса не вызывает await. Старые строки не стираются. Сброс папки
    только если этот вызов создал живое имя, и только после сброса файла.
    """
    if not executor_id or not order_id or not level_id:
        return
    row = {
        "executor_id": str(executor_id),
        "order_id": str(order_id),
        "level_id": str(level_id),
        "price": None if price in (None, "") else str(price),
        "ts": time.time(),
    }
    if take_profit_price not in (None, ""):
        row["take_profit_price"] = str(take_profit_price)
    payload = (json.dumps(row) + "\n").encode("utf-8")
    created_name = False
    path = order_ledger_path()
    wrote, created_now = _append_level_payload(payload)
    if not wrote:
        return
    if created_now and not _fsync_directory(path):
        return


def _append_level_payload(payload: bytes) -> tuple:
    """Дописать байты и сбросить файл. Папку не трогает. False — повтор."""
    _bind_process_to_ledger_path()
    path = order_ledger_path()
    directory = os.path.dirname(path) or "."
    descriptor = None
    created = False
    try:
        os.makedirs(directory, exist_ok=True)
        created = not os.path.exists(path)
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        size_before = os.lseek(descriptor, 0, os.SEEK_END)
        if size_before:
            os.lseek(descriptor, size_before - 1, os.SEEK_SET)
            last = os.read(descriptor, 1)
            os.lseek(descriptor, 0, os.SEEK_END)
            if last != b"\n":
                os.write(descriptor, b"\n")
        os.write(descriptor, payload)
        try:
            os.fsync(descriptor)
        except OSError as exc:
            logger.warning("level place line was not synced: %s", exc)
            try:
                os.ftruncate(descriptor, size_before)
                os.fsync(descriptor)
            except OSError:
                pass
            return False, created
    except (OSError, TypeError, ValueError) as exc:
        logger.warning("level place line was not synced: %s", exc)
        return False, created
    finally:
        if descriptor is not None:
            os.close(descriptor)
    return True, created


def _drop_grown_close_line(path: str, size_before: int) -> bool:
    """Истина только если хвост этого вызова уже не на диске."""
    try:
        if not os.path.exists(path):
            return True
        if os.path.getsize(path) <= size_before:
            return True
        descriptor = os.open(path, os.O_RDWR)
        try:
            os.ftruncate(descriptor, size_before)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return os.path.getsize(path) <= size_before
    except OSError:
        return False




def append_published_close(executor_id: str, order_id: str, level_id: str, price: Any = None) -> bool:

    """Строка продажи с уровнем и ролью. Ложь — строка не опубликована."""

    if not executor_id or not order_id or not level_id:

        return False

    row = {

        "executor_id": str(executor_id),

        "order_id": str(order_id),

        "level_id": str(level_id),

        "role": "close",

        "published": True,

        "price": None if price in (None, "") else str(price),

        "ts": time.time(),

    }

    with LEDGER_LOCK:
        path = order_ledger_path()
        try:
            size_before = os.path.getsize(path) if os.path.exists(path) else 0
        except OSError:
            size_before = None
        try:
            kept = _append_ledger_line(row)
        except (OSError, TypeError, ValueError):
            kept = False
        if kept:
            return True
        if size_before is not None:
            _drop_grown_close_line(path, size_before)
        return False









def published_close_rows(executor_id: str) -> list:

    """Опубликованные строки закрытия. Строка входа сюда не входит."""

    found = []

    try:

        rows = read_ledger_filtered()

    except _LedgerReadError:
        return None

    for row in rows:

        if str(row.get("executor_id") or "") != str(executor_id):

            continue

        if row.get("role") != "close" or row.get("published") is not True:

            continue

        level_id = str(row.get("level_id") or "")

        order_id = str(row.get("order_id") or "")

        if level_id and order_id:

            found.append(row)

    return found



def seat_recorded_level_opens(executor: Any, executor_id: str) -> bool:
    """После apply_checkpoint: номер входа на уровень из строки, не по цене.

    False — журнал не прочитан. Это не пустая книга. Закрытие в слот входа
    не пишется. Если в слоте другой id, сажается записанный номер входа.
    """
    from hummingbot.strategy_v2.models.executors import TrackedOrder

    if not executor_id:
        return True
    try:
        rows = read_ledger_filtered()
    except _LedgerReadError:
        return False
    placed: Dict[str, str] = {}
    noted: Dict[str, str] = {}
    for row in rows:
        if str(row.get("executor_id") or "") != str(executor_id):
            continue
        if is_proven_close_row(row):
            continue
        level_id = str(row.get("level_id") or "")
        order_id = str(row.get("order_id") or "")
        if not level_id or not order_id:
            continue
        # Пометка тейка — не новая постановка. Она не затирает живой номер.
        if row.get("take_profit_price") not in (None, ""):
            noted[level_id] = order_id
            continue
        placed[level_id] = order_id
    finished = {
        str(item)
        for item in (getattr(executor, "_notebook_completed_cycle_opens", None) or ())
    }
    for level in list(getattr(executor, "grid_levels", None) or []):
        level_id = str(getattr(level, "id", "") or "")
        order_id = placed.get(level_id) or noted.get(level_id)
        if not order_id:
            continue
        # Номер завершённого круга не возвращается в слот: уровень должен
        # остаться пустым для штатного цикла (план okx-resume-complete-level).
        if str(order_id) in finished:
            continue
        current = str(getattr(getattr(level, "active_open_order", None), "order_id", "") or "")
        if current == order_id:
            continue
        level.active_open_order = TrackedOrder(order_id=order_id)
        update = getattr(level, "update_state", None)
        if callable(update):
            update()
    return True


def remember_order_now(executor_id: str, order_id: Optional[str]) -> None:
    """Durably note an order id before the async database write runs.

    Append only. Rewriting the file here waits on a multi-day ledger and delays
    the next order the exchange has not accepted yet.
    """
    if not executor_id or not order_id:
        return
    row = {"executor_id": executor_id, "order_id": order_id, "ts": time.time()}
    with LEDGER_LOCK:
        _append_ledger_line(row)


def remember_open_before_send(
    executor_id: str,
    order_id: Optional[str],
    level_id: Optional[str],
    price: Optional[str],
    account_name: Optional[str] = None,
    connector: Optional[str] = None,
    trading_pair: Optional[str] = None,
    ts: Optional[float] = None,
) -> bool:
    """Marker and live name first. The marker stays until a later read sees the file."""
    if not executor_id or not order_id:
        return False
    record = {
        "executor_id": executor_id,
        "order_id": order_id,
        "level_id": level_id,
        "price": price,
        "account_name": account_name,
        "connector": connector,
        "trading_pair": trading_pair,
        "ts": time.time() if ts is None else ts,
    }
    if not _owed_complete(record):
        return False
    with LEDGER_LOCK:
        if not _append_marker_line(record):
            return False
        if not _append_ledger_line(record, marker_ready=True):
            return False
        return _fsync_live_name()


def prune_order_ledger(protected: Optional[set] = None, proven: Optional[set] = None) -> None:
    """Drop ids older than the keep window once, before resume places again.

    Not on the place path. A row with no ts and no price is not kept, as
    today. Every other old row — numeric ts below the cutoff, or a missing
    ts with a price — is kept unless the started owner proved its price ban:
    ``proven`` carries (executor_id, order_id) pairs whose started executor
    already leads the id and whose committed checkpoint holds the same order
    id with a price, so the send gate keeps blocking that price without the
    file. ``protected`` is neither permission nor proof: an unproven old row
    stays whether or not it is listed there. A failed read must not replace
    the live file.
    """
    path = order_ledger_path()
    with LEDGER_LOCK:
        if not os.path.exists(path):
            return
        try:
            size = os.path.getsize(path)
        except OSError as exc:
            logger.warning("order ledger prune skipped: %s", exc)
            return
        try:
            existing = _read_ledger()
        except _LedgerReadError:
            logger.warning("order ledger prune skipped; file left unchanged")
            return
        if size > 0 and not existing:
            logger.warning("order ledger prune skipped; file has no readable rows")
            return
        if ledger_order_ids() is None:
            logger.warning("order ledger prune skipped; file was not read")
            return
        cutoff = time.time() - LEDGER_KEEP_SECONDS
        kept = []
        for item in existing:
            raw_ts = item.get("ts")
            no_ts = raw_ts in (None, "")
            try:
                ts = 0.0 if no_ts else float(raw_ts)
            except (TypeError, ValueError):
                no_ts = True
                ts = 0.0
            if ts >= cutoff:
                kept.append(item)
                continue
            if proven is not None and (item.get("executor_id"), item.get("order_id")) in proven:
                continue
            if no_ts and item.get("price") in (None, ""):
                # Today's garbage rule: no ts and no price holds nothing.
                continue
            # An old line with a ts, and a priced line with no ts, wait for proof.
            kept.append(item)
        if len(kept) == len(existing):
            return
        _write_ledger(kept)


def ledger_debt_marker() -> str:
    directory = os.path.dirname(order_ledger_path()) or "."
    parent = os.path.dirname(directory) or "."
    return os.path.join(parent, ".order-ledger-debt")


def reset_ledger_process_state() -> None:
    """Drop this process's memory of the ledger. The marker file stays."""
    global _MARKER_FD, _MARKER_FD_PATH, _MARKER_NAME_SYNCED, _MARKER_UNLINK_DIR_OWED
    global _LEDGER_NAME_FSYNC_OWED, _LEDGER_PARENT_FSYNC_OWED, _BOUND_LEDGER_PATH
    global _PARTIAL_ABSENT, _LEDGER_SNAPSHOT
    _PARTIAL_ABSENT = set()
    _LEDGER_SNAPSHOT = None
    _bump_ledger_version()
    ledger_cache_reset_stats()
    if _MARKER_FD is not None:
        try:
            os.close(_MARKER_FD)
        except OSError:
            pass
    _MARKER_FD = None
    _MARKER_FD_PATH = None
    _MARKER_NAME_SYNCED = False
    _MARKER_UNLINK_DIR_OWED = False
    _LEDGER_NAME_FSYNC_OWED = None
    _LEDGER_PARENT_FSYNC_OWED = None
    _BOUND_LEDGER_PATH = order_ledger_path()


def _bind_process_to_ledger_path() -> None:
    global _BOUND_LEDGER_PATH
    path = order_ledger_path()
    if _BOUND_LEDGER_PATH != path:
        reset_ledger_process_state()


def _bind_marker_fd() -> None:
    global _MARKER_FD, _MARKER_FD_PATH, _MARKER_NAME_SYNCED
    path = ledger_debt_marker()
    if _MARKER_FD is not None and _MARKER_FD_PATH != path:
        try:
            os.close(_MARKER_FD)
        except OSError:
            pass
        _MARKER_FD = None
        _MARKER_NAME_SYNCED = False
    _MARKER_FD_PATH = path


# Ids a full read proved the exchange never accepted. The marker file is not rewritten.
_PARTIAL_ABSENT: set = set()


def note_partial_absent(order_id: Optional[str]) -> None:
    """Skip one owed id on read. Do not unlink or rewrite the marker file."""
    if order_id:
        _PARTIAL_ABSENT.add(str(order_id))


def _marker_debt_is_only_absent() -> bool:
    if not _PARTIAL_ABSENT or not os.path.exists(ledger_debt_marker()):
        return False
    owed = owed_records()
    if owed is None:
        return False
    return not any(str(item.get("order_id") or "") not in _PARTIAL_ABSENT for item in owed)


def buy_blocked() -> bool:
    """A live marker, or a directory entry not yet durable, is not permission to buy."""
    _bind_process_to_ledger_path()
    if _MARKER_UNLINK_DIR_OWED or _LEDGER_NAME_FSYNC_OWED or _LEDGER_PARENT_FSYNC_OWED:
        return True
    if not os.path.exists(ledger_debt_marker()):
        return False
    if not _PARTIAL_ABSENT:
        return True
    owed = owed_records()
    if owed is None:
        return True
    return any(str(item.get("order_id") or "") not in _PARTIAL_ABSENT for item in owed)


def _marker_name_synced() -> bool:
    return bool(_MARKER_NAME_SYNCED and (_MARKER_FD is not None or os.path.exists(ledger_debt_marker())))


def _owed_complete(record: Dict[str, Any]) -> bool:
    return all(record.get(key) not in (None, "") for key in OWED_FIELDS)


def _append_marker_line(record: Dict[str, Any]) -> bool:
    """Append. Do not truncate, rewrite, or open the marker a second time."""
    global _MARKER_FD, _MARKER_NAME_SYNCED
    _bind_process_to_ledger_path()
    _bind_marker_fd()
    marker = ledger_debt_marker()
    directory = os.path.dirname(marker) or "."
    if not os.path.isdir(directory):
        return False
    if _MARKER_FD is None:
        try:
            _MARKER_FD = os.open(marker, os.O_RDWR | os.O_CREAT | os.O_APPEND, 0o644)
        except OSError:
            _MARKER_FD = None
            return False
        _MARKER_NAME_SYNCED = False
    try:
        os.write(_MARKER_FD, (json.dumps(record) + "\n").encode("utf-8"))
        os.fsync(_MARKER_FD)
    except OSError:
        return False
    if not _fsync_directory(marker):
        return False
    _MARKER_NAME_SYNCED = True
    return True


def _marker_text() -> Optional[str]:
    """Read the held descriptor. A new process opens the marker once, without truncation."""
    global _MARKER_FD
    _bind_marker_fd()
    marker = ledger_debt_marker()
    if _MARKER_FD is None:
        if not os.path.exists(marker):
            return ""
        try:
            _MARKER_FD = os.open(marker, os.O_RDWR | os.O_APPEND, 0o644)
        except OSError:
            return None
    try:
        os.lseek(_MARKER_FD, 0, os.SEEK_SET)
        chunks = []
        while True:
            piece = os.read(_MARKER_FD, 65536)
            if not piece:
                break
            chunks.append(piece)
        os.lseek(_MARKER_FD, 0, os.SEEK_END)
    except OSError:
        return None
    try:
        return b"".join(chunks).decode("utf-8")
    except UnicodeDecodeError:
        return None


def _fsync_live_file(path: str) -> bool:
    """Fsync the file that the live name already points at. Not a create, not a truncate."""
    try:
        descriptor = os.open(path, os.O_RDWR)
    except OSError:
        return False
    try:
        os.fsync(descriptor)
    except OSError:
        return False
    finally:
        os.close(descriptor)
    return True


def _fsync_new_directory(directory: str) -> bool:
    """Fsync the parent after the new directory name exists. A miss keeps the debt."""
    global _LEDGER_PARENT_FSYNC_OWED
    if not _fsync_directory(directory):
        return False
    parent = os.path.dirname(directory) or "."
    if parent != directory and not _fsync_directory(parent):
        return False
    if _LEDGER_PARENT_FSYNC_OWED == directory:
        _LEDGER_PARENT_FSYNC_OWED = None
    return True


def _journal_has_owed(owed: List[Dict[str, Any]]) -> bool:
    try:
        rows = _read_ledger()
    except _LedgerReadError:
        return False
    by_id = {str(row.get("order_id")): row for row in rows if row.get("order_id")}
    for item in owed:
        order_id = item.get("order_id")
        if not order_id:
            continue
        live = by_id.get(str(order_id))
        if live is None:
            return False
        if item.get("price") is not None and str(live.get("price")) != str(item.get("price")):
            return False
    return True


def _rows_plus_owed(rows: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
    if os.path.exists(ledger_debt_marker()):
        owed = owed_records()
        if owed is None:
            return None
    else:
        owed = []
    kept = list(rows)
    seen = {item.get("order_id") for item in kept}
    for item in owed:
        if item.get("order_id") and item.get("order_id") not in seen:
            kept.append(item)
            seen.add(item.get("order_id"))
    return kept


def _marker_synced_before_name(rows: List[Dict[str, Any]]) -> bool:
    """Marker file and marker directory are durable before mkdir, O_CREAT, or replace."""
    payload = list(rows) or [{"ts": time.time()}]
    for row in payload:
        if not _append_marker_line(row):
            return False
    return _marker_name_synced()


def _unlink_marker() -> bool:
    """Unlink is the only way old owed bytes are removed. Directory fsync comes after it."""
    global _MARKER_FD, _MARKER_NAME_SYNCED, _MARKER_UNLINK_DIR_OWED
    marker = ledger_debt_marker()
    if _MARKER_FD is not None:
        try:
            os.close(_MARKER_FD)
        except OSError:
            pass
        _MARKER_FD = None
    if os.path.exists(marker):
        try:
            os.unlink(marker)
        except OSError:
            return False
    _MARKER_NAME_SYNCED = False
    _MARKER_UNLINK_DIR_OWED = True
    if not _fsync_directory(marker):
        return False
    _MARKER_UNLINK_DIR_OWED = False
    return not os.path.exists(marker)


def _release_marker_if_journal_has_owed() -> bool:
    marker = ledger_debt_marker()
    if not os.path.exists(marker) and not _MARKER_UNLINK_DIR_OWED:
        return not buy_blocked()
    path = order_ledger_path()
    if not os.path.exists(path) or _LEDGER_NAME_FSYNC_OWED == path or _LEDGER_PARENT_FSYNC_OWED:
        return False
    owed = owed_records()
    if owed is None or not _journal_has_owed(owed):
        return False
    if not _unlink_marker():
        return False
    return not buy_blocked()


def _fsync_live_name() -> bool:
    """Fsync the live file and its directory. Do not unlink the marker."""
    global _LEDGER_NAME_FSYNC_OWED
    path = order_ledger_path()
    if not os.path.exists(path) or not _fsync_live_file(path):
        return False
    if _LEDGER_NAME_FSYNC_OWED == path or os.path.exists(ledger_debt_marker()):
        if not _fsync_directory(path):
            return False
        if _LEDGER_NAME_FSYNC_OWED == path:
            _LEDGER_NAME_FSYNC_OWED = None
    return not _LEDGER_NAME_FSYNC_OWED and not _LEDGER_PARENT_FSYNC_OWED


def _commit_live_name() -> bool:
    """Fsync the live file, then its directory, and only then drop the marker."""
    if not _fsync_live_name():
        return False
    if _LEDGER_PARENT_FSYNC_OWED:
        return False
    if not os.path.exists(ledger_debt_marker()) and not _MARKER_UNLINK_DIR_OWED:
        return not buy_blocked()
    return _release_marker_if_journal_has_owed()


def recover_folder_debt() -> bool:
    """A new process fsyncs the live file before it may clear the marker. Never sends."""
    with LEDGER_LOCK:
        return _recover_folder_debt_locked()


def _recover_folder_debt_locked() -> bool:
    global _LEDGER_NAME_FSYNC_OWED
    _bind_process_to_ledger_path()
    if not os.path.exists(ledger_debt_marker()) and not _MARKER_UNLINK_DIR_OWED:
        return not _LEDGER_NAME_FSYNC_OWED and not _LEDGER_PARENT_FSYNC_OWED
    path = order_ledger_path()
    if not os.path.exists(path):
        return False
    if not _fsync_live_file(path):
        return False
    owed = owed_records()
    if owed is None:
        return False
    if not _journal_has_owed(owed):
        for item in owed:
            if not item.get("order_id"):
                continue
            if not _append_ledger_line(item, marker_ready=True):
                return False
        if not _fsync_live_file(path):
            return False
    if not _fsync_directory(path):
        return False
    if _LEDGER_NAME_FSYNC_OWED == path:
        _LEDGER_NAME_FSYNC_OWED = None
    return _release_marker_if_journal_has_owed()


def ledger_order_ids() -> Optional[set]:
    """Every id still in the file. None means the file was not read or marker debt survived."""
    _bind_process_to_ledger_path()
    if _LEDGER_NAME_FSYNC_OWED or _LEDGER_PARENT_FSYNC_OWED or _MARKER_UNLINK_DIR_OWED:
        return None
    if os.path.exists(ledger_debt_marker()) and not _marker_debt_is_only_absent():
        return None
    try:
        rows = _ledger_snapshot()["raw"]
    except _LedgerReadError:
        return None
    return {
        item.get("order_id")
        for item in rows
        if item.get("order_id") and str(item.get("order_id")) not in _PARTIAL_ABSENT
    }


def owed_records() -> Optional[List[Dict[str, Any]]]:
    """None means the marker is debt and must not be treated as an empty list."""
    _bind_process_to_ledger_path()
    marker = ledger_debt_marker()
    if not os.path.exists(marker) and _MARKER_FD is None:
        return []
    text = _marker_text()
    if text is None or not text.strip():
        return None
    rows = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            return None
        if not isinstance(row, dict):
            return None
        rows.append(row)
    return rows or None


def note_owed(record: Dict[str, Any]) -> bool:
    """Append one complete owed line. False means do not send."""
    if not _owed_complete(record):
        return False
    with LEDGER_LOCK:
        return _append_marker_line(record)


def gate_base_is_zero(body: Dict[str, Any]) -> bool:
    """Cancelled with base 0. Missing amount or left is not zero."""
    if body.get("status") != "closed" or body.get("finish_as") != "cancelled":
        return False
    if "amount" not in body or "left" not in body:
        return False
    try:
        base = Decimal(str(body["amount"])) - Decimal(str(body["left"]))
    except (ArithmeticError, ValueError, TypeError):
        return False
    return base == 0


def four_proofs(
    status: Dict[str, Any],
    *,
    in_opens: bool,
    in_flight: bool,
    in_lost: bool,
    history_read: bool = False,
) -> bool:
    """Absence from both lists is not enough. History must have been read in this step."""
    if not history_read or in_opens or in_flight or in_lost:
        return False
    return gate_base_is_zero(status)


def ledger_executor_ids() -> Optional[List[str]]:
    """None while the marker is debt. After it is cleared, ids in the file, not an empty stub."""
    _bind_process_to_ledger_path()
    if os.path.exists(ledger_debt_marker()) or _MARKER_UNLINK_DIR_OWED:
        return None
    if _LEDGER_NAME_FSYNC_OWED or _LEDGER_PARENT_FSYNC_OWED:
        return None
    try:
        rows = _read_ledger()
    except _LedgerReadError:
        return None
    return [item.get("executor_id") for item in rows if item.get("executor_id")]


def recent_order_ids(executor_id: str) -> List[str]:
    """Ids noted for this executor inside the keep window, newest unique."""
    if not executor_id:
        return []
    try:
        rows = read_ledger_filtered()
    except _LedgerReadError:
        logger.warning("order ledger unreadable during resume; no extra ids")
        return []
    cutoff = time.time() - LEDGER_KEEP_SECONDS
    latest: Dict[str, float] = {}
    for row in rows:
        if row.get("executor_id") != executor_id or not row.get("order_id"):
            continue
        # Строка с id уровня сажается на свой уровень, не как голый номер.
        if row.get("level_id"):
            continue
        try:
            ts = float(row.get("ts") or 0)
        except (TypeError, ValueError):
            continue
        if ts < cutoff:
            continue
        latest[str(row["order_id"])] = ts
    return list(latest)


def accounted_orders(executor_id: str, checkpoint: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Order ids this executor must keep leading, with a price when one is known.

    Two independent reads: this executor's journal rows of any age, and the
    committed column holds of the same executor. Not a seating rule: no level
    is chosen here and the resume seating inputs are not fed.
    """
    if not executor_id:
        return {}
    found: Dict[str, Any] = {}
    try:
        rows = read_ledger_filtered()
    except _LedgerReadError:
        logger.warning("order ledger unreadable during resume; holds still lead")
        rows = []
    for row in rows:
        if row.get("executor_id") != executor_id or not row.get("order_id"):
            continue
        order_id = str(row["order_id"])
        price = row.get("price")
        if order_id not in found or (found[order_id] in (None, "") and price not in (None, "")):
            found[order_id] = price
    for hold in (checkpoint or {}).get("holds") or []:
        order_id = hold.get("order_id")
        if not order_id:
            continue
        order_id = str(order_id)
        price = hold.get("price")
        if order_id not in found or (found[order_id] in (None, "") and price not in (None, "")):
            found[order_id] = price
    return found


def seed_accounted_orders(executor: Any, accounted: Dict[str, Any], connector: Any = None) -> None:
    """Put accounted ids into the executor's ledger before start().

    A live in-flight object is bound to the number's accounting, not to a level
    slot chosen by price. The connector listener writes filled and gone into
    these rows.
    """
    ledger = order_ledger(executor)
    inflight = getattr(connector, "in_flight_orders", None)
    if not isinstance(inflight, dict):
        inflight = {}
    for order_id, price in (accounted or {}).items():
        if not order_id:
            continue
        row = ledger.setdefault(order_id, {
            "order_id": order_id,
            "filled": False,
            "gone": False,
        })
        if price not in (None, ""):
            row.setdefault("price", price)
        live = inflight.get(order_id)
        if live is not None and hasattr(live, "to_json"):
            row["order"] = live.to_json()


def note_placed(executor: Any, order_id: Optional[str]) -> None:
    if not order_id:
        return
    row = order_ledger(executor).setdefault(order_id, {
        "order_id": order_id,
        "filled": False,
        "gone": False,
    })
    row["gone"] = False


def note_filled(executor: Any, order_id: Optional[str], order_json: Optional[Dict[str, Any]] = None) -> None:
    """A fill is recorded when the event arrives, not on the next one-second tick."""
    if not order_id:
        return
    row = order_ledger(executor).setdefault(order_id, {
        "order_id": order_id,
        "filled": True,
        "gone": False,
    })
    row["filled"] = True
    row["gone"] = False
    if order_json:
        row["order"] = order_json


def note_gone(executor: Any, order_id: Optional[str]) -> None:
    """Drop an id only after the exchange has confirmed cancel or failure.

    A still-open in-flight order is not gone, even if the grid already cleared
    the level slot.
    """
    if not order_id:
        return
    if _order_still_open(executor, order_id):
        return
    row = order_ledger(executor).get(order_id)
    if row is None or row.get("filled"):
        return
    row["gone"] = True


def _order_still_open(executor: Any, order_id: str) -> bool:
    connectors = getattr(executor, "connectors", None) or {}
    for connector in connectors.values():
        live = (getattr(connector, "in_flight_orders", None) or {}).get(order_id)
        if live is not None and getattr(live, "is_open", False):
            return True
    return False


def remember_current_slots(executor: Any) -> None:
    """Copy ids the executor currently holds into the ledger, including fills."""
    pairs = (
        ("open_order", "active_open_order"),
        ("close_order", "active_close_order"),
    )
    for level in getattr(executor, "grid_levels", []) or []:
        for role, attr in pairs:
            _remember_tracked(executor, getattr(level, attr, None), role, getattr(level, "id", None))
    for role, attr in (
        ("open_order", "_open_order"),
        ("close_order", "_close_order"),
        ("take_profit_order", "_take_profit_limit_order"),
        ("order", "_order"),
    ):
        _remember_tracked(executor, getattr(executor, attr, None), role, None)
    for tracked in list(getattr(executor, "_open_orders", []) or []) + list(getattr(executor, "_close_orders", []) or []):
        _remember_tracked(executor, tracked, "open_order", None)


def _remember_tracked(executor: Any, tracked: Any, role: str, level_id: Optional[str]) -> None:
    order_id = getattr(tracked, "order_id", None) if tracked is not None else None
    if not order_id:
        return
    note_placed(executor, order_id)
    row = order_ledger(executor)[order_id]
    row["role"] = role
    if level_id:
        row["level_id"] = level_id
    live = getattr(tracked, "order", None)
    if live is not None and hasattr(live, "to_json"):
        row["order"] = live.to_json()
        if getattr(live, "is_filled", False):
            note_filled(executor, order_id, row["order"])


def _saved_order_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "order_id": row["order_id"],
        "filled": bool(row.get("filled")),
        "order": row.get("order"),
        "level_id": row.get("level_id"),
        "role": row.get("role"),
    }


def merge_ledger(body: Dict[str, Any], ledger: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Put ledger ids back onto empty slots. A cleared slot must not forget a live order."""
    kept = [row for row in ledger.values() if row.get("order_id") and not row.get("gone")]
    if body.get("type") == "grid_executor":
        used = set()
        for level in body.get("levels") or []:
            for key in ("open_order", "close_order"):
                item = level.get(key) or {}
                if item.get("order_id"):
                    used.add(item["order_id"])
                    fresher = ledger.get(item["order_id"])
                    if fresher and fresher.get("filled"):
                        payload = _saved_order_payload(fresher)
                        if payload.get("price") in (None, ""):
                            kept_price = item.get("price")
                            if kept_price in (None, ""):
                                kept_price = level.get("price")
                            if kept_price not in (None, ""):
                                payload["price"] = kept_price
                        level[key] = payload
        marked = {
            str(item.get("order_id"))
            for item in body.get("holds") or []
            if isinstance(item, dict) and item.get("status") == "canceled" and item.get("base") == "0"
        }
        for row in kept:
            if row["order_id"] in used or str(row["order_id"]) in marked:
                continue
            role = row.get("role") if row.get("role") in ("open_order", "close_order") else "open_order"
            target = next((level for level in body.get("levels") or [] if level.get("id") == row.get("level_id")), None)
            if target is not None and not (target.get(role) or {}).get("order_id"):
                target[role] = _saved_order_payload(row)
                used.add(row["order_id"])
            else:
                body.setdefault("retained_orders", []).append(_saved_order_payload(row))
    else:
        named = {item.get("order_id") for item in _saved_orders(body)}
        extra = [_saved_order_payload(row) for row in kept if row["order_id"] not in named]
        if extra:
            body["retained_orders"] = extra
    return body


def capture_executor(executor: Any, executor_type: str) -> Dict[str, Any]:
    """JSON-ready ownership snapshot. Unknown types are refused, not guessed."""
    if executor_type not in _CAPTURERS:
        raise ValueError(f"cannot checkpoint executor type {executor_type}")
    remember_current_slots(executor)
    body = _CAPTURERS[executor_type](executor)
    body["version"] = CHECKPOINT_VERSION
    body["type"] = executor_type
    return merge_ledger(body, order_ledger(executor))


def parse_checkpoint(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(parsed, dict):
        return None
    if parsed.get("version") != CHECKPOINT_VERSION:
        return None
    if parsed.get("type") not in SUPPORTED_TYPES:
        return None
    return parsed


# ── Кэш расшифровки памяти ──────────────────────────────────────────────
#
# Тело памяти сетки читается сотни раз за шаг, а расшифровка одного тела
# стоит десятки миллисекунд. 9 октября 2026 на Tokyo шаг не успевал
# завершиться: программа стояла на процессоре и не открывала порт.
# Разбор теперь один на текст — дальше читатели берут готовое.
#
# Ключ — хеш полной длины от полного текста. Не номер исполнителя: у
# погашенных строк executor_id скрыт (остаётся row_executor_id), ключ по
# номеру склеил бы такие строки в одну расшифровку и потерял бы занятую
# цену — на ту же цену встала бы вторая заявка.

_CHECKPOINT_CACHE: Dict[str, Any] = {}
_CHECKPOINT_DECORATED: Dict[str, Any] = {}
_CHECKPOINT_CACHE_STATS = {"parses": 0, "copies": 0}
_CHECKPOINT_CACHE_MISS = object()


def checkpoint_text_key(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def checkpoint_master(raw: Any) -> Optional[Dict[str, Any]]:
    """Разобранное тело из кэша, сам мастер. Только для построения наборов:
    тело общее для всех читателей этого текста, его не правят."""
    if isinstance(raw, dict):
        return raw
    if not raw:
        return None
    key = checkpoint_text_key(raw)
    master = _CHECKPOINT_CACHE.get(key, _CHECKPOINT_CACHE_MISS)
    if master is _CHECKPOINT_CACHE_MISS:
        master = parse_checkpoint(raw)
        _CHECKPOINT_CACHE[key] = master
        _CHECKPOINT_CACHE_STATS["parses"] += 1
    return master


def parse_checkpoint_cached(raw: Any) -> Optional[Dict[str, Any]]:
    """Глубокая копия разобранного тела: правка копии не остаётся в кэше.

    Остаётся для читателей, которые правят тело (путь старта и восстановления).
    Читающий читатель берёт checkpoint_view без копии."""
    if isinstance(raw, dict):
        note_checkpoint_copy()
        return copy.deepcopy(raw)
    master = checkpoint_master(raw)
    if master is None:
        return None
    note_checkpoint_copy()
    return copy.deepcopy(master)


def checkpoint_view(raw: Any) -> Optional[Dict[str, Any]]:
    """Разобранное тело без копии: общий для читателей, только чтение.

    Правящий читатель берёт copy.deepcopy(view) или parse_checkpoint_cached."""
    return checkpoint_master(raw)


def slot_price(slot: Any, level: Dict[str, Any], allow_level_price: bool):
    """Цена слота: сам слот, его запись заявки, для открытия — цена уровня."""
    if not isinstance(slot, dict):
        return None
    price = slot.get("price")
    if price in (None, ""):
        order = slot.get("order")
        if isinstance(order, dict):
            price = order.get("price")
    if price in (None, "") and allow_level_price:
        price = level.get("price")
    if price in (None, ""):
        return None
    return price


def decorate_checkpoint_body(body: Dict[str, Any]) -> Dict[str, Any]:
    """Декор прежнего _price_checkpoint по уже своей копии тела: держатели
    дополняются ценами слотов; колонка не назначается и retained_orders не трогаются."""
    holds = [item for item in (body.get("holds") or []) if isinstance(item, dict)]
    known = {str(item.get("order_id")) for item in holds if item.get("order_id")}
    for level in body.get("levels") or []:
        if not isinstance(level, dict):
            continue
        for key, allow_level in (("open_order", True), ("close_order", False)):
            slot = level.get(key) or {}
            if not isinstance(slot, dict):
                continue
            order_id = slot.get("order_id")
            if not order_id or str(order_id) in known:
                continue
            price = slot_price(slot, level, allow_level)
            if price in (None, ""):
                continue
            holds.append({"order_id": order_id, "price": str(price)})
            known.add(str(order_id))
    body["holds"] = holds
    return body


def decorate_checkpoint_copy(parsed: Any) -> Dict[str, Any]:
    """Прежний _price_checkpoint: декорированная копия своего тела."""
    note_checkpoint_copy()
    body = copy.deepcopy(parsed) if isinstance(parsed, dict) else {}
    return decorate_checkpoint_body(body)


def checkpoint_decorated(raw: Any) -> Dict[str, Any]:
    """Декорированное тело — один раз на текст, общий для читателей: не правится.

    Ответ ровно decorate_checkpoint_copy(parse_checkpoint(raw)). Срок кэша тот же,
    что у мастера: чистит checkpoint_cache_prune — каждая запись тела даёт новый
    текст, и без чистки копии копились бы в памяти. Правящий читатель берёт
    copy.deepcopy."""
    if isinstance(raw, dict) or not raw:
        return decorate_checkpoint_copy(raw)
    key = checkpoint_text_key(raw)
    hit = _CHECKPOINT_DECORATED.get(key, _CHECKPOINT_CACHE_MISS)
    if hit is _CHECKPOINT_CACHE_MISS:
        master = checkpoint_master(raw)
        hit = decorate_checkpoint_copy(master)
        _CHECKPOINT_DECORATED[key] = hit
    return hit


def note_checkpoint_copy() -> None:
    """Счётчик копий тела для свидетеля шага: все копии, не только parse_checkpoint_cached."""
    _CHECKPOINT_CACHE_STATS["copies"] += 1


def checkpoint_cache_prune(texts) -> None:
    """В кэше остаются только тексты текущих строк. Каждая запись тела даёт
    новый текст, и без срока жизни копии накапливаются в памяти."""
    keep = {checkpoint_text_key(text) for text in texts or [] if isinstance(text, str) and text}
    for key in [key for key in _CHECKPOINT_CACHE if key not in keep]:
        _CHECKPOINT_CACHE.pop(key, None)
    for key in [key for key in _CHECKPOINT_DECORATED if key not in keep]:
        _CHECKPOINT_DECORATED.pop(key, None)


def checkpoint_cache_stats() -> Dict[str, int]:
    return dict(_CHECKPOINT_CACHE_STATS)


def checkpoint_cache_reset_stats() -> None:
    _CHECKPOINT_CACHE_STATS["parses"] = 0
    _CHECKPOINT_CACHE_STATS["copies"] = 0


def _saved_orders(node: Any) -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []
    if isinstance(node, dict):
        if "order_id" in node and "filled" in node:
            found.append(node)
        for value in node.values():
            found.extend(_saved_orders(value))
    elif isinstance(node, list):
        for value in node:
            found.extend(_saved_orders(value))
    return found


def saved_order_ids(checkpoint: Optional[Dict[str, Any]]) -> set:
    if not checkpoint:
        return set()
    return {item["order_id"] for item in _saved_orders(checkpoint) if item.get("order_id")}


def has_exposure(checkpoint: Dict[str, Any]) -> bool:
    """True when the snapshot already owns an order or an on-chain position."""
    if checkpoint.get("type") == "lp_executor" and (
        checkpoint.get("position_address") or checkpoint.get("saw_open_attempt")
    ):
        return True
    return any(item.get("order_id") for item in _saved_orders(checkpoint))


def _prices_match(left: Any, right: Any) -> bool:
    try:
        a = Decimal(str(left))
        b = Decimal(str(right))
    except (ArithmeticError, ValueError, TypeError):
        return False
    if a == 0 or b == 0:
        return a == b
    return abs(a - b) / max(abs(a), abs(b)) <= PRICE_MATCH


def _live_index(live_orders: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {order["order_id"]: order for order in live_orders if order.get("order_id")}


def resolve_saved_order(
    saved: Optional[Dict[str, Any]],
    live_by_id: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """How to reattach one saved order.

    live: the connector still tracks it — use that object.
    snapshot: it filled and left the book — use the saved order JSON.
    None: it is gone and was not filled — the level may place a replacement.
    """
    if not saved or not saved.get("order_id"):
        return None
    live = live_by_id.get(saved["order_id"])
    if live is not None:
        return {"order_id": saved["order_id"], "source": "live", "order": saved.get("order")}
    if saved.get("filled") and saved.get("order"):
        return {"order_id": saved["order_id"], "source": "snapshot", "order": saved["order"]}
    return None


# An order this close to the market is "at the current price", not at its level.
TOUCH_BAND = Decimal("0.003")


def _price_missing(value: Any) -> bool:
    """True for a missing quote, including NaN, which does not raise."""
    if value is None:
        return True
    if isinstance(value, float):
        return value != value
    if isinstance(value, Decimal):
        return value.is_nan()
    try:
        parsed = Decimal(str(value))
    except (ArithmeticError, ValueError, TypeError):
        return True
    return parsed.is_nan()


def _near_market(price: Any, market: Any) -> bool:
    try:
        mid = Decimal(str(market))
        quoted = Decimal(str(price))
    except (ArithmeticError, ValueError, TypeError):
        return False
    if mid == 0:
        return False
    return abs(quoted - mid) / abs(mid) <= TOUCH_BAND


def _saved_ids(checkpoint: Dict[str, Any]) -> set:
    claimed = set()
    for level in checkpoint.get("levels") or []:
        for key in ("open_order", "close_order"):
            saved = level.get(key) or {}
            if saved.get("order_id"):
                claimed.add(saved["order_id"])
    for saved in checkpoint.get("retained_orders") or []:
        if saved.get("order_id"):
            claimed.add(saved["order_id"])
    return claimed - {
        item for item in close_hold_ids(checkpoint)
        if _hold_role(checkpoint, item) in ("close", "market_close", "close_order")
    }


def merge_recent_ids(checkpoint: Dict[str, Any], order_ids: List[str]) -> Dict[str, Any]:
    """Add ids written to disk a moment before the database note."""
    claimed = _saved_ids(checkpoint)
    extra = [order_id for order_id in order_ids if order_id not in claimed]
    if not extra:
        return checkpoint
    updated = dict(checkpoint)
    retained = list(updated.get("retained_orders") or [])
    for order_id in extra:
        retained.append({
            "order_id": order_id,
            "filled": False,
            "order": None,
            "role": "open_order",
        })
    updated["retained_orders"] = retained
    return updated


def would_place_at_touch(level_price: Any, market_price: Any, side: Optional[str]) -> bool:
    """True when the grid would put this level on the current price, not its own."""
    try:
        price = Decimal(str(level_price))
        market = Decimal(str(market_price))
    except (ArithmeticError, ValueError, TypeError):
        return False
    if side == "BUY":
        return price >= market
    if side == "SELL":
        return price <= market
    return False


def _order_id(order: Any) -> Optional[str]:
    if isinstance(order, dict):
        raw = order.get("order_id") or order.get("client_order_id")
    else:
        raw = getattr(order, "client_order_id", None) or getattr(order, "order_id", None)
    return str(raw) if raw else None


def _order_price(order: Any) -> Any:
    if isinstance(order, dict):
        return order.get("price")
    return getattr(order, "price", None)


def _is_open(order: Any) -> bool:
    if isinstance(order, dict):
        return bool(order.get("is_open", True))
    return bool(getattr(order, "is_open", False))


def _side_name(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    name = getattr(raw, "name", None)
    text = str(name or raw).upper()
    if text.endswith("BUY"):
        return "BUY"
    if text.endswith("SELL"):
        return "SELL"
    return None


def resolve_close_bans(
    checkpoint: Dict[str, Any],
    live_orders: List[Any],
    side_name: Optional[str],
    column_ids: Optional[set] = None,
) -> set:
    """A role-less line is an entry only when the fresh list says so.

    Moves ids whose side reads as the grid entry side into retained_orders and
    returns the bans that stay. column_ids, when given, loses those ids too, so
    a read entry does not block every level's first close forever.
    """
    bans = [str(item) for item in checkpoint.get(CLOSE_BAN_KEY) or [] if item]
    if not bans:
        return set()
    live_by_id = {}
    for order in live_orders:
        order_id = getattr(order, "client_order_id", None) or _order_id(order)
        if order_id:
            live_by_id[str(order_id)] = order
    kept = []
    entries = []
    for order_id in bans:
        order = live_by_id.get(order_id)
        side = _order_side(order) if order is not None else None
        if side is not None and side_name is not None and side == side_name:
            entries.append(order_id)
        else:
            kept.append(order_id)
    if entries:
        retained = list(checkpoint.get("retained_orders") or [])
        known = {item.get("order_id") for item in retained if isinstance(item, dict)}
        for order_id in entries:
            if order_id not in known:
                retained.append({
                    "order_id": order_id, "filled": False, "order": None, "role": "open_order",
                })
        checkpoint["retained_orders"] = retained
        checkpoint[CLOSE_BAN_KEY] = kept
        if column_ids is not None:
            column_ids.difference_update(entries)
    return set(kept)


def restore_close_memory(executor: Any, checkpoint: Dict[str, Any], restore_ids: List[str]) -> None:
    """Put the post-resolve ban back after a thrown seating.

    restore_ids are the bans resolve left (a read entry is not among them).
    Ids a partial apply already added to the checkpoint stay: the ban list is
    merged, never replaced. The snapshot after start keeps every id.
    """
    target = {str(item) for item in restore_ids if item}
    target.update(
        str(item) for item in checkpoint.get(CLOSE_BAN_KEY) or [] if item
    )
    checkpoint[CLOSE_BAN_KEY] = sorted(target)
    holds = list(getattr(executor, "_close_holds", []) or [])
    known = {item.get("order_id") for item in holds}
    for item in checkpoint.get(CLOSE_HOLD_KEY) or []:
        order_id = item.get("order_id") if isinstance(item, dict) else None
        if order_id and order_id not in known:
            holds.append(item if isinstance(item, dict) else {"order_id": order_id})
            known.add(order_id)
    executor._close_holds = holds
    bans = set(getattr(executor, "_close_ban_ids", set()) or set())
    bans.update(target)
    executor._close_ban_ids = bans
    column_ids = set(getattr(executor, "_column_close_ids", set()) or set())
    column_ids.update(bans)
    executor._column_close_ids = column_ids


def _order_side(order: Any) -> Optional[str]:
    if isinstance(order, dict):
        return _side_name(order.get("side") or order.get("trade_type"))
    return _side_name(getattr(order, "trade_type", None) or getattr(order, "side", None))


def _level_side(level: Dict[str, Any]) -> Optional[str]:
    return _side_name(level.get("side"))


def _copy_levels(levels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    copied = []
    for level in levels:
        item = dict(level)
        if item.get("open_order"):
            item["open_order"] = dict(item["open_order"])
        copied.append(item)
    return copied


def seat_known_opens(
    levels: List[Dict[str, Any]],
    live_orders: List[Any],
    allowed_ids: set,
) -> set:
    """Seat an order this checkpoint already owns onto one empty level.

    A foreign id is not seated. Two matching levels are not a guess: the order
    stays loose and the resume plan caps a new order at that price.
    """
    claimed = set()
    for level in levels:
        for key in ("open_order", "close_order"):
            saved = level.get(key) or {}
            if saved.get("order_id"):
                claimed.add(saved["order_id"])
    seated = set()
    for order in live_orders:
        order_id = _order_id(order)
        if not order_id or order_id not in allowed_ids or order_id in claimed or not _is_open(order):
            continue
        if order_id in _close_slot_ids(levels):
            continue
        side = _order_side(order)
        if side is None:
            continue
        matches = [
            level for level in levels
            if not (level.get("open_order") or {}).get("order_id")
            and _level_side(level) == side
            and _prices_match(_order_price(order), level.get("price"))
        ]
        if len(matches) != 1:
            continue
        payload = {"order_id": order_id, "filled": False, "order": None}
        if hasattr(order, "to_json"):
            payload["order"] = order.to_json()
        matches[0]["open_order"] = payload
        claimed.add(order_id)
        seated.add(order_id)
    return seated


def matches_blocked_price(level_price: Any, blocked_prices: List[Any]) -> bool:
    return any(_prices_match(level_price, blocked) for blocked in blocked_prices)


def grid_resume_plan(
    checkpoint: Dict[str, Any],
    live_orders: List[Dict[str, Any]],
    market_price: Any = None,
    side: Optional[str] = None,
) -> Dict[str, Any]:
    """How a grid may continue when one order was not in the last note.

    The grid stays alive. Orders whose numbers we have keep working. An order
    that is still open and not seated on a level blocks a new order at that
    price. Orders that would land on the current market price are capped.
    """
    levels = _copy_levels(checkpoint.get("levels") or [])
    seat_known_opens(levels, live_orders, _saved_ids(checkpoint))
    attached = set()
    for level in levels:
        for key in ("open_order", "close_order"):
            saved = level.get(key) or {}
            if saved.get("order_id"):
                attached.add(saved["order_id"])
    loose = [
        order for order in live_orders
        if _is_open(order) and _order_id(order) and _order_id(order) not in attached
    ]
    if not loose:
        return {
            "touch_budget": None,
            "notice": None,
            "ambiguous_ids": [],
            "blocked_prices": [],
            "blocked_orders": [],
            "touch_hold_ids": [],
            "touch_zero_unknown_market": False,
        }
    unknown_market = _price_missing(market_price)
    near = [] if unknown_market else [
        order for order in loose if _near_market(_order_price(order), market_price)
    ]
    budget = 0 if near or unknown_market else 1
    blocked_prices = [
        _text(_order_price(order)) for order in loose if _order_price(order) is not None
    ]
    blocked_orders = [
        {"order_id": _order_id(order), "price": _text(_order_price(order))}
        for order in loose
        if _order_id(order)
    ]
    pair = checkpoint.get("trading_pair") or "the grid"
    side_name = "buy" if side == "BUY" else "sell" if side == "SELL" else "either"
    if budget == 0:
        detail = (
            "An unrecognized order is already at the current price, "
            "so no new order is placed there."
        )
    else:
        detail = "At most one new order is placed at the current price."
    notice = (
        f"Grid {pair} ({side_name}) continued after restart. "
        f"{len(loose)} order(s) were missing from the last checkpoint. {detail} "
        "Other levels that would land on that same price are not restored. "
        "Known orders keep working."
    )
    return {
        "touch_budget": budget,
        "notice": notice,
        "ambiguous_ids": [_order_id(order) for order in loose],
        "blocked_prices": blocked_prices,
        "blocked_orders": blocked_orders,
        "touch_hold_ids": [_order_id(order) for order in near if _order_id(order)],
        "touch_zero_unknown_market": unknown_market,
    }


def grid_can_resume(
    checkpoint: Dict[str, Any],
    live_orders: List[Dict[str, Any]],
) -> Optional[str]:
    """A grid with a checkpoint always comes back. The plan caps new orders."""
    return None


def lp_can_resume(checkpoint: Dict[str, Any]) -> Optional[str]:
    """A fresh LP start mints a new position. Refuse if an open was already tried."""
    if checkpoint.get("position_address"):
        return None
    if checkpoint.get("saw_open_attempt"):
        return "LP open was already attempted and the position address was not saved"
    return None


def can_resume(checkpoint: Optional[Dict[str, Any]], live_orders: Optional[List[Dict[str, Any]]] = None) -> Optional[str]:
    """None when this checkpoint may be started again. Otherwise the reason."""
    if not checkpoint:
        return "no checkpoint"
    if checkpoint.get("type") not in SUPPORTED_TYPES:
        return f"unsupported type {checkpoint.get('type')}"
    if checkpoint.get("type") == "grid_executor":
        return grid_can_resume(checkpoint, live_orders or [])
    if checkpoint.get("type") == "lp_executor":
        return lp_can_resume(checkpoint)
    # A checkpoint that has not recorded an order yet must not start beside an
    # open order on the same pair: that order may be the one this crash lost,
    # and a fresh start would place a second.
    if not has_exposure(checkpoint):
        unnamed = [
            order for order in (live_orders or [])
            if order.get("is_open") and order.get("order_id")
        ]
        if unnamed:
            return "exchange has an open order this checkpoint does not name"
    return None


def live_order_view(order: Any) -> Dict[str, Any]:
    return {
        "order_id": getattr(order, "client_order_id", None),
        "price": _text(getattr(order, "price", None)),
        "trading_pair": getattr(order, "trading_pair", None),
        "side": _order_side(order),
        "is_open": bool(getattr(order, "is_open", False)),
        "is_filled": bool(getattr(order, "is_filled", False)),
    }


class _RecordedFillBody:
    """Тело для штатного шага, когда полного снимка нет. Новый номер не чеканит."""

    def __init__(self, amount: Decimal, price: Decimal, body: Dict[str, Any]) -> None:
        self.amount = amount
        self.price = price
        self.executed_amount_base = amount
        self.executed_amount_quote = Decimal("0")
        self.is_filled = True
        self.is_done = True
        self.is_open = False
        self.base_asset = "BASE"
        self.quote_asset = "QUOTE"
        self.order_fills: Dict[str, Any] = {}
        self._body = body

    def cumulative_fee_paid(self, token: Any = None) -> Decimal:
        return Decimal("0")

    def to_json(self) -> Dict[str, Any]:
        return dict(self._body)


def _recorded_fill_body(saved: Dict[str, Any], order: Dict[str, Any]) -> Optional[_RecordedFillBody]:
    amount_raw = order.get("executed_amount_base")
    price_raw = saved.get("price")
    if amount_raw in (None, "") or price_raw in (None, ""):
        return None
    return _RecordedFillBody(Decimal(str(amount_raw)), Decimal(str(price_raw)), dict(order))


def _attach(saved: Optional[Dict[str, Any]], live_by_id: Dict[str, Any]) -> Any:
    """A TrackedOrder bound to the live in-flight order, or to the saved fill."""
    from hummingbot.core.data_type.in_flight_order import InFlightOrder
    from hummingbot.strategy_v2.models.executors import TrackedOrder

    if not saved or not saved.get("order_id"):
        return None
    resolved = resolve_saved_order(saved, {
        order_id: {"order_id": order_id} for order_id in live_by_id
    })
    # resolve_saved_order only needs the id to choose live vs snapshot vs drop.
    live = live_by_id.get(saved["order_id"])
    if live is not None:
        tracked = TrackedOrder(order_id=saved["order_id"])
        tracked.order = live
        return tracked
    if resolved and resolved["source"] == "snapshot" and resolved.get("order"):
        order = resolved["order"]
        tracked = TrackedOrder(order_id=str(saved["order_id"]))
        # Дырявый снимок не роняет подъём. Номер берём из записи, не из тела.
        if isinstance(order, dict):
            tracked._broken_snapshot = dict(order)
        if not isinstance(order, dict) or not order.get("client_order_id"):
            tracked.order = _recorded_fill_body(saved, order if isinstance(order, dict) else {})
            return tracked
        try:
            tracked.order = InFlightOrder.from_json(order)
        except Exception:
            tracked.order = None
            return tracked
        tracked._broken_snapshot = None
        return tracked
    return None




def seed_ledger(executor: Any, checkpoint: Dict[str, Any]) -> None:
    """The resumed executor keeps the same ids, so the next cancel can drop them.

    Номера завершённого круга не сеются: иначе merge_ledger вписал бы их обратно
    в пустые слоты, а читатели посадки взяли бы мёртвый вход (план
    okx-resume-complete-level).
    """
    ledger = order_ledger(executor)
    finished = {
        str(item)
        for item in (getattr(executor, "_notebook_completed_cycle_opens", None) or ())
    }
    finished.update(str(item) for item in partial_released_ids(checkpoint))
    for item in _saved_orders(checkpoint):
        order_id = item.get("order_id")
        if not order_id:
            continue
        if str(order_id) in finished:
            continue
        ledger[order_id] = {
            "order_id": order_id,
            "filled": bool(item.get("filled")),
            "gone": False,
            "order": item.get("order"),
            "level_id": item.get("level_id"),
            "role": item.get("role"),
        }


def _completed_pair_entries(raw: Dict[str, Any], opened: Dict[str, Any], close: Dict[str, Any], pair: Any, leverage: Any) -> Optional[List[Dict[str, Any]]]:
    """Обе записи _filled_orders круга из записанной правды, или None."""
    side = str(raw.get("side") or "")
    if side not in ("BUY", "SELL"):
        return None
    open_entry = completed_side_entry(
        opened,
        trade_type=side,
        position_name="OPEN" if side == "BUY" else "CLOSE",
        order_type_name=raw.get("open_order_type"),
        trading_pair=pair,
        leverage=leverage,
    )
    close_side = "SELL" if side == "BUY" else "BUY"
    close_entry = completed_side_entry(
        close,
        trade_type=close_side,
        position_name="CLOSE",
        order_type_name=raw.get("take_profit_order_type"),
        trading_pair=pair,
        leverage=leverage,
    )
    if open_entry is None or close_entry is None:
        return None
    return [open_entry, close_entry]


def _tracked_filled(tracked: Any) -> bool:
    """Слот исполнен. Чужая заглушка без is_filled — не исполнение."""
    if tracked is None:
        return False
    try:
        return bool(tracked.is_filled)
    except Exception:
        return False


def _format_completed_levels(executor: Any, checkpoint: Dict[str, Any]) -> None:
    """Оформить завершённые круги до штатного COMPLETE-пути (план
    okx-resume-complete-level).

    Обе стороны кладутся в _filled_orders, уровень сбрасывается в NOT_ACTIVE —
    та же уборка, какую штатный тик делает живьём. Повторный подъём при пустых
    слотах оформляет круг из записи в колонке. Номера круга остаются в списке
    пропуска: журнал исполнителя их не сеет, пересадка их не сажает.
    """
    raw_levels = checkpoint.get("levels") or []
    grid_levels = list(getattr(executor, "grid_levels", None) or [])
    completed_opens = set(str(item) for item in partial_released_ids(checkpoint))
    filled_orders = list(getattr(executor, "_filled_orders", None) or [])
    pair = ""
    leverage = 1
    config = getattr(executor, "config", None)
    if config is not None:
        pair = getattr(config, "trading_pair", "") or ""
        leverage = getattr(config, "leverage", 1) or 1
    for level, raw in zip(grid_levels, raw_levels):
        if not isinstance(raw, dict):
            continue
        # Обе стороны filled — условие завершённого круга (план), не штатный
        # update_state: тот ставит COMPLETE и по одной исполненной продаже,
        # а живой неисполненный вход сбрасывать нельзя.
        if not (_tracked_filled(getattr(level, "active_open_order", None))
                and _tracked_filled(getattr(level, "active_close_order", None))):
            continue
        entries = _completed_pair_entries(
            raw, raw.get("open_order") or {}, raw.get("close_order") or {}, pair, leverage,
        )
        if entries is None:
            # Недоказанное не вычёркиваем: поведение этого уровня как сегодня.
            continue
        filled_orders.extend(entries)
        completed_opens.update(str(entry.get("client_order_id") or "") for entry in entries)
        reset = getattr(level, "reset_level", None)
        if callable(reset):
            reset()
    released_ids = partial_released_ids(checkpoint)
    for raw in raw_levels:
        if not isinstance(raw, dict):
            continue
        opened = raw.get("open_order") or {}
        close = raw.get("close_order") or {}
        open_slot_id = str(opened.get("order_id") or "")
        close_slot_id = str(close.get("order_id") or "")
        if open_slot_id:
            # Живой вход — не завершённый круг, не трогаем.
            continue
        record = (checkpoint.get(COMPLETED_CYCLE_KEY) or {}).get(str(raw.get("id") or ""))
        if not isinstance(record, dict):
            continue
        record_close = str((record.get("close") or {}).get("order_id") or "")
        # Код-ревью 20261110-072441: слот может держать номер продажи этого круга
        # (посадка первого такта уехала в колонку) — оформляем из записи и лечим
        # уровень до штатного COMPLETE-пути, иначе следующий снимок мёртв.
        if close_slot_id and close_slot_id != record_close and close_slot_id not in released_ids:
            # Чужая незавершённая продажа — не наш случай.
            continue
        meta = record.get("level") if isinstance(record.get("level"), dict) else {}
        merged_raw = dict(raw)
        merged_raw.update({key: meta[key] for key in ("side", "open_order_type", "take_profit_order_type") if meta.get(key) is not None})
        entries = _completed_pair_entries(
            merged_raw, record.get("open") or {}, record.get("close") or {}, pair, leverage,
        )
        if entries is None:
            continue
        filled_orders.extend(entries)
        completed_opens.update(str(entry.get("client_order_id") or "") for entry in entries)
        level = next(
            (item for item in grid_levels if str(getattr(item, "id", "") or "") == str(raw.get("id") or "")),
            None,
        )
        if level is not None and (getattr(level, "active_close_order", None) is not None
                                   or getattr(level, "active_open_order", None) is not None):
            reset = getattr(level, "reset_level", None)
            if callable(reset):
                reset()
    executor._filled_orders = filled_orders
    executor._notebook_completed_cycle_opens = frozenset(completed_opens)


def apply_checkpoint(executor: Any, checkpoint: Dict[str, Any], live_orders: List[Any]) -> None:
    """Put the saved ownership back on a freshly built executor. Call before start()."""
    from decimal import Decimal as D
    from hummingbot.core.data_type.common import OrderType, TradeType
    from hummingbot.strategy_v2.executors.grid_executor.data_types import GridLevel
    from hummingbot.strategy_v2.executors.lp_executor.data_types import LPExecutorState

    live_by_id = {
        getattr(order, "client_order_id", None): order
        for order in live_orders
        if getattr(order, "client_order_id", None)
    }
    kind = checkpoint.get("type")

    if kind == "grid_executor":
        strip_close_holds_from_saved(checkpoint)
        seat_known_opens(
            checkpoint.get("levels") or [],
            live_orders,
            _saved_ids(checkpoint),
        )
        restored = []
        for raw in checkpoint.get("levels") or []:
            level = GridLevel(
                id=raw["id"],
                price=D(raw["price"]),
                amount_quote=D(raw["amount_quote"]),
                take_profit=D(raw["take_profit"]),
                side=TradeType[raw["side"]],
                open_order_type=OrderType[raw["open_order_type"]],
                take_profit_order_type=OrderType[raw["take_profit_order_type"]],
            )
            open_raw = raw.get("open_order")
            if isinstance(open_raw, dict) and open_raw.get("price") in (None, "") and raw.get("price") not in (None, ""):
                open_raw = dict(open_raw)
                open_raw["price"] = raw.get("price")
            level.active_open_order = _attach(open_raw, live_by_id)
            close_raw = raw.get("close_order") or {}
            partial_close = (
                close_raw.get("order_id")
                and close_raw.get("level_id")
                and close_raw.get("price") not in (None, "")
            )
            if partial_close:
                level.active_close_order = _attach_close_slot(close_raw, live_by_id)
            elif level.active_open_order is not None:
                level.active_close_order = _attach(raw.get("close_order"), live_by_id)
            else:
                bare = close_raw.get("order_id")
                if bare:
                    bans = list(checkpoint.get(CLOSE_BAN_KEY) or [])
                    if bare not in bans:
                        bans.append(bare)
                    checkpoint[CLOSE_BAN_KEY] = bans
            restored.append(level)
        executor.grid_levels = restored
        executor._close_order = _attach(checkpoint.get("close_order"), live_by_id)
        if executor._close_order is None:
            for item in checkpoint.get(CLOSE_HOLD_KEY) or []:
                if isinstance(item, dict) and item.get("role") == "market_close" and item.get("order_id"):
                    executor._close_order = _attach({"order_id": item["order_id"]}, live_by_id)
                    break
        executor._close_holds = list(checkpoint.get(CLOSE_HOLD_KEY) or [])
        executor._partial_seated = True
        # Завершённый круг оформляем сами, до штатного update_grid_levels:
        # его COMPLETE-путь читает completely_filled_event, которого нет у заглушки
        # компактного тела, и кладёт компактное тело в _filled_orders (план
        # okx-resume-complete-level). Уборка — та же, что у живого тика.
        _format_completed_levels(executor, checkpoint)
        if hasattr(executor, "update_grid_levels"):
            executor.update_grid_levels()
        seed_ledger(executor, checkpoint)
        return

    if kind == "position_executor":
        executor._open_order = _attach(checkpoint.get("open_order"), live_by_id)
        executor._close_order = _attach(checkpoint.get("close_order"), live_by_id)
        executor._take_profit_limit_order = _attach(checkpoint.get("take_profit_order"), live_by_id)
        seed_ledger(executor, checkpoint)
        return

    if kind == "dca_executor":
        executor._open_orders = [
            tracked for tracked in (
                _attach(item, live_by_id) for item in checkpoint.get("open_orders") or []
            ) if tracked is not None
        ]
        executor._close_orders = [
            tracked for tracked in (
                _attach(item, live_by_id) for item in checkpoint.get("close_orders") or []
            ) if tracked is not None
        ]
        seed_ledger(executor, checkpoint)
        return

    if kind == "order_executor":
        executor._order = _attach(checkpoint.get("order"), live_by_id)
        seed_ledger(executor, checkpoint)
        return

    if kind == "twap_executor":
        plan = {}
        for item in checkpoint.get("plan") or []:
            plan[float(item["timestamp"])] = _attach(item.get("order"), live_by_id)
        executor._order_plan = plan
        if checkpoint.get("start_timestamp") is not None:
            executor._start_timestamp = float(checkpoint["start_timestamp"])
        seed_ledger(executor, checkpoint)
        return

    if kind == "arbitrage_executor":
        buy = _attach(checkpoint.get("buy_order"), live_by_id)
        sell = _attach(checkpoint.get("sell_order"), live_by_id)
        if buy is not None:
            executor._buy_order = buy
        if sell is not None:
            executor._sell_order = sell
        seed_ledger(executor, checkpoint)
        return

    if kind == "xemm_executor":
        maker = _attach(checkpoint.get("maker_order"), live_by_id)
        taker = _attach(checkpoint.get("taker_order"), live_by_id)
        if maker is not None:
            executor.maker_order = maker
        if taker is not None:
            executor.taker_order = taker
        seed_ledger(executor, checkpoint)
        return

    if kind == "lp_executor":
        state = LPExecutorState.model_validate(checkpoint.get("state_json") or {})
        state.active_open_order = _attach(checkpoint.get("open_order"), live_by_id)
        state.active_close_order = _attach(checkpoint.get("close_order"), live_by_id)
        executor.lp_position_state = state
        seed_ledger(executor, checkpoint)
        return

    raise ValueError(f"cannot apply checkpoint for {kind}")

def cap_arguments(plan: Any) -> Dict[str, Any]:
    """Copy installer inputs without turning a missing key into an empty list."""
    if not isinstance(plan, dict):
        return {}
    keys = (
        "touch_budget",
        "blocked_orders",
        "blocked_prices",
        "touch_hold_ids",
        "touch_zero_unknown_market",
    )
    return {key: plan[key] for key in keys if key in plan}


def _decimal(value: Any) -> Optional[Decimal]:
    if isinstance(value, Decimal):
        return None if value.is_nan() else value
    if isinstance(value, float) and value != value:
        return None
    try:
        parsed = Decimal(str(value))
    except (ArithmeticError, ValueError, TypeError):
        return None
    if parsed.is_nan():
        return None
    return parsed


def _positive_tick(executor: Any) -> Optional[Decimal]:
    rules = getattr(executor, "trading_rules", None)
    tick = _decimal(getattr(rules, "min_price_increment", None))
    if tick is None or tick <= 0:
        return None
    return tick


def _quantize(price: Any, tick: Decimal) -> Optional[Decimal]:
    number = _decimal(price)
    if number is None:
        return None
    return (number // tick) * tick


def _same_tick(left: Any, right: Any, tick: Optional[Decimal]) -> Optional[bool]:
    """True when both prices land on one tick. None means the tick cannot be used."""
    if tick is None:
        return None
    quantized_left = _quantize(left, tick)
    quantized_right = _quantize(right, tick)
    if quantized_left is None or quantized_right is None:
        return None
    return quantized_left == quantized_right


def _side_from_config(executor: Any) -> Optional[str]:
    side = getattr(getattr(executor, "config", None), "side", None)
    name = getattr(side, "name", None)
    if name in ("BUY", "SELL"):
        return name
    return None


def _spread(executor: Any) -> Optional[Decimal]:
    spread = _decimal(getattr(getattr(executor, "config", None), "safe_extra_spread", None))
    if spread is None or spread < 0:
        return None
    return spread


def _quote(executor: Any) -> Optional[Decimal]:
    return _decimal(getattr(executor, "current_open_quote", None))


def _touch_formula(quote: Decimal, side: str, spread: Decimal) -> Decimal:
    if side == "BUY":
        return quote * (1 - spread)
    return quote * (1 + spread)


def _placement(level_price: Any, quote: Decimal, side: str, spread: Decimal) -> Optional[Decimal]:
    price = _decimal(level_price)
    if price is None:
        return None
    if side == "BUY" and price >= quote:
        return _touch_formula(quote, side, spread)
    if side == "SELL" and price <= quote:
        return _touch_formula(quote, side, spread)
    return price


def _pair_name(executor: Any) -> Optional[str]:
    return getattr(getattr(executor, "config", None), "trading_pair", None)


def _on_pair(order: Any, pair: Optional[str]) -> bool:
    if not pair:
        return True
    order_pair = getattr(order, "trading_pair", None)
    if order_pair is None and isinstance(order, dict):
        order_pair = order.get("trading_pair")
    return order_pair in (None, pair)


def _read_order_book(executor: Any) -> Optional[Dict[str, Any]]:
    """Active book plus tracker caches. None means the active book was not read."""
    connectors = getattr(executor, "connectors", None)
    if not connectors:
        return None
    pair = _pair_name(executor)
    active: List[Any] = []
    cached: List[Any] = []
    lost: List[Any] = []
    ready = True
    try:
        values = list(connectors.values())
        if not values:
            return None
        for connector in values:
            if getattr(connector, "ready", None) is not True:
                ready = False
            inflight = getattr(connector, "in_flight_orders", None)
            if not isinstance(inflight, dict):
                return None
            for order in inflight.values():
                if _on_pair(order, pair):
                    active.append(order)
            tracker = getattr(connector, "_order_tracker", None)
            if tracker is None:
                continue
            for order in (getattr(tracker, "cached_orders", None) or {}).values():
                if _on_pair(order, pair):
                    cached.append(order)
            for order in (getattr(tracker, "lost_orders", None) or {}).values():
                if _on_pair(order, pair):
                    lost.append(order)
    except Exception:
        logger.warning("grid resume book unreadable", exc_info=True)
        return None
    return {"ready": ready, "active": active, "cached": cached, "lost": lost}


def _cached_finished(book: Optional[Dict[str, Any]], remembered: set) -> set:
    """Completions already in the tracker cache. An unread active book is not a completion."""
    done = set(remembered)
    if not book:
        return done
    for order in book["cached"]:
        order_id = _order_id(order)
        if order_id and not _is_open(order):
            done.add(order_id)
    return done


def _finished_ids(book: Optional[Dict[str, Any]], remembered: set) -> set:
    done = _cached_finished(book, remembered)
    if not book or not book["ready"]:
        return done
    for order in book["active"]:
        order_id = _order_id(order)
        if order_id and not _is_open(order):
            done.add(order_id)
    return done


def _rows(value: Any) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(value, list):
        return None
    return [row for row in value if isinstance(row, dict)]


def _unfinished_rows(rows: Optional[List[Dict[str, Any]]], finished: set) -> List[Dict[str, Any]]:
    if not rows:
        return []
    kept = []
    for row in rows:
        order_id = row.get("order_id")
        if order_id and order_id in finished:
            continue
        kept.append(row)
    return kept


def _eternal_prices(prices: Any, pairs: Optional[List[Dict[str, Any]]], tick: Optional[Decimal]) -> List[Any]:
    if not isinstance(prices, list):
        return []
    if not isinstance(pairs, list):
        return list(prices)
    eternal = []
    for price in prices:
        paired = False
        for row in pairs:
            same = _same_tick(price, row.get("price"), tick)
            if same is True:
                paired = True
                break
        if not paired:
            eternal.append(price)
    return eternal


def _order_at_price(book: Optional[Dict[str, Any]], price: Any, tick: Optional[Decimal]) -> bool:
    if book is None or tick is None or price is None:
        return True
    for order in list(book["active"]) + list(book["lost"]):
        if order in book["active"] and not _is_open(order):
            continue
        if _same_tick(_order_price(order), price, tick) is True:
            return True
    return False


def _should_install(arguments: Dict[str, Any]) -> bool:
    orders = arguments.get("blocked_orders", None)
    holds = arguments.get("touch_hold_ids", None)
    prices = arguments.get("blocked_prices", None)
    if isinstance(orders, list) and orders:
        return True
    if isinstance(holds, list) and holds:
        return True
    if isinstance(prices, list) and prices and not isinstance(orders, list):
        return True
    if arguments.get("touch_zero_unknown_market"):
        return True
    if "touch_budget" in arguments and arguments["touch_budget"] is not None:
        return True
    return False


def install_grid_resume_cap(executor: Any, plan: Any) -> bool:
    """Wrap order proposals. False when there is nothing to limit."""
    arguments = cap_arguments(plan)
    if not _should_install(arguments):
        return False
    _bind_precise_cap(executor, arguments)
    return True


def blank_grid_cap(executor: Any) -> None:
    """Last resort: do not leave the original proposer in place."""
    executor.get_open_orders_to_create = lambda: []


def conservative_grid_cap(executor: Any, plan: Any) -> None:
    """Block known prices without ever turning a zero into one."""
    arguments = cap_arguments(plan)
    if not _should_install(arguments):
        return
    _bind_conservative_cap(executor, arguments)


def _known_prices(arguments: Dict[str, Any]) -> List[Any]:
    prices = list(arguments.get("blocked_prices") or [])
    orders = arguments.get("blocked_orders")
    if isinstance(orders, list):
        for row in orders:
            if isinstance(row, dict) and row.get("price") is not None:
                prices.append(row.get("price"))
    return prices


def _bind_conservative_cap(executor: Any, arguments: Dict[str, Any]) -> None:
    initial = arguments["touch_budget"] if "touch_budget" in arguments else 0
    executor._touch_initial_budget = initial
    executor._touch_place_budget = 0 if initial == 0 else initial
    executor._touch_zero_released = False
    original = executor.get_open_orders_to_create
    banned = _known_prices(arguments)

    def wrapped():
        try:
            proposed = list(original())
        except Exception:
            logger.warning("grid proposal failed under conservative cap", exc_info=True)
            return []
        allowed = []
        for level in proposed:
            price = getattr(level, "price", None)
            if any(str(price) == str(item) for item in banned):
                continue
            if initial == 0 and _conservative_touch(level, executor):
                continue
            allowed.append(level)
        return allowed

    executor.get_open_orders_to_create = wrapped


def _conservative_touch(level: Any, executor: Any) -> bool:
    side = getattr(getattr(executor, "config", None), "side", None)
    name = getattr(side, "name", None)
    quote = _decimal(getattr(executor, "current_open_quote", None))
    price = _decimal(getattr(level, "price", None))
    if name not in ("BUY", "SELL") or quote is None or price is None:
        return True
    if name == "BUY":
        return price >= quote
    return price <= quote


def _bind_precise_cap(executor: Any, arguments: Dict[str, Any]) -> None:
    initial = arguments["touch_budget"] if "touch_budget" in arguments else None
    executor._touch_initial_budget = initial
    executor._touch_place_budget = initial
    executor._touch_zero_released = initial in (1, None)
    executor._seen_finished = set()
    executor._seen_open = set()
    executor._cap_arguments = arguments
    original = executor.get_open_orders_to_create

    def wrapped():
        try:
            proposed = list(original())
        except Exception:
            logger.warning("grid proposal failed under resume cap", exc_info=True)
            return []
        try:
            return _filter_proposals(executor, proposed)
        except Exception:
            logger.warning("grid resume cap failed closed", exc_info=True)
            return []

    executor.get_open_orders_to_create = wrapped


def _filter_proposals(executor: Any, proposed: List[Any]) -> List[Any]:
    arguments = executor._cap_arguments
    book = _read_order_book(executor)
    if book is None or not book["ready"]:
        executor._seen_finished = _cached_finished(book, executor._seen_finished)
        return []
    finished = _finished_ids(book, executor._seen_finished)
    seen_open = _remember_open_ids(book, executor._seen_open)
    executor._seen_finished = finished
    executor._seen_open = seen_open
    tick = _positive_tick(executor)
    side = _side_from_config(executor)
    quote = _quote(executor)
    spread = _spread(executor)
    pairs = _rows(arguments.get("blocked_orders")) if "blocked_orders" in arguments else None
    prices = arguments.get("blocked_prices")
    if tick is None and (pairs or (isinstance(prices, list) and prices)):
        return []
    if side is None or quote is None or spread is None:
        return []
    touch_entry = _touch_formula(quote, side, spread)
    unfinished = _unfinished_rows(pairs, finished)
    eternal = _eternal_prices(prices, pairs, tick)
    if _uncomparable(unfinished, eternal, book, tick):
        return []
    allowed = []
    touch_returned = False
    for level in proposed:
        level_price = getattr(level, "price", None)
        placement = _placement(level_price, quote, side, spread)
        if placement is None or tick is None:
            continue
        touching = _same_tick(placement, touch_entry, tick) is True
        if _blocked(placement, tick, unfinished, eternal, book):
            continue
        if not touching:
            allowed.append(level)
            continue
        if touch_returned:
            continue
        if not _allow_touch(
            executor, arguments, book, finished, seen_open, unfinished, eternal, touch_entry, tick,
        ):
            continue
        if not _spend_touch(
            executor, arguments, book, finished, seen_open, unfinished, eternal, touch_entry, tick,
        ):
            continue
        touch_returned = True
        allowed.append(level)
    return allowed


def _uncomparable(unfinished, eternal, book, tick) -> bool:
    """A price we cannot compare is not proof that it differs from the level."""
    sources = [row.get("price") for row in unfinished]
    sources.extend(eternal)
    if book:
        for order in book["active"]:
            if _is_open(order):
                sources.append(_order_price(order))
        for order in book["lost"]:
            sources.append(_order_price(order))
    for price in sources:
        if _decimal(price) is None or tick is None or _quantize(price, tick) is None:
            return True
    return False


def _blocked(
    placement: Decimal,
    tick: Decimal,
    unfinished: List[Dict[str, Any]],
    eternal: List[Any],
    book: Dict[str, Any],
) -> bool:
    for row in unfinished:
        if row.get("price") is None:
            continue
        if _same_tick(row.get("price"), placement, tick) is True:
            return True
    for price in eternal:
        if _same_tick(price, placement, tick) is True:
            return True
    return _order_at_price(book, placement, tick)


def _holds_done(arguments: Dict[str, Any], finished: set) -> bool:
    holds = arguments.get("touch_hold_ids") or []
    if not isinstance(holds, list):
        return False
    return all(item in finished for item in holds)


def _remember_open_ids(book: Optional[Dict[str, Any]], remembered: set) -> set:
    seen = set(remembered)
    if not book:
        return seen
    for order in book["active"]:
        order_id = _order_id(order)
        if order_id and _is_open(order):
            seen.add(order_id)
    return seen


def _lost_ids(book: Optional[Dict[str, Any]]) -> set:
    if not book:
        return set()
    return {order_id for order in book["lost"] if (order_id := _order_id(order))}


def _unseen_or_lost(arguments: Dict[str, Any], book, finished: set, seen_open: set) -> bool:
    """An id we have not seen finished still holds the zero, whatever its price."""
    if "blocked_orders" not in arguments or not isinstance(arguments.get("blocked_orders"), list):
        return False
    lost = _lost_ids(book)
    for row in arguments["blocked_orders"]:
        if not isinstance(row, dict):
            return True
        order_id = row.get("order_id")
        if not order_id or order_id in lost:
            return True
        if order_id not in finished and order_id not in seen_open:
            return True
    return False


def _allow_touch(executor, arguments, book, finished, seen_open, unfinished, eternal, touch_entry, tick) -> bool:
    remaining = executor._touch_place_budget
    if remaining is None:
        return True
    if remaining > 0:
        return True
    return _can_release_zero(
        executor, arguments, book, finished, seen_open, unfinished, eternal, touch_entry, tick,
    )


def _can_release_zero(executor, arguments, book, finished, seen_open, unfinished, eternal, touch_entry, tick) -> bool:
    if executor._touch_initial_budget != 0 or executor._touch_zero_released:
        return False
    if book is None or not book["ready"] or tick is None:
        return False
    if not _holds_done(arguments, finished):
        return False
    if _unseen_or_lost(arguments, book, finished, seen_open):
        return False
    if "blocked_orders" not in arguments and isinstance(arguments.get("blocked_prices"), list) and arguments["blocked_prices"]:
        return False
    for row in unfinished:
        if _decimal(row.get("price")) is None:
            return False
        if _same_tick(row.get("price"), touch_entry, tick) is True:
            return False
    for price in eternal:
        same = _same_tick(price, touch_entry, tick)
        if same is None or same:
            return False
    if _order_at_price(book, touch_entry, tick):
        return False
    return True


def _spend_touch(executor, arguments, book, finished, seen_open, unfinished, eternal, touch_entry, tick) -> bool:
    remaining = executor._touch_place_budget
    if remaining is None:
        return True
    if remaining == 0:
        if not _can_release_zero(
            executor, arguments, book, finished, seen_open, unfinished, eternal, touch_entry, tick,
        ):
            return False
        executor._touch_place_budget = 1
        remaining = 1
    if remaining <= 0:
        return False
    executor._touch_place_budget = remaining - 1
    executor._touch_zero_released = True
    return True


OPEN_PAGE_LIMIT = 100


def open_list_venue(class_name: str) -> Optional[str]:
    """Where we already know how to ask. None means do not invent a URL."""
    name = (class_name or "").lower()
    if "perpetual" in name and "gate" in name:
        return None
    if "okx" in name:
        return "okx"
    if "gate" in name:
        return "gate"
    return None


def new_exchange_slot() -> Dict[str, Any]:
    """One shared result. Unread is not an empty book."""
    return {"read": False, "orders": []}


def exchange_open_request(venue: str, symbol: str, page: int = 1, after: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """One authenticated request for open orders of this pair. None if the venue is unknown."""
    if not venue or not symbol:
        return None
    if venue == "gate":
        # Do not send account=spot. The connector places without account, so a spot-only
        # list can miss a unified-account order and look like an empty book.
        return {
            "path": "spot/orders",
            "params": {
                "currency_pair": symbol,
                "status": "open",
                "page": page,
                "limit": OPEN_PAGE_LIMIT,
            },
            "is_auth_required": True,
        }
    if venue == "okx":
        params = {"instId": symbol, "limit": str(OPEN_PAGE_LIMIT)}
        if after:
            params["after"] = after
        return {
            "path": "/api/v5/trade/orders-pending",
            "params": params,
            "is_auth_required": True,
        }
    return None


def exchange_pass_key(account: str, connector: Any, pair: str) -> tuple:
    """One pass per account, connector instance, and pair. A name alone is not the key."""
    return (account, id(connector), pair)


def _exchange_side(raw: Any) -> Optional[str]:
    text = str(raw or "").upper()
    if text in ("BUY", "SELL"):
        return text
    return None


def _unread_page() -> Dict[str, Any]:
    return {"read": False, "orders": [], "done": False, "cursor": None}


def _page(orders: List[Dict[str, Any]], done: bool, cursor: Optional[str]) -> Dict[str, Any]:
    return {"read": True, "orders": orders, "done": done, "cursor": cursor}


def _gate_groups(body: Any) -> bool:
    if not isinstance(body, list) or not body:
        return False
    return any(isinstance(item, dict) and "orders" in item for item in body)


def parse_exchange_open_page(
    body: Any,
    venue: str,
    symbol: str,
    cursor_kind: Optional[str] = None,
) -> Dict[str, Any]:
    """One page. A full page returns its orders and a cursor. It is not an unread void."""
    if cursor_kind == "before":
        return _unread_page()
    if venue == "gate":
        return _parse_gate_page(body, symbol)
    if venue == "okx":
        return _parse_okx_page(body)
    return _unread_page()


def _parse_gate_page(body: Any, symbol: str) -> Dict[str, Any]:
    if _gate_groups(body) or not isinstance(body, list):
        return _unread_page()
    orders = []
    for row in body:
        if not isinstance(row, dict):
            return _unread_page()
        pair = row.get("currency_pair")
        if pair != symbol:
            continue
        order_id = row.get("text")
        price = row.get("price")
        side = _exchange_side(row.get("side"))
        if not order_id or price is None or row.get("side") in (None, ""):
            return _unread_page()
        orders.append({
            "order_id": str(order_id),
            "price": None if side is None else _text(price),
            "side": side,
            "unknown_side": side is None,
        })
    full = len(orders) >= OPEN_PAGE_LIMIT
    return _page(orders, done=not full, cursor="page+1" if full else None)


def _parse_okx_page(body: Any) -> Dict[str, Any]:
    if not isinstance(body, dict) or str(body.get("code")) != "0" or not isinstance(body.get("data"), list):
        return _unread_page()
    data = body["data"]
    orders = []
    for row in data:
        if not isinstance(row, dict):
            return _unread_page()
        state = str(row.get("state") or "")
        if state not in ("live", "partially_filled"):
            continue
        order_id = row.get("clOrdId")
        price = row.get("px")
        side = _exchange_side(row.get("side"))
        if not order_id or price is None or row.get("side") in (None, ""):
            return _unread_page()
        orders.append({
            "order_id": str(order_id),
            "price": None if side is None else _text(price),
            "side": side,
            "unknown_side": side is None,
        })
    if len(data) >= OPEN_PAGE_LIMIT:
        last = data[-1]
        cursor = last.get("ordId") if isinstance(last, dict) else None
        if not cursor:
            return _unread_page()
        return _page(orders, done=False, cursor=str(cursor))
    return _page(orders, done=True, cursor=None)


def merge_exchange_open_pages(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Join pages. An unread tail is not an empty success and does not drop earlier orders into one."""
    if not pages:
        return {"read": False, "orders": []}
    collected: List[Dict[str, Any]] = []
    for page in pages:
        if not isinstance(page, dict) or not page.get("read"):
            return {"read": False, "orders": []}
        collected.extend(page.get("orders") or [])
        if page.get("done"):
            return {"read": True, "orders": collected}
    return {"read": False, "orders": []}


def foreign_order_ids(
    rows: List[Dict[str, Any]],
    ledger_ids: Dict[str, List[str]],
    self_id: str,
) -> set:
    """Ids owned by other executors, from checkpoints and the ledger. Not this grid's ids."""
    foreign = set()
    for row in rows:
        owner = row.get("executor_id")
        if not owner or owner == self_id:
            continue
        foreign.update(saved_order_ids(parse_checkpoint(row.get("checkpoint"))))
        foreign.update(ledger_ids.get(owner) or [])
    return foreign


def hold_new_opens(executor: Any) -> None:
    """Replace the open proposer. Leave close orders alone. Do not call blank_grid_cap."""
    executor.get_open_orders_to_create = lambda: []


def install_occupied_open_brake(
    executor: Any,
    exchange_pass: Any,
    foreign_ids_fn: Any,
    own_ids: Optional[set] = None,
    prior_tracker_ids: Optional[set] = None,
    own_prices: Optional[Dict[str, Any]] = None,
) -> None:
    """Block a second open at a price the exchange or the book still holds.

    ``own_prices`` maps an accounted order id to its recorded price. While that
    id is neither filled nor gone, its tick stays blocked even when the
    exchange list was read and is empty.
    """
    original = executor.get_open_orders_to_create
    if not isinstance(exchange_pass, dict):
        exchange_pass = new_exchange_slot()
    state = {
        "exchange": exchange_pass,
        "foreign_ids_fn": foreign_ids_fn,
        "own_ids": set(own_ids or []),
        "own_prices": dict(own_prices or {}),
        "prior": set(prior_tracker_ids or []),
        "released": set(),
        "held": set(),
    }
    executor._occupied_wants_open = not bool(exchange_pass.get("read"))

    def wrapped():
        exchange = state["exchange"]
        if not isinstance(exchange, dict) or not exchange.get("read"):
            # Do not call the touch cap. Spending it here would burn the one
            # current-price order before the list exists.
            executor._occupied_wants_open = True
            return []
        executor._occupied_wants_open = False
        try:
            proposed = list(original())
        except Exception:
            logger.warning("occupied open proposal failed", exc_info=True)
            return []
        try:
            return _filter_occupied_opens(executor, proposed, state)
        except Exception:
            logger.warning("occupied open brake failed closed", exc_info=True)
            return []

    executor.get_open_orders_to_create = wrapped


def _filter_occupied_opens(executor: Any, proposed: List[Any], state: Dict[str, Any]) -> List[Any]:
    exchange = state["exchange"]
    if not exchange.get("read"):
        return []
    book = _read_active_book(executor)
    if book is None:
        return []
    _remember_strict_finishes(book, state)
    foreign = _call_foreign(state)
    if foreign is None:
        return []
    side = _side_from_config(executor)
    if side is None:
        return []
    for order_id in list(state["held"]):
        if _is_foreign(order_id, state, foreign):
            state["held"].discard(order_id)
    _collect_held(exchange, book, state, foreign, side)
    if _held_blocks_all(exchange, book, state):
        return []
    tick = _positive_tick(executor)
    if tick is None:
        return []
    banned = _own_price_bans(executor, state)
    allowed = []
    for level in proposed:
        placement = _open_placement(executor, getattr(level, "price", None))
        if placement is None or _held_matches(exchange, book, state, placement, tick, side):
            continue
        if _price_is_banned(placement, tick, banned):
            continue
        allowed.append(level)
    return allowed


def _own_price_bans(executor: Any, state: Dict[str, Any]) -> List[Any]:
    """Prices of accounted ids that are still neither filled nor gone."""
    prices = []
    ledger = order_ledger(executor)
    for order_id, price in (state.get("own_prices") or {}).items():
        if price in (None, ""):
            continue
        row = ledger.get(order_id) or {}
        if row.get("filled") or row.get("gone"):
            continue
        prices.append(price)
    return prices


def _price_is_banned(placement: Decimal, tick: Optional[Decimal], prices: List[Any]) -> bool:
    for price in prices:
        if _same_tick(price, placement, tick) is not False:
            return True
    return False


def _read_active_book(executor: Any) -> Optional[Dict[str, Any]]:
    """Active book. ready is not required. None means the dictionary was not read."""
    try:
        connectors = getattr(executor, "connectors", None)
        if not connectors:
            return None
        values = list(connectors.values())
        if not values:
            return None
        pair = _pair_name(executor)
        active = []
        cached = []
        for connector in values:
            inflight = getattr(connector, "in_flight_orders", None)
            if not isinstance(inflight, dict):
                return None
            for order in inflight.values():
                if _on_pair(order, pair):
                    active.append(order)
            tracker = getattr(connector, "_order_tracker", None)
            if tracker is None:
                continue
            for order in (getattr(tracker, "cached_orders", None) or {}).values():
                if _on_pair(order, pair):
                    cached.append(order)
        return {"active": active, "cached": cached}
    except Exception:
        logger.warning("occupied open book unreadable", exc_info=True)
        return None


def _finish_name(order: Any) -> Optional[str]:
    if isinstance(order, dict):
        raw = order.get("current_state") or order.get("state")
    else:
        raw = getattr(order, "current_state", None)
        if raw is None:
            raw = getattr(order, "state", None)
    name = getattr(raw, "name", None)
    text = str(name or raw or "").upper()
    if "FAIL" in text or "NOT_FOUND" in text or "NOTFOUND" in text:
        return "FAILED"
    if text in ("FILLED", "CANCELED", "CANCELLED"):
        return "CANCELED" if text.startswith("CANCEL") else "FILLED"
    return None


def _remember_strict_finishes(book: Dict[str, Any], state: Dict[str, Any]) -> None:
    for order in list(book["active"]) + list(book["cached"]):
        order_id = _order_id(order)
        if not order_id or order_id not in state["prior"] or _is_open(order):
            continue
        if _finish_name(order) in ("FILLED", "CANCELED"):
            state["released"].add(order_id)
            state["held"].discard(order_id)


def _call_foreign(state: Dict[str, Any]) -> Optional[set]:
    try:
        found = state["foreign_ids_fn"]()
    except Exception:
        logger.warning("occupied open foreign ids failed", exc_info=True)
        return None
    if not isinstance(found, (set, list, tuple)):
        return None
    return {str(item) for item in found if item}


def _is_foreign(order_id: str, state: Dict[str, Any], foreign: set) -> bool:
    if order_id in state["own_ids"]:
        return False
    return order_id in foreign


def _collect_held(exchange: Dict[str, Any], book: Dict[str, Any], state: Dict[str, Any], foreign: set, side: str) -> None:
    for row in exchange.get("orders") or []:
        order_id = str(row.get("order_id") or "")
        if not order_id or order_id in state["released"] or _is_foreign(order_id, state, foreign):
            continue
        row_side = row.get("side")
        if row.get("unknown_side") or row_side in (None, side):
            state["held"].add(order_id)
    for order in book["active"]:
        if not _is_open(order):
            continue
        order_id = _order_id(order)
        if not order_id or order_id in state["released"] or _is_foreign(order_id, state, foreign):
            continue
        order_side = _order_side(order)
        if order_side in (None, side):
            state["held"].add(order_id)


def _comparable_held(exchange: Dict[str, Any], book: Dict[str, Any], state: Dict[str, Any]) -> set:
    found = set()
    for row in exchange.get("orders") or []:
        order_id = str(row.get("order_id") or "")
        if order_id in state["held"] and row.get("price") is not None and not row.get("unknown_side"):
            found.add(order_id)
    for order in book["active"]:
        order_id = _order_id(order)
        if order_id in state["held"] and _is_open(order) and _order_price(order) is not None and _order_side(order):
            found.add(order_id)
    return found


def _held_blocks_all(exchange: Dict[str, Any], book: Dict[str, Any], state: Dict[str, Any]) -> bool:
    """An id we still hold but can no longer price is not proof of a different tick."""
    if not state["held"]:
        return False
    if state["held"] - _comparable_held(exchange, book, state):
        return True
    return False


def _open_placement(executor: Any, level_price: Any) -> Optional[Decimal]:
    side = _side_from_config(executor)
    quote = _quote(executor)
    spread = _spread(executor)
    if side is None or quote is None or spread is None:
        return None
    return _placement(level_price, quote, side, spread)


def _held_matches(
    exchange: Dict[str, Any],
    book: Dict[str, Any],
    state: Dict[str, Any],
    placement: Decimal,
    tick: Decimal,
    side: str,
) -> bool:
    for row in exchange.get("orders") or []:
        if str(row.get("order_id") or "") not in state["held"]:
            continue
        if row.get("side") not in (None, side):
            continue
        if _same_tick(row.get("price"), placement, tick) is True:
            return True
    for order in book["active"]:
        order_id = _order_id(order)
        if order_id not in state["held"] or not _is_open(order):
            continue
        if _order_side(order) not in (None, side):
            continue
        if _same_tick(_order_price(order), placement, tick) is True:
            return True
    return False


def checkpoint_plus(read_text: Optional[str], addition: Dict[str, Any]) -> Optional[str]:
    """Full copy of the read column plus one hold. A missing old field is not a body."""
    if read_text:
        try:
            base = json.loads(read_text)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(base, dict):
            return None
    else:
        base = {"version": CHECKPOINT_VERSION, "type": "grid_executor", "levels": []}
    body = json.loads(json.dumps(base))
    holds = list(body.get("holds") or [])
    order_id = addition.get("order_id")
    if order_id and not any(item.get("order_id") == order_id for item in holds):
        holds.append(dict(addition))
    body["holds"] = holds
    encoded = json.dumps(body, sort_keys=True)
    if read_text and not _keeps_previous_fields(base, json.loads(encoded)):
        return None
    return encoded


def _keeps_previous_fields(before: Any, after: Any) -> bool:
    if isinstance(before, dict):
        if not isinstance(after, dict):
            return False
        for key, value in before.items():
            if key not in after or not _keeps_previous_fields(value, after[key]):
                return False
        return True
    if isinstance(before, list):
        if not isinstance(after, list) or len(after) < len(before):
            return False
        return all(_keeps_previous_fields(left, right) for left, right in zip(before, after))
    return before == after


def claim_quant(reserves: Dict[tuple, str], connector_key: str, pair: str, price: str) -> bool:
    """One quant on this connector. A second level, even of the same grid, does not take it."""
    key = (connector_key, pair, price)
    if key in reserves:
        return False
    reserves[key] = price
    return True


def release_quant(reserves: Dict[tuple, str], connector_key: str, pair: str, price: str) -> None:
    reserves.pop((connector_key, pair, price), None)


def pick_owner(rows: List[Dict[str, Any]], account: str, connector: str, pair: str) -> Optional[str]:
    """Smallest executor on this pair, account, and connector. A foreign pair is not an owner."""
    names = sorted(
        row["executor_id"]
        for row in rows
        if row.get("account") == account
        and row.get("connector") == connector
        and row.get("pair") == pair
        and row.get("executor_id")
    )
    return names[0] if names else None


def local_create_missed_place(reached_place: bool) -> bool:
    """True only when this process never awaited _place_order. A throw is not that."""
    return not reached_place


def unread_blocks_buy(read_ok: bool) -> bool:
    return not read_ok


def finished_list_request(
    venue: str,
    symbol: str,
    ts: Optional[float] = None,
    now: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    """Closed orders for this pair. A known ts is uncovered without from and to."""
    if venue != "gate" or not symbol:
        return None
    params = {
        "currency_pair": symbol,
        "status": "finished",
        "page": 1,
        "limit": OPEN_PAGE_LIMIT,
    }
    if ts is not None:
        try:
            moment = float(ts)
            end = float(now if now is not None else time.time())
        except (TypeError, ValueError):
            return None
        params["from"] = int(moment)
        params["to"] = int(end if end >= moment else moment)
    return {
        "path": "spot/orders",
        "params": params,
        "is_auth_required": True,
    }


def raw_list_page(body: Any, symbol: str) -> Dict[str, Any]:
    """One raw page. A full page is not done. A non-list is unread, not an empty book."""
    if not isinstance(body, list):
        return {"read": False, "done": False, "orders": []}
    orders = []
    for row in body:
        if not isinstance(row, dict):
            return {"read": False, "done": False, "orders": []}
        if row.get("currency_pair") != symbol:
            continue
        orders.append(row)
    return {"read": True, "done": len(body) < OPEN_PAGE_LIMIT, "orders": orders}


def history_covers_ts(request: Optional[Dict[str, Any]], ts: Any, page: Optional[Dict[str, Any]]) -> bool:
    """A full page without done, a failed read, or a window that misses ts is unread."""
    if not isinstance(page, dict) or not page.get("read") or not page.get("done"):
        return False
    if ts is None:
        return True
    params = (request or {}).get("params") or {}
    if "from" not in params or "to" not in params:
        return _okx_window_covers(params, ts)
    try:
        moment = float(ts)
    except (TypeError, ValueError):
        return False
    return float(params["from"]) <= moment <= float(params["to"])


def _okx_window_covers(params: Dict[str, Any], ts: Any) -> bool:
    """OKX begin/end are milliseconds. The mark is seconds. Gate has neither key."""
    if "begin" not in params or "end" not in params:
        return False
    try:
        moment = float(ts)
        begin = int(params["begin"]) / 1000
        end = int(params["end"]) / 1000
    except (TypeError, ValueError):
        return False
    return begin <= moment <= end


_OKX_CLOSED_STATES = {"filled", "canceled", "mmp_canceled"}
OKX_HISTORY_MS = 7 * 24 * 60 * 60 * 1000
OKX_ARCHIVE_MS = 90 * 24 * 60 * 60 * 1000


def okx_closed_request(
    symbol: str,
    after: Optional[str] = None,
    now: Optional[float] = None,
    begin: Optional[str] = None,
    end: Optional[str] = None,
    inst_type: str = "SWAP",
    archive: bool = False,
) -> Optional[Dict[str, Any]]:
    """Full 7-day closed list. begin is never a known mark."""
    if not symbol:
        return None
    if begin is None or end is None:
        moment = time.time() if now is None else float(now)
        end_ms = int(moment * 1000)
        begin = str(end_ms - OKX_HISTORY_MS)
        end = str(end_ms)
    params = {
        "instType": inst_type,
        "instId": symbol,
        "limit": str(OPEN_PAGE_LIMIT),
        "begin": str(begin),
        "end": str(end),
    }
    if after:
        params["after"] = str(after)
    return {
        "path": "/api/v5/trade/orders-history-archive" if archive else "/api/v5/trade/orders-history",
        "params": params,
        "is_auth_required": True,
    }


def parse_okx_closed_page(body: Any, begin_ms: int, end_ms: int) -> Dict[str, Any]:
    """Closed page. Do not use the open parser: it drops filled and canceled."""
    if not isinstance(body, dict) or str(body.get("code")) != "0" or not isinstance(body.get("data"), list):
        return _unread_page()
    data = body["data"]
    orders = []
    for row in data:
        if not isinstance(row, dict):
            return _unread_page()
        px = row.get("px")
        if px in (None, ""):
            continue
        state = str(row.get("state") or "")
        if state not in _OKX_CLOSED_STATES:
            return _unread_page()
        try:
            created = int(str(row.get("cTime")))
        except (TypeError, ValueError):
            return _unread_page()
        if created < int(begin_ms) or created > int(end_ms):
            continue
        client_id = row.get("clOrdId")
        fill_raw = str(row.get("accFillSz") or "0")
        try:
            float(fill_raw)
            fill_size = fill_raw
        except (TypeError, ValueError):
            fill_size = "0"
        item = {
            "order_id": "" if not client_id else str(client_id),
            "price": str(px),
            "side": _exchange_side(row.get("side")),
            "unknown_side": _exchange_side(row.get("side")) is None,
            "ts": created,
            "state": state,
            "fill_size": fill_size,
        }
        if client_id:
            item["text"] = str(client_id)
        orders.append(item)
    if len(data) >= OPEN_PAGE_LIMIT:
        last = data[-1]
        cursor = last.get("ordId") if isinstance(last, dict) else None
        if not cursor:
            return _unread_page()
        return _page(orders, done=False, cursor=str(cursor))
    return _page(orders, done=True, cursor=None)


def okx_blank_px_fills(body: Any) -> list:
    """Рыночная строка OKX с пустой ценой и исполненным объёмом. Общий разбор страницы не трогаем."""
    if not isinstance(body, dict) or str(body.get("code")) != "0" or not isinstance(body.get("data"), list):
        return []
    found = []
    for row in body["data"]:
        if not isinstance(row, dict):
            continue
        if row.get("px") not in (None, ""):
            continue
        client_id = str(row.get("clOrdId") or "")
        if not client_id:
            continue
        try:
            filled = float(str(row.get("accFillSz") or "0")) > 0
        except (TypeError, ValueError):
            filled = False
        if not filled:
            continue
        found.append({
            "order_id": client_id,
            "text": client_id,
            "state": "filled",
            "side": str(row.get("side") or ""),
            "fill_size": str(row.get("accFillSz")),
            "px": "",
            "avgPx": str(row.get("avgPx") or ""),
        })
    return found


def gate_delete_request(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """DELETE by the exchange id on the row whose text is the client id."""
    exchange_id = raw.get("id")
    pair = raw.get("currency_pair")
    if not raw.get("text") or not exchange_id or not pair:
        return None
    return {
        "path": f"spot/orders/{exchange_id}",
        "params": {"currency_pair": pair},
        "is_auth_required": True,
    }


def must_keep_executor(executor_id: str, journal_ids: Optional[List[str]], column_text: Optional[str]) -> bool:
    """Unread journal or a stored client id is not a reason to drop the row."""
    if journal_ids is None:
        return True
    if executor_id in journal_ids:
        return True
    return bool(column_text and executor_id and "order_id" in column_text)


def send_blocked(
    *,
    ledger_ok: bool,
    column_ok: bool,
    opens_ok: bool,
    finished_ok: bool = False,
    history_covers: bool = False,
    prices: List[Any],
    placement: Any,
    tick: Optional[Decimal],
) -> bool:
    """Unread ledger, column, open list, or finished history is not permission to buy."""
    if not ledger_ok or not column_ok or not opens_ok or not finished_ok or not history_covers:
        return True
    return recorded_price_blocks(prices, placement, tick)


def recorded_price_blocks(prices: List[Any], placement: Any, tick: Optional[Decimal]) -> bool:
    """A stored placement price holds the tick. No price and no tick are not permission."""
    for price in prices:
        if not price or tick is None:
            return True
        same = _same_tick(price, placement, tick)
        if same is None or same is True:
            return True
    return False


def checkpoint_keeping_holds(read_text: Optional[str], captured: Dict[str, Any]) -> Optional[str]:
    """Captured fields replace the column. A hold already committed is not dropped."""
    try:
        body = json.loads(json.dumps(captured))
    except (TypeError, ValueError):
        return None
    if not isinstance(body, dict):
        return None
    if read_text:
        try:
            previous = json.loads(read_text)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(previous, dict):
            return None
        holds = list(body.get("holds") or [])
        seen = {item.get("order_id") for item in holds if isinstance(item, dict)}
        for item in previous.get("holds") or []:
            if isinstance(item, dict) and item.get("order_id") not in seen:
                holds.append(item)
        if holds:
            body["holds"] = holds
        for key in ("close_holds", "close_bans"):
            current = list(body.get(key) or [])
            known = set()
            for item in current:
                if isinstance(item, dict) and item.get("order_id"):
                    known.add(str(item.get("order_id")))
                elif item:
                    known.add(str(item))
            for item in previous.get(key) or []:
                token = str(item.get("order_id")) if isinstance(item, dict) else str(item or "")
                if token and token not in known:
                    current.append(item)
                    known.add(token)
            if current:
                body[key] = current
        marked = {
            str(item.get("order_id"))
            for item in holds
            if isinstance(item, dict) and item.get("status") == "canceled" and item.get("base") == "0"
        }
        for level in body.get("levels") or []:
            if not isinstance(level, dict):
                continue
            open_id = str((level.get("open_order") or {}).get("order_id") or "")
            if open_id and open_id in marked:
                level["open_order"] = {}
        apply_partial_keep(body, previous)
        for key in ("close_holds", "close_bans"):
            current = list(body.get(key) or [])
            known = set()
            for item in current:
                if isinstance(item, dict) and item.get("order_id"):
                    known.add(str(item.get("order_id")))
                elif item:
                    known.add(str(item))
            for item in previous.get(key) or []:
                token = str(item.get("order_id")) if isinstance(item, dict) else str(item or "")
                if token and token not in known:
                    current.append(item)
                    known.add(token)
            if current:
                body[key] = current
        marked = {
            str(item.get("order_id"))
            for item in holds
            if isinstance(item, dict) and item.get("status") == "canceled" and item.get("base") == "0"
        }
        for level in body.get("levels") or []:
            if not isinstance(level, dict):
                continue
            open_id = str((level.get("open_order") or {}).get("order_id") or "")
            if open_id and open_id in marked:
                level["open_order"] = {}
    return json.dumps(body, sort_keys=True)


def snapshot_body(read_text: Optional[str], captured: Dict[str, Any]) -> Optional[str]:
    """Copy the column, then add. A snapshot must not erase holds, levels, or marks."""
    return _merge_without_dropping(read_text, captured)


def _merge_without_dropping(read_text: Optional[str], captured: Dict[str, Any]) -> Optional[str]:
    if read_text:
        try:
            base = json.loads(read_text)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(base, dict):
            return None
    else:
        base = {}
    merged = json.loads(json.dumps(base))
    for key, value in captured.items():
        if key not in merged:
            merged[key] = value
        elif key == "holds" and isinstance(value, list):
            existing = list(merged.get("holds") or [])
            seen = {item.get("order_id") for item in existing if isinstance(item, dict)}
            for item in value:
                if isinstance(item, dict) and item.get("order_id") not in seen:
                    existing.append(item)
            merged["holds"] = existing
    if read_text and not _keeps_previous_fields(base, merged):
        return None
    return json.dumps(merged, sort_keys=True)


def _pending_absent(pages: Any, order_id: str) -> bool:
    """True only when a finished pending read does not contain this id."""
    if not isinstance(pages, list) or not pages:
        return False
    for index, page in enumerate(pages):
        if not isinstance(page, dict) or str(page.get("code")) != "0":
            return False
        data = page.get("data")
        if not isinstance(data, list):
            return False
        if any(not isinstance(row, dict) or row.get("px") in (None, "") for row in data):
            return False
        if len(data) >= 100 and index == len(pages) - 1:
            return False
        if any(str(row.get("clOrdId") or "") == order_id for row in data):
            return False
    return True


def okx_mark_if_proven(
    read_text: Optional[str],
    order_id: str,
    level_id: str,
    raw: Any,
    pages: Any,
    in_flight: bool,
    in_lost: bool,
    connector_name: str = "okx_perpetual",
) -> Optional[str]:
    """Add a cancel hold beside the still-present open id. None means do not write."""
    if connector_name != "okx_perpetual":
        return None
    if not order_id or in_flight or in_lost or not _pending_absent(pages, order_id):
        return None
    if not isinstance(raw, dict) or str(raw.get("code")) != "0":
        return None
    data = raw.get("data")
    if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
        return None
    row = data[0]
    if str(row.get("clOrdId") or "") != order_id:
        return None
    if str(row.get("state") or "") not in ("canceled", "mmp_canceled"):
        return None
    if "accFillSz" not in row or str(row.get("accFillSz")) != "0":
        return None
    if row.get("px") in (None, ""):
        return None
    try:
        current = json.loads(read_text) if read_text else {}
    except (json.JSONDecodeError, TypeError):
        return None
    owns = False
    for level in current.get("levels") or []:
        if str(level.get("id") or "") != str(level_id):
            continue
        if str((level.get("open_order") or {}).get("order_id") or "") == order_id:
            owns = True
    if not owns:
        return None
    holds = [item for item in current.get("holds") or [] if isinstance(item, dict)]
    marked = False
    for item in holds:
        if str(item.get("order_id") or "") != order_id:
            continue
        item["status"] = "canceled"
        item["base"] = "0"
        item["price"] = str(row.get("px"))
        item["level_id"] = level_id
        marked = True
    if not marked:
        holds.append({
            "order_id": order_id,
            "level_id": level_id,
            "status": "canceled",
            "base": "0",
            "price": str(row.get("px")),
        })
    current["holds"] = holds
    encoded = json.dumps(current, sort_keys=True)
    if not _mark_kept(encoded, order_id, level_id):
        return None
    return encoded


def _mark_kept(text: Optional[str], order_id: str, level_id: str) -> bool:
    try:
        body = json.loads(text or "")
    except (json.JSONDecodeError, TypeError):
        return False
    holds = body.get("holds") or []
    mark = next((item for item in holds if str(item.get("order_id") or "") == order_id), None)
    if not mark or mark.get("status") != "canceled" or mark.get("base") != "0":
        return False
    if not mark.get("price") or str(mark.get("level_id") or "") != str(level_id):
        return False
    found = False
    for level in body.get("levels") or []:
        if str(level.get("id") or "") != str(level_id):
            continue
        found = True
        if str((level.get("open_order") or {}).get("order_id") or "") != order_id:
            return False
    if not found:
        return False
    return True


async def write_okx_mark(db, executor_id, order_id, level_id, raw, pages, in_flight, in_lost, slot_id, connector_name="okx_perpetual"):
    """One compare-and-swap, then a new session. Never clears the slot or sends."""
    from database.repositories.executor_repository import ExecutorRepository

    async def _finish(slot):
        send = getattr(db, "send", None)
        if send is not None and getattr(send, "calls", None):
            raise RuntimeError("this piece must not send")
        return slot

    async with db.get_session_context() as session:
        repo = session if hasattr(session, "replace_checkpoint_if_unchanged") else ExecutorRepository(session)
        row = await repo.get_executor_by_id(executor_id)
        seen = None if row is None else row.checkpoint
        body = okx_mark_if_proven(
            seen, order_id, level_id, raw, pages, in_flight, in_lost, connector_name,
        )
        if body is None:
            return await _finish(slot_id)
        changed = await repo.replace_checkpoint_if_unchanged(executor_id, seen, body)
        if changed != 1:
            raise RuntimeError("checkpoint compare-and-swap changed no row")
    async with db.get_session_context() as session:
        reader = session if hasattr(session, "get_executor_by_id") else ExecutorRepository(session)
        fresh = await reader.get_executor_by_id(executor_id)
    visible = None if fresh is None else fresh.checkpoint
    if visible != body or not _mark_kept(visible, order_id, level_id):
        raise RuntimeError("new session does not see the mark")
    return await _finish(slot_id)


def visible_cancel_ids(text: Optional[str]) -> set:
    """Order ids whose cancel mark a new session can already see."""
    try:
        body = json.loads(text or "")
    except (json.JSONDecodeError, TypeError):
        return set()
    found = set()
    for item in body.get("holds") or []:
        if not isinstance(item, dict):
            continue
        if item.get("status") == "canceled" and item.get("base") == "0" and item.get("price") and item.get("order_id") and item.get("level_id"):
            found.add((str(item.get("level_id")), str(item.get("order_id"))))
    return found


def unhold_body(read_text: Optional[str], level_id: str) -> Optional[str]:
    """Drop only the open id that matches this level's visible cancel mark."""
    marks = {order_id for marked_level, order_id in visible_cancel_ids(read_text) if marked_level == str(level_id)}
    if not marks:
        return None
    try:
        body = json.loads(read_text)
    except (json.JSONDecodeError, TypeError):
        return None
    changed = False
    for level in body.get("levels") or []:
        if str(level.get("id") or "") != str(level_id):
            continue
        open_id = str((level.get("open_order") or {}).get("order_id") or "")
        if open_id and open_id in marks:
            level["open_order"] = {}
            changed = True
    if not changed:
        return None
    return json.dumps(body, sort_keys=True)


def _open_id_matching_mark(text: Optional[str], level_id: str) -> Optional[str]:
    marks = {order_id for marked_level, order_id in visible_cancel_ids(text) if marked_level == str(level_id)}
    try:
        body = json.loads(text or "")
    except (json.JSONDecodeError, TypeError):
        return None
    for level in body.get("levels") or []:
        if str(level.get("id") or "") != str(level_id):
            continue
        open_id = str((level.get("open_order") or {}).get("order_id") or "")
        if open_id and open_id in marks:
            return open_id
    return None


def _mark_visible_and_open_gone(text: Optional[str], level_id: str, order_id: str) -> bool:
    if not order_id or not any(mark == (str(level_id), order_id) for mark in visible_cancel_ids(text)):
        return False
    try:
        body = json.loads(text or "")
    except (json.JSONDecodeError, TypeError):
        return False
    for level in body.get("levels") or []:
        if str(level.get("id") or "") != str(level_id):
            continue
        return str((level.get("open_order") or {}).get("order_id") or "") != order_id
    return False


async def write_unhold(db, executor_id, level_id, slot):
    """Clear one slot only after a new session sees the mark and the emptied open."""
    from database.repositories.executor_repository import ExecutorRepository

    async with db.get_session_context() as session:
        repo = session if hasattr(session, "replace_checkpoint_if_unchanged") else ExecutorRepository(session)
        row = await repo.get_executor_by_id(executor_id)
        seen = None if row is None else row.checkpoint
        current_id = str(getattr(getattr(slot, "active_open_order", None), "order_id", None) or getattr(slot, "active_open_order", "") or "")
        removed = _open_id_matching_mark(seen, level_id)
        if removed and current_id and current_id != removed:
            return slot
        if removed is None:
            if _mark_visible_and_open_gone(seen, level_id, current_id) and slot is not None:
                slot.active_open_order = None
            return slot
        body = unhold_body(seen, level_id)
        if body is None:
            return slot
        changed = await repo.replace_checkpoint_if_unchanged(executor_id, seen, body)
        if changed != 1:
            raise RuntimeError("checkpoint compare-and-swap changed no row")
    async with db.get_session_context() as session:
        reader = session if hasattr(session, "get_executor_by_id") else ExecutorRepository(session)
        fresh = await reader.get_executor_by_id(executor_id)
    visible = None if fresh is None else fresh.checkpoint
    if visible != body or not any(mark[0] == str(level_id) for mark in visible_cancel_ids(visible)):
        raise RuntimeError("new session does not see the unhold")
    if slot is not None and current_id == removed and _mark_visible_and_open_gone(visible, level_id, removed):
        slot.active_open_order = None
    send = getattr(db, "send", None)
    if send is not None and getattr(send, "calls", None):
        raise RuntimeError("this piece must not send")
    return slot


def install_unhold_guard(executor, read_column) -> None:
    """After the open shell. A visible mark returns before send."""
    original_open = getattr(executor, "get_open_orders_to_create", None)
    original_adjust = getattr(executor, "adjust_and_place_open_order", None)

    def wrapped_open():
        text = read_column()
        offered = original_open() if original_open is not None else []
        if text is None:
            return offered
        return skip_marked_levels(offered, text)

    def wrapped_adjust(level, *args, **kwargs):
        text = read_column()
        level_id = str(getattr(level, "id", "") or "")
        if text is not None and any(mark[0] == level_id for mark in visible_cancel_ids(text)):
            return None
        if original_adjust is None:
            return None
        return original_adjust(level, *args, **kwargs)

    if original_open is not None:
        executor.get_open_orders_to_create = wrapped_open
    if original_adjust is not None:
        executor.adjust_and_place_open_order = wrapped_adjust


def skip_marked_levels(levels, text: Optional[str]):
    """Do not offer a level whose cancel mark a new session can see."""
    marked = {level_id for level_id, _order_id in visible_cancel_ids(text)}
    if text is None:
        return []
    return [level for level in levels if str(getattr(level, "id", "") or "") not in marked]


# One channel for this close call. Empty means the connector mints its own id.
_RESERVED_CLOSE_ID: ContextVar[Optional[str]] = ContextVar("reserved_close_id", default=None)
# Заморозка: проверка метки, которую задача канала зовёт перед _create_order.
_CLOSE_FROZEN_CHECK: ContextVar[Optional[Any]] = ContextVar("close_frozen_check", default=None)
_WRAPPED_CONNECTORS: set = set()


def durable_sent_true(order_id: str) -> bool:
    merged = read_merged_ledger().get(str(order_id)) or {}
    return merged.get("sent") is True


def line_for_send(
    executor_id: str,
    order_id: str,
    role: str,
    level_id: Optional[str] = None,
    sent: bool = False,
    price: Any = None,
) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "executor_id": executor_id,
        "order_id": order_id,
        "ts": time.time(),
        "role": role,
        "sent": sent,
    }
    if level_id:
        row["level_id"] = level_id
    if price not in (None, ""):
        row["price"] = str(price)
    return row


def role_close_row(executor_id: str, order_id: str) -> Optional[Dict[str, Any]]:

    """Строка с ролью и числовым временем. Короткая запись без роли — не это."""

    if not executor_id or not order_id:

        return None

    try:

        rows = read_ledger_filtered()

    except _LedgerReadError:

        return None

    for row in reversed(rows):

        if str(row.get("executor_id") or "") != str(executor_id):

            continue

        if str(row.get("order_id") or "") != str(order_id):

            continue

        if row.get("role") not in ("close", "market_close"):

            continue

        if numeric_ts(row.get("ts")) is None:

            continue

        return row

    return None



def record_returned_close(executor_id: str, order_id: str) -> bool:

    """Записать номер, который штатная постановка уже вернула. Ложь — не «постановки не было»."""

    if not executor_id or not order_id:

        return False

    existing = role_close_row(executor_id, order_id)

    if existing is not None and existing.get("sent") is True:

        return True

    row = line_for_send(str(executor_id), str(order_id), "market_close", sent=True)

    if not append_durable_row(row):

        return False

    return role_close_row(executor_id, order_id) is not None



def seat_live_returned_close(executor: Any, order_id: str) -> None:

    """Пока процесс жив, уже возвращённый номер сидит в ячейке. Чужой не затираем."""

    if not order_id:

        return

    current = str(getattr(getattr(executor, "_close_order", None), "order_id", "") or "")

    if current and current != str(order_id):

        return

    if current == str(order_id):

        return

    from hummingbot.strategy_v2.models.executors import TrackedOrder

    executor._close_order = TrackedOrder(order_id=str(order_id))



def seat_role_close_before_next(executor: Any, executor_id: str) -> Optional[str]:

    """После падения сажаем только строку с ролью, до следующего закрытия."""

    current = str(getattr(getattr(executor, "_close_order", None), "order_id", "") or "")

    if current:

        return current

    try:

        rows = read_ledger_filtered()

    except _LedgerReadError:

        return None

    for row in reversed(rows):

        if str(row.get("executor_id") or "") != str(executor_id):

            continue

        if row.get("role") not in ("close", "market_close"):

            continue

        if numeric_ts(row.get("ts")) is None:

            continue

        order_id = str(row.get("order_id") or "")

        if not order_id:

            continue

        seat_live_returned_close(executor, order_id)

        return order_id

    return None



def close_id_marked_open(executor_id: str, order_id: str) -> bool:
    """Номер продажи не становится покупкой, даже из короткой записи."""
    return order_id in recent_ids_kept_as_open(executor_id)


def recent_ids_kept_as_open(executor_id: str) -> List[str]:
    """Короткая запись и строка продажи в покупки не идут."""
    kept = []
    for order_id in recent_order_ids(executor_id):
        if role_close_row(executor_id, order_id):
            continue
        if _bare_only(executor_id, order_id):
            continue
        kept.append(order_id)
    return kept


def _bare_only(executor_id: str, order_id: str) -> bool:
    try:
        rows = read_ledger_filtered()
    except _LedgerReadError:
        return False
    seen = False
    for row in rows:
        if str(row.get("executor_id") or "") != str(executor_id):
            continue
        if str(row.get("order_id") or "") != str(order_id):
            continue
        seen = True
        if row.get("role") or row.get("level_id"):
            return False
    return seen

    """Номер продажи не становится покупкой, даже из короткой записи."""

    return False


def install_connector_close_channel(connector: Any) -> None:
    """One wrapper per connector for the process. An empty channel still mints."""
    if connector is None or id(connector) in _WRAPPED_CONNECTORS:
        return
    _WRAPPED_CONNECTORS.add(id(connector))

    def _wrap(name: str) -> None:
        original = getattr(connector, name, None)
        if original is None or getattr(original, "_close_channel", False):
            return

        def wrapped(*args, **kwargs):
            reserved = _RESERVED_CLOSE_ID.get()
            if not reserved:
                return original(*args, **kwargs)
            create = getattr(connector, "_create_order", None)
            if create is None:
                return reserved
            frozen = _CLOSE_FROZEN_CHECK.get()
            order_type = args[2] if len(args) > 2 else kwargs.get("order_type")
            trade_type = "BUY" if name == "buy" else "SELL"
            position_action = kwargs.get("position_action")
            try:
                from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
                trade_type = TradeType.BUY if name == "buy" else TradeType.SELL
                market = getattr(OrderType, "MARKET", None)
                if order_type is None and market is not None:
                    order_type = market
                if position_action is None:
                    position_action = PositionAction.CLOSE
            except Exception:
                logger.exception("close channel could not name the order")
            if position_action is None or getattr(position_action, "name", "") == "NIL":
                logger.warning("close channel refused an unnamed position action id=…%s", str(reserved)[-8:])
                return reserved
            fields = {
                "trade_type": trade_type,
                "order_id": reserved,
                "trading_pair": args[0] if args else kwargs.get("trading_pair"),
                "amount": args[1] if len(args) > 1 else kwargs.get("amount"),
                "order_type": order_type,
                "price": args[3] if len(args) > 3 else kwargs.get("price"),
                "position_action": position_action,
            }

            async def queued_create():
                # Свежая перечитка метки внутри задачи, непосредственно перед
                # _create_order, без await между перечиткой и вызовом: проверка
                # до buy/sell этот разрыв не закрывает (план stop-freeze п.2).
                if frozen is not None:
                    try:
                        try:
                            held = frozen(reserved)
                        except TypeError:
                            held = frozen()
                        if await held if hasattr(held, "__await__") else held:
                            logger.info(
                                "close channel held the send under the stop mark id=…%s",
                                str(reserved)[-8:],
                            )
                            return
                    except Exception:
                        # Ошибка проверки — не слать (план stop-freeze п.2).
                        logger.exception("close channel stop mark check failed")
                        return
                await create(**fields)

            try:
                from hummingbot.core.utils.async_utils import safe_ensure_future
                safe_ensure_future(queued_create())
            except Exception:
                logger.exception("close channel could not queue _create_order")
                return reserved
            return reserved

        wrapped._close_channel = True
        setattr(connector, name, wrapped)

    _wrap("buy")
    _wrap("sell")


def hold_ids_of(executor: Any) -> set:
    found = set()
    for item in getattr(executor, "_close_holds", []) or []:
        if isinstance(item, dict) and item.get("order_id"):
            found.add(str(item["order_id"]))
    return found


def remember_hold(executor: Any, order_id: str, **fields: Any) -> None:
    holds = list(getattr(executor, "_close_holds", []) or [])
    for item in holds:
        if item.get("order_id") == order_id:
            item.update({key: value for key, value in fields.items() if value is not None})
            executor._close_holds = holds
            return
    row = {"order_id": order_id}
    row.update({key: value for key, value in fields.items() if value is not None})
    holds.append(row)
    executor._close_holds = holds


def place_level_close(executor: Any, executor_id: str, level: Any) -> Optional[str]:
    """One level close. No True, no exchange call."""
    from hummingbot.core.data_type.common import OrderType, TradeType

    level_id = getattr(level, "id", None)
    if not level_id or getattr(level, "active_open_order", None) is None:
        return None
    refuse_bare = getattr(executor, "_refuse_unpriced_close", None)
    if refuse_bare is not None and refuse_bare():
        return None
    refuse = getattr(executor, "_refuse_close_price", None)
    if refuse is not None and refuse(getattr(level, "price", None)):
        return None
    owns = getattr(executor, "_partial_owns_level", None)
    if owns is not None and owns(level):
        return None
    # Правило номера, п.6: своя продажа уровня уже вычеканена (журнал или
    # чекпоинт) — второй номер не чеканить, слать тот же или стоять.
    second = getattr(executor, "_refuse_second_close", None)
    if second is not None and second(level):
        return None
    connector = getattr(getattr(executor, "_strategy", None), "connectors", None)
    order_id = getattr(executor, "_next_close_id", None)
    if order_id is None:
        try:
            from hummingbot.connector.utils import get_new_client_order_id
            market = _executor_connector(executor)
            order_id = get_new_client_order_id(
                is_buy=False,
                trading_pair=executor.config.trading_pair,
                hbot_order_id_prefix=getattr(market, "client_order_id_prefix", ""),
                max_id_len=getattr(market, "client_order_id_max_length", 32),
            )
        except Exception:
            return None
    price = getattr(level, "price", None)
    quiet = line_for_send(executor_id, order_id, "close", level_id=str(level_id), sent=False, price=price)
    if not append_durable_row(quiet):
        return None
    sent = line_for_send(executor_id, order_id, "close", level_id=str(level_id), sent=True, price=price)
    if not append_durable_row(sent):
        return None
    token = _RESERVED_CLOSE_ID.set(order_id)
    frozen_token = _CLOSE_FROZEN_CHECK.set(getattr(executor, "_refuse_frozen_close", None))
    try:
        market = _executor_connector(executor)
        install_connector_close_channel(market)
        side = getattr(level, "side", None)
        if refuse_bare is not None and refuse_bare():
            return None
        method = market.buy if side == TradeType.SELL else market.sell
        method(
            executor.config.trading_pair,
            getattr(level, "amount_quote", 0),
            OrderType.LIMIT,
            getattr(level, "price", 0),
        )
    finally:
        _RESERVED_CLOSE_ID.reset(token)
        _CLOSE_FROZEN_CHECK.reset(frozen_token)
    remember_hold(executor, order_id, role="close", level_id=str(level_id), price=price)
    tracked = type("Tracked", (), {})()
    tracked.order_id = order_id
    tracked.order = None
    tracked.price = price
    level.active_close_order = tracked
    return order_id


def _mint_close_id(executor: Any, is_buy: bool) -> Optional[str]:
    market = _executor_connector(executor)
    config = getattr(executor, "config", None)
    pair = getattr(config, "trading_pair", None)
    if market is None or not pair:
        return None
    try:
        from hummingbot.connector.utils import get_new_client_order_id
        return get_new_client_order_id(
            is_buy=is_buy,
            trading_pair=pair,
            hbot_order_id_prefix=getattr(market, "client_order_id_prefix", ""),
            max_id_len=getattr(market, "client_order_id_max_length", 32),
        )
    except Exception:
        return None


def _known_close_id(executor: Any, executor_id: str) -> Optional[str]:
    """Свой номер продажи из слота или строки close/market_close. Не по цене."""
    slot = str(getattr(getattr(executor, "_close_order", None), "order_id", "") or "")
    if slot:
        return slot
    try:
        rows = read_ledger_filtered()
    except _LedgerReadError:
        return None
    for row in reversed(rows):
        if str(row.get("executor_id") or "") != str(executor_id):
            continue
        if row.get("role") not in ("close", "market_close"):
            continue
        if numeric_ts(row.get("ts")) is None:
            continue
        order_id = str(row.get("order_id") or "")
        if order_id:
            return order_id
    return None


def place_market_close(executor: Any, executor_id: str) -> Optional[str]:
    """One market flatten. Original place_order is not called."""
    seated = getattr(executor, "_shutdown_close_id", None)
    shutdown = bool(seated) and bool(getattr(executor, "_shutdown_close_seated", False))
    if seated and not shutdown:
        return None
    if not shutdown:
        refuse_market = getattr(executor, "_refuse_market_close", None)
        if refuse_market is not None and refuse_market():
            return None
        refuse_bare = getattr(executor, "_refuse_unpriced_close", None)
        if refuse_bare is not None and refuse_bare():
            return None
    order_id = seated or getattr(executor, "_next_close_id", None) or _mint_close_id(executor, is_buy=False)
    if not order_id:
        return None
    amount = getattr(executor, "_shutdown_close_amount", None) if seated else 1
    if seated and (amount is None or amount <= 0):
        return None
    quiet = line_for_send(executor_id, order_id, "market_close", sent=False)
    if not append_durable_row(quiet):
        return None
    token = _RESERVED_CLOSE_ID.set(order_id)
    frozen_token = _CLOSE_FROZEN_CHECK.set(getattr(executor, "_refuse_frozen_close", None))
    try:
        market = _executor_connector(executor)
        if market is not None:
            install_connector_close_channel(market)
            market.sell(executor.config.trading_pair, amount, None, None)
    finally:
        _RESERVED_CLOSE_ID.reset(token)
        _CLOSE_FROZEN_CHECK.reset(frozen_token)
    if not append_durable_row(line_for_send(executor_id, order_id, "market_close", sent=True)):
        return None
    remember_hold(executor, order_id, role="market_close")
    try:
        from hummingbot.strategy_v2.models.executors import TrackedOrder
        executor._close_order = TrackedOrder(order_id=order_id)
    except Exception:
        logger.exception("shutdown close could not use TrackedOrder")
        tracked = type("Tracked", (), {"is_done": False, "executed_amount_base": Decimal("0")})()
        tracked.order_id = order_id
        tracked.order = None
        executor._close_order = tracked
    return order_id


def _executor_connector(executor: Any) -> Any:
    strategy = getattr(executor, "_strategy", None)
    name = getattr(getattr(executor, "config", None), "connector_name", None)
    if strategy is None or name is None:
        return None
    getter = getattr(strategy, "connectors", None)
    if isinstance(getter, dict):
        return getter.get(name)
    return None


_BRAKE_HELD_LOGGED: Dict[tuple, float] = {}


def _brake_held(executor_id: str, place: str, reason: str, interval: float = 300.0) -> None:
    """Свидетель держания тормоза: молчать нельзя, спамить тоже."""
    try:
        now = time.monotonic()
        slot = (str(executor_id), place)
        if now - float(_BRAKE_HELD_LOGGED.get(slot, 0.0)) < float(interval):
            return
        _BRAKE_HELD_LOGGED[slot] = now
        logging.getLogger(__name__).warning(
            "brake-held place=%s executor=%s reason=%s", place, executor_id, reason,
        )
    except Exception:
        return


def _journal_readable() -> bool:
    """Непрочитанный журнал — не «номера нет»."""
    try:
        read_ledger_filtered()
    except _LedgerReadError:
        return False
    return True


def _sell_is_live(executor: Any, order_id: str) -> bool:
    """Живая продажа. Пустой слот без полёта отправкой не считается."""
    market = _executor_connector(executor)
    inflight = getattr(market, "in_flight_orders", None) or {}
    if order_id in inflight:
        return True
    tracked = getattr(executor, "_close_order", None)
    if str(getattr(tracked, "order_id", "") or "") != order_id:
        return False
    if getattr(tracked, "order", None) is None:
        return False
    return not bool(getattr(tracked, "is_done", False))


def _live_delivery_blocks(executor: Any, close_id: str) -> bool:
    """Живой номер доставки без ответа 51400. Запись подъёма не стираем."""
    gone = set(getattr(executor, "_ids_51400", None) or [])
    market = _executor_connector(executor)
    inflight = getattr(market, "in_flight_orders", None) or {}
    for text in inflight:
        if str(text) != close_id and str(text) not in gone:
            return True
    return False


def _level_close_id(executor: Any) -> str:
    """Номер в ячейке продажи уровня. Чужой по цене сюда не сажаем."""
    for level in getattr(executor, "grid_levels", []) or []:
        seated = getattr(level, "active_close_order", None)
        text = str(getattr(seated, "order_id", "") or "")
        if text:
            return text
    return ""


def _close_sent(executor_id: str, order_id: str) -> Optional[bool]:
    """None — журнал не прочитан. False — строки sent=true ещё нет."""
    try:
        rows = read_ledger_filtered()
    except _LedgerReadError:
        return None
    sent = False
    for row in rows:
        if str(row.get("executor_id") or "") != str(executor_id):
            continue
        if str(row.get("order_id") or "") != order_id:
            continue
        if row.get("role") not in ("close", "market_close"):
            continue
        if row.get("sent") is True or row.get("sent") == "true":
            sent = True
    return sent


def _occupied_foreign(executor: Any, own_id: str) -> bool:
    """In-flight уже держит номер, который не наш. Новый id не чеканим."""
    market = _executor_connector(executor)
    inflight = getattr(market, "in_flight_orders", None) or {}
    level_id = _level_close_id(executor)
    for text in inflight:
        if str(text) not in {own_id, level_id} and str(text):
            return True
    return False


def _send_landed(executor: Any, order_id: str) -> bool:
    """Строка sent=true до sell/create отправкой не считается."""
    if _sell_is_live(executor, order_id):
        return True
    tracked = getattr(executor, "_close_order", None)
    if str(getattr(tracked, "order_id", "") or "") == order_id and bool(getattr(tracked, "is_done", False)):
        return True
    if str(getattr(tracked, "order_id", "") or "") == order_id and getattr(tracked, "order", None) is not None:
        return True
    return False




def _own_order_ids(executor: Any, executor_id: str) -> set:
    found = set()
    slot = str(getattr(getattr(executor, "_close_order", None), "order_id", "") or "")
    if slot:
        found.add(slot)
    for level in getattr(executor, "grid_levels", []) or []:
        for seated in (getattr(level, "active_open_order", None), getattr(level, "active_close_order", None)):
            text = str(getattr(seated, "order_id", "") or "")
            if text:
                found.add(text)
    try:
        rows = read_ledger_filtered()
    except _LedgerReadError:
        return found
    for row in rows:
        if str(row.get("executor_id") or "") != str(executor_id):
            continue
        text = str(row.get("order_id") or "")
        if text:
            found.add(text)
    return found


def _book_ids(executor: Any) -> set:
    found = set()
    market = _executor_connector(executor)
    for text in getattr(market, "in_flight_orders", None) or {}:
        if str(text):
            found.add(str(text))
    lost = getattr(getattr(market, "_order_tracker", None), "lost_orders", None) or {}
    if isinstance(lost, dict):
        for text, order in lost.items():
            oid = str(getattr(order, "client_order_id", None) or text or "")
            if oid:
                found.add(oid)
    for item in getattr(executor, "_tick_opens", None) or []:
        if isinstance(item, dict):
            oid = str(item.get("text") or item.get("order_id") or "")
        else:
            oid = str(getattr(item, "client_order_id", None) or item or "")
        if oid:
            found.add(oid)
    return found


def other_grid_order_unproven(executor: Any, close_id: str, executor_id: str = "") -> bool:
    """Другая заявка этой сетки ещё в открытых, в полёте или в потерянных. Чужая пара не держит."""
    books = _book_ids(executor)
    own = _own_order_ids(executor, executor_id)
    for oid in own:
        if oid != str(close_id) and oid in books:
            return True
    return False

def step_close_witness(frozen: bool, witness: str) -> str:
    """Непрочитанный замороженный шаг — не «продажи нет»."""
    if frozen:
        return "frozen-unread"
    return f"close=…{str(witness)[-8:]}" if witness else "close=none"

def _exchange_close_filled(executor: Any, close_id: str) -> bool:
    """Исполнение с биржи. Пустая ячейка с is_done этим не считается."""
    tracked = getattr(executor, "_close_order", None)
    order = getattr(tracked, "order", None)
    if (
        str(getattr(tracked, "order_id", "") or "") == str(close_id)
        and order is not None
        and getattr(order, "is_filled", False)
    ):
        return True
    for row in getattr(executor, "_tick_finished", None) or []:
        if not isinstance(row, dict):
            continue
        text = str(row.get("text") or row.get("order_id") or "")
        if text != str(close_id):
            continue
        try:
            filled = float(row.get("fill_size") or 0) > 0
        except (TypeError, ValueError):
            filled = False
        if row.get("is_filled") or filled:
            return True
    for row in getattr(executor, "_tick_blank_fills", None) or []:
        if not isinstance(row, dict):
            continue
        text = str(row.get("text") or row.get("order_id") or "")
        if text != str(close_id):
            continue
        try:
            if float(row.get("fill_size") or 0) > 0:
                return True
        except (TypeError, ValueError):
            continue
    return False


def recorded_close_holds_retry(executor: Any, executor_id: str) -> bool:
    """Записанная продажа жива или биржа уже показала исполнение. Чужой номер не считается."""
    close_id = _known_close_id(executor, executor_id) or ""
    if not close_id:
        return False
    if str(close_id) in _book_ids(executor):
        return True
    return _exchange_close_filled(executor, close_id)

def marked_stop_ready(executor: Any, executor_id: str) -> bool:
    """Финал только после отправленной и уже не живой продажи. 51400 сам не отпускает."""
    if not _journal_readable():
        return False
    close_id = _known_close_id(executor, executor_id) or ""
    if not close_id or not _exchange_close_filled(executor, close_id):
        return False
    if str(close_id) in _book_ids(executor):
        return False
    level_id = _level_close_id(executor)
    if level_id and level_id != close_id and level_id in _book_ids(executor):
        return False
    if other_grid_order_unproven(executor, close_id, executor_id):
        return False
    if _sell_is_live(executor, close_id):
        return False
    return True

def install_close_without_level(executor: Any, executor_id: str, frozen: Any = None) -> bool:
    """Fail-closed brake. False means start must not run.

    Occupied-open only wraps get_open_orders_to_create. This one wraps closes.
    `frozen` — проверка метки «стоп запрошен»: путь гашения держится и молча
    не остаётся (свидетель brake-held).
    """
    if getattr(executor, "_close_brake", False):
        return True
    original_get = getattr(executor, "get_close_orders_to_create", None)
    original_control = getattr(executor, "control_close_order", None)
    original_shutdown = getattr(executor, "control_shutdown_process", None)
    original_adjust = getattr(executor, "adjust_and_place_close_order", None)
    original_market = getattr(executor, "place_close_order_and_cancel_open_orders", None)
    original_loop = getattr(executor, "control_loop", None)
    if original_get is None or original_control is None or original_shutdown is None:
        return False

    def blocked() -> bool:
        if not getattr(executor, "_close_seat_ready", False):
            return True
        if not getattr(executor, "_close_list_ready", False):
            return True
        if hold_ids_of(executor) or getattr(executor, "_close_ban_ids", None):
            return True
        if getattr(executor, "_column_close_ids", None):
            return True
        close = getattr(executor, "_close_order", None)
        if getattr(close, "order_id", None):
            return True
        for level in getattr(executor, "grid_levels", []) or []:
            seated = getattr(level, "active_close_order", None)
            if getattr(seated, "order_id", None):
                return True
        return False

    def hold(place: str) -> bool:
        """Кнопка отпускает get_close. stop() без доказательства не отпускает."""
        proven = bool(getattr(executor, "_shutdown_close_proven", False))
        if place in ("control_shutdown", "stop") and proven:
            return False
        marked = False
        if frozen is not None:
            try:
                marked = bool(frozen())
            except Exception:
                marked = False
        if not marked and place in ("get_close", "control_close", "control_shutdown", "stop"):
            return False
        if marked and place in ("get_close", "control_shutdown"):
            return False
        if place == "stop":
            _brake_held(executor_id, place, "blocked" if blocked() else "unproven")
            return True
        if marked:
            _brake_held(executor_id, place, "frozen: awaiting stage 2")
            return True
        if blocked():
            _brake_held(executor_id, place, "blocked")
            return True
        return False

    def place_unready() -> bool:
        return (
            not getattr(executor, "_close_seat_ready", False)
            or not getattr(executor, "_close_list_ready", False)
        )

    def blocked_besides_place() -> bool:
        if hold_ids_of(executor) or getattr(executor, "_close_ban_ids", None):
            return True
        if getattr(executor, "_column_close_ids", None):
            return True
        close = getattr(executor, "_close_order", None)
        if getattr(close, "order_id", None):
            return True
        for level in getattr(executor, "grid_levels", []) or []:
            seated = getattr(level, "active_close_order", None)
            if getattr(seated, "order_id", None):
                return True
        return False

    def _shown_open(level: Any) -> bool:
        order = getattr(getattr(level, "active_open_order", None), "order", None)
        return bool(getattr(order, "is_filled", False))

    def get_close(*args, **kwargs):
        if frozen is not None:
            try:
                if not bool(frozen()):
                    return original_get(*args, **kwargs)
            except Exception:
                return original_get(*args, **kwargs)
        if any(_shown_open(level) for level in getattr(executor, "grid_levels", []) or []):
            return original_get(*args, **kwargs)
        if blocked_besides_place() and hold("get_close"):
            return []
        if hold("get_close") and not place_unready():
            return []
        return original_get(*args, **kwargs)

    async def control_close(*args, **kwargs):
        marked = False
        if frozen is not None:
            try:
                marked = bool(frozen())
            except Exception:
                marked = False
        # Занятую ячейку читает штатное закрытие. Свой номер не чеканим.
        if marked:
            return await original_control(*args, **kwargs)
        if hold("control_close"):
            return None
        return await original_control(*args, **kwargs)

    async def control_shutdown(*args, **kwargs):
        if hold("control_shutdown"):
            return None
        return await original_shutdown(*args, **kwargs)

    def marked_now() -> bool:
        if frozen is None:
            return False
        try:
            return bool(frozen())
        except Exception:
            return False

    def adjust(level, *args, **kwargs):
        if frozen is not None:
            try:
                if not bool(frozen()):
                    return original_adjust(level, *args, **kwargs)
            except Exception:
                return original_adjust(level, *args, **kwargs)
        if _shown_open(level) and not marked_now():
            return original_adjust(level, *args, **kwargs)
        if hold("adjust"):
            return None
        owns = getattr(executor, "_partial_owns_level", None)
        if owns is not None and owns(level):
            return None
        refuse = getattr(executor, "_refuse_close_price", None)
        if refuse is not None and refuse(getattr(level, "price", None)):
            return None
        return place_level_close(executor, executor_id, level)

    async def control_loop(*args, **kwargs):
        try:
            return await original_loop(*args, **kwargs)
        except Exception:
            if hold_ids_of(executor) or not getattr(executor, "_close_seat_ready", False):
                logger.exception("close hold kept the executor after on_start failed")
                return None
            raise

    executor.get_close_orders_to_create = get_close
    executor.control_close_order = control_close
    executor.control_shutdown_process = control_shutdown
    def market(*args, **kwargs):
        if original_market is None:
            return None
        return original_market(*args, **kwargs)

    if original_adjust is not None:
        executor.adjust_and_place_close_order = adjust
    if original_market is not None:
        executor.place_close_order_and_cancel_open_orders = market
    original_stop = getattr(executor, "stop", None)

    def stop(*args, **kwargs):
        marked = False
        if frozen is not None:
            try:
                marked = bool(frozen())
            except Exception:
                marked = False
        if marked:
            author = getattr(executor, "_author_stop", None) or original_stop
            if author is None:
                return None
            return author(*args, **kwargs)
        if hold("stop"):
            return None
        if original_stop is None:
            return None
        return original_stop(*args, **kwargs)

    if original_stop is not None:
        executor.stop = stop
    if original_loop is not None:
        executor.control_loop = control_loop
    executor._close_brake = True
    executor._close_seat_ready = False
    if not hasattr(executor, "_close_holds"):
        executor._close_holds = []
    return True

_PARTIAL_FIVE = ("trading_pair", "order_type", "trade_type", "leverage", "position")


def _close_slot_ids(levels: List[Dict[str, Any]]) -> set:
    found = set()
    for level in levels or []:
        order_id = (level.get("close_order") or {}).get("order_id")
        if order_id:
            found.add(str(order_id))
        for item in level.get("partial_closes") or []:
            if isinstance(item, dict) and item.get("order_id"):
                found.add(str(item.get("order_id")))
    return found


def _attach_close_slot(saved: Optional[Dict[str, Any]], live_by_id: Dict[str, Any]) -> Any:
    """Seat a committed close by its text. A flat number is not a seat."""
    tracked = _attach(saved, live_by_id)
    if tracked is not None or not isinstance(saved, dict) or not saved.get("order_id"):
        return tracked
    if not saved.get("level_id") or saved.get("price") in (None, ""):
        return None
    from hummingbot.strategy_v2.models.executors import TrackedOrder
    return TrackedOrder(order_id=str(saved["order_id"]))


def partial_has_five(row: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(row, dict):
        return False
    return all(row.get(key) not in (None, "") for key in _PARTIAL_FIVE)


def partial_lines(rows: Optional[List[Dict[str, Any]]], order_id: str) -> List[Dict[str, Any]]:
    """The lines of one number in journal order. Same answer, no full-file scan.

    Shallow copies out: the snapshot's rows are shared and read-only, a reader
    that edits its line edits its own copy.
    """
    item = _rows_answers(rows)["orders"].get(str(order_id))
    return [dict(row) for row in item["lines"]] if item else []


def partial_send_marked(rows: Optional[List[Dict[str, Any]]], order_id: str) -> bool:
    """The second line, not a lone five-field row, is the send mark."""
    item = _rows_answers(rows)["orders"].get(str(order_id))
    return bool(item and item["send_marked"])


def partial_old_remember(row: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(row, dict):
        return False
    if not row.get("order_id") or not row.get("level_id") or partial_has_five(row):
        return False
    return True


def partial_annotation_line(row: Optional[Dict[str, Any]]) -> bool:
    """A repeat annotation: the take-profit tick without any sending record."""
    if not isinstance(row, dict):
        return False
    if not row.get("order_id") or not row.get("level_id"):
        return False
    if partial_has_five(row):
        return False
    if row.get("account_name") not in (None, "") or row.get("connector") not in (None, ""):
        return False
    return row.get("take_profit_price") not in (None, "")


def partial_close_send_line(row: Optional[Dict[str, Any]]) -> bool:
    """A close-send line: its price is already the take-profit tick, not a placement."""
    if not isinstance(row, dict):
        return False
    if not row.get("order_id") or not row.get("level_id"):
        return False
    if partial_has_five(row):
        return False
    if row.get("account_name") not in (None, "") or row.get("connector") not in (None, ""):
        return False
    return row.get("take_profit_price") in (None, "")


def _answers_for_rows(rows: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    """One pass over the rows: per-number answers, ready holders and kinds.

    Every answer is the same expression the readers ran over all rows, computed
    once over the same lines in the same order: nothing added, nothing lost.
    `old_remember_plain` is the «remember, not an annotation» rule, whose
    `item is not row` guard is a no-op: the guarded rule only runs on an
    annotation line, which never satisfies `not partial_annotation_line(row)`.

    Holders are grouped by the row's executor_id: the query answer is the
    anonymous group (rows without an executor) union the queried group — the
    same `executor_id not in (None, "", executor_id)` filter as before.
    """
    orders: Dict[str, Dict[str, Any]] = {}
    unowned_ids: set = set()
    close_price_ids: Dict[str, set] = {}
    min_ts: Any = None
    for row in rows or []:
        key = str(row.get("order_id") or "")
        item = orders.setdefault(key, {"lines": []})
        item["lines"].append(row)
        if row.get("order_id") and not row.get("executor_id"):
            unowned_ids.add(str(row.get("order_id")))
        if row.get("role") in ("close", "market_close") and row.get("price") not in (None, ""):
            close_price_ids.setdefault(str(row.get("price")), set()).add(str(row.get("order_id") or ""))
        ts = row.get("ts")
        if ts is not None:
            if min_ts is None:
                min_ts = ts
            else:
                try:
                    if ts < min_ts:
                        min_ts = ts
                except TypeError:
                    # Несопоставимые типы ts не роняют сборку снимка: иначе битый
                    # журнал отключил бы и те читателей, которым ts не нужен
                    # (замечание проверяющего кода).
                    pass
    for item in orders.values():
        lines = item["lines"]
        item["send_marked"] = len(lines) >= 2 and any(partial_has_five(row) for row in lines[1:])
        item["old_remember"] = any(partial_old_remember(row) for row in lines)
        item["old_remember_plain"] = any(
            partial_old_remember(row) and not partial_annotation_line(row) for row in lines
        )
        item["has_five"] = any(partial_has_five(row) for row in lines)
        item["lone_five"] = item["has_five"] and not item["send_marked"] and not item["old_remember"]
        item["price_known"] = any(row.get("price") not in (None, "") for row in lines)
        item["mark_owner"] = next(
            (row.get("executor_id") for row in reversed(lines) if row.get("executor_id")), None
        )
        if item["send_marked"]:
            item["kind"] = "send-mark"
        elif item["old_remember_plain"]:
            item["kind"] = "remember"
        else:
            item["kind"] = "annotation-only"
    holding_anon: set = set()
    holding_by_executor: Dict[Any, set] = {}
    for row in rows or []:
        order_id = row.get("order_id")
        level_id = row.get("level_id")
        if not order_id or not level_id:
            continue
        if row.get("role") in ("close", "market_close"):
            continue
        item = orders.get(str(order_id)) or {}
        if item.get("send_marked"):
            pass
        elif not partial_old_remember(row):
            continue
        elif partial_annotation_line(row) and item.get("old_remember_plain"):
            continue
        holder = row.get("executor_id")
        bucket = holding_anon if holder in (None, "") else holding_by_executor.setdefault(holder, set())
        bucket.add(str(order_id))
    return {
        "orders": orders,
        "holding_anon": holding_anon,
        "holding_by_executor": holding_by_executor,
        "unowned_ids": unowned_ids,
        "close_price_ids": close_price_ids,
        "min_ts": min_ts,
    }


def partial_lone_five(rows: Optional[List[Dict[str, Any]]], order_id: str) -> bool:
    """A lone five-field row: five fields, but neither send-mark nor old remember."""
    if not order_id:
        return False
    item = _rows_answers(rows)["orders"].get(str(order_id))
    return bool(item and item["lone_five"])


def partial_holding_ids(rows: Optional[List[Dict[str, Any]]], executor_id: str) -> set:
    """Old remember and a successful send mark hold. A lone five-field row does not.

    An annotation line is not its own holder when a remember or send-mark line
    of the same id already holds it; alone, it still holds.

    Ready holders from the snapshot: the anonymous group plus the queried one.
    """
    if rows is None:
        return set()
    view = _rows_answers(rows)
    found = set(view["holding_anon"])
    found |= view["holding_by_executor"].get(executor_id) or set()
    return found


def partial_hold_kinds(
    rows: Optional[List[Dict[str, Any]]],
    parsed: Optional[Dict[str, Any]],
    executor_id: str,
) -> Dict[str, str]:
    """For each held id without a visible end: which kind of line holds it."""
    kinds: Dict[str, str] = {}
    if rows is None:
        return kinds
    orders = _rows_answers(rows)["orders"]
    for order_id in partial_holding_ids(rows, executor_id):
        if parsed is not None and partial_end_visible(parsed, str(order_id)):
            continue
        item = orders.get(str(order_id)) or {}
        kinds[str(order_id)] = item.get("kind") or "annotation-only"
    return kinds


def partial_column_hold_levels(parsed: Optional[Dict[str, Any]]) -> List[str]:
    """Level ids whose open or close slot still holds without a release mark."""
    found: List[str] = []
    for level in (parsed or {}).get("levels") or []:
        if not isinstance(level, dict) or not level.get("id"):
            continue
        if partial_level_released(parsed, str(level.get("id"))):
            continue
        slot = level.get("open_order") or {}
        close = level.get("close_order") or {}
        if slot.get("order_id") and slot.get("level_id") and not partial_end_visible(parsed, str(slot.get("order_id"))):
            found.append(str(level.get("id")))
        elif close.get("order_id") and close.get("level_id"):
            found.append(str(level.get("id")))
    return found


def partial_released_ids(parsed: Optional[Dict[str, Any]]) -> set:
    ends = (parsed or {}).get("partial_ends") or {}
    found = set(str(key) for key in ends)
    for mark in ends.values():
        if isinstance(mark, dict):
            found.update(str(item) for item in (mark.get("order_ids") or []) if item)
            if mark.get("level_id"):
                found.add("level:" + str(mark.get("level_id")))
    return found


def partial_price_released_ids(parsed: Optional[Dict[str, Any]]) -> set:
    """Цена отпущена — только концы уровней (метки с level_id), не отдельные номера.

    Конец отдельного номера (метка без level_id) отпускает номер для стопа и
    финализации, но цену не снимает: тик тейка живого уровня должен оставаться
    занятым, иначе на него встанет чужая заявка и склеится по цене с закрытием
    уровня, а стоп снова отвергнется.
    """
    ends = (parsed or {}).get("partial_ends") or {}
    found = set()
    for key, mark in ends.items():
        if not isinstance(mark, dict) or not mark.get("level_id"):
            continue
        found.add(str(key))
        found.add("level:" + str(mark.get("level_id")))
        found.update(str(item) for item in (mark.get("order_ids") or []) if item)
    return found


def partial_end_visible(parsed: Optional[Dict[str, Any]], order_id: str) -> bool:
    ends = (parsed or {}).get("partial_ends") or {}
    mark = ends.get(str(order_id))
    return isinstance(mark, dict) and bool(mark.get("kind")) and "price" not in mark


STOP_MARK_KEY = "stop-requested"


def stop_mark_visible(parsed: Optional[Dict[str, Any]]) -> bool:
    """Метка «стоп запрошен» — единственная запись среза заморозки.

    Форма — метка в partial_ends без level_id, ключ не равен ни одному order_id,
    order_ids пуст: только такую форму штатный захват чекпоинта переносит вперёд,
    и она не отпускает цену уровня.
    """
    mark = ((parsed or {}).get("partial_ends") or {}).get(STOP_MARK_KEY)
    return isinstance(mark, dict) and mark.get("kind") == "stop_requested"


def partial_level_released(parsed: Optional[Dict[str, Any]], level_id: str) -> bool:
    return "level:" + str(level_id) in partial_released_ids(parsed)


RESUME_COMPLETE_KIND = "resume_complete"
COMPLETED_CYCLE_KEY = "partial_completed"


def completed_cycle_levels(parsed: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Levels whose cycle is proven finished: both sides filled with ids and prices."""
    found: List[Dict[str, Any]] = []
    for level in (parsed or {}).get("levels") or []:
        if not isinstance(level, dict):
            continue
        opened = level.get("open_order") or {}
        close = level.get("close_order") or {}
        if opened.get("filled") is not True or close.get("filled") is not True:
            continue
        if not opened.get("order_id") or not close.get("order_id"):
            continue
        if opened.get("price") in (None, "") or close.get("price") in (None, ""):
            continue
        if not isinstance(opened.get("order"), dict) or not isinstance(close.get("order"), dict):
            continue
        level_id = str(level.get("id") or "")
        if not level_id:
            continue
        found.append({
            "level_id": level_id,
            "open_id": str(opened["order_id"]),
            "close_id": str(close["order_id"]),
        })
    return found


def resume_complete_mutate(level_id: str, open_id: str, close_id: str):
    """Один подъёмный CAS: метка конца, запись круга и чистые слоты одной записью.

    Живой путь продаж (_partial_release) не зовётся и не меняется: у его mutate
    нет места для записи тел исполнения, а вторая запись следом стирала бы
    единственную копию размеров при обрыве между записями.
    """

    def mutate(body: Dict[str, Any]) -> None:
        if stop_mark_visible(body):
            raise RuntimeError("stop mark")
        level = None
        for item in body.get("levels") or []:
            if str(item.get("id") or "") == str(level_id):
                level = item
                break
        if not isinstance(level, dict):
            raise RuntimeError("level missing")
        opened = level.get("open_order") or {}
        close = level.get("close_order") or {}
        if str(opened.get("order_id") or "") != str(open_id) or str(close.get("order_id") or "") != str(close_id):
            raise RuntimeError("cycle changed")
        record = {
            "open": json.loads(json.dumps(opened)),
            "close": json.loads(json.dumps(close)),
            "level": {key: level.get(key) for key in (
                "side", "open_order_type", "take_profit_order_type", "price", "amount_quote", "take_profit",
            )},
        }
        ends = dict(body.get("partial_ends") or {})
        ends[str(open_id)] = {
            "kind": RESUME_COMPLETE_KIND,
            "level_id": str(level_id),
            "order_ids": [str(open_id), str(close_id)],
        }
        body["partial_ends"] = ends
        completed = dict(body.get(COMPLETED_CYCLE_KEY) or {})
        completed[str(level_id)] = record
        body[COMPLETED_CYCLE_KEY] = completed
        level.pop("take_profit_price", None)
        level["close_order"] = None
        level["partial_closes"] = []
        slot = level.get("open_order") or {}
        if slot.get("filled"):
            level["open_order"] = None

    return mutate


def resume_complete_visible(level_id: str, open_id: str):
    """Метка и запись круга видны в перечитанном теле, цена внутрь метки не легла."""

    def visible(parsed: Optional[Dict[str, Any]]) -> bool:
        ends = (parsed or {}).get("partial_ends") or {}
        mark = ends.get(str(open_id))
        if not (isinstance(mark, dict) and mark.get("kind") == RESUME_COMPLETE_KIND):
            return False
        if str(mark.get("level_id") or "") != str(level_id):
            return False
        if "price" in mark:
            return False
        record = ((parsed or {}).get(COMPLETED_CYCLE_KEY) or {}).get(str(level_id))
        return isinstance(record, dict) and isinstance(record.get("open"), dict) and isinstance(record.get("close"), dict)

    return visible


def completed_side_entry(
    slot: Dict[str, Any],
    *,
    trade_type: str,
    position_name: str,
    order_type_name: Any,
    trading_pair: Any,
    leverage: Any,
) -> Optional[Dict[str, Any]]:
    """Запись для _filled_orders из записанной правды. Новый номер не чеканится.

    Полное тело идёт как есть; компактному достраивается форма штатного to_json
    по номеру и цене слота, размеру тела исполнения и записи уровня.
    """
    body = slot.get("order") if isinstance(slot.get("order"), dict) else {}
    if body.get("client_order_id"):
        entry = dict(body)
        entry.setdefault("cumulative_fee_paid_base", 0.0)
        entry.setdefault("cumulative_fee_paid_quote", 0.0)
        entry.setdefault("creation_timestamp", 0)
        entry.setdefault("last_update_timestamp", 0)
        entry.setdefault("order_fills", {})
        return entry
    base_raw = body.get("executed_amount_base")
    price_raw = slot.get("price")
    if not slot.get("order_id") or price_raw in (None, "") or base_raw in (None, ""):
        return None
    try:
        base = Decimal(str(base_raw))
        price = Decimal(str(price_raw))
    except (ArithmeticError, ValueError, TypeError):
        return None
    if base <= 0 or price <= 0:
        return None
    return {
        "client_order_id": str(slot["order_id"]),
        "trading_pair": trading_pair,
        "order_type": str(order_type_name or "LIMIT"),
        "trade_type": trade_type,
        "amount": str(base),
        "price": str(price),
        "executed_amount_base": str(base),
        "executed_amount_quote": str(base * price),
        "last_state": "5",
        "leverage": str(leverage or 1),
        "position": position_name,
        "creation_timestamp": 0,
        "last_update_timestamp": 0,
        "order_fills": {},
        "exchange_order_id": None,
        "cumulative_fee_paid_base": 0.0,
        "cumulative_fee_paid_quote": 0.0,
    }
    return "level:" + str(level_id) in partial_released_ids(parsed)


def _level_by_id(parsed: Optional[Dict[str, Any]], level_id: str) -> Optional[Dict[str, Any]]:
    for level in (parsed or {}).get("levels") or []:
        if isinstance(level, dict) and str(level.get("id")) == str(level_id):
            return level
    return None


def partial_seat_readable(slot: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(slot, dict):
        return False
    order = slot.get("order")
    return bool(
        slot.get("order_id")
        and slot.get("filled") is True
        and slot.get("level_id")
        and isinstance(order, dict)
        and order.get("client_order_id")
    )


def partial_seating_visible(parsed: Optional[Dict[str, Any]], order_id: str) -> bool:
    """Visible only together with the take-profit quant. Rowcount is not this."""
    for level in (parsed or {}).get("levels") or []:
        if not isinstance(level, dict):
            continue
        slot = level.get("open_order") or {}
        if str(slot.get("order_id") or "") != str(order_id):
            continue
        if not partial_seat_readable(slot):
            continue
        if level.get("take_profit_price") in (None, ""):
            return False
        return True
    return False


def partial_feed_prices(
    rows: Optional[List[Dict[str, Any]]],
    parsed: Optional[Dict[str, Any]],
    extra_released: Optional[set] = None,
) -> List[Any]:
    """Placement feeds until the seat is visible. The take-profit field feeds until the end mark."""
    if rows is None:
        return [None]
    released = partial_price_released_ids(parsed)
    if extra_released:
        released.update(str(item) for item in extra_released)
    prices: List[Any] = []
    for row in rows:
        order_id = str(row.get("order_id") or "")
        if not order_id or order_id in released:
            continue
        lines = partial_lines(rows, order_id)
        lone_five = partial_has_five(row) and not partial_send_marked(rows, order_id) and not any(
            partial_old_remember(item) for item in lines
        )
        if lone_five:
            continue
        if row.get("price") not in (None, "") and not partial_seating_visible(parsed, order_id):
            prices.append(row.get("price"))
        if row.get("take_profit_price") not in (None, ""):
            prices.append(row.get("take_profit_price"))
    for level in (parsed or {}).get("levels") or []:
        if not isinstance(level, dict) or partial_level_released(parsed, str(level.get("id"))):
            continue
        level_ids = {
            str((level.get("open_order") or {}).get("order_id") or ""),
            str((level.get("close_order") or {}).get("order_id") or ""),
        }
        level_ids.discard("")
        if level_ids and level_ids & released:
            # Слот отменён, но тейк в теле ещё стоит: позиция открыта, тик
            # должен остаться в фиде, иначе на него встанет вторая покупка.
            if level.get("take_profit_price") not in (None, ""):
                prices.append(level.get("take_profit_price"))
            continue
        if level.get("take_profit_price") not in (None, ""):
            prices.append(level.get("take_profit_price"))
        close = level.get("close_order") or {}
        if close.get("price") not in (None, "") and str(close.get("order_id") or "") not in released:
            prices.append(close.get("price"))
        slot = level.get("open_order") or {}
        if (
            slot.get("price") not in (None, "")
            and not partial_seat_readable(slot)
            and str(slot.get("order_id") or "") not in released
        ):
            prices.append(slot.get("price"))
    return prices


def partial_column_prices(
    parsed: Optional[Dict[str, Any]],
    extra_released: Optional[set] = None,
) -> List[Any]:
    return partial_feed_prices([], parsed, extra_released)


def partial_held_level_ids(
    rows: Optional[List[Dict[str, Any]]],
    parsed: Optional[Dict[str, Any]],
    executor_id: str,
) -> set:
    if rows is None:
        return {"*"}
    found = set()
    for order_id in partial_holding_ids(rows, executor_id):
        if partial_end_visible(parsed, order_id):
            continue
        for row in partial_lines(rows, order_id):
            if row.get("level_id"):
                found.add(str(row.get("level_id")))
    for level in (parsed or {}).get("levels") or []:
        if not isinstance(level, dict) or not level.get("id"):
            continue
        if partial_level_released(parsed, str(level.get("id"))):
            continue
        slot = level.get("open_order") or {}
        if slot.get("level_id") and slot.get("order_id"):
            found.add(str(level.get("id")))
        close = level.get("close_order") or {}
        if close.get("order_id") and close.get("level_id"):
            found.add(str(level.get("id")))
    return found


def partial_exposure(rows: Optional[List[Dict[str, Any]]], parsed: Optional[Dict[str, Any]], executor_id: str) -> bool:
    if rows is None:
        return True
    if partial_holding_ids(rows, executor_id):
        return True
    for level in (parsed or {}).get("levels") or []:
        slot = (level or {}).get("open_order") or {}
        if slot.get("order_id") and slot.get("level_id"):
            return True
    return False


def partial_stop_forbidden(
    rows: Optional[List[Dict[str, Any]]],
    parsed: Optional[Dict[str, Any]],
    executor_id: str,
    *,
    proof_ready: bool,
    unproven_ids: Optional[set] = None,
) -> bool:
    """Штатный стоп проходит при готовом гашении. Слоты уровней его не держат.

    Держим только номера с неизвестной судьбой, которые ещё могут оказаться на
    бирже: durable miss без sent (отправка не помечена дошедшей) и номера из
    `unproven_ids` (не подтверждены биржей, списки их время не покрывают).
    Без переданного набора покрытие не предполагается — иначе старый вызов
    снова запирает стоп по любому номеру без конца.
    Подтверждённую живую заявку отменяет cancel_open_orders внутри early_stop,
    доказанно-мёртвую гасит шаг сборки концов — держать стоп нельзя, иначе
    отмены и гашение не запускаются никогда. Нечитаемая история — не разрешение
    стопить.
    """
    if rows is None:
        return partial_exposure(None, parsed, executor_id)
    holding = partial_holding_ids(rows, executor_id)
    if not holding:
        return False
    unproven = set()
    for order_id in holding:
        text = str(order_id)
        if partial_end_visible(parsed, text):
            continue
        if order_is_unsent(text) or order_send_unknown(text) or order_miss_unreadable():
            unproven.add(text)
            continue
        # Без переданного набора покрытие неизвестно: держать по нему нельзя,
        # иначе старый вызов снова запирает стоп по любому номеру без конца.
        if unproven_ids is not None and text in unproven_ids:
            unproven.add(text)
    if not unproven:
        return False
    if not proof_ready:
        return True
    return True


def partial_decimal(value: Any) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except (ArithmeticError, ValueError, TypeError):
        return None
    if parsed.is_nan():
        return None
    return parsed


def partial_raw_base(raw: Optional[Dict[str, Any]]) -> Optional[Decimal]:
    """amount minus left. A missing field is not zero. filled_total is not the base."""
    if not isinstance(raw, dict) or "amount" not in raw or "left" not in raw:
        return None
    amount = partial_decimal(raw.get("amount"))
    left = partial_decimal(raw.get("left"))
    if amount is None or left is None:
        return None
    return amount - left


def partial_base_zero(raw: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(raw, dict) or "amount" not in raw or "left" not in raw:
        return False
    amount = partial_decimal(raw.get("amount"))
    left = partial_decimal(raw.get("left"))
    return amount is not None and left is not None and left == amount


def partial_is_closed_cancel(raw: Optional[Dict[str, Any]]) -> bool:
    return isinstance(raw, dict) and raw.get("status") == "closed" and raw.get("finish_as") == "cancelled"


def partial_fill_base(raw: Optional[Dict[str, Any]]) -> Optional[Decimal]:
    if not partial_is_closed_cancel(raw):
        return None
    base = partial_raw_base(raw)
    amount = partial_decimal((raw or {}).get("amount"))
    if base is None or amount is None or base <= 0 or base >= amount:
        return None
    return base


def partial_full_fill(raw: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(raw, dict) or raw.get("finish_as") != "filled":
        return False
    base = partial_raw_base(raw)
    amount = partial_decimal(raw.get("amount"))
    return base is not None and amount is not None and base == amount


def partial_text_in(rows: Optional[List[Dict[str, Any]]], text: str) -> bool:
    return any(str(row.get("text") or "") == str(text) for row in rows or [])


def partial_find_text(rows: Optional[List[Dict[str, Any]]], text: str) -> Optional[Dict[str, Any]]:
    for row in rows or []:
        if str(row.get("text") or "") == str(text):
            return row
    return None


def partial_lists_cover(fresh: Optional[Dict[str, Any]], ts: Any) -> bool:
    """A short page without the row ts is not proof the text is gone."""
    if not isinstance(fresh, dict) or ts is None:
        return False
    if not fresh.get("opens_read") or not fresh.get("opens_done"):
        return False
    if not fresh.get("finished_read") or not fresh.get("finished_done"):
        return False
    request = fresh.get("finished_request") or {}
    params = request.get("params") or {}
    if "from" not in params or "to" not in params:
        return False
    return history_covers_ts(request, ts, {"read": True, "done": True})


def partial_same_tick(left: Any, right: Any, tick: Optional[Decimal]) -> bool:
    return _same_tick(left, right, tick) is True


def partial_take_profit_quant(
    connector: Any,
    pair: str,
    price: Any,
    take_profit: Any,
    side: str,
) -> Optional[str]:
    """Quantize from the row price and the level take-profit. Do not store the raw product."""
    base = partial_decimal(price)
    profit = partial_decimal(take_profit)
    if base is None or profit is None or connector is None or not hasattr(connector, "quantize_order_price"):
        return None
    factor = Decimal("1") - profit if side == "SELL" else Decimal("1") + profit
    try:
        quant = connector.quantize_order_price(pair, base * factor)
    except Exception:
        return None
    if quant in (None, ""):
        return None
    return str(quant)


def partial_order_body(
    *,
    client_order_id: str,
    trading_pair: Any,
    order_type: Any,
    trade_type: Any,
    amount: Any,
    price: Any,
    exchange_order_id: Any,
    leverage: Any,
    position: Any,
    executed_base: Any,
    executed_quote: Any,
) -> Optional[Dict[str, Any]]:
    """Body for from_json. A missing field is not zero."""
    fields = {
        "client_order_id": client_order_id,
        "trading_pair": trading_pair,
        "order_type": order_type,
        "trade_type": trade_type,
        "amount": amount,
        "price": price,
        "exchange_order_id": exchange_order_id,
        "last_state": "5",
        "leverage": leverage,
        "position": position,
        "executed_amount_base": executed_base,
        "executed_amount_quote": executed_quote,
    }
    if any(value in (None, "") for value in fields.values()):
        return None
    try:
        from hummingbot.core.data_type.in_flight_order import InFlightOrder
        InFlightOrder.from_json(dict(fields))
    except Exception:
        return None
    return fields


def partial_commit_body(read_text: Optional[str], mutate: Any, target_id: Optional[str] = None) -> Optional[str]:
    """Full copy plus one level change. Other levels disappearing is not success."""
    if read_text:
        try:
            base = json.loads(read_text)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(base, dict):
            return None
    else:
        base = {"version": 1, "type": "grid_executor", "levels": [], "holds": []}
    body = json.loads(json.dumps(base))
    before = {
        level.get("id"): level
        for level in body.get("levels") or []
        if isinstance(level, dict)
    }
    try:
        mutate(body)
    except Exception:
        return None
    after = {
        level.get("id"): level
        for level in body.get("levels") or []
        if isinstance(level, dict)
    }
    for level_id, level in before.items():
        if level_id not in after:
            return None
        if target_id is not None and str(level_id) == str(target_id):
            continue
        if after[level_id] != level:
            return None
    return json.dumps(body, sort_keys=True)


def partial_ensure_level(body: Dict[str, Any], level_id: str, seed: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    levels = body.setdefault("levels", [])
    for level in levels:
        if isinstance(level, dict) and str(level.get("id")) == str(level_id):
            return level
    if not seed or not seed.get("id"):
        return None
    levels.append(json.loads(json.dumps(seed)))
    return levels[-1]


def apply_partial_keep(body: Dict[str, Any], previous: Dict[str, Any]) -> None:
    """Copy an end mark forward. Do not restore a released take-profit price."""
    ends = previous.get("partial_ends") or {}
    if ends:
        body["partial_ends"] = json.loads(json.dumps(ends))
    # Запись завершённого круга переживает захват и переносится вперёд, как
    # partial_ends: обрыв между меткой и оформлением не должен терять размеры.
    prev_completed = previous.get(COMPLETED_CYCLE_KEY) or {}
    if prev_completed:
        body[COMPLETED_CYCLE_KEY] = json.loads(json.dumps(prev_completed))
    released = partial_released_ids(previous)
    holds = []
    for item in body.get("holds") or []:
        if isinstance(item, dict) and str(item.get("order_id") or "") in released:
            continue
        holds.append(item)
    if holds or body.get("holds"):
        body["holds"] = holds
    previous_levels = {
        level.get("id"): level
        for level in previous.get("levels") or []
        if isinstance(level, dict)
    }
    for level in body.get("levels") or []:
        if not isinstance(level, dict):
            continue
        prev = previous_levels.get(level.get("id"))
        if not isinstance(prev, dict):
            continue
        if partial_level_released(previous, str(level.get("id"))):
            level.pop("take_profit_price", None)
            level.pop("partial_closes", None)
            # Код-ревью 20261110-072441: close, чей номер уже в метке конца или в
            # записи круга, на отпущенном уровне без входа не оставляем — иначе
            # колонка держит «filled-продажу без входа» и следующий подъём мёртв.
            released_ids = partial_released_ids(previous)
            completed = (previous.get(COMPLETED_CYCLE_KEY) or {}).get(str(level.get("id"))) or {}
            record_ids = set()
            if isinstance(completed, dict):
                for side in ("open", "close"):
                    record_ids.add(str((completed.get(side) or {}).get("order_id") or ""))
            close = level.get("close_order") or {}
            close_id = str(close.get("order_id") or "")
            open_id = str((level.get("open_order") or {}).get("order_id") or "")
            if close_id and not open_id and (close_id in released_ids or close_id in record_ids):
                level["close_order"] = None
            elif not close.get("price"):
                level["close_order"] = None
            continue
        slot = level.get("open_order") or {}
        prev_slot = prev.get("open_order") or {}
        incomplete = (
            not slot.get("price")
            and not level.get("take_profit_price")
        ) or not slot.get("level_id") or "partial_closes" not in level
        if prev.get("take_profit_price") and not level.get("take_profit_price"):
            level["take_profit_price"] = prev.get("take_profit_price")
        if prev.get("partial_closes") and not level.get("partial_closes"):
            level["partial_closes"] = json.loads(json.dumps(prev.get("partial_closes")))
        if partial_seat_readable(prev_slot) and not partial_seat_readable(slot):
            level["open_order"] = json.loads(json.dumps(prev_slot))
        prev_close = prev.get("close_order") or {}
        close = level.get("close_order") or {}
        rich_close = (
            prev_close.get("order_id")
            and prev_close.get("level_id")
            and prev_close.get("price") not in (None, "")
        )
        flat_close = not close.get("level_id") or close.get("price") in (None, "")
        # Снятую запись мёртвого закрытия не воскрешать: её номер отпущен,
        # а живая запись слота снова держит и стоп, и охрану шага гашения.
        if rich_close and flat_close and not partial_end_visible(
            previous, str(prev_close.get("order_id") or "")
        ):
            level["close_order"] = json.loads(json.dumps(prev_close))
        if incomplete and prev.get("take_profit_price"):
            level["take_profit_price"] = prev.get("take_profit_price")
