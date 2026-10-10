"""Пробы лечения двух горячих мест (план durable-open-hotspots).

Держат три вещи: ответы шести читателей журнала ровно равны прежним петлям над
_read_ledger() (включая форму с записью в наборах — фильтр не подмешивается);
тела памяти перестают копироваться на каждое обращение, а декор один на текст;
запись мимо кэша видна тому же шагу — иначе цена номера выпала бы из занятых
и встала бы вторая заявка на ту же цену.
"""

import copy
import json

import pytest

from test.test_checkpoint_parse_cache import _bare_service
from test.test_executor_checkpoint import _Repo, _load_executor_service

mod = _load_executor_service()
ExecutorService = mod.ExecutorService

from contextlib import asynccontextmanager  # noqa: E402
from types import SimpleNamespace  # noqa: E402

import utils.executor_checkpoint as ledger_mod  # noqa: E402
from utils.executor_checkpoint import (  # noqa: E402
    _LedgerReadError,
    add_miss_order,
    add_sent_order,
    append_durable_row,
    checkpoint_cache_prune,
    checkpoint_cache_reset_stats,
    checkpoint_cache_stats,
    checkpoint_decorated,
    checkpoint_master,
    checkpoint_view,
    ledger_cache_reset_stats,
    ledger_cache_stats,
    ledger_close_price_taken,
    ledger_journal_mark,
    ledger_min_ts,
    ledger_price_known,
    ledger_raw_rows,
    ledger_unowned_ids,
    parse_checkpoint,
    parse_checkpoint_cached,
    partial_annotation_line,
    partial_has_five,
    partial_holding_ids,
    partial_old_remember,
    read_ledger_filtered,
    reset_ledger_process_state,
)


# ── прежнее чтение журнала: то, что шесть читателей делали до правки ──────────


def _old_read():
    return ledger_mod._read_ledger()


def _old_price_known(order_id):
    for row in _old_read() or []:
        if str(row.get("order_id") or "") == str(order_id) and row.get("price") not in (None, ""):
            return True
    return False


def _old_journal_mark(order_id):
    found = False
    owner = None
    for row in _old_read() or []:
        if str(row.get("order_id") or "") != str(order_id):
            continue
        found = True
        if row.get("executor_id"):
            owner = row.get("executor_id")
    if owner:
        return "owned", owner
    return ("unowned", None) if found else ("absent", None)


def _old_close_price_taken(price, ignore_id=""):
    for row in _old_read() or []:
        if str(row.get("order_id") or "") == str(ignore_id or ""):
            continue
        if row.get("role") not in ("close", "market_close"):
            continue
        if row.get("price") not in (None, "") and str(row.get("price")) == str(price):
            return True
    return False


def _old_unowned_ids():
    return {
        str(row.get("order_id"))
        for row in _old_read() or []
        if row.get("order_id") and not row.get("executor_id")
    }


def _old_min_ts():
    found = []
    for row in _old_read() or []:
        if row.get("ts") is not None:
            found.append(row.get("ts"))
    return min(found) if found else None


# ── прежний декор тела: _price_checkpoint/_slot_price из базы bac7f35 ─────────


def _old_slot_price(slot, level, allow_level_price):
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


def _old_decorated(parsed):
    """Тело старого _price_checkpoint из bac7f35, скопированное в пробу."""
    body = copy.deepcopy(parsed) if isinstance(parsed, dict) else {}
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
            price = _old_slot_price(slot, level, allow_level)
            if price in (None, ""):
                continue
            holds.append({"order_id": order_id, "price": str(price)})
            known.add(str(order_id))
    body["holds"] = holds
    return body


def _old_decorated_text(text):
    return _old_decorated(parse_checkpoint(text))


# ── прежние предикаты занятых цен (база bac7f35, до кэша снимка) ─────────────


def _old_lines(rows, order_id):
    return [row for row in rows or [] if str(row.get("order_id") or "") == str(order_id)]


def _old_send_marked(rows, order_id):
    lines = _old_lines(rows, order_id)
    if len(lines) < 2:
        return False
    return any(partial_has_five(row) for row in lines[1:])


def _old_lone_five(rows, order_id):
    if not order_id or _old_send_marked(rows, order_id):
        return False
    lines = [row for row in rows or [] if str(row.get("order_id") or "") == order_id]
    if any(partial_old_remember(row) for row in lines):
        return False
    return any(partial_has_five(row) for row in lines)


def _old_holding_ids(rows, executor_id):
    found = set()
    if rows is None:
        return found
    for row in rows:
        if row.get("executor_id") not in (None, "", executor_id):
            continue
        order_id = row.get("order_id")
        level_id = row.get("level_id")
        if not order_id or not level_id:
            continue
        if row.get("role") in ("close", "market_close"):
            continue
        if _old_send_marked(rows, str(order_id)):
            found.add(str(order_id))
            continue
        if not partial_old_remember(row):
            continue
        if partial_annotation_line(row) and any(
            partial_old_remember(item) and not partial_annotation_line(item)
            for item in _old_lines(rows, str(order_id))
            if item is not row
        ):
            continue
        found.add(str(order_id))
    return found


# ── занятые цены: прежняя и нынешняя формулы _durable_open ─────────────────


def _old_collect_stored(rows, executor_id):
    """Формула базы: сырой журнал, прежние предикаты (блок stored из _durable_open)."""
    stored = []
    journal_ts = []
    held = _old_holding_ids(rows, executor_id)
    for row in rows:
        order_key = str(row.get("order_id") or "")
        if row.get("ts") is not None:
            journal_ts.append(row.get("ts"))
        if order_key in held:
            continue
        if _old_lone_five(rows, order_key):
            continue
        if row.get("price") not in (None, ""):
            stored.append((order_key, row.get("price")))
        if row.get("take_profit_price") not in (None, ""):
            stored.append((order_key, row.get("take_profit_price")))
    return stored, journal_ts


def _new_collect_stored(service, executor_id):
    """Нынешний блок stored из _durable_open, строка в строку."""
    stored = []
    journal_ts = []
    ledger_rows = read_ledger_filtered()
    held_now = partial_holding_ids(ledger_rows, executor_id)
    for row in ledger_rows:
        order_key = str(row.get("order_id") or "")
        if row.get("ts") is not None:
            journal_ts.append(row.get("ts"))
        if order_key in held_now:
            continue
        if service._partial_lone_five(ledger_rows, order_key):
            continue
        if row.get("price") not in (None, ""):
            stored.append((order_key, row.get("price")))
        if row.get("take_profit_price") not in (None, ""):
            stored.append((order_key, row.get("take_profit_price")))
    return stored, journal_ts


def _old_collect_journal_prices(rows):
    prices = []
    for row in rows:
        order_key = str(row.get("order_id") or "")
        if not order_key:
            continue
        if _old_lone_five(rows, order_key):
            continue
        if row.get("price") not in (None, ""):
            prices.append((order_key, row.get("price")))
        if row.get("take_profit_price") not in (None, ""):
            prices.append((order_key, row.get("take_profit_price")))
    return prices


def _new_collect_journal_prices(service):
    prices = []
    ledger_rows = read_ledger_filtered()
    for row in ledger_rows:
        order_key = str(row.get("order_id") or "")
        if not order_key:
            continue
        if service._partial_lone_five(ledger_rows, order_key):
            continue
        if row.get("price") not in (None, ""):
            prices.append((order_key, row.get("price")))
        if row.get("take_profit_price") not in (None, ""):
            prices.append((order_key, row.get("take_profit_price")))
    return prices


def _old_unpriced_close_blocks(service, executor):
    """Формула базы: прежняя _close_price_known с петлёй над _read_ledger()."""
    pair = getattr(getattr(executor, "config", None), "trading_pair", None)
    for other in service._active_executors.values():
        other_pair = getattr(getattr(other, "config", None), "trading_pair", None)
        if pair and other_pair and other_pair != pair:
            continue
        ids = {str(item) for item in getattr(other, "_close_ban_ids", set()) or set() if item}
        for item in getattr(other, "_close_holds", []) or []:
            if isinstance(item, dict) and item.get("order_id"):
                ids.add(str(item.get("order_id")))
        close = getattr(other, "_close_order", None)
        if getattr(close, "order_id", None):
            ids.add(str(close.order_id))
        for order_id in ids:
            if _old_close_price_known(service, order_id):
                continue
            if service._close_left_the_book(other, order_id):
                continue
            return True
    return False


def _old_close_price_known(service, order_id):
    for row in _old_read() or []:
        if str(row.get("order_id") or "") == str(order_id) and row.get("price") not in (None, ""):
            return True
    wanted = str(order_id)
    for entry in service._hold_index():
        if entry["close_price_map"].get(wanted) not in (None, ""):
            return True
        if entry["close_hold_price_map"].get(wanted) not in (None, ""):
            return True
    return False


# ── прежний фильтр мёртвых: то, что делал _parsed_without_dead до правки ──────


def _old_parsed_without_dead(service, parsed, fresh):
    import copy

    body = copy.deepcopy(parsed) if isinstance(parsed, dict) else parsed
    if not isinstance(body, dict) or not service._lists_ready(fresh):
        return parsed
    levels = []
    for level in body.get("levels") or []:
        if not isinstance(level, dict):
            levels.append(level)
            continue
        copy_level = dict(level)
        for key in ("open_order", "close_order"):
            slot = copy_level.get(key) or {}
            if isinstance(slot, dict) and slot.get("order_id") and service._proven_dead(str(slot.get("order_id")), fresh):
                copy_level[key] = {}
                if key == "open_order":
                    copy_level["take_profit_price"] = None
        levels.append(copy_level)
    body["levels"] = levels
    body["holds"] = [
        item for item in (body.get("holds") or [])
        if not (isinstance(item, dict) and service._proven_dead(str(item.get("order_id") or ""), fresh))
    ]
    return body


# ── прежний список цен колонки: вложенный пересчёт released ───────────────────


def _old_column_prices(service, rows, pair):
    from utils.executor_checkpoint import (
        order_is_unsent,
        partial_column_prices,
        partial_released_ids,
    )

    prices = []
    for row in rows:
        if row.get("trading_pair") != pair:
            continue
        if "price_checkpoint" not in row:
            return None
        if row.get("checkpoint") and checkpoint_master(row.get("checkpoint")) is None:
            return None
        parsed = row.get("price_checkpoint")
        if isinstance(parsed, str):
            parsed = checkpoint_master(parsed)
        released = set()
        for item in rows:
            if item.get("trading_pair") != pair:
                continue
            released.update(partial_released_ids(checkpoint_master(item.get("checkpoint"))))
        fresh = service._tick_fresh(pair)
        feed = service._parsed_without_dead(parsed, fresh) if service._lists_ready(fresh) else parsed
        for hold in (feed or {}).get("holds") or []:
            if hold.get("price") in (None, ""):
                continue
            hold_id = str(hold.get("order_id") or "")
            if hold_id and order_is_unsent(hold_id):
                continue
            prices.append((hold_id, hold.get("price")))
        for saved in (feed or {}).get("retained_orders") or []:
            if saved.get("price") in (None, ""):
                continue
            saved_id = str(saved.get("order_id") or "")
            if saved_id and order_is_unsent(saved_id):
                continue
            prices.append((saved_id, saved.get("price")))
        for item in partial_column_prices(feed, None):
            if item not in (None, ""):
                prices.append(item)
    return prices


# ── формы строк журнала ────────────────────────────────────────────────────────


def _rows_forms():
    return [
        {"order_id": "a", "price": "0.1", "executor_id": "ex", "ts": 5.0},
        {"order_id": "a", "price": "0.11"},
        {"order_id": "a", "executor_id": "ex2", "ts": 3.0},
        {"order_id": "b", "price": ""},
        {"order_id": "c", "price": 0},
        {"price": "0.2", "ts": 8.0},
        {"order_id": "d", "role": "close", "price": "0.3", "ts": 7.0},
        {"order_id": "e", "role": "market_close", "price": "0.4"},
        {"order_id": "f", "role": "open", "price": "0.5"},
        {"order_id": "g", "role": "close"},
        {"order_id": "h", "price": "0.6", "ts": 2.0, "executor_id": "hx"},
    ]


def _write_journal(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _boot(monkeypatch, path, rows):
    _write_journal(path, rows)
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    ledger_cache_reset_stats()
    checkpoint_cache_reset_stats()


def _query_ids():
    return ["a", "b", "c", "d", "e", "f", "g", "h", "absent", "", None, "0.2"]


def _query_prices():
    return ["0.1", "0.11", "0.3", "0.4", "0.5", "0.6", "0.30", 0, "0", None, "", "0.2"]


# ── проба 1: ответы читателей журнала ровно равны прежним петлям ───────────────


def test_journal_readers_match_the_old_loops(monkeypatch, tmp_path):
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", _rows_forms())
    assert ledger_raw_rows() == _old_read(), "сырой взгляд снимка разошёлся с _read_ledger()"
    for order_id in _query_ids():
        assert ledger_price_known(order_id) == _old_price_known(order_id), (
            f"известность цены {order_id!r} разошлась со старой петлёй"
        )
        assert ledger_journal_mark(order_id) == _old_journal_mark(order_id), (
            f"метка журнала {order_id!r} разошлась со старой петлёй"
        )
    for price in _query_prices():
        for ignore in ("", "d", "absent", None):
            assert ledger_close_price_taken(price, ignore) == _old_close_price_taken(price, ignore), (
                f"занятость цены close-строки {price!r} ignore={ignore!r} разошлась со старой петлёй"
            )
    assert ledger_unowned_ids() == _old_unowned_ids()
    assert ledger_min_ts() == _old_min_ts() == 2.0


# ── проба 2: фильтр наборов не подмешивается (форма с miss без sent) ─────────


def test_journal_readers_stay_raw_when_sets_hide_a_row(monkeypatch, tmp_path):
    """Номер в order-miss и не в order-sent скрыт из read_ledger_filtered, но
    его close-строка с ценой обязана держать цену занятой: иначе встанет
    вторая заявка на ту же цену. Все читатели читают сырой журнал."""
    rows = _rows_forms() + [
        {"order_id": "m1", "role": "close", "price": "0.9", "ts": 1.0, "executor_id": "ex"},
    ]
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", rows)
    add_miss_order("m1")
    assert not any(str(row.get("order_id") or "") == "m1" for row in read_ledger_filtered()), (
        "форма сломана: наборы не скрыли строку"
    )
    for order_id in ("m1", "a", "absent"):
        assert ledger_price_known(order_id) == _old_price_known(order_id)
        assert ledger_journal_mark(order_id) == _old_journal_mark(order_id)
    assert ledger_close_price_taken("0.9", "") is True, (
        "цена close-строки номера в miss без sent перестала держать цену занятой"
    )
    assert ledger_close_price_taken("0.9", "") == _old_close_price_taken("0.9", "")
    assert ledger_close_price_taken("0.9", "m1") is False
    assert ledger_unowned_ids() == _old_unowned_ids()
    assert ledger_min_ts() == _old_min_ts()
    assert ledger_raw_rows() == _old_read()
    add_sent_order("m1")
    assert ledger_close_price_taken("0.9", "") == _old_close_price_taken("0.9", "")


# ── проба 3: декор тела ровно равен прежнему _price_checkpoint ────────────────


def _body_forms():
    failure_levels = []
    for i in range(50):
        failure_levels.append({
            "id": "L%d" % i,
            "price": "0.08%02d" % i,
            "open_order": {"order_id": "open-%d" % i, "level_id": "L%d" % i, "price": "0.08%02d" % i},
            "close_order": {"order_id": "close-%d" % i, "level_id": "L%d" % i} if i % 2 else {},
            "take_profit_price": "0.09%02d" % i,
        })
    failure = {"version": 1, "type": "grid_executor", "levels": failure_levels,
               "holds": [{"order_id": "hold-x", "price": "0.1"}],
               "close_holds": [{"order_id": "chold-x", "price": "0.2"}],
               "retained_orders": [{"order_id": "ret-x", "price": "0.3"}]}
    bare = {"version": 1, "type": "grid_executor", "levels": [], "holds": []}
    slots = {"version": 1, "type": "grid_executor", "levels": [
        {"id": "L0", "price": "0.5", "open_order": {"order_id": "o0", "level_id": "L0"}, "close_order": {}},
        {"id": "L1", "open_order": {"order_id": "o1", "order": {"price": "0.7"}}, "close_order": {"order_id": "c1"}},
        {"id": "L2", "open_order": {}, "close_order": {}},
        "not-a-dict",
    ], "holds": [{"order_id": "o0", "price": "0.5"}, "not-a-dict"]}
    return [failure, bare, slots]


@pytest.mark.parametrize("body", _body_forms())
def test_decorated_body_matches_the_old_price_checkpoint(body):
    text = json.dumps(body, sort_keys=True)
    assert checkpoint_decorated(text) == _old_decorated_text(text), (
        "декор кэша разошёлся со старым _price_checkpoint(parse_checkpoint(text)) из базы"
    )
    assert checkpoint_decorated(body) == _old_decorated(body), (
        "декор от своего тела разошёлся со старым _price_checkpoint из базы"
    )
    assert checkpoint_decorated("") == _old_decorated_text("")
    assert checkpoint_decorated("{not json") == _old_decorated_text("{not json")


# ── проба 4: декор один на текст и чистится вместе с мастером ─────────────────


def test_decorated_is_cached_per_text_and_pruned(monkeypatch, tmp_path):
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", [])
    text = json.dumps(_body_forms()[0], sort_keys=True)
    first = checkpoint_decorated(text)
    stats = checkpoint_cache_stats()["copies"]
    for _ in range(50):
        assert checkpoint_decorated(text) is first
    assert checkpoint_cache_stats()["copies"] == stats, "декор пересобирается на каждое обращение"
    assert ledger_mod._CHECKPOINT_DECORATED, "кэш декора пуст"
    checkpoint_cache_prune(["other-text"])
    assert not ledger_mod._CHECKPOINT_DECORATED, "кэш декора пережил чистку срока жизни"
    assert not ledger_mod._CHECKPOINT_CACHE, "мастер-кэш пережил чистку срока жизни"
    fresh = json.dumps(_body_forms()[1], sort_keys=True)
    assert checkpoint_decorated(fresh) is not first, "новый текст не даёт новый декор"
    checkpoint_cache_prune([fresh])
    assert len(ledger_mod._CHECKPOINT_DECORATED) == 1


# ── проба 5: копия только когда фильтр что-то выкинул ─────────────────────────


class _Service:
    pass


def _parsed_service():
    service = _bare_service([])
    return service


def test_parsed_without_dead_copies_only_when_filtering():
    service = _parsed_service()
    body = _body_forms()[2]
    fresh = {"opens": [], "finished": []}
    assert service._lists_ready(fresh) is False
    assert service._parsed_without_dead(body, fresh) is body, "без готовых списков тело должно отдаваться как есть"

    fresh = {"opens": [], "finished": [], "opens_read": True, "opens_done": True, "finished_read": True, "finished_done": True}
    out = service._parsed_without_dead(body, fresh)
    assert out == _old_parsed_without_dead(service, body, fresh), "ответ фильтра разошёлся с прежним"
    assert out is body, "без мёртвых номеров тело копировать нельзя"

    dead = {"opens": [], "finished": [{"text": "o0", "finish_as": "cancelled"}],
            "opens_read": True, "opens_done": True, "finished_read": True, "finished_done": True}
    out = service._parsed_without_dead(body, dead)
    assert out == _old_parsed_without_dead(service, body, dead), "фильтр мёртвых разошёлся с прежним"
    assert out is not body, "выкинутые номера требуют копии"
    assert body["levels"][0]["open_order"], "правка копии осталась в общем теле"


# ── проба 6: список цен колонки равен прежнему вложенному пересчёту ───────────


@asynccontextmanager
async def _session():
    yield SimpleNamespace()


def _column_service(monkeypatch, rows):
    class _Rows:
        async def get_checkpoint_rows(self):
            return rows

    monkeypatch.setattr(mod, "ExecutorRepository", lambda session: _Rows())
    service = ExecutorService.__new__(ExecutorService)
    service.db_manager = SimpleNamespace(get_session_context=lambda: _session())
    service.__dict__["_active_executors"] = {}
    service.__dict__["_executor_metadata"] = {}
    return service


def _column_rows():
    rows = []
    for index, body in enumerate(_body_forms()):
        text = json.dumps(body, sort_keys=True)
        rows.append({
            "executor_type": "grid_executor",
            "account_name": "main",
            "connector_name": "okx_perpetual_demo",
            "trading_pair": "DOGE-USDT",
            "controller_id": "main",
            "created_at": None,
            "config": "{}",
            "checkpoint": text,
            "executor_id": "grid-%d" % index,
            "price_checkpoint": checkpoint_decorated(text),
        })
    return rows


async def test_column_prices_matches_the_old_nested_pass(monkeypatch, tmp_path):
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", [])
    rows = _column_rows()
    service = _column_service(monkeypatch, rows)
    fresh = await service._column_prices("DOGE-USDT")
    assert fresh == _old_column_prices(service, rows, "DOGE-USDT"), (
        "hoist released изменил список цен колонки"
    )
    assert fresh, "форма пустая: сверка ничего не держит"


# ── проба 7: читающий путь не делает копий тела и не читает файл заново ───────


def test_read_path_makes_no_body_copies(monkeypatch, tmp_path):
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", _rows_forms())
    body = _body_forms()[0]
    text = json.dumps(body, sort_keys=True)
    rows = [{"executor_id": "grid-a", "checkpoint": text, "trading_pair": "DOGE-USDT"}]
    service = _bare_service(rows)
    checkpoint_decorated(text)
    service._parsed_without_dead(checkpoint_view(text), {})
    read_ledger_filtered()
    ledger_price_known("a")
    copies = checkpoint_cache_stats()["copies"]
    reads = ledger_cache_stats()["reads"]
    for _ in range(200):
        service._partial_parsed("grid-a")
        service._close_price_known("close-3")
        ledger_price_known("a")
        ledger_journal_mark("a")
        ledger_close_price_taken("0.3")
        service._parsed_without_dead(service._partial_parsed("grid-a"), {})
        checkpoint_decorated(text)
    assert checkpoint_cache_stats()["copies"] == copies, (
        "читающий путь снова копирует тело на каждое обращение"
    )
    assert ledger_cache_stats()["reads"] == reads, "читающий путь снова читает файл журнала"


# ── проба 8: взгляд общий, копию берёт правящий читатель ──────────────────────


def test_view_is_shared_and_copy_paths_stay_isolated(monkeypatch, tmp_path):
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", [])
    body = _body_forms()[1]
    text = json.dumps(body, sort_keys=True)
    rows = [{"executor_id": "grid-a", "checkpoint": text, "trading_pair": "DOGE-USDT"}]
    service = _bare_service(rows)
    first = service._partial_parsed("grid-a")
    second = service._partial_parsed("grid-a")
    assert first is second is checkpoint_view(text), "взгляд перестал быть общим"
    copy = parse_checkpoint_cached(text)
    assert copy is not first
    copy["levels"].append({"id": "L9"})
    assert service._partial_parsed("grid-a") == first, "правка копии видна следующему чтению"
    assert first == body


# ── проба 9: обход читающих вызовов не меняет мастер и декор ──────────────────


def test_reader_walk_leaves_master_and_decorated_unchanged(monkeypatch, tmp_path):
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", _rows_forms())
    body = _body_forms()[0]
    text = json.dumps(body, sort_keys=True)
    rows = [{"executor_id": "grid-a", "checkpoint": text, "trading_pair": "DOGE-USDT"}]
    service = _bare_service(rows)
    master_before = json.dumps(checkpoint_master(text), sort_keys=True)
    decorated_before = json.dumps(checkpoint_decorated(text), sort_keys=True)

    parsed = service._partial_parsed("grid-a")
    service._own_ids("grid-a")
    service._own_level_id("grid-a", "open-1")
    service._recorded_close_id("grid-a", "L1")
    service._partial_close_ids("grid-a")
    service._partial_hold_witness("grid-a")
    service._partial_column_pending("grid-a")
    service._partial_forbids("grid-a")
    service._close_price_known("chold-x")
    service._parsed_without_dead(parsed, {})
    service._partial_gone_blocked("grid-a", "close-1")

    assert json.dumps(checkpoint_master(text), sort_keys=True) == master_before, (
        "читающий вызов поправил общий мастер"
    )
    assert json.dumps(checkpoint_decorated(text), sort_keys=True) == decorated_before, (
        "читающий вызов поправил общий декор"
    )


# ── проба 10: запись мимо кэша видна тому же шагу ─────────────────────────────


def test_writer_bypass_sees_fresh_journal_answers(monkeypatch, tmp_path):
    path = tmp_path / "data" / "order-ledger.jsonl"
    _boot(monkeypatch, path, [])
    assert ledger_price_known("n1") is False
    monkeypatch.setattr(ledger_mod, "_ledger_file_key", lambda _path: ("frozen",))
    assert append_durable_row({"order_id": "n1", "price": "0.1", "executor_id": "ex", "ts": 1.0})
    assert ledger_price_known("n1") is True, "записанная цена не видна следующему чтению"
    assert ledger_journal_mark("n1") == ("owned", "ex")
    assert append_durable_row({
        "order_id": "m2", "role": "close", "price": "0.9", "executor_id": "ex", "ts": 2.0,
    })
    assert ledger_close_price_taken("0.9", "") is True, "записанная close-цена не видна следующему чтению"
    assert ledger_min_ts() == 1.0
    assert ledger_unowned_ids() == set()


# ── проба 11: запись тела в базу даёт новый декор ─────────────────────────────


async def test_checkpoint_write_gives_a_new_decorated(monkeypatch, tmp_path):
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", [])
    rows = _column_rows()
    service = _column_service(monkeypatch, rows)
    first = await service._column_prices("DOGE-USDT")
    body = json.loads(rows[0]["checkpoint"])
    body["holds"].append({"order_id": "new-hold", "price": "0.77"})
    rows[0]["checkpoint"] = json.dumps(body, sort_keys=True)
    rows[0]["price_checkpoint"] = checkpoint_decorated(rows[0]["checkpoint"])
    service._set_hold_rows(rows)
    second = await service._column_prices("DOGE-USDT")
    assert first != second, "записанное тело не изменило список цен"
    assert ("new-hold", "0.77") in second, (
        "цена записанного держателя не видна следующему чтению"
    )


# ── проба 12: сбой чтения отвечает как прежде ─────────────────────────────────


def test_read_failure_answers_are_the_old_ones(monkeypatch, tmp_path):
    path = tmp_path / "data" / "order-ledger.jsonl"
    _boot(monkeypatch, path, _rows_forms())
    body = _body_forms()[0]
    text = json.dumps(body, sort_keys=True)
    rows = [{"executor_id": "grid-a", "checkpoint": text, "trading_pair": "DOGE-USDT"}]
    service = _bare_service(rows)
    assert service._close_price_known("chold-x") is True, (
        "форма сломана: тело держит известную цену закрытия"
    )
    monkeypatch.setattr(
        mod, "owed_records", lambda: [{"trading_pair": "DOGE-USDT", "ts": 1.0}]
    )
    path.unlink()
    path.mkdir()
    with pytest.raises(_LedgerReadError):
        ledger_price_known("a")
    assert service._close_price_known("chold-x") is False, (
        "сбой чтения журнала начал сверяться с памятью: раньше был немедленный False"
    )
    assert ledger_mod._LEDGER_SNAPSHOT is None, "снимок при сбое чтения не кэшируется"
    assert service._ledger_close_price_taken("0.3") is True
    assert service._journal_mark("a") == ("unread", None)
    assert service._raw_unowned_ids() == set()
    assert service._oldest_ts({"pair": "DOGE-USDT"}) is None, (
        "сбой чтения журнала не должен отдавать min из owed_records"
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(tmp_path / "none" / "order-ledger.jsonl"))
    reset_ledger_process_state()
    assert ledger_price_known("a") is False
    assert ledger_journal_mark("a") == ("absent", None)
    assert ledger_unowned_ids() == set()
    assert ledger_min_ts() is None
    assert ledger_raw_rows() == []


# ── проба 13: внешняя правка файла подхватывается ─────────────────────────────


def test_external_file_change_is_picked_up(monkeypatch, tmp_path):
    path = tmp_path / "data" / "order-ledger.jsonl"
    _boot(monkeypatch, path, _rows_forms())
    assert ledger_price_known("late") is False
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"order_id": "late", "price": "7.7", "role": "close", "ts": 9.5}) + "\n")
    assert ledger_price_known("late") is True, "внешняя правка журнала не подхвачена"
    assert ledger_close_price_taken("7.7", "") is True
    assert ledger_min_ts() == 2.0


# ── проба 14: занятые цены stored/journal_prices равны прежней формуле ─────────


def _price_rows_forms():
    return [
        {"order_id": "a", "price": "0.1", "executor_id": "ex", "level_id": "L1", "ts": 5.0},
        {"order_id": "a", "price": "0.11", "executor_id": "ex", "level_id": "L1", "ts": 6.0},
        {"order_id": "b", "take_profit_price": "0.21", "executor_id": "ex", "level_id": "L2"},
        {"order_id": "c", "price": "0.3", "executor_id": "other", "level_id": "L1"},
        {"order_id": "d", "price": "0.4"},
        {"order_id": "", "price": "0.5"},
        {"order_id": "e", "price": "0.6", "executor_id": "ex", "level_id": "L3", "ts": 1.0},
    ]


def test_occupied_price_lists_match_the_old_pass(monkeypatch, tmp_path):
    """Списки занятых цен _durable_open до и после равны: цена номера не
    выпадает из занятых и не встаёт вторая заявка на ту же цену."""
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", _price_rows_forms())
    service = _bare_service([])
    raw = _old_read()
    old_stored, old_ts = _old_collect_stored(raw, "ex")
    new_stored, new_ts = _new_collect_stored(service, "ex")
    assert new_stored == old_stored, "список stored разошёлся с прежней формулой"
    assert new_ts == old_ts
    assert _new_collect_journal_prices(service) == _old_collect_journal_prices(raw), (
        "список journal_prices разошёлся с прежней формулой"
    )
    assert ("c", "0.3") in new_stored and ("d", "0.4") in new_stored, (
        "форма сломана: занятые цены не собраны"
    )
    assert ("a", "0.1") not in new_stored, (
        "форма сломана: удержанный номер должен исключаться из списка занятых"
    )


# ── проба 15: ответ _unpriced_close_blocks равен прежнему ─────────────────────


class _Other:
    class config:
        trading_pair = "DOGE-USDT"

    _close_ban_ids = {"close-ban"}
    _close_holds = [{"order_id": "chold-x"}, {"order_id": "close-a"}]
    _close_order = SimpleNamespace(order_id="close-live")


class _Self:
    class config:
        trading_pair = "DOGE-USDT"


def test_unpriced_close_blocks_matches_the_old(monkeypatch, tmp_path):
    """«Заявка без хозяина»: тормоз незакрытой цены тот же, что и до правки."""
    rows = [{"executor_id": "grid-a", "checkpoint": json.dumps(_body_forms()[0]), "trading_pair": "DOGE-USDT"}]
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", [
        {"order_id": "close-live", "price": "0.9", "role": "close", "executor_id": "other"},
        {"order_id": "close-a", "price": "0.91", "role": "close", "executor_id": "other"},
        {"order_id": "close-ban", "price": "0.92", "role": "close", "executor_id": "other"},
    ])
    service = _bare_service(rows)
    service.__dict__["_active_executors"] = {"other": _Other()}
    assert service._unpriced_close_blocks(_Self()) is False
    assert _old_unpriced_close_blocks(service, _Self()) is False

    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", [])
    service = _bare_service(rows)
    service.__dict__["_active_executors"] = {"other": _Other()}
    assert service._unpriced_close_blocks(_Self()) is True, (
        "тормоз незакрытой цены пропал: чужая заявка останется без хозяина"
    )
    assert _old_unpriced_close_blocks(service, _Self()) is True


# ── проба 16: гейт «column prices unread» на битом тексте в базе ──────────────


async def test_column_gate_closes_on_a_broken_text(monkeypatch, tmp_path):
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", [])
    rows = _column_rows()
    rows.append({
        "executor_type": "grid_executor", "account_name": "main",
        "connector_name": "okx_perpetual_demo", "trading_pair": "DOGE-USDT",
        "controller_id": "main", "created_at": None, "config": "{}",
        "checkpoint": "{not json", "executor_id": "grid-broken",
    })
    service = _column_service(monkeypatch, rows)
    assert await service._column_prices("DOGE-USDT") is None, (
        "битый текст в базе перестал закрывать гейт column prices unread"
    )


# ── проба 17: обход sweep при сбое чтения пустой ─────────────────────────────


async def test_sweep_pass_is_empty_on_read_failure(monkeypatch, tmp_path):
    path = tmp_path / "data" / "order-ledger.jsonl"
    _boot(monkeypatch, path, [
        {"order_id": "a", "price": "0.1", "executor_id": "ex", "ts": 5.0},
        {"order_id": "b", "price": "0.2", "executor_id": "ex", "ts": 6.0},
    ])
    service = _bare_service([])
    service.db_manager = None
    committed = []

    async def _fake_commit(self, owner, order_id, price):
        committed.append((owner, order_id, price))
        return True

    monkeypatch.setattr(ExecutorService, "_commit_price_hold", _fake_commit)
    monkeypatch.setattr(mod, "recover_folder_debt", lambda: None)
    await service._sweep_unproven_orders()
    assert committed, "форма сломана: обход sweep ничего не собрал"

    committed.clear()
    path.unlink()
    path.mkdir()
    await service._sweep_unproven_orders()
    assert committed == [], "обход sweep при сбое чтения журнала не пустой"


# ── проба 18: смешанные типы ts не роняют сборку снимка ────────────────────────


def test_mixed_ts_types_do_not_break_the_snapshot(monkeypatch, tmp_path):
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", [
        {"order_id": "a", "price": "0.1", "executor_id": "ex", "ts": 3.0},
        {"order_id": "b", "price": "0.2", "ts": "later"},
        {"order_id": "c", "price": "0.3"},
    ])
    assert ledger_price_known("a") is True, "снимок не собрался из-за смешанных типов ts"
    assert ledger_journal_mark("c") == ("unowned", None)
    assert ledger_unowned_ids() == {"b", "c"}, "чужие номера снова без хозяина из-за битого ts"
    assert ledger_close_price_taken("0.2") is False
    assert ledger_min_ts() in (3.0, "later")
    service = _bare_service([])
    monkeypatch.setattr(mod, "owed_records", lambda: [])
    assert service._oldest_ts({"pair": "DOGE-USDT"}) == ledger_min_ts()
