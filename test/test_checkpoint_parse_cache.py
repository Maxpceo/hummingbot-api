"""Пробы кэша расшифровки памяти сетки (план checkpoint-parse-cache).

Держат три вещи: разбор один на текст и шаг быстрый; набор каждого читателя
повторяет его прежнее чтение тела один в один; кэш не открывает дыру второй
заявки на ту же цену.
"""

import json
import time

import pytest

from test.test_executor_checkpoint import (
    _Gate,
    _Kept,
    _Repo,
    _grid,
    _level,
    _load_executor_service,
    _running_row,
    _service,
    reset_ledger_process_state,
)

mod = _load_executor_service()
ExecutorService = mod.ExecutorService

from database.repositories.executor_repository import _price_checkpoint  # noqa: E402

from utils.executor_checkpoint import (  # noqa: E402
    checkpoint_cache_reset_stats,
    checkpoint_cache_stats,
    parse_checkpoint,
    parse_checkpoint_cached,
    partial_level_released,
    partial_released_ids,
    saved_order_ids,
)


# ── прежнее чтение тела: то, что читатели делали до правки ─────────────────


def _old_unread(service, rows, pair=None):
    for row in rows:
        if pair and row.get("trading_pair") not in (None, pair):
            continue
        text = row.get("checkpoint") or ""
        if text and parse_checkpoint(text) is None:
            return True
    return False


def _old_order_owner(service, rows, order_id):
    if _old_unread(service, rows):
        return None
    wanted = str(order_id)
    for row in rows:
        text = row.get("checkpoint") or ""
        parsed = parse_checkpoint(text)
        if text and parsed is None:
            continue
        ids = set(saved_order_ids(parsed))
        for hold in (parsed or {}).get("holds") or []:
            if hold.get("order_id"):
                ids.add(str(hold.get("order_id")))
        for level in (parsed or {}).get("levels") or []:
            if not isinstance(level, dict):
                continue
            for key in ("open_order", "close_order"):
                slot = level.get(key) or {}
                if isinstance(slot, dict) and slot.get("order_id"):
                    ids.add(str(slot.get("order_id")))
        if wanted not in ids:
            continue
        identity = service._row_identity(row)
        if identity:
            return identity
    return None


def _old_seated_open_id(rows, text, pair):
    if not text:
        return False
    for row in rows:
        if row.get("trading_pair") not in (None, "", pair):
            continue
        parsed = row.get("checkpoint")
        if not isinstance(parsed, dict):
            parsed = parse_checkpoint(row.get("checkpoint"))
        if not isinstance(parsed, dict):
            continue
        for hold in parsed.get("holds") or []:
            if isinstance(hold, dict) and str(hold.get("order_id") or "") == text:
                return True
        for level in parsed.get("levels") or []:
            if not isinstance(level, dict):
                continue
            slot = level.get("open_order") or {}
            if isinstance(slot, dict) and str(slot.get("order_id") or "") == text:
                return True
    return False


def _old_close_ids(service, rows):
    found = service._ram_close_ids()
    for row in rows:
        parsed = row.get("checkpoint")
        parsed = parsed if isinstance(parsed, dict) else parse_checkpoint(row.get("checkpoint"))
        if isinstance(parsed, dict):
            found.update(service._checkpoint_close_ids(parsed, past=True, ends=True))
    return found


def _old_partial_text_protected(service, rows, text, partial_rows):
    if not text:
        return False
    if partial_rows is None:
        return True
    released = set()
    for row in rows:
        released.update(partial_released_ids(parse_checkpoint(row.get("checkpoint"))))
    if text in released:
        return False
    for row in partial_rows:
        if str(row.get("order_id") or "") == text and row.get("level_id"):
            return True
    for row in rows:
        parsed = parse_checkpoint(row.get("checkpoint"))
        for level in (parsed or {}).get("levels") or []:
            if partial_level_released(parsed, str(level.get("id"))):
                continue
            ids = [
                str((level.get("open_order") or {}).get("order_id") or ""),
                str((level.get("close_order") or {}).get("order_id") or ""),
            ]
            ids.extend(
                str(item.get("order_id") or "")
                for item in level.get("partial_closes") or []
                if isinstance(item, dict)
            )
            if text in ids:
                return True
    return False


def _old_close_price_known(rows, order_id):
    wanted = str(order_id)
    for row in rows:
        parsed = parse_checkpoint(row.get("checkpoint"))
        for level in (parsed or {}).get("levels") or []:
            close = (level or {}).get("close_order") or {}
            if str(close.get("order_id") or "") == wanted and close.get("price") not in (None, ""):
                return True
        for item in (parsed or {}).get("close_holds") or []:
            if isinstance(item, dict) and str(item.get("order_id") or "") == wanted:
                if item.get("price") not in (None, ""):
                    return True
    return False


def _old_ban_holds(rows, pair):
    items = []
    for row in rows:
        if row.get("trading_pair") not in (None, "", pair):
            continue
        parsed = row.get("price_checkpoint")
        if not isinstance(parsed, dict):
            parsed = parse_checkpoint(row.get("checkpoint"))
        for hold in (parsed or {}).get("holds") or []:
            if hold.get("price") not in (None, ""):
                items.append((str(hold.get("order_id") or ""), hold.get("price")))
    return items


def _old_released_texts(rows, pair):
    found = set()
    for row in rows:
        if row.get("trading_pair") not in (None, "", pair):
            continue
        found.update(
            str(item) for item in partial_released_ids(parse_checkpoint(row.get("checkpoint")))
            if item and not str(item).startswith("level:")
        )
    return found


def _old_column_ids(rows, pair):
    found = set()
    for row in rows:
        if row.get("trading_pair") != pair:
            continue
        text = row.get("checkpoint") or ""
        parsed = parse_checkpoint(text)
        if text and parsed is None:
            return None
        found.update(saved_order_ids(parsed))
        for hold in (parsed or {}).get("holds") or []:
            if hold.get("order_id"):
                found.add(hold["order_id"])
    return found


# ── каркас ─────────────────────────────────────────────────────────────────


def _close_level(level_id, price, close_id, close_price):
    level = _level(level_id, price)
    level["close_order"] = {"order_id": close_id, "level_id": level_id, "price": close_price}
    return level


def _dead_row(row_executor_id, checkpoint, pair="INJ-USDT"):
    """Погашенная строка: executor_id скрыт, остаётся row_executor_id."""
    return {
        "row_executor_id": row_executor_id,
        "executor_type": "grid_executor",
        "account_name": "main",
        "connector_name": "gate_io",
        "trading_pair": pair,
        "controller_id": "main",
        "config": "{}",
        "checkpoint": checkpoint,
        "status": "TERMINATED",
    }


def _bare_service(rows):
    service = ExecutorService.__new__(ExecutorService)
    service.__dict__["_active_executors"] = {}
    service.__dict__["_executor_metadata"] = {}
    service._set_hold_rows(rows)
    return service


def _fake_executor(pair="INJ-USDT"):
    class _Cfg:
        trading_pair = pair

    class _Exec:
        config = _Cfg()
        grid_levels = []

    return _Exec()


def _heavy_body(levels, retained=1500):
    body = _grid(levels)
    body["retained_orders"] = [
        {
            "order": {
                "client_order_id": f"t-HBOTcid{i:06d}",
                "exchange_order_id": str(3900000000000000000 + i),
                "amount": "0.38",
                "executed_amount_base": "0",
                "creation_timestamp": 1791539000.0 + i,
                "last_state": "1",
            },
            "level_id": None,
            "filled": False,
        }
        for i in range(retained)
    ]
    return body


# ── 1. разбор один на текст, шаг быстрый ───────────────────────────────────


def test_one_parse_per_text_and_fast_reads(monkeypatch, tmp_path):
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(tmp_path / "data" / "order-ledger.jsonl"))
    reset_ledger_process_state()
    rows = [
        _dead_row(f"g{i}", json.dumps(_heavy_body([_level("L0", str(100 + i))], retained=1500)))
        for i in range(8)
    ]
    rows.append(_dead_row("g8", json.dumps(_heavy_body([_level("L0", "108")], retained=1501))))
    service = _bare_service(rows)
    from utils.executor_checkpoint import checkpoint_cache_prune

    checkpoint_cache_prune([])
    checkpoint_cache_reset_stats()
    service._set_hold_rows(rows)

    started = time.monotonic()
    for _ in range(20):
        service._close_cancel_ids()
        service._checkpoint_order_owner("t-HBOTcid")
        service._seated_open_id("t-HBOTcid", "INJ-USDT")
        service._checkpoint_unread()
        service._partial_text_protected("t-HBOTcid")
        service._released_texts("INJ-USDT")
    took = time.monotonic() - started

    stats = checkpoint_cache_stats()
    assert stats["parses"] == len(rows), "разбор должен быть один на каждый текст"
    assert took < 1.0, f"чтение наборов держало {took:.2f}s — шаг остался медленным"


# ── 2. две погашенные строки держат обе цены закрытия ──────────────────────


def test_two_dead_rows_keep_both_close_prices():
    body_a = _grid([_close_level("L0", "100", "close-a", "101")])
    body_b = _grid([_close_level("L0", "100", "close-b", "102")])
    rows = [
        _dead_row("dead-a", json.dumps(body_a)),
        _dead_row("dead-b", json.dumps(body_b)),
    ]
    service = _bare_service(rows)
    prices = [str(item) for item in service._occupied_close_prices(_fake_executor())]
    assert "101" in prices and "102" in prices, (
        "погашенные строки склеились: одна цена закрытия потерялась"
    )
    assert service._close_cancel_ids() >= {"close-a", "close-b"}


# ── 3. только что записанное тело видно следующему чтению ──────────────────


async def test_fresh_write_is_seen_by_the_next_price_read(monkeypatch, tmp_path):
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(tmp_path / "data" / "order-ledger.jsonl"))
    reset_ledger_process_state()
    body = _grid([_close_level("L0", "100", "close-old", "101")])
    row = _running_row("grid-a", checkpoint=json.dumps(body))
    repo = _Repo([row])

    class _RepoFactory:
        def __init__(self, session):
            self.__dict__.update(repo.__dict__)

        async def get_executor_by_id(self, executor_id):
            return await repo.get_executor_by_id(executor_id)

        async def replace_checkpoint_if_unchanged(self, executor_id, seen, body_text):
            return await repo.replace_checkpoint_if_unchanged(executor_id, seen, body_text)

        async def get_checkpoint_rows(self):
            return await repo.get_checkpoint_rows()

    class _DB:
        def get_session_context(self):
            class _Ctx:
                async def __aenter__(self_inner):
                    return None

                async def __aexit__(self_inner, *args):
                    return False

            return _Ctx()

    service = _bare_service([row])
    service.db_manager = _DB()
    import services.executor_service as es

    monkeypatch.setattr(es, "ExecutorRepository", _RepoFactory)

    def mutate(new_body):
        level = (new_body.get("levels") or [])[0]
        level["close_order"] = {"order_id": "close-new", "level_id": "L0", "price": "103"}
        return new_body

    def visible(parsed):
        close = ((parsed or {}).get("levels") or [{}])[0].get("close_order") or {}
        return close.get("order_id") == "close-new"

    assert await service._partial_cas("grid-a", mutate, visible) is True
    prices = [str(item) for item in service._occupied_close_prices(_fake_executor())]
    assert "103" in prices, "только что записанная цена закрытия не занята — встанет вторая заявка"


# ── 4. правка тела из кэша не остаётся в кэше ──────────────────────────────


def test_cached_body_copy_is_private():
    body = _grid([_close_level("L0", "100", "close-a", "101")])
    text = json.dumps(body)
    taken = parse_checkpoint_cached(text)
    taken["levels"][0]["close_order"] = None
    taken["levels"][0]["take_profit_price"] = "999"
    again = parse_checkpoint_cached(text)
    assert (again["levels"][0]["close_order"] or {}).get("order_id") == "close-a"
    assert again["levels"][0].get("take_profit_price") != "999"


# ── 5. сверка читателей со старым чтением тела ─────────────────────────────


def test_readers_match_the_old_body_reading():
    body = _grid([
        _level("L0", "100", open_order={"order_id": "open-a", "level_id": "L0", "price": "100"}),
        _close_level("L1", "101", "close-a", "102"),
    ])
    body["holds"] = [{"order_id": "hold-a", "price": "99"}]
    body["partial_ends"] = {"L0": {"order_ids": ["ended-a"]}}
    body["partial_closes"] = []
    rows = [
        _dead_row("dead-a", json.dumps(body)),
        _dead_row("dead-b", json.dumps(_grid([_level("L2", "200")]))),
    ]
    service = _bare_service(rows)
    service._partial_rows = lambda: []

    for text in ("open-a", "hold-a", "close-a", "ended-a", "unknown"):
        assert service._checkpoint_order_owner(text) == _old_order_owner(service, rows, text), (
            f"владелец номера {text} разошёлся со старым чтением"
        )
        assert service._seated_open_id(text, "INJ-USDT") == _old_seated_open_id(rows, text, "INJ-USDT"), (
            f"сидение номера {text} разошлось со старым чтением"
        )
        assert service._partial_text_protected(text) == _old_partial_text_protected(
            service, rows, text, []
        ), f"защита текста {text} разошлась со старым чтением"
    assert service._close_cancel_ids() == _old_close_ids(service, rows)
    assert service._checkpoint_unread() == _old_unread(service, rows)


async def test_more_readers_match_the_old_body_reading(monkeypatch, tmp_path):
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(tmp_path / "data" / "order-ledger.jsonl"))
    reset_ledger_process_state()
    level = _close_level("L0", "100", "close-a", "102")
    level["take_profit_price"] = "103"
    body = _grid([level])
    body["holds"] = [{"order_id": "hold-a", "price": "99"}]
    body["close_holds"] = [{"order_id": "hold-close", "price": "105"}]
    body["partial_ends"] = {"L0": {"order_ids": ["ended-a"]}}
    rows = [
        _dead_row("dead-a", json.dumps(body)),
        _dead_row("dead-b", json.dumps(_grid([_level("L2", "200")]))),
    ]
    for row in rows:
        row["price_checkpoint"] = _price_checkpoint(parse_checkpoint(row.get("checkpoint")))
    service = _bare_service(rows)
    service._partial_rows = lambda: []

    for text in ("close-a", "hold-close", "hold-a", "ended-a", "absent"):
        assert service._close_price_known(text) == _old_close_price_known(rows, text), (
            f"известность цены закрытия {text} разошлась со старым чтением"
        )
    assert service._released_texts("INJ-USDT") == _old_released_texts(rows, "INJ-USDT")
    assert await service._column_order_ids("INJ-USDT") == _old_column_ids(rows, "INJ-USDT")
    fresh = {"opens": [], "finished": []}
    assert sorted(service._ban_pairs("INJ-USDT", fresh)) == sorted(_old_ban_holds(rows, "INJ-USDT"))
    assert (
        sorted(str(item) for item in service._occupied_close_prices(_fake_executor()))
        == sorted(str(item) for item in _old_occupied_close_prices(service, rows))
    )


def _old_occupied_close_prices(service, rows):
    found = []
    released = set()
    for row in rows:
        released.update(partial_released_ids(parse_checkpoint(row.get("checkpoint"))))
    for row in service._partial_rows() or []:
        order_id = str(row.get("order_id") or "")
        if not order_id or order_id in released:
            continue
        if row.get("take_profit_price") not in (None, "") and order_id not in released:
            if any(
                str((level.get("close_order") or {}).get("order_id") or "") == order_id
                for parsed_row in rows
                for level in (parse_checkpoint(parsed_row.get("checkpoint")) or {}).get("levels") or []
            ):
                found.append(row.get("take_profit_price"))
    for parsed_row in rows:
        parsed = parse_checkpoint(parsed_row.get("checkpoint"))
        for level in (parsed or {}).get("levels") or []:
            close = (level or {}).get("close_order") or {}
            close_id = str(close.get("order_id") or "")
            if close_id and close_id not in released and close.get("price") not in (None, ""):
                found.append(close.get("price"))
    return found


# ── 6. номер из журнала закрытий не снимается ──────────────────────────────


def test_ended_id_still_on_the_book_is_not_cancelled():
    body = _grid([_close_level("L0", "100", "close-a", "104")])
    body["partial_ends"] = {"L0": {"order_ids": ["ended-a"]}}
    body["close_holds"] = [{"order_id": "hold-close", "price": "105"}]
    rows = [_dead_row("dead-a", json.dumps(body))]
    service = _bare_service(rows)
    service._partial_rows = lambda: [
        {"order_id": "ended-a", "price": "100", "role": "close"},
        {"order_id": "hold-close", "price": "105", "role": "close"},
    ]
    assert "ended-a" in service._close_cancel_ids(), (
        "номер из partial_ends начали снимать — его тик освободится и встанет вторая заявка"
    )
    assert "hold-close" in service._close_cancel_ids()
    prices = [str(item) for item in service._occupied_close_prices(_fake_executor())]
    assert "104" in prices, "цена close-слота перестала быть занятой"


# ── 7. цена сидящего заявления блокирует второе открытие ───────────────────


def test_seated_slot_price_still_bans_the_price():
    from database.repositories.executor_repository import _price_checkpoint

    level = _level("L0", "100", open_order={"order_id": "open-a", "level_id": "L0", "price": "100"})
    body = _grid([level])
    row = _dead_row("dead-a", json.dumps(body))
    row["price_checkpoint"] = _price_checkpoint(body)
    service = _bare_service([row])
    pairs = service._ban_pairs("INJ-USDT", {"opens": [], "finished": []})
    assert ("open-a", "100") in [(str(a), str(b)) for a, b in pairs], (
        "цена сидящего заявления перестала попадать в запрет"
    )


# ── 8. цена close-номера узнаётся по close_holds, а не по holds ──────────────────


def test_close_price_known_reads_close_holds_and_not_holds():
    """Ровно два случая из вердикта код-ревью: holds с ценой — это запрет
    цены, close_holds с ценой — сохранённая цена закрытия."""
    body = _grid([_level("L0", "100")])
    body["holds"] = [{"order_id": "x-ban", "price": "105"}]
    body["close_holds"] = [{"order_id": "x-close", "price": "106"}]
    rows = [_dead_row("dead-a", json.dumps(body))]
    service = _bare_service(rows)
    assert service._close_price_known("x-close") is True, (
        "цену close-номера из close_holds перестали узнавать — закроются все отправки на паре"
    )
    assert service._close_price_known("x-ban") is False, (
        "holds с ценой начали считать сохранённой ценой — отпустится тормоз и встанет вторая заявка"
    )


# ── 9. снятый номер не держит цену в запрете ─────────────────────────────────────


def test_unsent_order_does_not_ban_the_price(monkeypatch, tmp_path):
    from utils.executor_checkpoint import add_miss_order, order_is_unsent

    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(tmp_path / "data" / "order-ledger.jsonl"))
    (tmp_path / "data").mkdir()
    reset_ledger_process_state()
    level = _level("L0", "100", open_order={"order_id": "open-unsent", "level_id": "L0", "price": "100"})
    body = _grid([level])
    row = _dead_row("dead-a", json.dumps(body))
    row["price_checkpoint"] = _price_checkpoint(parse_checkpoint(row.get("checkpoint")))
    service = _bare_service([row])

    assert add_miss_order("open-unsent") is True
    assert order_is_unsent("open-unsent") is True

    pairs = service._ban_pairs("INJ-USDT", {"opens": [], "finished": []})
    assert ("open-unsent", "100") not in [(str(a), str(b)) for a, b in pairs], (
        "номер, не дошедший до книги, снова держит цену в запрете"
    )


# ── кэш не копит старые тела ───────────────────────────────────────────────


def test_cache_prune_drops_old_texts():
    from utils.executor_checkpoint import checkpoint_cache_prune

    first = json.dumps(_grid([_level("L0", "100")]))
    second = json.dumps(_grid([_level("L0", "200")]))
    parse_checkpoint_cached(first)
    parse_checkpoint_cached(second)
    checkpoint_cache_prune([second])
    checkpoint_cache_reset_stats()
    parse_checkpoint_cached(first)
    assert checkpoint_cache_stats()["parses"] == 1, "старый текст остался в кэше"
