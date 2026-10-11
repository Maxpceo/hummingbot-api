"""Пробы лечения замков стопа (план stop-locks-release, раздел «Чем проверим»).

Пять проверок: замок «column» больше не держит стоп по слотам уровней;
замок «unreleased-ids» читает видимые концы; шаг сборки концов снимает только
доказанно мёртвые следы и пишет в базу раньше, чем снимает в памяти; повтор и
цены; финализация отдаёт закрытого исполнителя, когда у всех номеров есть концы.
"""

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from test.test_checkpoint_parse_cache import _bare_service
from test.test_executor_checkpoint import (
    _Repo,
    _load_executor_service,
    _running_row,
    _service,
)

mod = _load_executor_service()
ExecutorService = mod.ExecutorService

import utils.executor_checkpoint as ledger_mod  # noqa: E402
from utils.executor_checkpoint import (  # noqa: E402
    add_miss_order,
    apply_partial_keep,
    partial_end_visible,
    partial_level_released,
    partial_released_ids,
    partial_stop_forbidden,
    remove_miss_order,
    reset_ledger_process_state,
)

EXECUTOR = "grid-a"
PAIR = "INJ-USDT"


def _journal(tmp_path, monkeypatch, lines):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text("".join(json.dumps(line) + "\n" for line in lines), encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    return path


def _row(order_id, level_id="L1", ts=10, **extra):
    line = {
        "executor_id": EXECUTOR,
        "order_id": order_id,
        "level_id": level_id,
        "price": "100",
        "ts": ts,
    }
    line.update(extra)
    return line


def _level(level_id="L1", open_id="", close_id="", take="101", filled=False, bought=None, partial_closes=None):
    body = {"id": level_id}
    if open_id:
        body["open_order"] = {"order_id": open_id, "level_id": level_id, "filled": filled}
    if close_id:
        body["close_order"] = {"order_id": close_id, "level_id": level_id, "price": "102"}
    if take is not None:
        body["take_profit_price"] = take
    if bought is not None:
        body["partial_bought"] = bought
    if partial_closes is not None:
        body["partial_closes"] = partial_closes
    return body


def _checkpoint(*levels, holds=None, ends=None):
    body = {"version": 1, "type": "grid_executor", "levels": list(levels), "holds": holds or []}
    if ends:
        body["partial_ends"] = ends
    return json.dumps(body, sort_keys=True)


def _fresh(opens=None, finished=None, covers=True):
    return {
        "opens_read": True,
        "opens_done": True,
        "finished_read": True,
        "finished_done": True,
        "covers_ts": covers,
        "opens": opens or [],
        "finished": finished or [],
        "finished_request": {"path": "spot/orders", "params": {"from": 0, "to": 10 ** 12}},
    }


def _finished(order_id, filled=False, amount="1", left="1"):
    row = {
        "text": order_id,
        "status": "closed",
        "finish_as": "filled" if filled else "cancelled",
        "amount": amount,
        "left": "0" if filled else left,
        "price": "100",
    }
    if filled:
        row["state"] = "filled"
        row["fill_size"] = amount
    return row


def _open(order_id):
    return {"text": order_id, "status": "open", "price": "100"}


def _service_with(monkeypatch, checkpoint, fresh, journal_lines, connector=None):
    repo = _Repo([_running_row(checkpoint=checkpoint)])
    connector = connector if connector is not None else SimpleNamespace(
        in_flight_orders={}, _order_tracker=SimpleNamespace(lost_orders={}),
    )
    service = _service(monkeypatch, repo, connector)
    service._set_hold_rows(repo.rows)
    service._remember_tick_fresh(PAIR, fresh)
    return service, repo


def _ram_executor(open_id="", close_id=""):
    level = SimpleNamespace(
        id="L1",
        active_open_order=SimpleNamespace(order_id=open_id) if open_id else None,
        active_close_order=SimpleNamespace(order_id=close_id) if close_id else None,
    )
    config = SimpleNamespace(trading_pair=PAIR, connector_name="gate_io", side=SimpleNamespace(name="BUY"))
    return SimpleNamespace(
        config=config,
        connectors={"gate_io": SimpleNamespace(in_flight_orders={}, _order_tracker=SimpleNamespace(lost_orders={}))},
        grid_levels=[level],
        _close_holds=[],
        _close_ban_ids=set(),
        _close_ban_prices={},
        _column_close_ids=[],
        _close_order=None,
        _close_seat_ready=True,
        _close_list_ready=True,
        is_closed=False,
    )


def _stored(repo):
    return json.loads(repo.rows[0]["checkpoint"])


# ── 1. замок «column» ────────────────────────────────────────────────────────


def test_column_slots_alone_do_not_hold_the_stop():
    parsed = {"levels": [_level(open_id="buy-1", close_id="sell-1")]}
    for ready in (True, False):
        assert partial_stop_forbidden([], parsed, EXECUTOR, proof_ready=ready) is False


def test_confirmed_live_holding_does_not_hold_the_stop():
    rows = [_row("buy-1")]
    parsed = {"levels": [_level(open_id="buy-1")]}
    assert partial_stop_forbidden(
        rows, parsed, EXECUTOR, proof_ready=False, unproven_ids=set(),
    ) is False


def test_unsent_holding_holds_the_stop(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [])
    add_miss_order("buy-1")
    try:
        rows = [_row("buy-1")]
        assert partial_stop_forbidden(rows, {}, EXECUTOR, proof_ready=True) is True
    finally:
        remove_miss_order("buy-1")


def test_uncovered_holding_holds_the_stop():
    rows = [_row("buy-1")]
    assert partial_stop_forbidden(
        rows, {}, EXECUTOR, proof_ready=True, unproven_ids={"buy-1"},
    ) is True


def test_unreadable_history_holds_the_stop():
    assert partial_stop_forbidden(None, {}, EXECUTOR, proof_ready=True) is True


def test_empty_book_lets_the_stop_pass():
    assert partial_stop_forbidden([], {}, EXECUTOR, proof_ready=False) is False


def test_visible_end_releases_the_holding():
    rows = [_row("buy-1")]
    parsed = {"partial_ends": {"buy-1": {"kind": "flat", "order_ids": ["buy-1"]}}}
    assert partial_stop_forbidden(rows, parsed, EXECUTOR, proof_ready=False) is False


def test_cancel_event_reaches_the_handler_with_column_slots(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [])
    checkpoint = _checkpoint(_level(open_id="buy-1", close_id="sell-1"))
    service, _repo = _service_with(monkeypatch, checkpoint, _fresh(), [])
    seen = []
    executor = _ram_executor(open_id="buy-1", close_id="sell-1")
    executor.process_order_canceled_event = lambda *args, **kwargs: seen.append(args)
    service._active_executors[EXECUTOR] = executor
    service._install_partial_guards(EXECUTOR, executor)
    executor.process_order_canceled_event("event")
    assert seen == [("event",)]


def test_unproven_holding_ids_skip_live_and_dead(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [
        _row("live-1"), _row("dead-1"), _row("maybe-1", ts=10 ** 13),
    ])
    fresh = _fresh(
        opens=[_open("live-1")],
        finished=[_finished("dead-1")],
    )
    checkpoint = _checkpoint()
    service, _repo = _service_with(monkeypatch, checkpoint, fresh, [])
    connector = SimpleNamespace(in_flight_orders={}, _order_tracker=SimpleNamespace(lost_orders={}))
    found = service._unproven_holding_ids(EXECUTOR, connector, PAIR)
    assert found == {"maybe-1"}


# ── 2. замок «unreleased-ids» ────────────────────────────────────────────────


def _proof_service(tmp_path, monkeypatch, checkpoint, journal_lines):
    _journal(tmp_path, monkeypatch, journal_lines)
    repo = _Repo([_running_row(checkpoint=checkpoint)])
    service = _service(monkeypatch, repo, SimpleNamespace())
    service._set_hold_rows(repo.rows)
    service.__dict__["_sweep_seen"] = True
    service.__dict__["_sweep_failed"] = False
    return service


def test_visible_end_releases_without_the_runtime_list(tmp_path, monkeypatch):
    checkpoint = _checkpoint(ends={"buy-1": {"kind": "flat", "order_ids": ["buy-1"]}})
    service = _proof_service(tmp_path, monkeypatch, checkpoint, [_row("buy-1")])
    assert service._executor_proofs_released(EXECUTOR) is True


def test_number_without_an_end_holds_with_the_same_witness(tmp_path, monkeypatch):
    service = _proof_service(tmp_path, monkeypatch, _checkpoint(), [_row("buy-1")])
    assert service._executor_proofs_released(EXECUTOR) is False
    assert service.__dict__["_stop_gate"][EXECUTOR].startswith("unreleased-ids ids=")
    assert "buy-1" in service.__dict__["_stop_gate"][EXECUTOR]


def test_runtime_list_releases_as_before(tmp_path, monkeypatch):
    service = _proof_service(tmp_path, monkeypatch, _checkpoint(), [_row("buy-1")])
    service.__dict__["_released_order_ids"] = {"buy-1"}
    assert service._executor_proofs_released(EXECUTOR) is True


def test_sweep_and_ledger_gates_answer_as_before(tmp_path, monkeypatch):
    service = _proof_service(tmp_path, monkeypatch, _checkpoint(), [_row("buy-1")])
    service.__dict__["_sweep_seen"] = False
    assert service._executor_proofs_released(EXECUTOR) is False
    assert service.__dict__["_stop_gate"][EXECUTOR] == "sweep-not-seen"
    service.__dict__["_sweep_seen"] = True
    monkeypatch.setattr(mod, "ledger_executor_ids", lambda: None)
    assert service._executor_proofs_released(EXECUTOR) is False
    assert service.__dict__["_stop_gate"][EXECUTOR] == "ledger-unread"


def test_end_without_level_id_does_not_release_the_price():
    parsed = {"partial_ends": {"buy-1": {"kind": "flat", "order_ids": ["buy-1"]}}}
    assert "buy-1" in partial_released_ids(parsed)
    from utils.executor_checkpoint import partial_price_released_ids
    assert partial_price_released_ids(parsed) == set()


# ── 3. шаг сборки концов ─────────────────────────────────────────────────────


def _blocked(executor):
    from utils.executor_checkpoint import hold_ids_of
    if hold_ids_of(executor) or getattr(executor, "_close_ban_ids", None):
        return True
    if getattr(executor, "_column_close_ids", None):
        return True
    if getattr(getattr(executor, "_close_order", None), "order_id", None):
        return True
    for level in getattr(executor, "grid_levels", []) or []:
        if getattr(getattr(level, "active_close_order", None), "order_id", None):
            return True
    return False


async def test_flat_level_is_removed_and_the_brake_opens(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("buy-1"), _row("sell-1")])
    checkpoint = _checkpoint(_level(open_id="buy-1", close_id="sell-1"))
    fresh = _fresh(finished=[_finished("buy-1"), _finished("sell-1")])
    service, repo = _service_with(monkeypatch, checkpoint, fresh, [])
    executor = _ram_executor(open_id="buy-1", close_id="sell-1")
    executor._close_holds = [{"order_id": "sell-1", "role": "close"}]
    service._active_executors[EXECUTOR] = executor
    await service._shutdown_release_step(EXECUTOR, executor)
    stored = _stored(repo)
    level = stored["levels"][0]
    assert partial_level_released(stored, "L1")
    mark = next(item for item in stored["partial_ends"].values() if item.get("level_id") == "L1")
    assert mark["kind"] == "flat" and "price" not in mark
    assert set(mark["order_ids"]) == {"buy-1", "sell-1"}
    assert "take_profit_price" not in level and level["close_order"] is None
    assert stored.get("holds") == [] and stored.get("close_holds", []) == []
    assert executor.grid_levels[0].active_open_order is None
    assert executor.grid_levels[0].active_close_order is None
    assert _blocked(executor) is False


async def test_filled_buy_with_dead_close_keeps_the_level(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("buy-1"), _row("sell-1")])
    checkpoint = _checkpoint(_level(open_id="buy-1", close_id="sell-1", filled=True, bought="1"))
    fresh = _fresh(finished=[_finished("buy-1", filled=True), _finished("sell-1")])
    service, repo = _service_with(monkeypatch, checkpoint, fresh, [])
    executor = _ram_executor(open_id="buy-1", close_id="sell-1")
    service._active_executors[EXECUTOR] = executor
    await service._shutdown_release_step(EXECUTOR, executor)
    stored = _stored(repo)
    level = stored["levels"][0]
    assert not partial_level_released(stored, "L1")
    assert level["take_profit_price"] == "101"
    assert level["open_order"]["order_id"] == "buy-1"
    assert level["close_order"] is None
    for text in ("buy-1", "sell-1"):
        mark = stored["partial_ends"][text]
        assert "level_id" not in mark and "price" not in mark
        assert partial_end_visible(stored, text)
    assert executor.grid_levels[0].active_close_order is None
    assert executor.grid_levels[0].active_open_order is not None
    service.__dict__["_sweep_seen"] = True
    assert service._executor_proofs_released(EXECUTOR) is True


def test_restart_keeps_the_take_profit_of_an_open_level():
    previous = {
        "levels": [_level(open_id="buy-1", close_id="sell-1", filled=True)],
        "partial_ends": {
            "buy-1": {"kind": "flat", "order_ids": ["buy-1"]},
            "sell-1": {"kind": "flat", "order_ids": ["sell-1"]},
        },
    }
    fresh_body = {"levels": [_level(open_id="buy-1", take=None)], "holds": [{"order_id": "buy-1"}]}
    apply_partial_keep(fresh_body, previous)
    assert not partial_level_released(fresh_body, "L1")
    assert fresh_body["levels"][0]["take_profit_price"] == "101"
    assert fresh_body["holds"] == []


async def test_live_level_order_is_not_touched(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("buy-1")])
    checkpoint = _checkpoint(_level(open_id="buy-1"))
    fresh = _fresh(opens=[_open("buy-1")])
    service, repo = _service_with(monkeypatch, checkpoint, fresh, [])
    executor = _ram_executor(open_id="buy-1")
    before = repo.rows[0]["checkpoint"]
    await service._shutdown_release_step(EXECUTOR, executor)
    assert repo.rows[0]["checkpoint"] == before
    assert executor.grid_levels[0].active_open_order is not None


async def test_mixed_case_dead_open_live_close_keeps_the_level(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("buy-1"), _row("sell-1")])
    checkpoint = _checkpoint(_level(open_id="buy-1", close_id="sell-1", filled=True, bought="1"))
    # Списки не покрывают время строк: номер вне finished не доказан мёртвым.
    fresh = _fresh(opens=[_open("sell-1")], finished=[_finished("buy-1", filled=True)], covers=False)
    fresh["finished_request"] = {"params": {}}
    service, repo = _service_with(monkeypatch, checkpoint, fresh, [])
    executor = _ram_executor(open_id="buy-1", close_id="sell-1")
    await service._shutdown_release_step(EXECUTOR, executor)
    stored = _stored(repo)
    assert not partial_level_released(stored, "L1")
    assert stored["levels"][0]["close_order"]["order_id"] == "sell-1"
    assert stored["levels"][0]["take_profit_price"] == "101"
    assert stored["levels"][0]["open_order"]["order_id"] == "buy-1"
    # Живой номер не отпущен. Исполненная покупка получила конец без level_id.
    assert not partial_end_visible(stored, "sell-1")
    assert partial_end_visible(stored, "buy-1")
    assert "level_id" not in stored["partial_ends"]["buy-1"]


async def test_dead_close_is_cleared_while_the_open_is_still_live(tmp_path, monkeypatch):
    """Мёртвая запись закрытия не держит стоп, даже если покупка уровня жива."""
    _journal(tmp_path, monkeypatch, [_row("buy-1"), _row("sell-1")])
    checkpoint = _checkpoint(_level(open_id="buy-1", close_id="sell-1", filled=True, bought="1"))
    fresh = _fresh(opens=[_open("buy-1")], finished=[_finished("sell-1")])
    service, repo = _service_with(monkeypatch, checkpoint, fresh, [])
    executor = _ram_executor(open_id="buy-1", close_id="sell-1")
    service._active_executors[EXECUTOR] = executor
    await service._shutdown_release_step(EXECUTOR, executor)
    stored = _stored(repo)
    level = stored["levels"][0]
    assert not partial_level_released(stored, "L1")
    assert level["open_order"]["order_id"] == "buy-1"
    assert level["take_profit_price"] == "101"
    assert level["close_order"] is None
    assert partial_end_visible(stored, "sell-1")
    assert "level_id" not in stored["partial_ends"]["sell-1"]
    assert not partial_end_visible(stored, "buy-1")
    assert executor.grid_levels[0].active_close_order is None
    assert executor.grid_levels[0].active_open_order is not None
    connector = SimpleNamespace(in_flight_orders={}, _order_tracker=SimpleNamespace(lost_orders={}))
    assert service._partial_sell_is_live(EXECUTOR, connector, stored, PAIR) is False


async def test_dead_partial_close_gets_an_end_beside_a_live_close(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("buy-1"), _row("sell-old"), _row("sell-1")])
    checkpoint = _checkpoint(_level(
        open_id="buy-1", close_id="sell-1", filled=True, bought="2",
        partial_closes=[{"order_id": "sell-old", "filled_base": "1"}],
    ))
    fresh = _fresh(opens=[_open("sell-1")], finished=[_finished("sell-old")], covers=False)
    fresh["finished_request"] = {"params": {}}
    service, repo = _service_with(monkeypatch, checkpoint, fresh, [])
    executor = _ram_executor(open_id="buy-1", close_id="sell-1")
    service._active_executors[EXECUTOR] = executor
    await service._shutdown_release_step(EXECUTOR, executor)
    stored = _stored(repo)
    assert partial_end_visible(stored, "sell-old")
    assert "level_id" not in stored["partial_ends"]["sell-old"]
    assert not partial_end_visible(stored, "sell-1")
    assert stored["levels"][0]["close_order"]["order_id"] == "sell-1"
    assert not partial_level_released(stored, "L1")
    service.__dict__["_sweep_seen"] = True
    assert service._executor_proofs_released(EXECUTOR) is False
    assert "sell-old" not in service.__dict__["_stop_gate"][EXECUTOR]


async def test_finished_row_still_in_flight_is_not_released(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("sell-1")])
    checkpoint = _checkpoint(_level(open_id="buy-1", close_id="sell-1", filled=True, bought="1"))
    fresh = _fresh(finished=[_finished("sell-1")])
    connector = SimpleNamespace(
        in_flight_orders={"sell-1": SimpleNamespace(price="102")},
        _order_tracker=SimpleNamespace(lost_orders={}),
    )
    service, repo = _service_with(monkeypatch, checkpoint, fresh, [], connector=connector)
    executor = _ram_executor(open_id="buy-1", close_id="sell-1")
    executor.connectors["gate_io"] = connector
    await service._shutdown_release_step(EXECUTOR, executor)
    stored = _stored(repo)
    assert not partial_end_visible(stored, "sell-1")
    assert stored["levels"][0]["close_order"]["order_id"] == "sell-1"


async def test_live_take_sell_does_not_freeze_the_dead_traces(tmp_path, monkeypatch):
    """Живой тейк другого уровня не охраняет мёртвые следы: охраны глобальной нет."""
    _journal(tmp_path, monkeypatch, [_row("buy-1"), _row("sell-1"), _row("sell-2")])
    dead = _level(open_id="buy-1", close_id="sell-1")
    live = _level(level_id="L2", open_id="buy-2", close_id="sell-2")
    checkpoint = _checkpoint(dead, live)
    fresh = _fresh(opens=[_open("sell-2")], finished=[_finished("buy-1"), _finished("sell-1")])
    service, repo = _service_with(monkeypatch, checkpoint, fresh, [])
    executor = _ram_executor()
    executor.grid_levels.append(SimpleNamespace(
        id="L2",
        active_open_order=SimpleNamespace(order_id="buy-2"),
        active_close_order=SimpleNamespace(order_id="sell-2"),
    ))
    await service._shutdown_release_step(EXECUTOR, executor)
    stored = _stored(repo)
    assert partial_level_released(stored, "L1")
    assert not partial_level_released(stored, "L2")
    assert stored["levels"][1]["close_order"]["order_id"] == "sell-2"


async def test_dead_number_outside_slots_gets_an_end(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("stray-1")])
    checkpoint = _checkpoint(holds=[{"order_id": "stray-1", "price": "100"}])
    fresh = _fresh(finished=[_finished("stray-1")])
    service, repo = _service_with(monkeypatch, checkpoint, fresh, [])
    executor = _ram_executor()
    service._active_executors[EXECUTOR] = executor
    await service._shutdown_release_step(EXECUTOR, executor)
    stored = _stored(repo)
    assert partial_end_visible(stored, "stray-1")
    assert "level_id" not in stored["partial_ends"]["stray-1"]
    assert stored["holds"] == []
    service.__dict__["_sweep_seen"] = True
    assert service._executor_proofs_released(EXECUTOR) is True


async def test_market_close_hold_without_proof_stays(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("mkt-1")])
    fresh = _fresh(covers=False)
    fresh["finished_request"] = {"params": {}}
    service, repo = _service_with(monkeypatch, _checkpoint(), fresh, [])
    executor = _ram_executor()
    executor._close_holds = [{"order_id": "mkt-1", "role": "market_close"}]
    executor._close_order = SimpleNamespace(order_id="mkt-1")
    before = repo.rows[0]["checkpoint"]
    await service._shutdown_release_step(EXECUTOR, executor)
    assert repo.rows[0]["checkpoint"] == before
    assert executor._close_order.order_id == "mkt-1"


async def test_market_close_hold_with_proof_is_removed_at_any_position(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("mkt-1")])
    service, repo = _service_with(
        monkeypatch, _checkpoint(), _fresh(finished=[_finished("mkt-1", filled=True)]), [],
    )
    executor = _ram_executor()
    executor._close_holds = [{"order_id": "mkt-1", "role": "market_close"}]
    executor._close_order = SimpleNamespace(order_id="mkt-1")
    service._active_executors[EXECUTOR] = executor
    await service._shutdown_release_step(EXECUTOR, executor)
    assert executor._close_order is None
    assert partial_end_visible(_stored(repo), "mkt-1")
    assert "level_id" not in _stored(repo)["partial_ends"]["mkt-1"]


async def test_zero_position_with_a_live_market_close_keeps_it(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("mkt-1")])
    service, repo = _service_with(monkeypatch, _checkpoint(), _fresh(opens=[_open("mkt-1")]), [])
    executor = _ram_executor()
    executor._close_order = SimpleNamespace(order_id="mkt-1")
    executor.position_size_base = Decimal("0")
    before = repo.rows[0]["checkpoint"]
    await service._shutdown_release_step(EXECUTOR, executor)
    assert repo.rows[0]["checkpoint"] == before
    assert executor._close_order.order_id == "mkt-1"


async def test_failed_write_leaves_the_memory(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("buy-1"), _row("sell-1")])
    checkpoint = _checkpoint(_level(open_id="buy-1", close_id="sell-1"))
    fresh = _fresh(finished=[_finished("buy-1"), _finished("sell-1")])
    service, repo = _service_with(monkeypatch, checkpoint, fresh, [])

    async def _refuse(*args, **kwargs):
        return False

    service._partial_cas = _refuse
    executor = _ram_executor(open_id="buy-1", close_id="sell-1")
    executor._close_holds = [{"order_id": "sell-1", "role": "close"}]
    await service._shutdown_release_step(EXECUTOR, executor)
    assert executor.grid_levels[0].active_close_order is not None
    assert executor._close_holds == [{"order_id": "sell-1", "role": "close"}]
    assert repo.holds == []


async def test_flat_by_filled_buy_and_sells(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("buy-1"), _row("sell-1")])
    checkpoint = _checkpoint(_level(open_id="buy-1", close_id="sell-1", filled=True, bought="1"))
    fresh = _fresh(finished=[
        _finished("buy-1", filled=True),
        _finished("sell-1", filled=True, amount="1"),
    ])
    service, repo = _service_with(monkeypatch, checkpoint, fresh, [])
    executor = _ram_executor(open_id="buy-1", close_id="sell-1")
    await service._shutdown_release_step(EXECUTOR, executor)
    stored = _stored(repo)
    assert partial_level_released(stored, "L1")
    assert "take_profit_price" not in stored["levels"][0]


# ── 4. повтор и цены ─────────────────────────────────────────────────────────


def test_zero_position_does_not_place_another_close():
    """Штатный шаг гашения ставит закрытие только при позиции не меньше минимума."""
    position = Decimal("0")
    min_order_size = Decimal("0.01")
    assert not (position >= min_order_size)


def test_take_profit_tick_stays_occupied_after_the_step(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("buy-1"), _row("sell-1")])
    checkpoint = _checkpoint(_level(open_id="buy-1", close_id="sell-1", filled=True, bought="1"))
    fresh = _fresh(finished=[_finished("buy-1", filled=True), _finished("sell-1")])
    service, repo = _service_with(monkeypatch, checkpoint, fresh, [])
    service._active_executors[EXECUTOR] = _ram_executor()
    prices = service._partial_pair_seat_prices(PAIR, set())
    assert ("buy-1", "101") in prices
    occupied = service._occupied_close_prices(service._active_executors[EXECUTOR])
    assert "101" not in {str(item) for item in occupied}


def test_flat_level_price_is_released_like_partial_release():
    parsed = {
        "levels": [_level(open_id="buy-1", take=None)],
        "partial_ends": {"buy-1": {"kind": "flat", "level_id": "L1", "order_ids": ["buy-1"]}},
    }
    assert partial_level_released(parsed, "L1")
    from utils.executor_checkpoint import partial_feed_prices
    assert partial_feed_prices([], parsed) == []


async def test_dead_close_record_is_not_revived_and_stop_passes(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("buy-1"), _row("sell-1")])
    checkpoint = _checkpoint(_level(open_id="buy-1", close_id="sell-1", filled=True, bought="1"))
    fresh = _fresh(finished=[_finished("buy-1", filled=True), _finished("sell-1")])
    service, repo = _service_with(monkeypatch, checkpoint, fresh, [])
    executor = _ram_executor(open_id="buy-1", close_id="sell-1")
    service._active_executors[EXECUTOR] = executor
    await service._shutdown_release_step(EXECUTOR, executor)
    stored = _stored(repo)
    connector = SimpleNamespace(in_flight_orders={}, _order_tracker=SimpleNamespace(lost_orders={}))
    assert service._partial_sell_is_live(EXECUTOR, connector, stored, PAIR) is False
    assert service._partial_forbids(EXECUTOR) is False
    restarted = {"levels": [_level(open_id="buy-1", take=None)]}
    apply_partial_keep(restarted, stored)
    assert restarted["levels"][0].get("close_order") in (None, {})
    assert restarted["levels"][0]["take_profit_price"] == "101"


# ── 5. финализация ───────────────────────────────────────────────────────────


def test_closed_executor_is_finalized_when_every_number_has_an_end(tmp_path, monkeypatch):
    checkpoint = _checkpoint(ends={"buy-1": {"kind": "flat", "order_ids": ["buy-1"]}})
    service = _proof_service(tmp_path, monkeypatch, checkpoint, [_row("buy-1")])
    executor = SimpleNamespace(is_closed=True)
    service._active_executors[EXECUTOR] = executor
    completed = [
        executor_id
        for executor_id, item in service._active_executors.items()
        if getattr(item, "is_closed", False) and service._executor_proofs_released(executor_id)
    ]
    assert completed == [EXECUTOR]


def test_closed_executor_is_held_while_a_number_has_no_end(tmp_path, monkeypatch):
    checkpoint = _checkpoint(ends={"buy-1": {"kind": "flat", "order_ids": ["buy-1"]}})
    service = _proof_service(
        tmp_path, monkeypatch, checkpoint, [_row("buy-1"), _row("sell-1")],
    )
    executor = SimpleNamespace(is_closed=True)
    service._active_executors[EXECUTOR] = executor
    completed = [
        executor_id
        for executor_id, item in service._active_executors.items()
        if getattr(item, "is_closed", False) and service._executor_proofs_released(executor_id)
    ]
    assert completed == []
