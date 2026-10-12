"""Пробы безопасной заморозки (план stop-freeze, раздел «Чем проверим»).

Пять проверок: метка «стоп запрошен» переживает штатный захват и перезагрузку;
`early_stop` не уходит до подтверждённой метки и идёт мимо отказа обёртки;
чужая склейка и неподтверждённая отправка держат с причиной; под меткой ни один
путь не чеканит и не шлёт, записи уровней не трогаются, цена тейка остаётся
занятой для соседней сетки; гашение и финализация не отдаются (brake-held).
"""

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

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
    STOP_MARK_KEY,
    checkpoint_keeping_holds,
    install_close_without_level,
    partial_end_visible,
    partial_price_released_ids,
    reset_ledger_process_state,
    stop_mark_visible,
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


def _level(level_id="L1", open_id="", close_id="", take="101", filled=False):
    body = {"id": level_id}
    if open_id:
        body["open_order"] = {"order_id": open_id, "level_id": level_id, "filled": filled}
    if close_id:
        body["close_order"] = {"order_id": close_id, "level_id": level_id, "price": "101"}
    if take is not None:
        body["take_profit_price"] = take
    return body


def _checkpoint(*levels, holds=None, ends=None):
    body = {"version": 1, "type": "grid_executor", "levels": list(levels), "holds": holds or []}
    if ends:
        body["partial_ends"] = ends
    return json.dumps(body, sort_keys=True)


class _Connector:
    def __init__(self):
        self.in_flight_orders = {}
        self._order_tracker = SimpleNamespace(lost_orders={})
        self.created = []

    async def _create_order(self, **kwargs):
        self.created.append(kwargs)


def _service_with(monkeypatch, checkpoint, connector=None, journal_lines=None, tmp_path=None):
    if journal_lines is not None:
        _journal(tmp_path, monkeypatch, journal_lines)
    repo = _Repo([_running_row(checkpoint=checkpoint)])
    connector = connector if connector is not None else _Connector()
    service = _service(monkeypatch, repo, connector)
    service._set_hold_rows(repo.rows)
    return service, repo, connector


def _stub_executor(connector, recorded):
    config = SimpleNamespace(
        trading_pair=PAIR, connector_name="gate_io", side=SimpleNamespace(name="BUY"), id=EXECUTOR,
    )

    def early_stop(keep_position=False):
        recorded.append("early_stop")

    async def _original_shutdown(*args, **kwargs):
        recorded.append("shutdown")

    async def _original_close(*args, **kwargs):
        recorded.append("close")

    def _original_stop(*args, **kwargs):
        recorded.append("stop")

    def _original_market(*args, **kwargs):
        recorded.append("market")

    def _original_get(*args, **kwargs):
        recorded.append("get_close")
        return []

    def _original_adjust(*args, **kwargs):
        recorded.append("adjust")

    async def _original_loop(*args, **kwargs):
        recorded.append("loop")

    return SimpleNamespace(
        config=config,
        connectors={"gate_io": connector},
        grid_levels=[],
        _close_holds=[],
        _close_ban_ids=set(),
        _close_ban_prices={},
        _column_close_ids=[],
        _close_order=None,
        _close_seat_ready=True,
        _close_list_ready=True,
        is_closed=False,
        _status="RUNNING",
        early_stop=early_stop,
        control_shutdown_process=_original_shutdown,
        control_close_order=_original_close,
        stop=_original_stop,
        place_close_order_and_cancel_open_orders=_original_market,
        get_close_orders_to_create=_original_get,
        adjust_and_place_close_order=_original_adjust,
        control_loop=_original_loop,
    )


def _stored(repo):
    return json.loads(repo.rows[0]["checkpoint"])


# ── 1. метка: форма, захват, перезагрузка ────────────────────────────────────


def test_stop_mark_form_survives_capture_and_drop_dead(monkeypatch):
    service, repo, _ = _service_with(monkeypatch, _checkpoint(_level(open_id="buy-1", filled=True)))
    recorded = []
    executor = _stub_executor(_Connector(), recorded)
    service._active_executors[EXECUTOR] = executor

    import asyncio
    assert asyncio.run(service._freeze_stop_mark(EXECUTOR)) is True

    stored = _stored(repo)
    mark = (stored.get("partial_ends") or {}).get(STOP_MARK_KEY)
    assert isinstance(mark, dict)
    assert mark.get("kind") == "stop_requested"
    assert "level_id" not in mark
    assert mark.get("order_ids") == []
    assert STOP_MARK_KEY not in {"buy-1"}
    assert partial_end_visible(stored.get("partial_ends") and stored, "buy-1") is False

    # Штатный захват публикует запись из RAM-слотов; метка без level_id едет вперёд.
    captured = {"version": 1, "type": "grid_executor", "levels": [_level(open_id="buy-1", filled=True)], "holds": []}
    published = checkpoint_keeping_holds(repo.rows[0]["checkpoint"], captured)
    assert stop_mark_visible(json.loads(published))

    # _drop_dead_close_records мусорит только в close_holds/close_bans: метка цела.
    after = service._drop_dead_close_records(published)
    assert stop_mark_visible(json.loads(after))


def test_stop_mark_absent_means_not_frozen(monkeypatch):
    service, repo, _ = _service_with(monkeypatch, _checkpoint(_level()))
    import asyncio
    assert asyncio.run(service._freeze_stop_mark(EXECUTOR)) is True
    # Свежая перечитка без метки (запись перетёрта) не размораживает RAM,
    # но чистый процесс без метки не заморожен.
    assert service._stop_mark_now("other") is False


# ── 2. метка до команды; early_stop мимо отказа обёртки ──────────────────────


def test_stop_does_not_send_early_stop_until_mark_confirmed(monkeypatch):
    service, repo, _ = _service_with(monkeypatch, _checkpoint(_level(open_id="buy-1")))
    recorded = []
    executor = _stub_executor(_Connector(), recorded)
    service._active_executors[EXECUTOR] = executor

    import asyncio

    async def call():
        return await service.stop_executor(EXECUTOR)

    # Порядок: запись метки в репозитории — раньше early_stop.
    answer = asyncio.run(call())
    assert answer["status"] == "stopping"
    assert recorded == ["early_stop"]
    assert [item[0] for item in repo.holds] == [EXECUTOR]
    assert stop_mark_visible(_stored(repo))

    # Повтор команды: метка уже есть — отмены досылаются, ответ «заморожена».
    answer = asyncio.run(call())
    assert answer["status"] == "frozen"
    assert recorded == ["early_stop", "early_stop"]


def test_stop_refuses_without_confirmed_mark_and_answers_the_reason(monkeypatch):
    service, repo, _ = _service_with(monkeypatch, _checkpoint(_level()))
    recorded = []
    executor = _stub_executor(_Connector(), recorded)
    service._active_executors[EXECUTOR] = executor

    import asyncio

    async def no_read(_executor_id):
        return False

    async def no_cas(*args, **kwargs):
        return False

    service._read_stop_mark_fresh = no_read
    service._partial_cas = no_cas

    async def call():
        return await service.stop_executor(EXECUTOR)

    answer = asyncio.run(call())
    assert answer["status"] == "not_accepted"
    assert answer.get("reason")
    assert recorded == []
    assert not stop_mark_visible(_stored(repo))


def test_wrap_stop_does_not_refuse_or_reset_running_under_the_mark(monkeypatch):
    service, repo, _ = _service_with(monkeypatch, _checkpoint(_level()))
    recorded = []
    connector = _Connector()
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor
    service._install_partial_guards(EXECUTOR, executor)
    service.__dict__.setdefault("_stop_mark_ram", {})[EXECUTOR] = True

    executor.early_stop(keep_position=False)
    assert recorded == ["early_stop"]
    assert executor._status == "RUNNING"

    service._partial_keep_running(executor)
    assert executor._status == "RUNNING"


# ── 3. свидетели: чужая склейка и неподтверждённая отправка ─────────────────


def test_stop_holds_on_unproven_send_and_names_the_reason(tmp_path, monkeypatch):
    _journal(tmp_path, monkeypatch, [_row("buy-1")])
    from utils.executor_checkpoint import add_miss_order
    add_miss_order("buy-1")
    try:
        service, repo, _ = _service_with(monkeypatch, _checkpoint(_level(open_id="buy-1")))
        recorded = []
        executor = _stub_executor(_Connector(), recorded)
        service._active_executors[EXECUTOR] = executor

        import asyncio

        async def call():
            return await service.stop_executor(EXECUTOR)

        answer = asyncio.run(call())
        assert answer["status"] == "not_accepted"
        assert "unproven-send" in answer["reason"]
        assert recorded == []
        assert not stop_mark_visible(_stored(repo))
    finally:
        from utils.executor_checkpoint import remove_miss_order
        remove_miss_order("buy-1")


def test_stop_holds_on_foreign_glue_by_price(monkeypatch):
    connector = _Connector()
    connector.in_flight_orders["foreign-1"] = SimpleNamespace(price="101", client_order_id="foreign-1")
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(open_id="buy-1", close_id="sell-1")), connector=connector,
    )
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor

    import asyncio

    async def call():
        return await service.stop_executor(EXECUTOR)

    answer = asyncio.run(call())
    assert answer["status"] == "not_accepted"
    assert "foreign-glue" in answer["reason"]
    assert recorded == []
    assert not stop_mark_visible(_stored(repo))


# ── 4. под меткой ничего не чеканится и не шлётся, записи целы ──────────────


def test_durable_open_and_send_do_not_mint_or_send_under_the_mark(monkeypatch):
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level(open_id="buy-1", filled=True)))
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor
    service.__dict__.setdefault("_stop_mark_ram", {})[EXECUTOR] = True

    import asyncio

    level = SimpleNamespace(id="L1", price=Decimal("100"), take_profit=Decimal("0.01"), active_open_order=None)
    asyncio.run(service._durable_open(EXECUTOR, executor, level, {}))
    assert connector.created == []

    asyncio.run(
        service._partial_send_committed(EXECUTOR, executor, connector, PAIR, "sell-1", "L1", "101", Decimal("1"))
    )
    assert connector.created == []


def test_release_and_case_freeze_keep_records_and_take_price(monkeypatch):
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level(open_id="buy-1", filled=True, close_id="sell-1")))
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor

    import asyncio
    assert asyncio.run(service._freeze_stop_mark(EXECUTOR)) is True
    before = _stored(repo)

    assert asyncio.run(service._partial_release(EXECUTOR, "buy-1", "L1", "stock_flat", connector, PAIR)) is False
    asyncio.run(service._partial_case_three(EXECUTOR, executor, connector, PAIR, "buy-1", "L1", Decimal("1"), {}))

    after = _stored(repo)
    assert after == before
    # Цена тейка не отпущена: соседняя сетка не встаёт на неё.
    assert "level:L1" not in partial_price_released_ids(after)
    assert (after["levels"][0] or {}).get("take_profit_price") == "101"
    assert ((after["levels"][0] or {}).get("close_order") or {}).get("order_id") == "sell-1"
    # Концов с level_id нет.
    for mark in (after.get("partial_ends") or {}).values():
        assert isinstance(mark, dict) and "level_id" not in mark


# ── 5. гашение и финализация не отдаются ────────────────────────────────────


def test_brake_holds_the_shutdown_path_under_the_mark(monkeypatch, caplog):
    recorded = []
    executor = _stub_executor(_Connector(), recorded)
    assert install_close_without_level(executor, EXECUTOR, frozen=lambda: True) is True

    import asyncio
    assert asyncio.run(executor.control_shutdown_process()) is None
    assert executor.stop() is None
    assert executor.place_close_order_and_cancel_open_orders() is None
    assert recorded == []
    assert any("brake-held" in line for line in caplog.messages)


def test_brake_passes_the_shutdown_path_when_clean_and_not_frozen():
    recorded = []
    executor = _stub_executor(_Connector(), recorded)
    assert install_close_without_level(executor, EXECUTOR, frozen=lambda: False) is True
    executor._close_seat_ready = True
    executor._close_list_ready = True

    import asyncio
    asyncio.run(executor.control_shutdown_process())
    assert "shutdown" in recorded


def test_stop_marks_nothing_but_the_mark(monkeypatch):
    service, repo, _ = _service_with(monkeypatch, _checkpoint(_level(open_id="buy-1", filled=True, close_id="sell-1")))
    recorded = []
    executor = _stub_executor(_Connector(), recorded)
    service._active_executors[EXECUTOR] = executor
    before = json.loads(_checkpoint(_level(open_id="buy-1", filled=True, close_id="sell-1")))

    def _explode(*args, **kwargs):
        raise AssertionError("shutdown release step must not run in this slice")

    service._shutdown_release_step = _explode

    import asyncio

    async def call():
        return await service.stop_executor(EXECUTOR)

    answer = asyncio.run(call())
    assert answer["status"] == "stopping"
    after = _stored(repo)
    # Единственное отличие записей — сама метка.
    for key, value in before.items():
        assert after.get(key) == value
    assert set(after.get("partial_ends") or {}) == {STOP_MARK_KEY}
