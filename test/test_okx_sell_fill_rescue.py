"""Спасательный круг для исполненных продаж. План okx-sell-fill-rescue. Без биржи.

Посадка исполненного close из страницы OKX (включая blank-px) в уровень,
чей круг открытия ещё жив. Метка круга — ts строки журнала открытия.
Сами ничего не отправляем.
"""

import asyncio
import json

from types import SimpleNamespace

from hummingbot.strategy_v2.models.executors import TrackedOrder

from test.test_stop_freeze import PAIR, _checkpoint, _journal, _row, _service_with
from test.test_stop_position_close import EXECUTOR


EARLY = "sell-early"
LATE = "sell-late"


def _level(open_id="buy-1", close=None):
    return {
        "id": "L1",
        "price": "100",
        "take_profit": "0.01",
        "side": "BUY",
        "open_order": {"order_id": open_id, "filled": True, "price": "100",
                       "order": {"executed_amount_base": "1"}},
        "close_order": close,
    }


def _fresh(rows=None, blank=None, ready=True, frm=0, to=10 ** 9):
    return {
        "opens_read": ready, "opens_done": ready,
        "finished_read": ready, "finished_done": ready,
        "opens": [], "finished": rows or [], "finished_blank_px": blank or [],
        "finished_request": {"params": {"from": frm * 1000, "to": to * 1000}},
    }


def _blank(order_id, size="1", avg="101", state="filled"):
    return {"order_id": order_id, "text": order_id, "state": state,
            "fill_size": size, "px": "", "avgPx": avg}


def _store_fresh(service, fresh):
    service.__dict__["_price_tick"] = (service.__dict__.get("_price_tick") or 0) + 1
    service.__dict__.setdefault("_tick_reads", {})[PAIR] = {
        "tick": service.__dict__["_price_tick"], "fresh": fresh,
    }


def _live_executor(connector, filled_open=True, open_id="buy-1", offer=None):
    open_slot = TrackedOrder(order_id=open_id)
    open_slot.order = SimpleNamespace(is_filled=filled_open, completely_filled_event=SimpleNamespace(is_set=lambda: filled_open))
    stub = SimpleNamespace(
        config=SimpleNamespace(trading_pair=PAIR, connector_name="gate_io"),
        connectors={"gate_io": connector},
        grid_levels=[SimpleNamespace(id="L1", active_open_order=open_slot, active_close_order=None)],
    )
    if offer is not None:
        stub.get_close_orders_to_create = lambda: list(offer)
    return stub


def _base(tmp_path, monkeypatch, fresh, checkpoint_levels, journal_lines):
    import shutil
    bots = tmp_path / "bots"
    if bots.exists():
        shutil.rmtree(bots)
    _journal(tmp_path, monkeypatch, journal_lines)
    service, repo, connector = _service_with(monkeypatch, _checkpoint(*checkpoint_levels), tmp_path=tmp_path)
    _store_fresh(service, fresh)
    return service, repo, connector


def test_seats_early_proved_close_not_the_late_refused(tmp_path, monkeypatch):
    """Ранняя исполненная market_close сажается; поздняя отказная её не заслоняет."""
    fresh = _fresh(blank=[_blank(EARLY)])
    lines = [
        _row("buy-1", ts=10),
        _row(EARLY, ts=11, role="market_close", sent=True),
        _row(LATE, ts=12, role="close", sent=True),
    ]
    service, _repo, connector = _base(tmp_path, monkeypatch, fresh, [_level()], lines)
    executor = _live_executor(connector)
    assert asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor, connector, PAIR)) is True

    live = executor.grid_levels[0]
    assert live.active_close_order is not None
    assert live.active_close_order.order_id == EARLY
    assert live.active_close_order.order.is_filled
    assert live.active_close_order.order.completely_filled_event.is_set()

    parsed = service._partial_parsed(EXECUTOR)
    body = parsed["levels"][0]
    assert body["close_order"]["order_id"] == EARLY
    assert body["close_order"]["filled"] is True
    assert body["close_order"]["order"]["executed_amount_base"] == "1"
    assert body["open_order"]["order_id"] == "buy-1"
    assert connector.created == []


def test_late_refused_alone_does_not_seat(tmp_path, monkeypatch):
    """Окно ушло и раннюю не доказывает — поздние отказные сами ничего не сажают."""
    fresh = _fresh(blank=[_blank(EARLY)], frm=0, to=10 ** 9)
    lines = [
        _row("buy-1", ts=10),
        _row(LATE, ts=12, role="close", sent=True),
    ]
    fresh["finished_blank_px"] = []
    service, _repo, connector = _base(tmp_path, monkeypatch, fresh, [_level()], lines)
    executor = _live_executor(connector)
    assert asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor, connector, PAIR)) is False
    assert executor.grid_levels[0].active_close_order is None
    parsed = service._partial_parsed(EXECUTOR)
    assert (parsed["levels"][0].get("close_order") or {}).get("order_id") is None


def test_no_seat_when_proof_missing(tmp_path, monkeypatch):
    """Списки не прочитаны, окно мимо, state не filled или размер ноль — не сажается."""
    lines = [_row("buy-1", ts=10), _row(EARLY, ts=11, role="market_close", sent=True)]
    cases = [
        _fresh(blank=[_blank(EARLY)], ready=False),
        _fresh(blank=[_blank(EARLY)], frm=1000, to=2000),
        _fresh(blank=[_blank(EARLY, state="canceled")]),
        _fresh(blank=[_blank(EARLY, size="0")]),
        _fresh(rows=[{"order_id": EARLY, "text": EARLY, "state": "live", "fill_size": "1"}]),
    ]
    for fresh in cases:
        service, _repo, connector = _base(tmp_path, monkeypatch, fresh, [_level()], lines)
        executor = _live_executor(connector)
        assert asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor, connector, PAIR)) is False
        assert executor.grid_levels[0].active_close_order is None


def test_no_seat_without_open_journal_row_or_with_new_circle(tmp_path, monkeypatch):
    """Нет строки журнала открытия — не сажаем. Новое открытие не снимаем и не сажаем на него старое."""
    fresh = _fresh(blank=[_blank(EARLY)])
    # у слота открытия нет строки журнала с ts
    lines = [_row(EARLY, ts=11, role="market_close", sent=True)]
    service, _repo, connector = _base(tmp_path, monkeypatch, fresh, [_level()], lines)
    executor = _live_executor(connector)
    assert asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor, connector, PAIR)) is False

    # новое открытие (круг сменился): тело нулевой метки, строка поздняя
    fresh2 = _fresh(blank=[_blank(EARLY)])
    lines2 = [
        _row("buy-1", ts=10),
        _row("buy-2", ts=20),
        _row(EARLY, ts=11, role="market_close", sent=True),
    ]
    checkpoint = [_level(open_id="buy-2")]
    service2, _repo2, connector2 = _base(tmp_path, monkeypatch, fresh2, checkpoint, lines2)
    executor2 = _live_executor(connector2, open_id="buy-2")
    assert asyncio.run(service2._rescue_seat_filled_close(EXECUTOR, executor2, connector2, PAIR)) is False
    assert executor2.grid_levels[0].active_close_order is None
    assert executor2.grid_levels[0].active_open_order.order_id == "buy-2"


def test_no_repeat_seat_when_close_already_seated(tmp_path, monkeypatch):
    fresh = _fresh(blank=[_blank(EARLY)])
    lines = [_row("buy-1", ts=10), _row(EARLY, ts=11, role="market_close", sent=True)]
    seated = _level(close={"order_id": "other", "filled": True})
    service, _repo, connector = _base(tmp_path, monkeypatch, fresh, [seated], lines)
    executor = _live_executor(connector)
    assert asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor, connector, PAIR)) is False
    assert executor.grid_levels[0].active_close_order is None
    parsed = service._partial_parsed(EXECUTOR)
    assert parsed["levels"][0]["close_order"]["order_id"] == "other"


def test_filter_blocks_only_own_proved_level(tmp_path, monkeypatch):
    fresh = _fresh(blank=[_blank(EARLY)])
    lines = [_row("buy-1", ts=10), _row(EARLY, ts=11, role="market_close", sent=True)]
    service, _repo, connector = _base(tmp_path, monkeypatch, fresh, [_level()], lines)
    assert service._rescue_close_blocked(EXECUTOR, "L1", fresh) is True

    unproved = _fresh(blank=[])
    assert service._rescue_close_blocked(EXECUTOR, "L1", unproved) is False
    assert service._rescue_close_blocked(EXECUTOR, "L2", fresh) is False


def test_wrapped_close_get_filters_proved_level(tmp_path, monkeypatch):
    """Обёртка предложений не отдаёт уровень с доказанным исполненным close."""
    fresh = _fresh(blank=[_blank(EARLY)])
    lines = [_row("buy-1", ts=10), _row(EARLY, ts=11, role="market_close", sent=True)]
    service, _repo, connector = _base(tmp_path, monkeypatch, fresh, [_level()], lines)
    level = SimpleNamespace(id="L1")
    other = SimpleNamespace(id="L2")
    executor = _live_executor(connector, offer=[level, other])
    service._install_partial_guards(EXECUTOR, executor)

    result = executor.get_close_orders_to_create()
    assert level not in result
    assert other in result

    unproved = _fresh(blank=[])
    _store_fresh(service, unproved)
    assert level in executor.get_close_orders_to_create()
