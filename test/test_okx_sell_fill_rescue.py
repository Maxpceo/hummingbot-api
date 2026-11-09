"""Спасательный круг для исполненных продаж. План okx-sell-fill-rescue. Без биржи.

Посадка исполненного close из страницы OKX (включая blank-px) в уровень,
чей круг открытия ещё жив. Метка круга — ts строки журнала открытия.
Сами ничего не отправляем.
"""

import asyncio
import json
from decimal import Decimal

from types import SimpleNamespace

from hummingbot.strategy_v2.models.executors import TrackedOrder

from test.test_stop_freeze import PAIR, _checkpoint, _journal, _row, _service_with
from test.test_stop_position_close import EXECUTOR


EARLY = "sell-early"
LATE = "sell-late"


def _level(open_id="buy-1", close=None, level_id="L1"):
    return {
        "id": level_id,
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
    executor = _live_executor(connector)
    assert service._rescue_close_blocked(EXECUTOR, "L1", fresh, executor) is True

    unproved = _fresh(blank=[])
    assert service._rescue_close_blocked(EXECUTOR, "L1", unproved, executor) is False
    assert service._rescue_close_blocked(EXECUTOR, "L2", fresh, executor) is False


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


def test_restart_reseats_slot_after_lift_peek(tmp_path, monkeypatch):
    """Подъём снял живой слот (blank-px без цены), чекпойнт close_order записан:
    круг сажает слот заново до предложений, чужой записанный номер не затирается."""
    fresh = _fresh(blank=[_blank(EARLY)])
    lines = [_row("buy-1", ts=10), _row(EARLY, ts=11, role="market_close", sent=True)]
    seated = _level(close={"order_id": EARLY, "filled": True, "price": "101",
                           "order": {"executed_amount_base": "1"}})
    service, _repo, connector = _base(tmp_path, monkeypatch, fresh, [seated], lines)
    executor = _live_executor(connector)
    assert executor.grid_levels[0].active_close_order is None
    assert asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor, connector, PAIR)) is True
    assert executor.grid_levels[0].active_close_order.order_id == EARLY
    assert executor.grid_levels[0].active_close_order.order.completely_filled_event.is_set()
    assert service._rescue_close_blocked(EXECUTOR, "L1", fresh, executor) is True

    # чужой записанный номер не затирается
    foreign = _level(close={"order_id": "other", "filled": True})
    service2, _repo2, connector2 = _base(tmp_path, monkeypatch, fresh, [foreign], lines)
    executor2 = _live_executor(connector2)
    assert asyncio.run(service2._rescue_seat_filled_close(EXECUTOR, executor2, connector2, PAIR)) is False
    assert executor2.grid_levels[0].active_close_order is None


def test_live_slot_beats_checkpoint_for_the_round(tmp_path, monkeypatch):
    """Чекпойнт ещё buy-1, живой слот уже buy-2 — старый sell-early не сажается,
    уровень из предложений не выкидывается, чекпойнт не пишется."""
    fresh = _fresh(blank=[_blank(EARLY)])
    lines = [
        _row("buy-1", ts=10),
        _row("buy-2", ts=20),
        _row(EARLY, ts=11, role="market_close", sent=True),
    ]
    service, _repo, connector = _base(tmp_path, monkeypatch, fresh, [_level()], lines)
    executor = _live_executor(connector, open_id="buy-2")
    assert asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor, connector, PAIR)) is False
    assert executor.grid_levels[0].active_close_order is None
    assert executor.grid_levels[0].active_open_order.order_id == "buy-2"
    assert service._rescue_close_blocked(EXECUTOR, "L1", fresh, executor) is False
    parsed = service._partial_parsed(EXECUTOR)
    assert (parsed["levels"][0].get("close_order") or {}).get("order_id") is None

    # живего уровня нет вовсе — чекпойнт не пишется
    executor3 = _live_executor(connector)
    executor3.grid_levels = []
    assert asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor3, connector, PAIR)) is False


def test_slot_checkpoint_mismatch_blocks_even_with_later_proof(tmp_path, monkeypatch):
    """Колонка buy-1, слот buy-2, close с ts позже строки buy-2 и страница filled —
    ничего не сажается, колонка не пишется, уровень не выкидывается из предложений."""
    fresh = _fresh(blank=[_blank(EARLY)])
    lines = [
        _row("buy-1", ts=10),
        _row("buy-2", ts=20),
        _row(EARLY, ts=25, role="market_close", sent=True),
    ]
    service, _repo, connector = _base(tmp_path, monkeypatch, fresh, [_level()], lines)
    executor = _live_executor(connector, open_id="buy-2")
    assert asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor, connector, PAIR)) is False
    assert executor.grid_levels[0].active_close_order is None
    assert executor.grid_levels[0].active_open_order.order_id == "buy-2"
    assert service._rescue_close_blocked(EXECUTOR, "L1", fresh, executor) is False
    parsed = service._partial_parsed(EXECUTOR)
    assert (parsed["levels"][0].get("close_order") or {}).get("order_id") is None


def test_foreign_executor_close_is_not_seated(tmp_path, monkeypatch):
    """Чужой исполнитель с тем же level_id: его доказанная продажа не сажается
    и не режет предложения нашего уровня."""
    fresh = _fresh(blank=[_blank(EARLY)])
    lines = [
        _row("buy-1", ts=10),
        {"executor_id": "grid-other", "order_id": "other-buy", "level_id": "L1", "price": "100", "ts": 11},
        {"executor_id": "grid-other", "order_id": EARLY, "level_id": "L1", "price": "100", "ts": 12,
         "role": "market_close", "sent": True},
    ]
    service, _repo, connector = _base(tmp_path, monkeypatch, fresh, [_level()], lines)
    executor = _live_executor(connector)
    assert asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor, connector, PAIR)) is False
    assert executor.grid_levels[0].active_close_order is None
    assert service._rescue_close_blocked(EXECUTOR, "L1", fresh, executor) is False
    parsed = service._partial_parsed(EXECUTOR)
    assert (parsed["levels"][0].get("close_order") or {}).get("order_id") is None


def _real_grid(mid=Decimal("100")):
    from decimal import Decimal as D
    from hummingbot.strategy_v2.executors.grid_executor.grid_executor import GridExecutor
    from hummingbot.strategy_v2.executors.grid_executor.data_types import GridExecutorConfig, GridLevelStates
    from hummingbot.strategy_v2.executors.position_executor.data_types import TripleBarrierConfig
    from hummingbot.core.data_type.common import TradeType

    connector = SimpleNamespace(
        trading_rules={PAIR: SimpleNamespace(min_notional_size=D("0.1"),
                                              min_price_increment=D("0.01"),
                                              min_base_amount_increment=D("0.001"))},
        in_flight_orders={},
        _order_tracker=SimpleNamespace(lost_orders={}),
        remove_listener=lambda *a, **k: None,
    )

    def get_price_by_type(trading_pair, price_type):
        return mid

    connector.get_price_by_type = get_price_by_type

    async def quiet_sleep(_seconds):
        return None

    strategy = SimpleNamespace(
        connectors={"gate_io": connector},
        current_timestamp=1,
        buy=lambda *a, **k: "new-buy",
        sell=lambda *a, **k: "new-sell",
        cancel=lambda *a, **k: None,
    )
    config = GridExecutorConfig(
        timestamp=1, connector_name="gate_io", trading_pair=PAIR,
        start_price=D("99"), end_price=D("101"), limit_price=D("0"),
        side=TradeType.BUY, total_amount_quote=D("12"),
        triple_barrier_config=TripleBarrierConfig(take_profit=D("0.01")), leverage=1,
    )
    executor = GridExecutor(strategy=strategy, config=config)
    executor._sleep = quiet_sleep
    executor.control_close_order = _no_close_order(executor)
    return executor, connector


def _no_close_order(executor):
    calls = []

    async def close_order():
        calls.append("close-order")
        return None

    executor._close_order_calls = calls
    return close_order


def _stock_open_body(service):
    body = service._stock_filled_order(
        client_id="buy-1", trading_pair=PAIR, trade_name="BUY",
        position_name="OPEN", amount=Decimal("1"), price=Decimal("100"), leverage=1,
    )
    return body


def test_stock_task_resets_level_without_crash(tmp_path, monkeypatch):
    """Посадка кругом + штатный control_task: уровень COMPLETE сбрасывается в
    NOT_ACTIVE, AttributeError на completely_filled_event нет."""
    from hummingbot.strategy_v2.executors.grid_executor.data_types import GridLevelStates
    from hummingbot.strategy_v2.models.executors import TrackedOrder as T

    fresh = _fresh(blank=[_blank(EARLY)])
    lines = [_row("buy-1", ts=10, level_id="L0"), _row(EARLY, ts=11, level_id="L0", role="market_close", sent=True)]
    service, _repo, connector_stub = _base(tmp_path, monkeypatch, fresh, [_level(level_id="L0")], lines)
    executor, _conn = _real_grid()
    level = executor.grid_levels[0]
    open_body = _stock_open_body(service)
    level.active_open_order = T(order_id="buy-1")
    level.active_open_order.order = open_body
    executor.connectors["gate_io"] = _conn

    assert asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor, _conn, PAIR)) is True
    assert level.active_close_order is not None

    asyncio.run(executor.control_task())
    assert level.state == GridLevelStates.NOT_ACTIVE
    assert level.active_close_order is None


def test_stock_shutdown_finishes_with_seated_close(tmp_path, monkeypatch):
    """Такт 1: RUNNING, середина за end_price — только SHUTTING_DOWN. Такт 2 после
    посадки кругом: штатный stop(), TERMINATED, control_close_order не вызван."""
    from hummingbot.strategy_v2.executors.grid_executor.data_types import GridLevelStates
    from hummingbot.strategy_v2.models.executors import TrackedOrder as T
    from hummingbot.strategy_v2.executors.executor_base import RunnableStatus

    fresh = _fresh(blank=[_blank(EARLY)])
    lines = [_row("buy-1", ts=10, level_id="L0"), _row(EARLY, ts=11, level_id="L0", role="market_close", sent=True)]
    service, _repo, connector_stub = _base(tmp_path, monkeypatch, fresh, [_level(level_id="L0")], lines)
    executor, _conn = _real_grid(mid=Decimal("102"))
    executor._status = RunnableStatus.RUNNING
    level = executor.grid_levels[0]
    level.active_open_order = T(order_id="buy-1")
    level.active_open_order.order = _stock_open_body(service)
    executor.connectors["gate_io"] = _conn

    asyncio.run(executor.control_task())
    assert executor._status == RunnableStatus.SHUTTING_DOWN
    assert executor._close_order_calls == []

    assert asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor, _conn, PAIR)) is True
    asyncio.run(executor.control_task())
    assert executor._close_order_calls == []
    assert executor.is_closed is True
    assert executor.status.name == "TERMINATED"


def test_restart_body_from_checkpoint_carries_the_event(tmp_path, monkeypatch):
    """Полный order JSON из круга поднимается from_json с completely_filled_event —
    первый штатный шаг после подъёма не падает и сбрасывает уровень."""
    from hummingbot.core.data_type.in_flight_order import InFlightOrder
    from hummingbot.strategy_v2.executors.grid_executor.data_types import GridLevelStates
    from hummingbot.strategy_v2.models.executors import TrackedOrder as T

    fresh = _fresh(blank=[_blank(EARLY)])
    lines = [_row("buy-1", ts=10, level_id="L0"), _row(EARLY, ts=11, level_id="L0", role="market_close", sent=True)]
    service, _repo, connector_stub = _base(tmp_path, monkeypatch, fresh, [_level(level_id="L0")], lines)
    executor, _conn = _real_grid()
    level = executor.grid_levels[0]
    level.active_open_order = T(order_id="buy-1")
    level.active_open_order.order = _stock_open_body(service)
    executor.connectors["gate_io"] = _conn
    asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor, _conn, PAIR))

    parsed = service._partial_parsed(EXECUTOR)
    order_json = parsed["levels"][0]["close_order"]["order"]
    assert order_json.get("client_order_id") == EARLY

    lifted = T(order_id=EARLY)
    lifted.order = InFlightOrder.from_json(order_json)
    assert lifted.order.completely_filled_event.is_set()

    level.active_close_order = lifted
    asyncio.run(executor.control_task())
    assert level.state == GridLevelStates.NOT_ACTIVE


def test_partial_fill_size_is_not_a_full_close(tmp_path, monkeypatch):
    """Размер страницы меньше исполненной базы открытия — полное исполнение не
    сажается, уровень не выкидывается из предложений: остаток продаёт штатная сетка."""
    fresh = _fresh(blank=[_blank(EARLY, size="0.4")])
    lines = [_row("buy-1", ts=10), _row(EARLY, ts=11, role="market_close", sent=True)]
    service, _repo, connector = _base(tmp_path, monkeypatch, fresh, [_level()], lines)
    executor = _live_executor(connector)
    assert asyncio.run(service._rescue_seat_filled_close(EXECUTOR, executor, connector, PAIR)) is False
    assert executor.grid_levels[0].active_close_order is None
    assert service._rescue_close_blocked(EXECUTOR, "L1", fresh, executor) is False
    parsed = service._partial_parsed(EXECUTOR)
    assert (parsed["levels"][0].get("close_order") or {}).get("order_id") is None
