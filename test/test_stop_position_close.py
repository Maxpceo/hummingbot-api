"""Пробы закрытия позиции сетки штатным гашением (план stop-position-close)."""

import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace

from test.test_stop_freeze import (
    _checkpoint,
    _journal,
    _level,
    _row,
    _service_with,
    _stub_executor,
)

EXECUTOR = "grid-a"
PAIR = "INJ-USDT"


def _fresh(finished=None):
    return {
        "opens_read": True,
        "opens_done": True,
        "finished_read": True,
        "finished_done": True,
        "opens": [],
        "finished": finished or [],
    }


def _arm(monkeypatch, service, connector, checkpoint_text):
    sold = []
    connector.sell = lambda *args, **kwargs: sold.append(args)
    executor = _stub_executor(connector, [])
    executor._strategy = SimpleNamespace(connectors={"gate_io": connector})
    executor.config.trading_pair = PAIR
    service._active_executors[EXECUTOR] = executor
    service._install_partial_guards(EXECUTOR, executor)

    async def no_stop(executor_id):
        return True

    async def fake_cas(executor_id, mutate, visible, target_id=None):
        body = json.loads(service._hold_rows[0]["checkpoint"])
        mutate(body)
        service._hold_rows[0]["checkpoint"] = json.dumps(body)
        return True

    monkeypatch.setattr(service, "_refresh_stop_mark", no_stop)
    monkeypatch.setattr(service, "_partial_cas", fake_cas)
    return executor, sold


def test_remainder_is_seated_before_the_sell_and_is_not_one(monkeypatch, tmp_path):
    level = _level(level_id="L1", open_id="buy-1", close_id="", take="0.10", filled=True)
    level["partial_bought"] = "10"
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", price="0.09487", sent=True, ts=10),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(level))
    fresh = _fresh(finished=[{
        "text": "buy-1", "finish_as": "filled", "amount": "10", "left": "0",
    }])
    service._remember_tick_fresh(PAIR, fresh)
    executor, sold = _arm(monkeypatch, service, connector, None)
    minted = []

    def mint(*args, **kwargs):
        minted.append("flat-1")
        return "flat-1"

    import hummingbot.connector.utils as connector_utils
    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    monkeypatch.setattr(checkpoint_mod, "install_connector_close_channel", lambda market: None)
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert minted == ["flat-1"]
    assert sold and sold[0][1] == Decimal("10")
    holds = json.loads(service._hold_rows[0]["checkpoint"]).get("holds") or []
    assert holds[0]["order_id"] == "flat-1"
    assert holds[0]["role"] == "market_close"
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert minted == ["flat-1"], "второй номер не чеканится"


def test_failed_seat_does_not_sell(monkeypatch, tmp_path):
    level = _level(level_id="L1", open_id="buy-1", filled=True)
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", sent=True, ts=10),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(level))
    fresh = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "amount": "4", "left": "0"}])
    service._remember_tick_fresh(PAIR, fresh)
    executor, sold = _arm(monkeypatch, service, connector, None)

    async def no_seat(*args, **kwargs):
        return False

    monkeypatch.setattr(service, "_seat_market_close", no_seat)
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert sold == []


def test_mark_lets_only_the_seated_close_through(monkeypatch, tmp_path):
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    executor, sold = _arm(monkeypatch, service, connector, None)
    executor._shutdown_close_id = "flat-1"
    executor._shutdown_close_seated = True

    async def run():
        assert await executor._refuse_frozen_close("flat-1") is False
        assert await executor._refuse_frozen_close("other") is True

    asyncio.run(run())


def test_recorded_market_close_blocks_a_second_fill_down_mint(monkeypatch, tmp_path):
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", sent=True, ts=10),
        _row("flat-1", level_id="", role="market_close", sent=True, ts=20),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level(level_id="L1", open_id="buy-1", filled=True)))
    executor = _stub_executor(connector, [])
    service._active_executors[EXECUTOR] = executor
    minted = []

    def mint(*args, **kwargs):
        minted.append("sell-new")
        return "sell-new"

    import hummingbot.connector.utils as connector_utils
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    async def not_frozen(executor_id):
        return False

    monkeypatch.setattr(service, "_refresh_stop_mark", not_frozen)
    asyncio.run(service._one_sell_after_fill_down(EXECUTOR, executor, connector, SimpleNamespace(id="L1")))
    assert minted == []
    assert service._recorded_market_close(EXECUTOR) == "flat-1"


def test_unproven_sell_does_not_finish_and_foreign_size_is_ignored(monkeypatch, tmp_path):
    level = _level(level_id="L1", open_id="buy-1", filled=True)
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", sent=True, ts=10),
        _row("foreign-buy", level_id="L9", role="open", sent=True, ts=11, executor_id="grid-b"),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(level))
    fresh = _fresh(finished=[
        {"text": "buy-1", "finish_as": "filled", "amount": "7", "left": "0"},
        {"text": "foreign-buy", "finish_as": "filled", "amount": "100", "left": "0"},
    ])
    assert service._shutdown_remainder(EXECUTOR, fresh) == Decimal("7")
    service._remember_tick_fresh(PAIR, fresh)
    executor, sold = _arm(monkeypatch, service, connector, None)
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert executor._close_order is not None
    assert getattr(executor, "_status", None) != "TERMINATED"
