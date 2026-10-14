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


def test_brake_reads_the_mark_without_passing_the_place(monkeypatch):
    """Боевая лямбда принимает номер исполнителя. Имя места ей передавать нельзя."""
    from utils.executor_checkpoint import install_close_without_level

    seen = []

    def mark_now(executor_id="grid-a"):
        seen.append(executor_id)
        return True

    async def original_shutdown():
        return "ran"

    executor = SimpleNamespace(
        _close_brake=False,
        _close_seat_ready=True,
        _close_list_ready=True,
        _close_ban_ids=set(),
        _column_close_ids=[],
        _close_order=None,
        _close_holds=[],
        grid_levels=[],
        get_close_orders_to_create=lambda: ["level-sell"],
        control_close_order=lambda: None,
        control_shutdown_process=original_shutdown,
        stop=lambda: "stopped",
        _shutdown_close_proven=False,
    )
    assert install_close_without_level(executor, EXECUTOR, frozen=lambda eid=EXECUTOR: mark_now(eid))
    assert executor.get_close_orders_to_create() == []
    assert executor.stop() is None
    assert seen == [EXECUTOR, EXECUTOR]
    executor._shutdown_close_proven = True
    seen.clear()

    async def run():
        return await executor.control_shutdown_process()

    assert asyncio.run(run()) == "ran"
    assert executor.stop() == "stopped"
    assert seen == []


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

    def mint(*args, **kwargs):
        return "flat-1"

    import hummingbot.connector.utils as connector_utils
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    assert service._shutdown_remainder(EXECUTOR, fresh) == Decimal("7")
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert getattr(executor, "_status", None) != "TERMINATED"


def test_two_entries_share_one_number_and_a_hold_is_not_cancelled(monkeypatch, tmp_path):
    level = _level(level_id="L1", open_id="buy-1", filled=True)
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", sent=True, ts=10),
    ])
    body = json.loads(_checkpoint(level))
    body["holds"] = [{"order_id": "flat-1", "role": "market_close"}, {"order_id": "open-2"}]
    service, repo, connector = _service_with(monkeypatch, json.dumps(body))
    fresh = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "amount": "3", "left": "0"}])
    service._remember_tick_fresh(PAIR, fresh)
    executor, sold = _arm(monkeypatch, service, connector, None)
    minted = []

    def mint(*args, **kwargs):
        minted.append("new")
        return "new"

    import hummingbot.connector.utils as connector_utils
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)

    async def both():
        await asyncio.gather(
            service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh),
            service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh),
        )

    asyncio.run(both())
    assert minted == []
    assert "flat-1" not in service._stop_unfinished_ids(EXECUTOR, fresh)
    assert "open-2" in service._stop_unfinished_ids(EXECUTOR, fresh)


def test_unread_journal_does_not_mint_a_level_sell(monkeypatch, tmp_path):
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    executor = _stub_executor(connector, [])
    minted = []

    def mint(*args, **kwargs):
        minted.append("sell-new")
        return "sell-new"

    import hummingbot.connector.utils as connector_utils

    async def not_frozen(executor_id):
        return False

    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    monkeypatch.setattr(service, "_refresh_stop_mark", not_frozen)
    monkeypatch.setattr(service, "_partial_rows", lambda: None)
    asyncio.run(service._one_sell_after_fill_down(EXECUTOR, executor, connector, SimpleNamespace(id="L1")))
    assert minted == []
    assert service._recorded_market_close(EXECUTOR) is None


def test_live_own_sell_blocks_the_market_flatten(monkeypatch, tmp_path):
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", sent=True, ts=10),
        _row("sell-live", level_id="L1", role="close", sent=True, ts=12),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    fresh = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "amount": "7", "left": "0"}])
    assert service._shutdown_remainder(EXECUTOR, fresh) is None


def test_a_buy_without_role_is_the_remainder(monkeypatch, tmp_path):
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", sent=True, ts=10),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    fresh = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "amount": "8", "left": "0"}])
    assert service._shutdown_remainder(EXECUTOR, fresh) == Decimal("8")
    assert service._shutdown_close_proven(EXECUTOR) is False


def test_two_journal_lines_of_one_partial_sell_do_not_cover(monkeypatch, tmp_path):
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", sent=True, ts=10),
        _row("flat-1", level_id="", role="market_close", sent=False, ts=20),
        _row("flat-1", level_id="", role="market_close", sent=True, ts=21),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    fresh = _fresh(finished=[
        {"text": "buy-1", "finish_as": "filled", "amount": "10", "left": "0"},
        {"text": "flat-1", "finish_as": "filled", "amount": "4", "left": "0"},
    ])
    assert service._shutdown_remainder(EXECUTOR, fresh) == Decimal("6")


def test_missing_shutdown_sell_is_sent_again_as_the_same_number(monkeypatch, tmp_path):
    """Номер гашения есть в журнале, но его нет ни на книге, ни в finished.
    Шлём тот же номер, второй не чеканим."""
    level = _level(level_id="L1", open_id="buy-1", filled=True)
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", sent=True, ts=10),
        _row("flat-1", level_id="", role="market_close", sent=True, ts=20),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(level))
    fresh = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "amount": "9", "left": "0"}])
    service._remember_tick_fresh(PAIR, fresh)
    executor, sold = _arm(monkeypatch, service, connector, None)
    minted = []

    def mint(*args, **kwargs):
        minted.append("new")
        return "new"

    import hummingbot.connector.utils as connector_utils
    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    monkeypatch.setattr(checkpoint_mod, "install_connector_close_channel", lambda market: None)
    assert service._shutdown_remainder(EXECUTOR, fresh) == Decimal("9")
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert minted == []
    assert sold and sold[0][1] == Decimal("9")
    assert getattr(executor, "_shutdown_close_id", None) == "flat-1"
    assert service._shutdown_close_proven(EXECUTOR) is False
