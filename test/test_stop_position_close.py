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

    async def no_phrase(order_id):
        return False

    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    monkeypatch.setattr(checkpoint_mod, "install_connector_close_channel", lambda market: None)
    monkeypatch.setattr(service, "_shutdown_close_refusal", no_phrase)
    assert service._shutdown_remainder(EXECUTOR, fresh) == Decimal("9")
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert minted == []
    assert sold and sold[0][1] == Decimal("9")
    assert getattr(executor, "_shutdown_close_id", None) == "flat-1"
    assert service._shutdown_close_proven(EXECUTOR) is False


def test_shutdown_sell_reaches_create_as_a_market_order(monkeypatch, tmp_path):
    """Канал не подменяем: в создание заявки уходят MARKET и сторона продажи."""
    level = _level(level_id="L1", open_id="buy-1", filled=True)
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", sent=True, ts=10),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(level))
    fresh = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "amount": "6", "left": "0"}])
    service._remember_tick_fresh(PAIR, fresh)
    executor, sold = _arm(monkeypatch, service, connector, None)
    created = []

    async def create_order(**kwargs):
        action = kwargs.get("position_action")
        if getattr(action, "name", None) != "CLOSE":
            raise ValueError(f"Invalid position action {action}")
        created.append(kwargs)

    async def place_order(*args, **kwargs):
        return None

    connector._create_order = create_order
    connector._place_order = place_order

    def mint(*args, **kwargs):
        return "flat-1"

    import hummingbot.connector.utils as connector_utils
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)

    async def go():
        await service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh)
        await asyncio.sleep(0)

    asyncio.run(go())
    assert created
    assert created[0]["order_id"] == "flat-1"
    assert created[0]["amount"] == Decimal("6")
    assert created[0]["order_type"].name == "MARKET"
    assert created[0]["trade_type"].name == "SELL"
    assert created[0]["position_action"].name == "CLOSE"
    assert executor._close_order.is_done is False


def test_finished_partial_close_mints_the_next_number_once(monkeypatch, tmp_path):
    level = _level(level_id="L1", open_id="buy-1", filled=True)
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", sent=True, ts=10),
        _row("flat-1", level_id="", role="market_close", sent=True, ts=20),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(level))
    fresh = _fresh(finished=[
        {"text": "buy-1", "finish_as": "filled", "amount": "10", "left": "0"},
        {"text": "flat-1", "finish_as": "filled", "amount": "4", "left": "0"},
    ])
    service._remember_tick_fresh(PAIR, fresh)
    executor, sold = _arm(monkeypatch, service, connector, None)
    minted = []

    def mint(*args, **kwargs):
        minted.append("flat-2")
        return "flat-2"

    import hummingbot.connector.utils as connector_utils
    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    monkeypatch.setattr(checkpoint_mod, "install_connector_close_channel", lambda market: None)
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert minted == ["flat-2"]
    assert sold and sold[0][1] == Decimal("6")
    assert getattr(executor, "_shutdown_close_id", None) == "flat-2"


def _two_shutdown_numbers(tmp_path, monkeypatch):
    """Память пустая: первый номер гашения закрыт частью, второй записан, но не в finished."""
    level = _level(level_id="L1", open_id="buy-1", filled=True)
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", sent=True, ts=10),
        _row("flat-1", level_id="", role="market_close", sent=True, ts=20),
        _row("flat-2", level_id="", role="market_close", sent=True, ts=30),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(level))
    fresh = _fresh(finished=[
        {"text": "buy-1", "finish_as": "filled", "amount": "10", "left": "0"},
        {"text": "flat-1", "finish_as": "filled", "amount": "4", "left": "0"},
    ])
    service._remember_tick_fresh(PAIR, fresh)
    executor, sold = _arm(monkeypatch, service, connector, None)
    return service, connector, executor, sold, fresh


def test_restart_resends_the_unfinished_shutdown_number(monkeypatch, tmp_path):
    """Новый объект исполнителя после перезапуска: памяти нет, а второй номер
    уже в журнале и не в finished. Третьего номера нет, уходит тот же второй."""
    service, connector, executor, sold, fresh = _two_shutdown_numbers(tmp_path, monkeypatch)
    minted = []

    def mint(*args, **kwargs):
        minted.append("flat-3")
        return "flat-3"

    import hummingbot.connector.utils as connector_utils
    import utils.executor_checkpoint as checkpoint_mod

    async def no_phrase(order_id):
        return False

    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    monkeypatch.setattr(checkpoint_mod, "install_connector_close_channel", lambda market: None)
    monkeypatch.setattr(service, "_shutdown_close_refusal", no_phrase)
    assert getattr(executor, "_shutdown_close_id", None) in (None, "")
    assert service._shutdown_remainder(EXECUTOR, fresh) == Decimal("6")
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert minted == []
    assert sold and sold[0][1] == Decimal("6")
    assert getattr(executor, "_shutdown_close_id", None) == "flat-2"


def test_restart_with_a_live_unfinished_number_is_silent(monkeypatch, tmp_path):
    """Тот же журнал, но второй номер ещё в полёте. Тишина: ни продажи, ни чеканки."""
    service, connector, executor, sold, fresh = _two_shutdown_numbers(tmp_path, monkeypatch)
    connector.in_flight_orders = {"flat-2": object()}
    minted = []

    def mint(*args, **kwargs):
        minted.append("flat-3")
        return "flat-3"

    import hummingbot.connector.utils as connector_utils
    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    monkeypatch.setattr(checkpoint_mod, "install_connector_close_channel", lambda market: None)
    assert service._shutdown_remainder(EXECUTOR, fresh) == Decimal("6")
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert minted == []
    assert sold == []


def _gone_error(code="51400"):
    return OSError(
        "Error cancelling order buy-2: "
        + str({"code": "1", "data": [{"clOrdId": "buy-2", "sCode": code, "sMsg": "gone"}]})
    )


def test_exchange_gone_stops_the_cancel_and_a_clock_error_does_not(monkeypatch, tmp_path):
    """51400 пишет конец и больше не шлёт отмену. 50102 конец не пишет."""
    _journal(tmp_path, monkeypatch, [
        _row("buy-2", level_id="L1", role="open", sent=True, ts=10),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level(level_id="L1", open_id="buy-2")))
    fresh = _fresh()
    service._remember_tick_fresh(PAIR, fresh)
    executor, _sold = _arm(monkeypatch, service, connector, None)
    calls = []

    async def place_cancel(order_id, row):
        calls.append(str(order_id))
        raise _gone_error()

    connector._place_cancel = place_cancel
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "buy-2", PAIR, EXECUTOR)) is True
    assert service._exchange_gone_end(EXECUTOR, "buy-2") is True
    assert "buy-2" not in service._stop_unfinished_ids(EXECUTOR, fresh)
    calls.clear()
    service.__dict__.setdefault("_stop_finish_last", {})[EXECUTOR] = 0.0
    asyncio.run(service._stop_deliver_cancels(EXECUTOR, executor, connector, PAIR))
    assert calls == []

    async def clock_cancel(order_id, row):
        raise _gone_error("50102")

    connector._place_cancel = clock_cancel
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "buy-3", PAIR, EXECUTOR)) is False
    assert service._exchange_gone_end(EXECUTOR, "buy-3") is False


def test_exchange_gone_buy_without_finished_does_not_prove_the_remainder(monkeypatch, tmp_path):
    """Покупка с концом 51400 и без finished остаток не доказывает.
    finished с исполнением входит в остаток. Снятие с нулём не морозит."""
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", sent=True, ts=10),
        _row("buy-2", level_id="L2", role="open", sent=True, ts=11),
    ])
    body = json.loads(_checkpoint(_level(level_id="L1", open_id="buy-1", filled=True)))
    body["partial_ends"] = {"buy-2": {"kind": "gone", "order_ids": ["buy-2"], "exchange": "51400"}}
    service, repo, connector = _service_with(monkeypatch, json.dumps(body))
    fresh = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "amount": "10", "left": "0"}])
    service._remember_tick_fresh(PAIR, fresh)
    executor, sold = _arm(monkeypatch, service, connector, None)
    assert service._shutdown_remainder(EXECUTOR, fresh) is None
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert sold == []

    filled = _fresh(finished=[
        {"text": "buy-1", "finish_as": "filled", "amount": "10", "left": "0"},
        {"text": "buy-2", "finish_as": "filled", "amount": "10", "left": "0"},
    ])
    assert service._shutdown_remainder(EXECUTOR, filled) == Decimal("20")

    body["partial_ends"]["sell-9"] = {"kind": "gone", "order_ids": ["sell-9"], "exchange": "51400"}
    service._hold_rows[0]["checkpoint"] = json.dumps(body)
    from utils.executor_checkpoint import reset_ledger_process_state
    ledger = tmp_path / "bots" / "data" / "order-ledger.jsonl"
    ledger.write_text("".join(json.dumps(line) + "\n" for line in [
        _row("buy-1", level_id="L1", role="open", sent=True, ts=10),
        _row("sell-9", level_id="L1", role="close", sent=True, ts=12),
    ]), encoding="utf-8")
    reset_ledger_process_state()
    cancelled = _fresh(finished=[
        {"text": "buy-1", "finish_as": "filled", "amount": "10", "left": "0"},
        {"text": "sell-9", "status": "closed", "finish_as": "cancelled", "amount": "4", "left": "4"},
    ])
    assert service._shutdown_remainder(EXECUTOR, cancelled) == Decimal("10")
    unproven = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "amount": "10", "left": "0"}])
    assert service._shutdown_remainder(EXECUTOR, unproven) is None

    okx_cancel = _fresh(finished=[
        {"text": "buy-1", "state": "filled", "fill_size": "10"},
        {"text": "sell-9", "state": "canceled", "fill_size": "0"},
    ])
    assert service._shutdown_remainder(EXECUTOR, okx_cancel) == Decimal("10")
    okx_fill = _fresh(finished=[
        {"text": "buy-1", "state": "filled", "fill_size": "10"},
        {"text": "sell-9", "state": "filled", "fill_size": "4"},
    ])
    assert service._shutdown_remainder(EXECUTOR, okx_fill) == Decimal("6")


def test_okx_zero_cancel_does_not_freeze_the_remainder(monkeypatch, tmp_path):
    """Строка OKX canceled и fill_size 0 не морозит остаток. filled входит."""
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", sent=True, ts=10),
        _row("buy-2", level_id="L2", role="open", sent=True, ts=11),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    cancelled = _fresh(finished=[
        {"text": "buy-1", "state": "filled", "fill_size": "10"},
        {"text": "buy-2", "state": "canceled", "fill_size": "0"},
    ])
    assert service._shutdown_remainder(EXECUTOR, cancelled) == Decimal("10")
    filled = _fresh(finished=[
        {"text": "buy-1", "state": "filled", "fill_size": "10"},
        {"text": "buy-2", "state": "filled", "fill_size": "10"},
    ])
    assert service._shutdown_remainder(EXECUTOR, filled) == Decimal("20")


def test_unread_book_does_not_write_the_gone_end(monkeypatch, tmp_path):
    """Книга этого тика не прочитана — конец не писать и отмену не считать доставленной."""
    _journal(tmp_path, monkeypatch, [
        _row("buy-2", level_id="L1", role="open", sent=True, ts=10),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level(level_id="L1", open_id="buy-2")))
    unread = _fresh()
    unread["opens_read"] = False
    service._remember_tick_fresh(PAIR, unread)
    executor, _sold = _arm(monkeypatch, service, connector, None)

    async def place_cancel(order_id, row):
        raise _gone_error()

    connector._place_cancel = place_cancel
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "buy-2", PAIR, EXECUTOR)) is False
    assert service._exchange_gone_end(EXECUTOR, "buy-2") is False


def _raise_gone(connector):
    async def place_cancel(order_id, row):
        raise _gone_error()

    connector._place_cancel = place_cancel


def test_other_pair_tick_keeps_a_ready_book_and_an_open_number_blocks(monkeypatch, tmp_path):
    """Чужая пара сдвинула номер шага. Готовая книга без номера пишет конец.
    Номер в открытых этой книги конец не пишет."""
    _journal(tmp_path, monkeypatch, [
        _row("buy-2", level_id="L1", role="open", sent=True, ts=10),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level(level_id="L1", open_id="buy-2")))
    service._remember_tick_fresh(PAIR, _fresh())
    service.__dict__["_price_tick"] = 9
    executor, _sold = _arm(monkeypatch, service, connector, None)
    _raise_gone(connector)
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "buy-2", PAIR, EXECUTOR)) is True
    assert service._exchange_gone_end(EXECUTOR, "buy-2") is True

    service._hold_rows[0]["checkpoint"] = _checkpoint(_level(level_id="L1", open_id="buy-4"))
    service._remember_tick_fresh(PAIR, _fresh())
    service._tick_reads[PAIR]["fresh"]["opens"] = [{"text": "buy-4"}]
    service.__dict__["_price_tick"] = 11
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "buy-4", PAIR, EXECUTOR)) is False
    assert service._exchange_gone_end(EXECUTOR, "buy-4") is False


def test_same_pair_waiting_for_a_new_book_does_not_write_the_old_one(monkeypatch, tmp_path):
    """Эта пара уже увеличила номер и ещё не положила книгу. Отмена чтения
    старый слот концу не отдаёт."""
    _journal(tmp_path, monkeypatch, [
        _row("buy-2", level_id="L1", role="open", sent=True, ts=10),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level(level_id="L1", open_id="buy-2")))
    service._remember_tick_fresh(PAIR, _fresh())
    executor, _sold = _arm(monkeypatch, service, connector, None)
    _raise_gone(connector)

    async def hung(*args, **kwargs):
        await asyncio.sleep(30)
        return _fresh()

    monkeypatch.setattr(service, "_fresh_raw_lists", hung)

    async def run():
        task = asyncio.create_task(service._store_tick_fresh(connector, PAIR))
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert await service._stop_deliver_cancel(executor, connector, "buy-2", PAIR, EXECUTOR) is False

    asyncio.run(run())
    assert service._exchange_gone_end(EXECUTOR, "buy-2") is False


def test_ready_book_is_taken_while_the_same_pair_is_still_reading(monkeypatch, tmp_path):
    """Книга пары уже прочитана и готова, чтение соседней сетки той же пары ещё
    идёт. Ответ 51400 пишет конец: готовая книга не считается непрочитанной.
    Живой прогон 15 октября: четыре сетки DOGE одной пары читают книгу каждый
    тик, окно «книга ещё не в слоте» не закрывалось ни разу — 2328 ответов
    51400 не записали ни одного конца."""
    _journal(tmp_path, monkeypatch, [
        _row("buy-2", level_id="L1", role="open", sent=True, ts=10),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level(level_id="L1", open_id="buy-2")))
    service._remember_tick_fresh(PAIR, _fresh())
    executor, _sold = _arm(monkeypatch, service, connector, None)
    _raise_gone(connector)

    async def slow(*args, **kwargs):
        await asyncio.sleep(30)
        return _fresh()

    monkeypatch.setattr(service, "_fresh_raw_lists", slow)

    async def run():
        task = asyncio.create_task(service._store_tick_fresh(connector, PAIR))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert task.done() is False, "чтение соседней сетки ещё идёт"
        assert await service._stop_deliver_cancel(executor, connector, "buy-2", PAIR, EXECUTOR) is True
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
    assert service._exchange_gone_end(EXECUTOR, "buy-2") is True


def test_live_okx_closed_page_shape_is_readable():
    """Форма страницы демо-счёта 15 октября, без настоящих номеров.

    Чтение без заявок: 68 строк, все state=filled, 16 с пустой ценой,
    время внутри окна, страница короче 100. Такая страница прочитана.
    """
    from utils.executor_checkpoint import parse_okx_closed_page

    begin, end = 1_000, 5_000
    rows = [
        {"clOrdId": f"filled-{i}", "ordId": str(i), "px": "0.09", "side": "buy",
         "state": "filled", "cTime": "2000", "accFillSz": "1"}
        for i in range(52)
    ]
    rows.extend(
        {"clOrdId": f"market-{i}", "ordId": str(100 + i), "px": "", "side": "sell",
         "state": "filled", "cTime": "2100", "accFillSz": "1"}
        for i in range(16)
    )
    parsed = parse_okx_closed_page({"code": "0", "data": rows}, begin, end)
    assert parsed.get("read") is True
    assert parsed.get("done") is True
    assert len(parsed.get("orders") or []) == 52
    assert all(row.get("state") == "filled" for row in parsed.get("orders") or [])


def test_closed_page_skips_a_row_outside_the_window_and_keeps_an_unknown_state_unread():
    from utils.executor_checkpoint import parse_okx_closed_page

    begin, end = 1_000, 2_000
    page = {
        "code": "0",
        "data": [
            {"clOrdId": "in", "ordId": "1", "px": "1", "side": "buy", "state": "filled", "cTime": "1500", "accFillSz": "2"},
            {"clOrdId": "out", "ordId": "2", "px": "1", "side": "buy", "state": "canceled", "cTime": "50", "accFillSz": "0"},
        ],
    }
    parsed = parse_okx_closed_page(page, begin, end)
    assert parsed.get("read") is True
    assert [row.get("text") for row in parsed.get("orders") or []] == ["in"]
    unknown = {
        "code": "0",
        "data": [
            {"clOrdId": "odd", "ordId": "3", "px": "1", "side": "buy", "state": "live", "cTime": "1500", "accFillSz": "0"},
        ],
    }
    assert parse_okx_closed_page(unknown, begin, end).get("read") is False
    empty_px = {
        "code": "0",
        "data": [
            {"clOrdId": "mkt", "ordId": "4", "px": "", "side": "sell", "state": "filled", "cTime": "1500", "accFillSz": "1"},
            {"clOrdId": "in2", "ordId": "5", "px": "1", "side": "buy", "state": "filled", "cTime": "1600", "accFillSz": "1"},
        ],
    }
    kept = parse_okx_closed_page(empty_px, begin, end)
    assert kept.get("read") is True
    assert [row.get("text") for row in kept.get("orders") or []] == ["in2"]


def _arm_with_task(monkeypatch, service, connector):
    """Каркас как у _arm, но у executor есть control_task — обёртка гашения вешается на него."""
    sold = []
    connector.sell = lambda *args, **kwargs: sold.append(args)
    executor = _stub_executor(connector, [])

    async def original_task(*args, **kwargs):
        executor._original_task_called = True

    executor.control_task = original_task
    executor._strategy = SimpleNamespace(connectors={"gate_io": connector})
    executor.config.trading_pair = PAIR
    service._active_executors[EXECUTOR] = executor
    service._install_partial_guards(EXECUTOR, executor)

    async def fake_cas(executor_id, mutate, visible, target_id=None):
        body = json.loads(service._hold_rows[0]["checkpoint"])
        mutate(body)
        service._hold_rows[0]["checkpoint"] = json.dumps(body)
        return True

    monkeypatch.setattr(service, "_partial_cas", fake_cas)
    return executor, sold


def _run_task_with_neighbour_read(monkeypatch, service, connector, executor, book, holder):
    """Обход гашения, в котором соседняя сетка той же пары уже читает книгу.

    Первое чтение (своё) кладёт готовую книгу в слот; соседнее начинается в окне
    `_refresh_stop_mark`, увеличивает общий номер шага и висит — `_tick_fresh`
    голоден, книга пары в слоте готова.
    """
    reads = {"n": 0}

    async def read(*args, **kwargs):
        reads["n"] += 1
        if reads["n"] > 1:
            await asyncio.sleep(30)
        return dict(book)

    async def freeze_and_start_neighbour(executor_id):
        holder["task"] = asyncio.create_task(service._store_tick_fresh(connector, PAIR))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return True

    async def no_cancels(*args, **kwargs):
        return None

    monkeypatch.setattr(service, "_fresh_raw_lists", read)
    monkeypatch.setattr(service, "_refresh_stop_mark", freeze_and_start_neighbour)
    monkeypatch.setattr(service, "_stop_deliver_cancels", no_cancels)
    monkeypatch.setattr(service, "_stop_mark_now", lambda executor_id: True)

    def mint(*args, **kwargs):
        return "flat-1"

    import hummingbot.connector.utils as connector_utils
    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    monkeypatch.setattr(checkpoint_mod, "install_connector_close_channel", lambda market: None)


async def _cancel_neighbour(holder):
    task = holder.get("task")
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_shutdown_sell_is_sent_when_the_step_counter_moves_past_the_book(monkeypatch, tmp_path):
    """Продажа остатка уходит, хотя общий номер шага уже сдвинула соседняя сетка:
    чтение той же пары в полёте, `_tick_fresh` голоден, книга пары в слоте готова."""
    level = _level(level_id="L1", open_id="buy-1", filled=True)
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", sent=True, ts=10),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(level))
    book = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "amount": "10", "left": "0"}])
    executor, sold = _arm_with_task(monkeypatch, service, connector)
    holder = {}
    _run_task_with_neighbour_read(monkeypatch, service, connector, executor, book, holder)

    async def run():
        await executor.control_task()
        assert service._tick_fresh(PAIR) is None, "прежний источник книги голоден"
        await _cancel_neighbour(holder)

    asyncio.run(run())
    assert sold and sold[0][1] == Decimal("10"), "продажа доказанного остатка должна уйти"
    assert getattr(executor, "_shutdown_close_id", None) == "flat-1"


def test_shutdown_sell_is_not_sent_on_an_unready_book(monkeypatch, tmp_path):
    """Книга пары не готова — продажа не уходит, хотя соседняя сетка и не мешает."""
    level = _level(level_id="L1", open_id="buy-1", filled=True)
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", sent=True, ts=10),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(level))
    book = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "amount": "10", "left": "0"}])
    book["opens_read"] = False
    executor, sold = _arm_with_task(monkeypatch, service, connector)
    holder = {}
    _run_task_with_neighbour_read(monkeypatch, service, connector, executor, book, holder)

    async def run():
        await executor.control_task()
        await _cancel_neighbour(holder)

    asyncio.run(run())
    assert sold == [], "непрочитанная книга — не доказательство остатка"


def test_shutdown_close_proven_reads_the_pair_book(monkeypatch, tmp_path):
    """Флаг доказательства остатка считается по книге пары, не по голодному `_tick_fresh`:
    остаток доказан нулём — тормоз control_shutdown отпускается, не доказан — держит."""
    level = _level(level_id="L1", open_id="buy-1", filled=True)
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", sent=True, ts=10),
        _row("flat-1", level_id="", role="market_close", sent=True, ts=20),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(level))
    proven = _fresh(finished=[
        {"text": "buy-1", "finish_as": "filled", "amount": "10", "left": "0"},
        {"text": "flat-1", "finish_as": "filled", "amount": "10", "left": "0"},
    ])
    service._remember_tick_fresh(PAIR, proven)
    executor, _sold = _arm(monkeypatch, service, connector, None)
    service.__dict__["_price_tick"] = service.__dict__.get("_price_tick", 1) + 1
    assert service._tick_fresh(PAIR) is None, "прежний источник книги голоден"
    assert service._shutdown_close_proven(EXECUTOR) is True

    unproven = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "amount": "10", "left": "0"}])
    service._remember_tick_fresh(PAIR, unproven)
    service.__dict__["_price_tick"] = service.__dict__.get("_price_tick", 1) + 1
    assert service._tick_fresh(PAIR) is None
    assert service._shutdown_close_proven(EXECUTOR) is False


class OkxPerpetualDerivative:
    """Имя класса включает okx и perpetual: площадка и тип инструмента."""

    def __init__(self, contract_size=Decimal("1000")):
        self.in_flight_orders = {}
        self._order_tracker = SimpleNamespace(lost_orders={})
        self._contract_sizes = {PAIR: contract_size} if contract_size is not None else {}
        self.client_order_id_prefix = "okx"
        self.client_order_id_max_length = 32

    def _format_size_to_amount(self, trading_pair, size):
        return Decimal(str(size)) * self._contract_sizes[trading_pair]


NOW = 1_700_000_000.0


def _freeze_now(monkeypatch):
    monkeypatch.setattr("services.executor_service.time.time", lambda: NOW)


def _archive_row(text, fill_size, price="0.1", state="filled"):
    return {
        "text": text,
        "order_id": text,
        "state": state,
        "fill_size": fill_size,
        "price": price,
    }


def test_old_buy_is_sold_from_the_archive_in_coins(monkeypatch, tmp_path):
    """Покупка старше 7 дней берётся из архива. В продажу уходят монеты.
    Чужой номер окно и сумму не меняет. В слот пары архив не пишется."""
    _freeze_now(monkeypatch)
    old = NOW - 10 * 24 * 60 * 60
    foreign = NOW - 20 * 24 * 60 * 60
    _journal(tmp_path, monkeypatch, [
        _row("buy-old", level_id="L1", role="open", sent=True, ts=old),
        _row("foreign-old", level_id="L9", role="open", sent=True, ts=foreign, executor_id="grid-b"),
    ])
    connector = OkxPerpetualDerivative()
    service, repo, connector = _service_with(
        monkeypatch, _checkpoint(_level(level_id="L1", open_id="buy-old", filled=True)), connector=connector,
    )
    book = _fresh(finished=[])
    executor, sold = _arm_with_task(monkeypatch, service, connector)
    holder = {}
    _run_task_with_neighbour_read(monkeypatch, service, connector, executor, book, holder)
    calls = []

    async def pages(connector, symbol, pair, inst_type, archive, begin, end):
        calls.append((symbol, pair, inst_type, archive, begin, end))
        return {
            "read": True,
            "done": True,
            "orders": [
                _archive_row("buy-old", "2.535"),
                _archive_row("foreign-old", "9"),
            ],
        }

    monkeypatch.setattr(service, "_okx_closed_pages", pages)
    minted = []

    def mint(*args, **kwargs):
        minted.append("flat-1")
        return "flat-1"

    import hummingbot.connector.utils as connector_utils
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)

    seen = {}

    async def run():
        await executor.control_task()
        seen["slot"] = service._pair_book(PAIR)
        seen["tick"] = service._tick_fresh(PAIR)
        await _cancel_neighbour(holder)

    asyncio.run(run())
    assert len(calls) == 1
    assert calls[0][0] == PAIR and calls[0][1] == PAIR
    assert calls[0][2] == "SWAP" and calls[0][3] is True
    assert calls[0][4] == str(int((old - 300) * 1000))
    assert calls[0][5] == str(int(NOW * 1000))
    assert sold and sold[0][1] == Decimal("2535")
    assert minted == ["flat-1"]
    assert seen["tick"] is None
    assert all(row.get("text") != "buy-old" for row in (seen["slot"] or {}).get("finished") or [])
    assert executor._shutdown_close_proven is False


def test_unread_or_empty_archive_does_not_sell(monkeypatch, tmp_path):
    """Непрочитанный архив и пустой архив — не ноль и не продажа."""
    _freeze_now(monkeypatch)
    old = NOW - 10 * 24 * 60 * 60
    _journal(tmp_path, monkeypatch, [
        _row("buy-old", level_id="L1", role="open", sent=True, ts=old),
    ])
    connector = OkxPerpetualDerivative()
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()), connector=connector)
    fresh = _fresh(finished=[])
    service._remember_tick_fresh(PAIR, fresh)
    executor, sold = _arm(monkeypatch, service, connector, None)

    async def unread(*args, **kwargs):
        return None

    monkeypatch.setattr(service, "_okx_closed_pages", unread)
    book = asyncio.run(service._shutdown_remainder_book(EXECUTOR, connector, PAIR, fresh))
    assert book is None
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert sold == []

    async def truncated(*args, **kwargs):
        return {"read": True, "done": False, "orders": [_archive_row("buy-old", "2")]}

    monkeypatch.setattr(service, "_okx_closed_pages", truncated)
    assert asyncio.run(service._shutdown_remainder_book(EXECUTOR, connector, PAIR, fresh)) is None

    async def empty(*args, **kwargs):
        return {"read": True, "done": True, "orders": []}

    monkeypatch.setattr(service, "_okx_closed_pages", empty)
    empty_book = asyncio.run(service._shutdown_remainder_book(EXECUTOR, connector, PAIR, fresh))
    assert empty_book is not None
    assert service._shutdown_remainder(EXECUTOR, empty_book) is None
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, empty_book))
    assert sold == []
    assert "buy-old" not in [row.get("text") for row in fresh.get("finished") or []]


def test_too_old_or_unmarked_buy_does_not_ask_the_archive(monkeypatch, tmp_path):
    """Старше 90 дней и номер без метки архив не спрашивают. Продажи нет."""
    _freeze_now(monkeypatch)
    ancient = NOW - 100 * 24 * 60 * 60
    _journal(tmp_path, monkeypatch, [
        _row("buy-ancient", level_id="L1", role="open", sent=True, ts=ancient),
        _row("buy-old", level_id="L2", role="open", sent=True, ts=NOW - 10 * 24 * 60 * 60),
    ])
    connector = OkxPerpetualDerivative()
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()), connector=connector)
    fresh = _fresh(finished=[])
    calls = []

    async def pages(*args, **kwargs):
        calls.append(args)
        return {"read": True, "done": True, "orders": [_archive_row("buy-ancient", "1")]}

    monkeypatch.setattr(service, "_okx_closed_pages", pages)
    assert asyncio.run(service._shutdown_remainder_book(EXECUTOR, connector, PAIR, fresh)) is None
    assert calls == []

    from utils.executor_checkpoint import reset_ledger_process_state
    ledger = tmp_path / "bots" / "data" / "order-ledger.jsonl"
    ledger.write_text(
        json.dumps(_row("buy-blank", level_id="L1", role="open", sent=True, ts=None)) + "\n",
        encoding="utf-8",
    )
    reset_ledger_process_state()
    assert asyncio.run(service._shutdown_remainder_book(EXECUTOR, connector, PAIR, fresh)) is None
    assert calls == []


def test_live_cancel_wins_over_archive_and_foreign_price_does_not_count(monkeypatch, tmp_path):
    """Живое снятие не заменяется архивом. Чужой номер и пустой clOrdId в сумму не входят."""
    _freeze_now(monkeypatch)
    old = NOW - 10 * 24 * 60 * 60
    _journal(tmp_path, monkeypatch, [
        _row("buy-old", level_id="L1", role="open", sent=True, ts=old),
        _row("buy-zero", level_id="L2", role="open", sent=True, ts=old),
    ])
    connector = OkxPerpetualDerivative()
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()), connector=connector)
    fresh = _fresh(finished=[{
        "text": "buy-zero", "state": "canceled", "fill_size": "0", "price": "0.1",
    }])

    async def pages(*args, **kwargs):
        return {
            "read": True,
            "done": True,
            "orders": [
                _archive_row("buy-old", "1"),
                _archive_row("buy-zero", "9", state="filled"),
                _archive_row("foreign-old", "8"),
                {"order_id": "", "state": "filled", "fill_size": "50", "price": "0.1"},
            ],
        }

    monkeypatch.setattr(service, "_okx_closed_pages", pages)
    book = asyncio.run(service._shutdown_remainder_book(EXECUTOR, connector, PAIR, fresh))
    assert book is not None
    assert service._shutdown_remainder(EXECUTOR, book) == Decimal("1000")
    assert not any(row.get("fill_size") == "50" for row in book.get("finished") or [])
    zero = next(row for row in book["finished"] if row.get("text") == "buy-zero")
    assert zero.get("fill_size") == "0"
    assert fresh["finished"][0]["fill_size"] == "0"


def test_missing_contract_size_and_mixed_units_do_not_sell(monkeypatch, tmp_path):
    """Нет размера контракта — не продаём. amount/left не умножаем. Смесь форм — не продаём."""
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", sent=True, ts=NOW - 60),
    ])
    connector = OkxPerpetualDerivative(contract_size=None)
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()), connector=connector)
    fresh = _fresh(finished=[{"text": "buy-1", "state": "filled", "fill_size": "2"}])
    assert asyncio.run(service._shutdown_remainder_book(EXECUTOR, connector, PAIR, fresh)) is None

    sized = OkxPerpetualDerivative()
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()), connector=sized)
    coins = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "amount": "10", "left": "0"}])
    book = asyncio.run(service._shutdown_remainder_book(EXECUTOR, sized, PAIR, coins))
    assert book is not None
    assert service._shutdown_remainder(EXECUTOR, book) == Decimal("10")
    executor, sold = _arm(monkeypatch, service, sized, None)

    def mint(*args, **kwargs):
        return "flat-1"

    import hummingbot.connector.utils as connector_utils
    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    monkeypatch.setattr(checkpoint_mod, "install_connector_close_channel", lambda market: None)
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, sized, PAIR, book))
    assert sold and sold[0][1] == Decimal("10")

    from utils.executor_checkpoint import reset_ledger_process_state
    ledger = tmp_path / "bots" / "data" / "order-ledger.jsonl"
    ledger.write_text("".join(json.dumps(line) + "\n" for line in [
        _row("buy-coin", level_id="L1", role="open", sent=True, ts=NOW - 60),
        _row("buy-raw", level_id="L2", role="open", sent=True, ts=NOW - 60),
    ]), encoding="utf-8")
    reset_ledger_process_state()
    mixed = _fresh(finished=[
        {"text": "buy-coin", "finish_as": "filled", "amount": "10", "left": "0"},
        {"text": "buy-raw", "state": "filled", "fill_size": "2"},
    ])
    assert asyncio.run(service._shutdown_remainder_book(EXECUTOR, sized, PAIR, mixed)) is None


def test_archive_copy_proves_zero_and_the_pair_book_alone_does_not(monkeypatch, tmp_path):
    """Флаг нуля считается по копии с архивом и в монетах. Голая книга пары старую покупку не закрывает."""
    _freeze_now(monkeypatch)
    old = NOW - 12 * 24 * 60 * 60
    _journal(tmp_path, monkeypatch, [
        _row("buy-old", level_id="L1", role="open", sent=True, ts=old),
        _row("flat-1", level_id="", role="market_close", sent=True, ts=NOW - 60),
    ])
    connector = OkxPerpetualDerivative()
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()), connector=connector)
    fresh = _fresh(finished=[{"text": "flat-1", "state": "filled", "fill_size": "1"}])
    service._remember_tick_fresh(PAIR, fresh)
    service.__dict__["_price_tick"] = service.__dict__.get("_price_tick", 1) + 1
    assert service._tick_fresh(PAIR) is None

    async def half(*args, **kwargs):
        return {"read": True, "done": True, "orders": [_archive_row("buy-old", "2")]}

    monkeypatch.setattr(service, "_okx_closed_pages", half)
    book = asyncio.run(service._shutdown_remainder_book(EXECUTOR, connector, PAIR, fresh))
    assert service._shutdown_remainder(EXECUTOR, book) == Decimal("1000")
    assert service._shutdown_close_proven(EXECUTOR, book) is False
    assert service._shutdown_close_proven(EXECUTOR) is False

    async def covered(*args, **kwargs):
        return {"read": True, "done": True, "orders": [_archive_row("buy-old", "1")]}

    monkeypatch.setattr(service, "_okx_closed_pages", covered)
    covered_book = asyncio.run(service._shutdown_remainder_book(EXECUTOR, connector, PAIR, fresh))
    assert service._shutdown_remainder(EXECUTOR, covered_book) == Decimal("0")
    assert service._shutdown_close_proven(EXECUTOR, covered_book) is True
    assert service._shutdown_close_proven(EXECUTOR) is False
    assert all(row.get("text") != "buy-old" for row in fresh.get("finished") or [])


def test_shutdown_flag_uses_the_archive_copy_on_the_tick(monkeypatch, tmp_path):
    """На тике флаг истинен по копии с архивом, не по голодному `_tick_fresh`."""
    _freeze_now(monkeypatch)
    old = NOW - 12 * 24 * 60 * 60
    _journal(tmp_path, monkeypatch, [
        _row("buy-old", level_id="L1", role="open", sent=True, ts=old),
        _row("flat-1", level_id="", role="market_close", sent=True, ts=NOW - 60),
    ])
    connector = OkxPerpetualDerivative()
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()), connector=connector)
    book = _fresh(finished=[{"text": "flat-1", "state": "filled", "fill_size": "1"}])
    executor, sold = _arm_with_task(monkeypatch, service, connector)
    holder = {}
    _run_task_with_neighbour_read(monkeypatch, service, connector, executor, book, holder)

    async def covered(*args, **kwargs):
        return {"read": True, "done": True, "orders": [_archive_row("buy-old", "1")]}

    monkeypatch.setattr(service, "_okx_closed_pages", covered)

    async def run():
        await executor.control_task()
        await _cancel_neighbour(holder)

    asyncio.run(run())
    assert sold == []
    assert executor._shutdown_close_proven is True


NOTHING = "you don't have any positions in this direction for this contract to reduce or close"


def _nothing_sell(monkeypatch, tmp_path, holds=True):
    level = _level(level_id="L1", open_id="buy-1", filled=True)
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", sent=True, ts=10),
        _row("flat-1", level_id="", role="market_close", sent=True, ts=20),
    ])
    body = json.loads(_checkpoint(level))
    if holds:
        body["holds"] = [{"order_id": "flat-1", "role": "market_close"}]
    service, repo, connector = _service_with(monkeypatch, json.dumps(body))
    fresh = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "amount": "9", "left": "0"}])
    service._remember_tick_fresh(PAIR, fresh)
    executor, sold = _arm(monkeypatch, service, connector, None)
    return service, executor, connector, sold, fresh


def test_nothing_to_close_writes_the_end_and_does_not_resend(monkeypatch, tmp_path):
    """Фраза «закрывать нечего» пишет конец. Тот же номер не уходит, новый не чеканится."""
    service, executor, connector, sold, fresh = _nothing_sell(monkeypatch, tmp_path)
    minted = []

    async def refusal(order_id):
        assert order_id == "flat-1"
        return True

    def mint(*args, **kwargs):
        minted.append("new")
        return "new"

    import hummingbot.connector.utils as connector_utils
    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(service, "_shutdown_close_refusal", refusal)
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    monkeypatch.setattr(checkpoint_mod, "install_connector_close_channel", lambda market: None)
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert sold == []
    assert minted == []
    assert service._nothing_to_close_end(EXECUTOR, "flat-1") is True
    parsed = service._partial_parsed(EXECUTOR)
    mark = parsed["partial_ends"]["flat-1"]
    assert "price" not in mark and "level_id" not in mark
    assert any(item.get("order_id") == "flat-1" for item in parsed.get("holds") or [])
    assert service._shutdown_close_proven(EXECUTOR, fresh) is True
    assert service._shutdown_remainder(EXECUTOR, fresh) == Decimal("9")


def test_nothing_to_close_end_must_be_visible_before_the_flag(monkeypatch, tmp_path):
    """Запись конца не прошла — флаг ложь. Конец виден — флаг истинен, продажа не уходит."""
    service, executor, connector, sold, fresh = _nothing_sell(monkeypatch, tmp_path)

    async def refusal(order_id):
        return True

    async def no_write(*args, **kwargs):
        return False

    monkeypatch.setattr(service, "_shutdown_close_refusal", refusal)
    monkeypatch.setattr(service, "_partial_cas", no_write)
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert sold == []
    assert service._shutdown_close_proven(EXECUTOR, fresh) is False

    body = json.loads(service._hold_rows[0]["checkpoint"])
    body["partial_ends"] = {"flat-1": {"kind": "nothing-to-close", "order_ids": ["flat-1"]}}
    service._hold_rows[0]["checkpoint"] = json.dumps(body)
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert sold == []
    assert service._shutdown_close_proven(EXECUTOR, fresh) is True


def test_other_or_unread_refusal_does_not_finish_the_close(monkeypatch, tmp_path):
    """Другой текст, пустая таблица и непрочитанная таблица конец не пишут."""
    assert service_text_rejects_the_clock()
    service, executor, connector, sold, fresh = _nothing_sell(monkeypatch, tmp_path)

    async def other(order_id):
        return False

    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(checkpoint_mod, "install_connector_close_channel", lambda market: None)
    monkeypatch.setattr(service, "_shutdown_close_refusal", other)
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert sold and sold[0][1] == Decimal("9")
    assert service._nothing_to_close_end(EXECUTOR, "flat-1") is False
    assert service._shutdown_close_proven(EXECUTOR, fresh) is False


def test_unread_refusal_does_not_finish_the_close(monkeypatch, tmp_path):
    """Непрочитанная таблица: продажу не слать, новый номер не чеканить, конец не писать."""
    service, executor, connector, sold, fresh = _nothing_sell(monkeypatch, tmp_path)
    minted = []

    async def unread(order_id):
        return None

    def mint(*args, **kwargs):
        minted.append("new")
        return "new"

    import hummingbot.connector.utils as connector_utils
    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    monkeypatch.setattr(checkpoint_mod, "install_connector_close_channel", lambda market: None)
    monkeypatch.setattr(service, "_shutdown_close_refusal", unread)
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert sold == []
    assert minted == []
    assert service._nothing_to_close_end(EXECUTOR, "flat-1") is False
    assert service._shutdown_close_proven(EXECUTOR, fresh) is False


def test_foreign_nothing_to_close_phrase_does_not_finish_our_number(monkeypatch, tmp_path):
    """Чужая фраза на чужом номере наш market_close не заканчивает."""
    service, executor, connector, sold, fresh = _nothing_sell(monkeypatch, tmp_path)
    asked = []

    async def refusal(order_id):
        asked.append(order_id)
        return order_id == "foreign-1"

    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(checkpoint_mod, "install_connector_close_channel", lambda market: None)
    monkeypatch.setattr(service, "_shutdown_close_refusal", refusal)
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert asked == ["flat-1"]
    assert sold and sold[0][1] == Decimal("9")
    assert service._nothing_to_close_end(EXECUTOR, "flat-1") is False
    assert service._nothing_to_close_end(EXECUTOR, "foreign-1") is False
    assert service._shutdown_close_proven(EXECUTOR, fresh) is False


def service_text_rejects_the_clock():
    from services.executor_service import ExecutorService
    probe = ExecutorService.__new__(ExecutorService)
    assert probe._nothing_to_close_text(NOTHING) is True
    assert probe._nothing_to_close_text("Error submitting order x: " + NOTHING) is True
    assert probe._nothing_to_close_text(NOTHING + " 50102") is False
    assert probe._nothing_to_close_text("Check API key and network connection") is False
    assert probe._nothing_to_close_text("") is False
    return True


def test_live_open_does_not_get_the_nothing_to_close_end(monkeypatch, tmp_path):
    """Номер ещё в открытых: конца нет, флаг ложь."""
    service, executor, connector, sold, fresh = _nothing_sell(monkeypatch, tmp_path)
    fresh["opens"] = [{"text": "flat-1"}]

    async def refusal(order_id):
        return True

    monkeypatch.setattr(service, "_shutdown_close_refusal", refusal)
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert sold == []
    assert service._nothing_to_close_end(EXECUTOR, "flat-1") is False
    assert service._shutdown_close_proven(EXECUTOR, fresh) is False


def test_visible_nothing_to_close_end_does_not_mint_a_second_sell(monkeypatch, tmp_path):
    """Конец виден, остаток не ноль: второй номер не чеканится, фраза размер не даёт."""
    service, executor, connector, sold, fresh = _nothing_sell(monkeypatch, tmp_path)
    body = json.loads(service._hold_rows[0]["checkpoint"])
    body["partial_ends"] = {"flat-1": {"kind": "nothing-to-close", "order_ids": ["flat-1"]}}
    service._hold_rows[0]["checkpoint"] = json.dumps(body)
    minted = []

    def mint(*args, **kwargs):
        minted.append("new")
        return "new"

    async def boom(order_id):
        raise AssertionError("таблицу заново не ждём")

    import hummingbot.connector.utils as connector_utils
    monkeypatch.setattr(connector_utils, "get_new_client_order_id", mint)
    monkeypatch.setattr(service, "_shutdown_close_refusal", boom)
    asyncio.run(service._send_shutdown_close(EXECUTOR, executor, connector, PAIR, fresh))
    assert sold == []
    assert minted == []
    assert service._shutdown_remainder(EXECUTOR, fresh) == Decimal("9")
    assert service._shutdown_close_proven(EXECUTOR, fresh) is True


class _Status:
    def __init__(self, name):
        self.name = name


def _finish_ready(monkeypatch, tmp_path, finished=None, opens=None, ends=None):
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", sent=True, ts=10),
    ])
    body = json.loads(_checkpoint(_level(level_id="L1", open_id="buy-1")))
    if ends:
        body["partial_ends"] = ends
    service, repo, connector = _service_with(monkeypatch, json.dumps(body))
    fresh = _fresh(finished=finished or [])
    fresh["opens"] = list(opens or [])
    service._remember_tick_fresh(PAIR, fresh)
    service.__dict__["_sweep_seen"] = True
    service.__dict__["_sweep_failed"] = False
    executor = _stub_executor(connector, [])
    executor.config.trading_pair = PAIR
    executor.connectors = {"gate_io": connector}
    service._active_executors[EXECUTOR] = executor
    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(checkpoint_mod, "order_miss_unreadable", lambda: False)
    monkeypatch.setattr("services.executor_service.order_miss_unreadable", lambda: False)
    return service, executor, connector, fresh


def test_proven_stop_enters_shutdown_once(monkeypatch, tmp_path):
    """Метка и доказанная продажа: один вход в завершение, без новой продажи."""
    service, executor, connector, fresh = _finish_ready(monkeypatch, tmp_path)
    service.__dict__.setdefault("_stop_mark_ram", {})[EXECUTOR] = True
    executor._shutdown_close_proven = True
    executor.status = _Status("RUNNING")
    calls = []

    def early_stop(keep_position=False):
        calls.append(keep_position)
        executor.status = _Status("SHUTTING_DOWN")

    executor.early_stop = early_stop
    assert service._maybe_enter_shutdown(EXECUTOR, executor) is True
    assert service._maybe_enter_shutdown(EXECUTOR, executor) is False
    assert calls == [False]
    assert executor.status.name == "SHUTTING_DOWN"


def test_unproven_stop_does_not_enter_shutdown(monkeypatch, tmp_path):
    """Нет метки или флаг ложь — в завершение не входим."""
    service, executor, connector, fresh = _finish_ready(monkeypatch, tmp_path)
    executor.status = _Status("RUNNING")
    executor.early_stop = lambda **kwargs: calls.append("x")
    calls = []
    executor.early_stop = lambda **kwargs: calls.append("x")
    assert service._maybe_enter_shutdown(EXECUTOR, executor) is False
    service.__dict__.setdefault("_stop_mark_ram", {})[EXECUTOR] = True
    executor._shutdown_close_proven = False
    assert service._maybe_enter_shutdown(EXECUTOR, executor) is False
    assert calls == []


def test_stop_held_does_not_finish_without_death_proof(monkeypatch, tmp_path):
    """Нет конца и нет finished — не завершать, даже если номера нет в открытых."""
    service, executor, connector, fresh = _finish_ready(monkeypatch, tmp_path)
    assert service._stop_held_may_finish(EXECUTOR, executor) is False
    fresh_unread = _fresh()
    fresh_unread["opens_read"] = False
    service._remember_tick_fresh(PAIR, fresh_unread)
    body = json.loads(service._hold_rows[0]["checkpoint"])
    body["partial_ends"] = {"buy-1": {"kind": "flat", "order_ids": ["buy-1"]}}
    service._hold_rows[0]["checkpoint"] = json.dumps(body)
    assert service._stop_held_may_finish(EXECUTOR, executor) is False
    opened = _fresh()
    opened["opens"] = [{"text": "buy-1"}]
    service._remember_tick_fresh(PAIR, opened)
    assert service._stop_held_may_finish(EXECUTOR, executor) is False
    service._remember_tick_fresh(PAIR, _fresh())
    connector.in_flight_orders = {"buy-1": object()}
    assert service._stop_held_may_finish(EXECUTOR, executor) is False
    connector.in_flight_orders = {}
    connector._order_tracker.lost_orders = {"buy-1": object()}
    assert service._stop_held_may_finish(EXECUTOR, executor) is False
    connector._order_tracker.lost_orders = {}
    import services.executor_service as service_mod
    monkeypatch.setattr(service_mod, "order_send_unknown", lambda order_id: order_id == "buy-1")
    assert service._stop_held_may_finish(EXECUTOR, executor) is False


def test_stop_held_finishes_only_when_every_number_is_dead(monkeypatch, tmp_path):
    """Конец или finished у каждого номера, и никто не жив: завершение можно отдать."""
    service, executor, connector, fresh = _finish_ready(
        monkeypatch, tmp_path,
        ends={"buy-1": {"kind": "flat", "order_ids": ["buy-1"]}},
    )
    assert "buy-1" not in [row.get("text") for row in fresh.get("opens") or []]
    assert service._stop_held_may_finish(EXECUTOR, executor) is True
    body = json.loads(service._hold_rows[0]["checkpoint"])
    body["partial_ends"] = {}
    service._hold_rows[0]["checkpoint"] = json.dumps(body)
    service._remember_tick_fresh(PAIR, _fresh(finished=[{
        "text": "buy-1", "finish_as": "filled", "amount": "1", "left": "0",
    }]))
    assert service._stop_held_may_finish(EXECUTOR, executor) is True


def test_stop_held_wrapper_does_not_call_the_inner_finish(monkeypatch, tmp_path):
    """Обёртка не отдаёт внутреннее завершение, пока номера нет в finished и нет конца."""
    service, executor, connector, fresh = _finish_ready(monkeypatch, tmp_path)
    called = []

    async def original(*args, **kwargs):
        called.append("inner")

    executor.control_shutdown_process = original
    executor.adjust_and_place_open_order = lambda level: None
    service._install_open_shell(EXECUTOR, executor)
    asyncio.run(executor.control_shutdown_process())
    assert called == []


class _CloseType:
    def __init__(self, name):
        self.name = name


def _cas_into_rows(service):
    async def fake_cas(executor_id, mutate, visible, target_id=None):
        body = json.loads(service._hold_rows[0]["checkpoint"])
        mutate(body)
        if not visible(body):
            return False
        service._hold_rows[0]["checkpoint"] = json.dumps(body)
        return True

    return fake_cas


def _proven_scene(
    monkeypatch, tmp_path, *,
    buy_end=False, with_close=True, finished=None, opens=None,
    extra_journal=None, bans=None, mark=True, brake=False,
):
    """Метка и продажа «закрывать нечего». buy-1 — свой номер, flat-1 — гашение."""
    lines = [_row("buy-1", level_id="L1", role="open", sent=True, ts=10)]
    if with_close:
        lines.append(_row("flat-1", level_id="", role="market_close", sent=True, ts=20))
    if extra_journal:
        lines.extend(extra_journal)
    _journal(tmp_path, monkeypatch, lines)
    body = json.loads(_checkpoint(_level(level_id="L1", open_id="buy-1")))
    ends = {}
    if with_close:
        ends["flat-1"] = {"kind": "nothing-to-close", "order_ids": ["flat-1"]}
    if buy_end:
        ends["buy-1"] = {"kind": "flat", "order_ids": ["buy-1"]}
    body["partial_ends"] = ends
    if bans:
        body["close_bans"] = list(bans)
    service, repo, connector = _service_with(monkeypatch, json.dumps(body))
    fresh = _fresh(finished=finished or [])
    fresh["opens"] = list(opens or [])
    service._remember_tick_fresh(PAIR, fresh)
    service.__dict__["_sweep_seen"] = True
    service.__dict__["_sweep_failed"] = False
    calls = []
    executor = _stub_executor(connector, calls)
    executor.config.trading_pair = PAIR
    executor.connectors = {"gate_io": connector}
    executor.close_type = _CloseType("EARLY_STOP")
    executor._current_retries = 11
    sold = []
    cancelled = []
    connector.sell = lambda *args, **kwargs: sold.append(args)
    executor.cancel_open_orders = lambda *args, **kwargs: cancelled.append("cancel")
    executor._strategy = SimpleNamespace(
        cancel=lambda *args, **kwargs: cancelled.append(kwargs.get("order_id")),
    )

    def stop(*args, **kwargs):
        if brake:
            assert executor._shutdown_close_proven is True
        calls.append("stop")

    async def shutdown(*args, **kwargs):
        calls.append("shutdown")

    def retries(*args, **kwargs):
        calls.append("retries")
        executor.close_type = _CloseType("FAILED")

    executor.stop = stop
    executor.control_shutdown_process = shutdown
    executor.evaluate_max_retries = retries
    executor.adjust_and_place_open_order = lambda level: None
    service._active_executors[EXECUTOR] = executor
    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(checkpoint_mod, "order_miss_unreadable", lambda: False)
    monkeypatch.setattr("services.executor_service.order_miss_unreadable", lambda: False)
    monkeypatch.setattr(service, "_partial_cas", _cas_into_rows(service))
    executor._shutdown_close_proven = False
    if mark:
        service.__dict__.setdefault("_stop_mark_ram", {})[EXECUTOR] = True
    if brake:
        from utils.executor_checkpoint import install_close_without_level
        assert install_close_without_level(
            executor, EXECUTOR,
            frozen=lambda eid=EXECUTOR: service._stop_mark_now(eid),
        )
    service._install_open_shell(EXECUTOR, executor)
    return service, executor, connector, calls, sold, cancelled


def test_proven_finish_stops_once_and_does_not_fail(monkeypatch, tmp_path):
    """Доказанное гашение: stop() один раз сквозь тормоз, тип EARLY_STOP."""
    service, executor, connector, calls, sold, cancelled = _proven_scene(
        monkeypatch, tmp_path, buy_end=True, brake=True,
    )
    assert executor._shutdown_close_proven is False
    asyncio.run(executor.control_shutdown_process())
    asyncio.run(executor.control_shutdown_process())
    executor.evaluate_max_retries()
    assert calls == ["stop"]
    assert executor.close_type.name == "EARLY_STOP"
    assert sold == []
    assert cancelled == []


def test_proven_finish_waits_without_an_end_or_a_finished_row(monkeypatch, tmp_path):
    """Свой номер без конца и без finished: stop() нет и FAILED нет."""
    service, executor, connector, calls, sold, cancelled = _proven_scene(monkeypatch, tmp_path)
    asyncio.run(executor.control_shutdown_process())
    executor.evaluate_max_retries()
    assert calls == []
    assert executor.close_type.name == "EARLY_STOP"

    opened = _fresh()
    opened["opens"] = [{"text": "buy-1"}]
    service._remember_tick_fresh(PAIR, opened)
    asyncio.run(executor.control_shutdown_process())
    executor.evaluate_max_retries()
    assert calls == []

    service._remember_tick_fresh(PAIR, _fresh(finished=[{
        "text": "buy-1", "finish_as": "filled", "amount": "1", "left": "0",
    }]))
    connector.in_flight_orders = {"buy-1": object()}
    asyncio.run(executor.control_shutdown_process())
    executor.evaluate_max_retries()
    assert "stop" not in calls
    assert executor.close_type.name == "EARLY_STOP"
    connector.in_flight_orders = {}
    connector._order_tracker.lost_orders = {"buy-1": object()}
    asyncio.run(executor.control_shutdown_process())
    assert "stop" not in calls
    connector._order_tracker.lost_orders = {}
    import services.executor_service as service_mod
    monkeypatch.setattr(service_mod, "order_send_unknown", lambda order_id: order_id == "buy-1")
    asyncio.run(executor.control_shutdown_process())
    assert calls == []
    assert executor.close_type.name == "EARLY_STOP"


def test_unread_book_does_not_stop_the_proven_finish(monkeypatch, tmp_path):
    """Непрочитанная книга: этот срез stop() не зовёт и FAILED сам не ставит."""
    service, executor, connector, calls, sold, cancelled = _proven_scene(
        monkeypatch, tmp_path, buy_end=True,
    )
    fresh = _fresh()
    fresh["finished_read"] = False
    service._remember_tick_fresh(PAIR, fresh)
    asyncio.run(executor.control_shutdown_process())
    assert calls == []
    assert executor.close_type.name == "EARLY_STOP"


def test_live_again_after_the_write_drops_the_end_and_does_not_stop(monkeypatch, tmp_path):
    """Пока запись шла, номер вернулся в открытые: конец снят, stop() нет."""
    service, executor, connector, calls, sold, cancelled = _proven_scene(
        monkeypatch, tmp_path,
        finished=[{"text": "buy-1", "finish_as": "filled", "amount": "1", "left": "0"}],
    )

    async def cas_then_open(executor_id, mutate, visible, target_id=None):
        body = json.loads(service._hold_rows[0]["checkpoint"])
        before = set((body.get("partial_ends") or {}))
        mutate(body)
        if not visible(body):
            return False
        service._hold_rows[0]["checkpoint"] = json.dumps(body)
        added = set((body.get("partial_ends") or {})) - before
        if "buy-1" in added:
            fresh = dict(service._pair_book(PAIR))
            fresh["opens"] = [{"text": "buy-1"}]
            service._remember_tick_fresh(PAIR, fresh)
        return True

    monkeypatch.setattr(service, "_partial_cas", cas_then_open)
    asyncio.run(executor.control_shutdown_process())
    ends = service._partial_parsed(EXECUTOR).get("partial_ends") or {}
    assert "buy-1" not in ends
    assert calls == []
    unfinished = service._stop_unfinished_ids(EXECUTOR, service._pair_book(PAIR))
    assert "buy-1" in unfinished


def test_failed_end_rollback_retries_and_does_not_stop(monkeypatch, tmp_path):
    """Снять ожившую метку не вышло — stop() нет. Следующий тик снимает её."""
    service, executor, connector, calls, sold, cancelled = _proven_scene(
        monkeypatch, tmp_path,
        finished=[{"text": "buy-1", "finish_as": "filled", "amount": "1", "left": "0"}],
    )
    drops = {"n": 0}

    async def cas_then_open(executor_id, mutate, visible, target_id=None):
        body = json.loads(service._hold_rows[0]["checkpoint"])
        before = set((body.get("partial_ends") or {}))
        mutate(body)
        if not visible(body):
            return False
        added = set((body.get("partial_ends") or {})) - before
        removed = before - set((body.get("partial_ends") or {}))
        if removed:
            drops["n"] += 1
            if drops["n"] == 1:
                return False
        service._hold_rows[0]["checkpoint"] = json.dumps(body)
        if "buy-1" in added:
            fresh = dict(service._pair_book(PAIR))
            fresh["opens"] = [{"text": "buy-1"}]
            service._remember_tick_fresh(PAIR, fresh)
        return True

    monkeypatch.setattr(service, "_partial_cas", cas_then_open)
    asyncio.run(executor.control_shutdown_process())
    assert "buy-1" in (service._partial_parsed(EXECUTOR).get("partial_ends") or {})
    assert calls == []
    asyncio.run(executor.control_shutdown_process())
    assert "buy-1" not in (service._partial_parsed(EXECUTOR).get("partial_ends") or {})
    assert calls == []


def test_finished_own_number_gets_an_end_before_stop(monkeypatch, tmp_path):
    """Строка finished без конца: конец без price и level_id, затем stop()."""
    service, executor, connector, calls, sold, cancelled = _proven_scene(
        monkeypatch, tmp_path,
        finished=[{"text": "buy-1", "finish_as": "filled", "amount": "1", "left": "0"}],
    )

    def stop(*args, **kwargs):
        mark = service._partial_parsed(EXECUTOR)["partial_ends"]["buy-1"]
        assert mark.get("kind") and "price" not in mark and "level_id" not in mark
        calls.append("stop")

    executor.stop = stop
    asyncio.run(executor.control_shutdown_process())
    assert calls == ["stop"]
    assert sold == []


def test_failed_end_write_does_not_stop(monkeypatch, tmp_path):
    """Запись конца не прошла — stop() нет."""
    service, executor, connector, calls, sold, cancelled = _proven_scene(
        monkeypatch, tmp_path,
        finished=[{"text": "buy-1", "finish_as": "filled", "amount": "1", "left": "0"}],
    )

    async def no_write(*args, **kwargs):
        return False

    monkeypatch.setattr(service, "_partial_cas", no_write)
    asyncio.run(executor.control_shutdown_process())
    assert calls == []
    assert "buy-1" not in (service._partial_parsed(EXECUTOR).get("partial_ends") or {})
    assert executor.close_type.name == "EARLY_STOP"


def test_empty_opens_and_orders_table_do_not_write_an_end(monkeypatch, tmp_path):
    """Пустой открытый список и FILLED в таблице orders конец не пишут."""
    service, executor, connector, calls, sold, cancelled = _proven_scene(monkeypatch, tmp_path)
    asked = []

    class _Orders:
        def __init__(self, session):
            pass

        async def get_order_by_client_id(self, text):
            asked.append(text)
            return SimpleNamespace(status="FILLED")

    monkeypatch.setattr(
        "database.repositories.order_repository.OrderRepository", _Orders,
    )
    assert asyncio.run(service._remember_own_dead_ends(EXECUTOR, executor)) is True
    assert asked == []
    assert "buy-1" not in (service._partial_parsed(EXECUTOR).get("partial_ends") or {})
    asyncio.run(executor.control_shutdown_process())
    assert "stop" not in calls


def test_foreign_ban_does_not_get_an_end_or_block_stop(monkeypatch, tmp_path):
    """Номер в close_bans чужого журнала: конца нет, отмены нет, свой стоп не держится."""
    foreign = _row("foreign-1", level_id="L9", role="open", sent=True, ts=30)
    foreign["executor_id"] = "grid-b"
    service, executor, connector, calls, sold, cancelled = _proven_scene(
        monkeypatch, tmp_path, buy_end=True, extra_journal=[foreign], bans=["foreign-1"],
    )
    asyncio.run(executor.control_shutdown_process())
    ends = service._partial_parsed(EXECUTOR).get("partial_ends") or {}
    assert "foreign-1" not in ends
    assert calls == ["stop"]
    assert cancelled == []
    assert sold == []


def test_unmarked_stop_does_not_use_the_new_finish(monkeypatch, tmp_path):
    """Нет метки: новый stop() не зовётся, штатный шаг остаётся."""
    service, executor, connector, calls, sold, cancelled = _proven_scene(
        monkeypatch, tmp_path, buy_end=True, with_close=False, mark=False,
    )
    asyncio.run(executor.control_shutdown_process())
    assert "stop" not in calls
    assert "shutdown" in calls


def test_unproven_stop_keeps_the_old_retry_wrap(monkeypatch, tmp_path):
    """Гашение не доказано: новый stop() не зовётся, счётчик остаётся прежним."""
    service, executor, connector, calls, sold, cancelled = _proven_scene(
        monkeypatch, tmp_path, buy_end=True, with_close=False, mark=True,
    )
    asyncio.run(executor.control_shutdown_process())
    executor.evaluate_max_retries()
    assert "stop" not in calls
    assert "shutdown" in calls
    assert executor.close_type.name == "FAILED"


def _retained_scene(monkeypatch, tmp_path, order_id="ret-1"):
    """Номер только в retained_orders. Журнала этой покупки нет."""
    _journal(tmp_path, monkeypatch, [])
    body = json.loads(_checkpoint(_level(level_id="L1")))
    body["retained_orders"] = [{
        "order_id": order_id, "filled": True, "role": "open_order", "price": "100",
    }]
    service, repo, connector = _service_with(monkeypatch, json.dumps(body))
    fresh = _fresh()
    service._remember_tick_fresh(PAIR, fresh)
    service.__dict__["_sweep_seen"] = True
    service.__dict__["_sweep_failed"] = False
    executor = _stub_executor(connector, [])
    executor.config.trading_pair = PAIR
    executor.connectors = {"gate_io": connector}
    service._active_executors[EXECUTOR] = executor
    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(checkpoint_mod, "order_miss_unreadable", lambda: False)
    monkeypatch.setattr("services.executor_service.order_miss_unreadable", lambda: False)
    monkeypatch.setattr(service, "_partial_cas", _cas_into_rows(service))
    return service, executor, connector, fresh


def test_retained_number_is_delivered_and_a_foreign_price_is_not(monkeypatch, tmp_path):
    """Свой номер только в retained_orders входит в отмену. Чужой по цене — нет."""
    service, executor, connector, fresh = _retained_scene(monkeypatch, tmp_path)
    pending = service._stop_unfinished_ids(EXECUTOR, fresh)
    assert "ret-1" in pending
    assert "foreign-same-price" not in pending


def test_retained_51400_does_not_release_finish(monkeypatch, tmp_path):
    """51400 гасит повтор отмены, но пустой журнал не закрывает сетку."""
    service, executor, connector, fresh = _retained_scene(monkeypatch, tmp_path)
    calls = []

    async def place_cancel(order_id, row):
        calls.append(str(order_id))
        raise _gone_error()

    connector._place_cancel = place_cancel
    executor.stop = lambda *args, **kwargs: calls.append("stop")
    executor.adjust_and_place_open_order = lambda level: None
    service.__dict__.setdefault("_stop_mark_ram", {})[EXECUTOR] = True
    service._install_open_shell(EXECUTOR, executor)
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "ret-1", PAIR, EXECUTOR)) is True
    assert service._exchange_gone_end(EXECUTOR, "ret-1") is True
    mark = service._partial_parsed(EXECUTOR)["partial_ends"]["ret-1"]
    assert "price" not in mark
    assert "ret-1" not in service._stop_unfinished_ids(EXECUTOR, fresh)
    assert service._executor_proofs_released(EXECUTOR) is False
    assert service._stop_held_may_finish(EXECUTOR, executor) is False
    asyncio.run(executor.control_shutdown_process())
    if service._executor_proofs_released(EXECUTOR):
        executor.stop()
        service._active_executors.pop(EXECUTOR, None)
    assert "stop" not in calls
    assert EXECUTOR in service._active_executors
    retained = service._partial_parsed(EXECUTOR).get("retained_orders") or []
    assert any(item.get("order_id") == "ret-1" for item in retained)


def test_retained_cancel_does_not_write_gone_while_live(monkeypatch, tmp_path):
    """Живой или непрочитанный номер из retained_orders конца не получает."""
    service, executor, connector, fresh = _retained_scene(monkeypatch, tmp_path)

    async def place_cancel(order_id, row):
        raise _gone_error()

    connector._place_cancel = place_cancel
    fresh_unread = _fresh()
    fresh_unread["opens_read"] = False
    service._remember_tick_fresh(PAIR, fresh_unread)
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "ret-1", PAIR, EXECUTOR)) is False
    assert service._exchange_gone_end(EXECUTOR, "ret-1") is False
    assert "ret-1" in service._stop_unfinished_ids(EXECUTOR, _fresh())

    service._remember_tick_fresh(PAIR, _fresh())
    opened = _fresh()
    opened["opens"] = [{"text": "ret-1"}]
    service._remember_tick_fresh(PAIR, opened)
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "ret-1", PAIR, EXECUTOR)) is False
    assert service._exchange_gone_end(EXECUTOR, "ret-1") is False

    service._remember_tick_fresh(PAIR, _fresh())
    connector.in_flight_orders = {"ret-1": object()}
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "ret-1", PAIR, EXECUTOR)) is False
    connector.in_flight_orders = {}
    connector._order_tracker.lost_orders = {"ret-1": object()}
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "ret-1", PAIR, EXECUTOR)) is False
    connector._order_tracker.lost_orders = {}
    connector.in_flight_orders = None
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "ret-1", PAIR, EXECUTOR)) is False
    connector.in_flight_orders = {}
    import services.executor_service as service_mod
    monkeypatch.setattr(service_mod, "order_send_unknown", lambda order_id: order_id == "ret-1")
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "ret-1", PAIR, EXECUTOR)) is False
    assert service._exchange_gone_end(EXECUTOR, "ret-1") is False
    assert "ret-1" in service._stop_unfinished_ids(EXECUTOR, _fresh())


def test_market_failure_wrapper_does_not_end_or_sell(monkeypatch, tmp_path):
    """Обёртка MarketOrderFailureEvent не конец и не новая продажа."""
    service, executor, connector, fresh = _retained_scene(monkeypatch, tmp_path)
    sold = []
    connector.sell = lambda *args, **kwargs: sold.append(args)

    async def place_cancel(order_id, row):
        raise OSError("Order failed: MarketOrderFailureEvent")

    connector._place_cancel = place_cancel
    assert service._nothing_to_close_text("Order failed: MarketOrderFailureEvent") is False
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "ret-1", PAIR, EXECUTOR)) is False
    assert service._exchange_gone_end(EXECUTOR, "ret-1") is False
    assert sold == []


def test_other_close_does_not_prove_a_retained_buy(monkeypatch, tmp_path):
    """Чужой close того же исполнителя не продажа номера из retained_orders."""
    _journal(tmp_path, monkeypatch, [
        _row("other-close", level_id="L9", role="close", sent=True, ts=30),
        _row("level-close", level_id="L1", role="close", sent=True, ts=31),
    ])
    body = json.loads(_checkpoint(_level(level_id="L1", close_id="level-close")))
    body["retained_orders"] = [
        {"order_id": "ret-1", "filled": True, "role": "open_order", "level_id": "L1", "price": "100"},
        {"order_id": "ret-2", "filled": True, "role": "open_order", "level_id": "L1", "price": "100"},
    ]
    body["partial_ends"] = {
        "ret-1": {"kind": "gone", "order_ids": ["ret-1"], "exchange": "51400"},
        "ret-2": {"kind": "gone", "order_ids": ["ret-2"], "exchange": "51400"},
    }
    service, repo, connector = _service_with(monkeypatch, json.dumps(body))
    fresh = _fresh(finished=[{
        "text": "ret-1", "finish_as": "filled", "amount": "1", "left": "0",
    }, {
        "text": "ret-2", "finish_as": "filled", "amount": "1", "left": "0",
    }, {
        "text": "other-close", "finish_as": "filled", "amount": "9", "left": "0",
    }, {
        "text": "level-close", "finish_as": "filled", "amount": "9", "left": "0",
    }])
    service._remember_tick_fresh(PAIR, fresh)
    service.__dict__["_sweep_seen"] = True
    service.__dict__["_sweep_failed"] = False
    executor = _stub_executor(connector, [])
    executor.config.trading_pair = PAIR
    executor.connectors = {"gate_io": connector}
    service._active_executors[EXECUTOR] = executor
    import utils.executor_checkpoint as checkpoint_mod
    monkeypatch.setattr(checkpoint_mod, "order_miss_unreadable", lambda: False)
    monkeypatch.setattr("services.executor_service.order_miss_unreadable", lambda: False)
    assert service._retained_finish_proven(EXECUTOR, "ret-1", fresh) is False
    assert service._retained_finish_proven(EXECUTOR, "ret-2", fresh) is False
    assert "ret-1" in service._retained_51400_unproven(EXECUTOR, fresh)
    assert service._executor_proofs_released(EXECUTOR) is False
    assert service._stop_held_may_finish(EXECUTOR, executor) is False


def _archive_ready(monkeypatch, tmp_path, rows):
    service, executor, connector, fresh = _retained_scene(monkeypatch, tmp_path)
    body = json.loads(service._hold_rows[0]["checkpoint"])
    body["partial_ends"] = {
        "ret-1": {"kind": "gone", "order_ids": ["ret-1"], "exchange": "51400"},
    }
    service._hold_rows[0]["checkpoint"] = json.dumps(body)
    service.__dict__.setdefault("_retained_archive_rows", {})[EXECUTOR] = rows
    return service, executor, connector, fresh


def test_archive_explicit_zero_releases_retained_finish(monkeypatch, tmp_path):
    """Явный accFillSz 0 и прочитанная книга отпускают завершение. Запись номера на месте."""
    raw = [{
        "clOrdId": "ret-1", "state": "canceled", "accFillSz": "0", "cTime": "1", "px": "1",
    }]
    service, executor, connector, fresh = _archive_ready(monkeypatch, tmp_path, raw)
    assert service._retained_finish_proven(EXECUTOR, "ret-1", fresh) is True
    assert service._executor_proofs_released(EXECUTOR) is True
    assert service._stop_held_may_finish(EXECUTOR, executor) is True
    retained = service._partial_parsed(EXECUTOR).get("retained_orders") or []
    assert any(item.get("order_id") == "ret-1" for item in retained)
    assert connector.created == []


def test_archive_unread_or_foreign_does_not_release(monkeypatch, tmp_path):
    """Непрочитанный архив, пустой архив и чужой номер завершение не отпускают."""
    service, executor, connector, fresh = _archive_ready(monkeypatch, tmp_path, None)
    assert service._retained_finish_proven(EXECUTOR, "ret-1", fresh) is False
    service.__dict__["_retained_archive_rows"][EXECUTOR] = []
    assert service._retained_finish_proven(EXECUTOR, "ret-1", fresh) is False
    service.__dict__["_retained_archive_rows"][EXECUTOR] = [{
        "clOrdId": "other", "state": "canceled", "accFillSz": "0",
    }]
    assert service._retained_finish_proven(EXECUTOR, "ret-1", fresh) is False
    assert EXECUTOR in service._active_executors


def test_open_book_blocks_archive_proof(monkeypatch, tmp_path):
    """Номер в открытых или непрочитанная книга: архив не отпускает, исполнитель в памяти."""
    raw = [{"clOrdId": "ret-1", "state": "canceled", "accFillSz": "0"}]
    service, executor, connector, fresh = _archive_ready(monkeypatch, tmp_path, raw)
    opened = _fresh()
    opened["opens"] = [{"text": "ret-1"}]
    assert service._retained_finish_proven(EXECUTOR, "ret-1", opened) is False
    unread = _fresh()
    unread["opens_read"] = False
    assert service._retained_finish_proven(EXECUTOR, "ret-1", unread) is False
    connector.in_flight_orders = {"ret-1": object()}
    assert service._retained_finish_proven(EXECUTOR, "ret-1", fresh) is False
    connector.in_flight_orders = {}
    connector._order_tracker.lost_orders = {"ret-1": object()}
    assert service._retained_finish_proven(EXECUTOR, "ret-1", fresh) is False
    connector._order_tracker.lost_orders = {}
    import services.executor_service as service_mod
    monkeypatch.setattr(service_mod, "order_send_unknown", lambda order_id: order_id == "ret-1")
    assert service._retained_finish_proven(EXECUTOR, "ret-1", fresh) is False
    assert EXECUTOR in service._active_executors
    retained = service._partial_parsed(EXECUTOR).get("retained_orders") or []
    assert any(item.get("order_id") == "ret-1" and item.get("price") == "100" for item in retained)
    assert connector.created == []


def test_parser_zero_without_acc_fill_does_not_release(monkeypatch, tmp_path):
    """Пустой accFillSz после parse_okx_closed_page — не снятие. Явный ноль — отдельная строка."""
    from utils.executor_checkpoint import parse_okx_closed_page
    body = {"code": "0", "data": [{
        "clOrdId": "ret-1", "state": "canceled", "accFillSz": "", "cTime": "1000", "px": "1",
        "ordId": "1", "side": "buy",
    }]}
    parsed = parse_okx_closed_page(body, 0, 2000)
    assert parsed.get("orders")
    assert parsed["orders"][0].get("fill_size") == "0"
    assert "accFillSz" not in parsed["orders"][0]
    booked = _fresh(finished=list(parsed["orders"]))
    service, executor, connector, fresh = _archive_ready(monkeypatch, tmp_path, parsed["orders"])
    service._remember_tick_fresh(PAIR, booked)
    assert service._retained_finish_proven(EXECUTOR, "ret-1", booked) is False
    assert service._retained_finish_proven(EXECUTOR, "ret-1", None) is False
    assert service._executor_proofs_released(EXECUTOR) is False
    retained = service._partial_parsed(EXECUTOR).get("retained_orders") or []
    assert any(item.get("order_id") == "ret-1" for item in retained)
    explicit = [{"clOrdId": "ret-1", "state": "canceled", "accFillSz": "0"}]
    assert service._archive_explicit_zero(explicit, "ret-1") is True


def test_archive_fill_without_level_sale_does_not_release(monkeypatch, tmp_path):
    """Живое исполнение и архивный ноль без продажи уровня завершение не отпускают."""
    raw = [{"clOrdId": "ret-1", "state": "canceled", "accFillSz": "0"}]
    service, executor, connector, fresh = _archive_ready(monkeypatch, tmp_path, raw)
    filled = _fresh(finished=[{
        "text": "ret-1", "finish_as": "filled", "amount": "1", "left": "0",
    }])
    service._remember_tick_fresh(PAIR, filled)
    assert service._retained_finish_proven(EXECUTOR, "ret-1", filled) is False
    assert service._executor_proofs_released(EXECUTOR) is False
    assert EXECUTOR in service._active_executors
    retained = service._partial_parsed(EXECUTOR).get("retained_orders") or []
    assert any(item.get("order_id") == "ret-1" for item in retained)


def test_retained_archive_window_ignores_a_newer_journal_mark():
    """Окно не шире 90 дней и не берёт чужую более новую метку."""
    from utils.executor_checkpoint import OKX_ARCHIVE_MS
    from services.executor_service import ExecutorService
    service = ExecutorService.__new__(ExecutorService)
    window = service._retained_archive_window(1_000.0)
    assert int(window["end"]) - int(window["begin"]) == OKX_ARCHIVE_MS


def test_retained_archive_log_names_unread_empty_and_raw_size(monkeypatch, tmp_path, caplog):
    """Непрочитанный архив, пустой ответ и сырой размер. Журнал завершение не отпускает."""
    import logging
    service, executor, connector, fresh = _archive_ready(monkeypatch, tmp_path, None)
    caplog.set_level(logging.WARNING, logger="services.executor_service")
    service._log_retained_archive(EXECUTOR, PAIR)
    assert "order=ret-1 result=unread" in caplog.text
    assert service._executor_proofs_released(EXECUTOR) is False
    assert EXECUTOR in service._active_executors

    caplog.clear()
    service._note_retained_archive(EXECUTOR, "read", [{"clOrdId": "other", "accFillSz": "9"}])
    service._log_retained_archive(EXECUTOR, PAIR)
    assert "order=ret-1 result=empty" in caplog.text
    assert "other" not in caplog.text
    assert service._executor_proofs_released(EXECUTOR) is False

    service._note_retained_archive(EXECUTOR, "read", [{
        "clOrdId": "ret-1", "state": "canceled", "accFillSz": "0",
    }])
    assert service._retained_archive_detail(EXECUTOR, "ret-1") == (
        "order=ret-1 result=row state=canceled accFillSz=0"
    )
    service._note_retained_archive(EXECUTOR, "read", [{
        "clOrdId": "ret-1", "state": "filled", "accFillSz": "1",
    }])
    assert service._retained_archive_detail(EXECUTOR, "ret-1") == (
        "order=ret-1 result=row state=filled accFillSz=1"
    )
    assert service._retained_finish_proven(EXECUTOR, "ret-1", fresh) is False
    service._note_retained_archive(EXECUTOR, "read", [{
        "clOrdId": "ret-1", "state": "canceled",
    }])
    assert service._retained_archive_detail(EXECUTOR, "ret-1") == (
        "order=ret-1 result=row state=canceled accFillSz=missing"
    )
    assert "accFillSz=0" not in service._retained_archive_detail(EXECUTOR, "ret-1")
    service._note_retained_archive(EXECUTOR, "read", [
        {"clOrdId": "ret-1", "state": "canceled", "accFillSz": "0"},
        {"clOrdId": "ret-1", "state": "filled", "accFillSz": "1"},
        {"clOrdId": "foreign", "state": "filled", "accFillSz": "9"},
    ])
    both = service._retained_archive_detail(EXECUTOR, "ret-1")
    assert both == (
        "order=ret-1 result=row state=canceled accFillSz=0 state=filled accFillSz=1"
    )
    assert "foreign" not in both


def test_retained_archive_log_repeats_only_on_a_new_result(monkeypatch, tmp_path, caplog):
    """Тот же исход молчит пять минут. Другой исход пишется сразу."""
    import logging
    service, executor, connector, fresh = _archive_ready(monkeypatch, tmp_path, None)
    caplog.set_level(logging.WARNING, logger="services.executor_service")
    service._log_retained_archive(EXECUTOR, PAIR)
    service._log_retained_archive(EXECUTOR, PAIR)
    assert caplog.text.count("result=unread") == 1
    caplog.clear()
    service._note_retained_archive(EXECUTOR, "read", [])
    service._log_retained_archive(EXECUTOR, PAIR)
    assert "result=empty" in caplog.text
    assert service._executor_proofs_released(EXECUTOR) is False


def test_refresh_logs_bad_body_and_does_not_release(monkeypatch, tmp_path, caplog):
    """Плохое тело архива — unread. Завершение и запись номера на месте."""
    import logging
    service, executor, connector, fresh = _archive_ready(monkeypatch, tmp_path, None)
    okx = OkxPerpetualDerivative()

    async def api_get(**kwargs):
        return {"code": "1", "data": []}

    async def symbol(*args, **kwargs):
        return "DOGE-USDT"

    okx._api_get = api_get
    monkeypatch.setattr(service, "_gate_symbol", symbol)
    caplog.set_level(logging.WARNING, logger="services.executor_service")
    asyncio.run(service._refresh_retained_archive(EXECUTOR, executor, okx, PAIR))
    assert "order=ret-1 result=unread:bad-body" in caplog.text
    assert service._executor_proofs_released(EXECUTOR) is False
    assert EXECUTOR in service._active_executors
    retained = service._partial_parsed(EXECUTOR).get("retained_orders") or []
    assert any(item.get("order_id") == "ret-1" for item in retained)
    assert (service.__dict__.get("_retained_archive_rows") or {}).get(EXECUTOR) is None


def test_refresh_logs_raw_size_and_skips_a_foreign_row(monkeypatch, tmp_path, caplog):
    """Короткая страница пишет сырой размер этого номера и не пишет чужой."""
    import logging
    import time
    service, executor, connector, fresh = _archive_ready(monkeypatch, tmp_path, None)
    okx = OkxPerpetualDerivative()
    now_ms = str(int(time.time() * 1000))

    async def api_get(**kwargs):
        return {"code": "0", "data": [{
            "clOrdId": "ret-1", "state": "canceled", "cTime": now_ms, "ordId": "1",
        }, {
            "clOrdId": "other", "state": "filled", "accFillSz": "9", "cTime": now_ms, "ordId": "2",
        }]}

    async def symbol(*args, **kwargs):
        return "DOGE-USDT"

    okx._api_get = api_get
    monkeypatch.setattr(service, "_gate_symbol", symbol)
    caplog.set_level(logging.WARNING, logger="services.executor_service")
    asyncio.run(service._refresh_retained_archive(EXECUTOR, executor, okx, PAIR))
    assert "order=ret-1 result=row state=canceled accFillSz=missing" in caplog.text
    assert "accFillSz=0" not in caplog.text
    assert "other" not in caplog.text
    assert service._executor_proofs_released(EXECUTOR) is False
