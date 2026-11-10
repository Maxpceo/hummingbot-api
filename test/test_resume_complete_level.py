"""Подъём сетки с завершённым уровнем. План okx-resume-complete-level. Без биржи.

Живая форма чекпойнта — сетка HyvjGnRA 9 ноября 2026: вход filled с компактным
телом без client_order_id, продажа filled с полным телом. Заглушка компактного
тела не имеет completely_filled_event — штатный COMPLETE-путь при подъёме падал.
"""

import json
from decimal import Decimal
from types import SimpleNamespace

from test.test_stop_freeze import _journal, _service_with
from utils.executor_checkpoint import (
    COMPLETED_CYCLE_KEY,
    RESUME_COMPLETE_KIND,
    apply_checkpoint,
    capture_executor,
    checkpoint_keeping_holds,
    completed_cycle_levels,
    order_ledger,
    partial_released_ids,
    resume_complete_mutate,
    resume_complete_visible,
    seat_recorded_level_opens,
)

EXECUTOR = "grid-hyj"
PAIR = "SOL-USDT"
OPEN_ID = "93027a12dac34fBCBSLUTc37b9c0b07c"
CLOSE_ID = "93027a12dac34fBCSSLUTce70fa6c158"


def _hyj_checkpoint():
    """Форма колонки HyvjGnRA: компактный вход + полная filled-продажа."""
    return {
        "version": 1,
        "type": "grid_executor",
        "levels": [
            {
                "id": "L0",
                "price": "86.01",
                "amount_quote": "11.28465",
                "take_profit": "0.0001",
                "side": "BUY",
                "open_order_type": "MARKET",
                "take_profit_order_type": "MARKET",
                "open_order": {
                    "order_id": OPEN_ID,
                    "level_id": "L0",
                    "filled": True,
                    "price": "86.7",
                    "order": {"executed_amount_base": "0.13"},
                },
                "close_order": {
                    "order_id": CLOSE_ID,
                    "level_id": "L0",
                    "filled": True,
                    "price": "86.59",
                    "role": "close_order",
                    "order": {
                        "client_order_id": CLOSE_ID,
                        "trading_pair": PAIR,
                        "order_type": "LIMIT",
                        "trade_type": "SELL",
                        "price": "86.59",
                        "amount": "0.13",
                        "exchange_order_id": None,
                        "executed_amount_base": "0.13",
                        "executed_amount_quote": "11.2567",
                        "last_state": "5",
                        "leverage": "1",
                        "position": "CLOSE",
                        "creation_timestamp": 0,
                        "last_update_timestamp": 0,
                        "order_fills": {},
                        "cumulative_fee_paid_base": 0.0,
                        "cumulative_fee_paid_quote": 0.0,
                    },
                },
            }
        ],
    }


def _executor():
    return SimpleNamespace(
        config=SimpleNamespace(trading_pair=PAIR, leverage=1),
        _held_position_orders=[],
        _open_fee_in_base=False,
    )


def _grid_reads(executor):
    from hummingbot.strategy_v2.executors.grid_executor.grid_executor import GridExecutor

    executor.mid_price = Decimal("86.5")
    executor._close_order = None
    executor._close_ban_ids = []
    executor._close_holds = []
    executor._column_close_ids = []
    GridExecutor.update_grid_levels(executor)
    GridExecutor.update_realized_pnl_metrics(executor)


def _level_state(level):
    state = getattr(level, "state", None)
    return getattr(state, "name", None) or str(state or "")


def test_hyj_form_resumes_and_formats_the_completed_cycle():
    """Проба 1: подъём не падает, круг оформлен обеими сторонами, метрики читаются."""
    executor = _executor()
    apply_checkpoint(executor, _hyj_checkpoint(), [])
    level = executor.grid_levels[0]
    assert _level_state(level) == "NOT_ACTIVE"
    assert level.active_open_order is None and level.active_close_order is None
    entries = executor._filled_orders
    assert len(entries) == 2
    opened = next(item for item in entries if item["trade_type"] == "BUY")
    closed = next(item for item in entries if item["trade_type"] == "SELL")
    assert opened["client_order_id"] == OPEN_ID
    assert closed["client_order_id"] == CLOSE_ID
    assert opened["executed_amount_quote"] == str(Decimal("0.13") * Decimal("86.7"))
    assert opened["cumulative_fee_paid_quote"] == 0.0
    assert closed["executed_amount_quote"] == "11.2567"
    _grid_reads(executor)
    assert executor.realized_pnl_quote == Decimal("11.2567") - Decimal(
        opened["executed_amount_quote"])


def test_full_open_body_variant_uses_own_bodies():
    """Проба 2: полное тело входа идёт собственным телом, тот же сброс."""
    checkpoint = _hyj_checkpoint()
    slot = checkpoint["levels"][0]["open_order"]
    slot["order"] = {
        "client_order_id": OPEN_ID,
        "trading_pair": PAIR,
        "order_type": "MARKET",
        "trade_type": "BUY",
        "price": "86.7",
        "amount": "0.13",
        "exchange_order_id": None,
        "executed_amount_base": "0.13",
        "executed_amount_quote": "11.271",
        "last_state": "5",
        "leverage": "1",
        "position": "OPEN",
        "creation_timestamp": 0,
        "last_update_timestamp": 0,
        "order_fills": {},
        "cumulative_fee_paid_base": 0.0,
        "cumulative_fee_paid_quote": 0.0,
    }
    executor = _executor()
    apply_checkpoint(executor, checkpoint, [])
    assert _level_state(executor.grid_levels[0]) == "NOT_ACTIVE"
    opened = next(item for item in executor._filled_orders if item["trade_type"] == "BUY")
    assert opened["client_order_id"] == OPEN_ID and opened["executed_amount_quote"] == "11.271"


def test_completed_cycle_detection_and_resume_cas_record():
    """Пробы 5 (подготовка): детектор видит круг HyvjGnRA, CAS-мутиция пишет всё сразу."""
    checkpoint = _hyj_checkpoint()
    cycles = completed_cycle_levels(checkpoint)
    assert [(item["level_id"], item["open_id"], item["close_id"]) for item in cycles] == [("L0", OPEN_ID, CLOSE_ID)]
    body = json.loads(json.dumps(checkpoint))
    resume_complete_mutate("L0", OPEN_ID, CLOSE_ID)(body)
    level = body["levels"][0]
    assert level["open_order"] is None and level["close_order"] is None
    mark = body["partial_ends"][OPEN_ID]
    assert mark["kind"] == RESUME_COMPLETE_KIND and mark["level_id"] == "L0"
    assert mark["order_ids"] == [OPEN_ID, CLOSE_ID]
    assert "price" not in mark
    record = body[COMPLETED_CYCLE_KEY]["L0"]
    assert record["open"]["order_id"] == OPEN_ID and record["close"]["order_id"] == CLOSE_ID
    assert record["open"]["order"]["executed_amount_base"] == "0.13"
    assert record["level"]["side"] == "BUY"
    assert resume_complete_visible("L0", OPEN_ID)(body) is True


def test_marked_column_survives_real_capture_and_merge():
    """Проба 5: метка до посадки; настоящий захват и склейка не воскрешают продажу."""
    checkpoint = _hyj_checkpoint()
    marked = json.loads(json.dumps(checkpoint))
    resume_complete_mutate("L0", OPEN_ID, CLOSE_ID)(marked)
    executor = _executor()
    apply_checkpoint(executor, checkpoint, [])
    assert order_ledger(executor) == {}
    captured = capture_executor(executor, "grid_executor")
    merged_text = checkpoint_keeping_holds(json.dumps(marked), captured)
    merged = json.loads(merged_text)
    level = merged["levels"][0]
    assert (level.get("open_order") or {}).get("order_id") in (None, "")
    assert (level.get("close_order") or {}).get("order_id") in (None, "")
    assert merged["partial_ends"][OPEN_ID]["kind"] == RESUME_COMPLETE_KIND
    record = merged[COMPLETED_CYCLE_KEY]["L0"]
    assert record["open"]["order_id"] == OPEN_ID
    assert record["open"]["price"] == "86.7"
    assert record["open"]["order"]["executed_amount_base"] == "0.13"
    assert record["close"]["order_id"] == CLOSE_ID
    assert record["close"]["price"] == "86.59"


def test_repeat_resume_formats_from_the_cycle_record():
    """Проба 6(б), вторая половина: пустые слоты и живая запись — круг оформлен из записи."""
    marked = json.loads(json.dumps(_hyj_checkpoint()))
    resume_complete_mutate("L0", OPEN_ID, CLOSE_ID)(marked)
    executor = _executor()
    apply_checkpoint(executor, marked, [])
    level = executor.grid_levels[0]
    assert _level_state(level) == "NOT_ACTIVE"
    entries = executor._filled_orders
    assert len(entries) == 2
    opened = next(item for item in entries if item["trade_type"] == "BUY")
    assert opened["client_order_id"] == OPEN_ID
    assert opened["executed_amount_quote"] == str(Decimal("0.13") * Decimal("86.7"))
    _grid_reads(executor)


def test_new_cycle_open_is_not_glued_to_the_old_sale(tmp_path, monkeypatch):
    """Проба 8: новый вход рядом с меткой — старая продажа не воскрешается, вход при хозяине."""
    _journal(tmp_path, monkeypatch, [
        {"executor_id": EXECUTOR, "order_id": "new-open-1", "level_id": "L0", "price": "86.1", "ts": 300},
    ])
    marked = json.loads(json.dumps(_hyj_checkpoint()))
    resume_complete_mutate("L0", OPEN_ID, CLOSE_ID)(marked)
    captured = {
        "version": 1,
        "type": "grid_executor",
        "levels": [
            {
                "id": "L0",
                "price": "86.01",
                "amount_quote": "11.28465",
                "take_profit": "0.0001",
                "side": "BUY",
                "open_order_type": "MARKET",
                "take_profit_order_type": "MARKET",
                "open_order": {"order_id": "new-open-1", "level_id": "L0", "price": "86.1"},
            }
        ],
    }
    merged = json.loads(checkpoint_keeping_holds(json.dumps(marked), captured))
    level = merged["levels"][0]
    assert (level.get("open_order") or {}).get("order_id") == "new-open-1"
    assert (level.get("close_order") or {}) in (None, {})
    executor = _executor()
    apply_checkpoint(executor, merged, [])
    assert _level_state(executor.grid_levels[0]) == "NOT_ACTIVE"
    assert not executor._filled_orders
    assert seat_recorded_level_opens(executor, EXECUTOR) is True
    seated = executor.grid_levels[0].active_open_order
    assert seated is not None and seated.order_id == "new-open-1"
    assert _level_state(executor.grid_levels[0]) == "OPEN_ORDER_PLACED"


def test_seat_skips_the_finished_cycle_open(tmp_path, monkeypatch):
    """Пробы 3 и 9: мёртвый номер входа не возвращается ни из памяти, ни из метки."""
    _journal(tmp_path, monkeypatch, [
        {"executor_id": EXECUTOR, "order_id": OPEN_ID, "level_id": "L0", "price": "86.7", "ts": 100},
        {"executor_id": EXECUTOR, "order_id": CLOSE_ID, "level_id": "L0", "role": "close",
         "published": True, "price": "86.59", "ts": 200},
    ])
    checkpoint = _hyj_checkpoint()
    executor = _executor()
    apply_checkpoint(executor, checkpoint, [])
    assert seat_recorded_level_opens(executor, EXECUTOR) is True
    assert executor.grid_levels[0].active_open_order is None
    # Долговечная метка конца закрывает тот же номер и без оформления в памяти.
    marked = json.loads(json.dumps(checkpoint))
    resume_complete_mutate("L0", OPEN_ID, CLOSE_ID)(marked)
    executor_two = _executor()
    apply_checkpoint(executor_two, marked, [])
    assert seat_recorded_level_opens(executor_two, EXECUTOR) is True
    assert executor_two.grid_levels[0].active_open_order is None


def test_live_unfilled_close_keeps_today_seating(tmp_path, monkeypatch):
    """Проба 4: непроданная продажа — пересадка входа как сегодня, чужой номер не клеится."""
    _journal(tmp_path, monkeypatch, [
        {"executor_id": EXECUTOR, "order_id": "own-1", "level_id": "L0", "price": "86.7", "ts": 100},
        {"executor_id": EXECUTOR, "order_id": "close-live-1", "level_id": "L0", "role": "close",
         "published": True, "price": "86.9", "ts": 200},
    ])
    checkpoint = _hyj_checkpoint()
    level = checkpoint["levels"][0]
    level["open_order"]["order_id"] = "own-1"
    level["close_order"] = {"order_id": "close-live-1", "level_id": "L0", "price": "86.9"}
    executor = _executor()
    apply_checkpoint(executor, checkpoint, [])
    executor.grid_levels[0].update_state()
    assert _level_state(executor.grid_levels[0]) == "CLOSE_ORDER_PLACED"
    assert not executor._filled_orders
    executor.grid_levels[0].active_open_order = SimpleNamespace(order_id="old-slot")
    assert seat_recorded_level_opens(executor, EXECUTOR) is True
    assert executor.grid_levels[0].active_open_order.order_id == "own-1"


def test_seed_ledger_skips_finished_cycle_ids():
    """Замечание 5 вердикта 035538: журнал исполнителя не держит номеров круга."""
    checkpoint = _hyj_checkpoint()
    executor = _executor()
    apply_checkpoint(executor, checkpoint, [])
    ledger = order_ledger(executor)
    assert OPEN_ID not in ledger and CLOSE_ID not in ledger


def test_stop_mark_visible_level(tmp_path, monkeypatch):
    """Проба 7 (метка стопа): подъёмный CAS конца не пишет, оформляет только память."""
    from utils.executor_checkpoint import STOP_MARK_KEY, stop_mark_visible

    checkpoint = _hyj_checkpoint()
    checkpoint["partial_ends"] = {STOP_MARK_KEY: {"kind": "stop_requested", "order_ids": []}}
    assert stop_mark_visible(checkpoint) is True
    assert completed_cycle_levels(checkpoint)
    service, repo, _connector = _service_with(monkeypatch, None, tmp_path=tmp_path)
    calls = []

    async def _no_cas(*args, **kwargs):
        calls.append(args)
        return False

    service._partial_cas = _no_cas
    import asyncio

    assert asyncio.run(service._mark_completed_cycles(EXECUTOR, checkpoint)) is True
    assert calls == []


def test_cas_failure_returns_reason_and_keeps_the_column_locked(tmp_path, monkeypatch):
    """Проба 7 (отказ записи): возврат до apply_checkpoint, замок записи держит."""
    import asyncio

    checkpoint = _hyj_checkpoint()
    reason = asyncio.run(_resume_reason(monkeypatch, checkpoint))
    assert reason == "complete level end mark was not committed"


async def _resume_reason(monkeypatch, checkpoint):
    """Полный путь _resume_one до подъёмного CAS, с подсменой тяжёлых соседей."""
    from test.test_executor_checkpoint import _Repo, _running_row, _service
    import services.executor_service as service_mod

    repo = _Repo([_running_row(executor_id=EXECUTOR, pair=PAIR, checkpoint=json.dumps(checkpoint))])
    repo.rows[0]["config"] = json.dumps({"id": EXECUTOR, "type": "grid_executor"})
    connector = SimpleNamespace(
        in_flight_orders={}, _order_tracker=SimpleNamespace(lost_orders={}),
    )
    service = _service(monkeypatch, repo, connector)

    async def _gate(row):
        return None

    service._marked_resume_gate = _gate
    service._partial_must_keep = lambda row: False
    service_mod.must_keep_executor = lambda *args, **kwargs: False
    service._get_trading_interface = lambda account: SimpleNamespace(
        current_timestamp=1, get_connector=lambda name: connector,
    )

    async def _prepare(*args, **kwargs):
        return None

    service._prepare_market = _prepare
    applied = []
    service_mod.apply_checkpoint = lambda *args, **kwargs: applied.append(args)

    class _ResumeExec:
        def __init__(self, strategy=None, config=None, update_interval=None, max_retries=None):
            self.config = config or SimpleNamespace(trading_pair=PAIR, connector_name="gate_io")
            self._close_ban_ids = set()
            self._close_may_open = False
            self.connectors = {}

        def place_order(self, *args, **kwargs):
            return "stub-1"

        def get_open_orders_to_create(self):
            return []

    service._validate_executor_config = lambda config, default_timestamp=0: (
        _ResumeExec, None, SimpleNamespace(id=EXECUTOR, trading_pair=PAIR, connector_name="gate_io"),
    )
    service.max_retries = 1
    service.update_interval = 1.0

    async def _failing_cas(*args, **kwargs):
        return False

    service._partial_cas = _failing_cas
    reason = await service._resume_one(repo.rows[0])
    assert EXECUTOR in (getattr(service, "_resume_column_hold", None) or set())
    assert EXECUTOR in service._kept_ids
    assert applied == []
    return reason


def test_resume_sets_the_column_hold_before_any_await(tmp_path, monkeypatch):
    """Проба 6(б): замок ставится в начале обработки строки, до регистрации и await'ов."""
    import asyncio

    row = {
        "executor_id": EXECUTOR,
        "executor_type": "grid_executor",
        "account_name": "main",
        "connector_name": "gate_io",
        "trading_pair": PAIR,
        "controller_id": "main",
        "config": None,
        "checkpoint": None,
        "status": "RUNNING",
    }
    service, _repo, _connector = _service_with(monkeypatch, None, tmp_path=tmp_path)
    reason = asyncio.run(service._resume_one(row))
    assert reason == "no stored config"
    assert EXECUTOR in (service.__dict__.get("_resume_column_hold") or set())


def test_checkpoint_executor_respects_the_column_hold(tmp_path, monkeypatch):
    """Проба 6(а): при замке контрольный цикл не подменяет геометрию уровня."""
    import asyncio

    service, repo, _connector = _service_with(monkeypatch, None, tmp_path=tmp_path)
    executor = _executor()
    from hummingbot.core.data_type.common import OrderType, TradeType
    from hummingbot.strategy_v2.executors.grid_executor.data_types import GridLevel

    executor.grid_levels = [GridLevel(
        id="L0", price=Decimal("86.01"), amount_quote=Decimal("11.28465"),
        take_profit=Decimal("0.0001"), side=TradeType.BUY,
        open_order_type=OrderType.MARKET, take_profit_order_type=OrderType.MARKET,
    )]
    executor.is_closed = False
    service._active_executors[EXECUTOR] = executor
    service._executor_metadata[EXECUTOR] = {
        "account_name": "main", "connector_name": "gate_io",
        "trading_pair": PAIR, "executor_type": "grid_executor",
    }
    service._resume_column_hold = {EXECUTOR}

    async def _replace(executor_id, seen=None, new_text=None, **kwargs):
        repo.holds.append((executor_id, new_text))
        return 1

    repo.replace_checkpoint_if_unchanged = _replace
    asyncio.run(service._checkpoint_executor(EXECUTOR))
    assert repo.holds == []
    service._resume_column_hold = set()
    asyncio.run(service._checkpoint_executor(EXECUTOR))
    assert repo.holds, "после снятия замка запись возобновляется"


def test_first_tick_reseats_the_finished_sale_accepted_residue(tmp_path, monkeypatch):
    """Проба 11, принятый остаток: первый такт сажает старую продажу, уровень молчит."""
    import asyncio

    marked = json.loads(json.dumps(_hyj_checkpoint()))
    resume_complete_mutate("L0", OPEN_ID, CLOSE_ID)(marked)
    _journal(tmp_path, monkeypatch, [
        {"executor_id": EXECUTOR, "order_id": OPEN_ID, "level_id": "L0", "price": "86.7", "ts": 100},
        {"executor_id": EXECUTOR, "order_id": CLOSE_ID, "level_id": "L0", "role": "close",
         "published": True, "price": "86.59", "ts": 200},
    ])
    service, _repo, _connector = _service_with(monkeypatch, None, tmp_path=tmp_path)
    executor = _executor()
    apply_checkpoint(executor, marked, [])
    service._active_executors[EXECUTOR] = executor
    service._stop_mark_now = lambda executor_id: False
    fresh = {
        "ready": True,
        "opens_read": True,
        "opens_done": True,
        "finished_read": True,
        "finished_done": True,
        "opens": [],
        "finished": [
            {"text": CLOSE_ID, "state": "filled", "side": "SELL",
             "fill_size": "0.13", "avgPx": "86.59"},
        ],
        "finished_blank_px": [],
    }
    assert service._seat_restart_closes(EXECUTOR, executor, fresh) is True
    level = executor.grid_levels[0]
    assert level.active_close_order is not None
    assert level.active_close_order.order_id == CLOSE_ID
    # Принятый остаток: уровень становится COMPLETE по одной посаженной продаже
    # (вход пуст) и замолкает — новых предложений и отказных продаж нет. Штатный
    # update_grid_levels на таком уровне не падает только с нашим ноябрьским
    # патчем грида (None-защита), в чистом образе его нет — проверяем состоянием.
    level.update_state()
    assert _level_state(level) == "COMPLETE"
    level.update_state()
    assert _level_state(level) == "COMPLETE"
