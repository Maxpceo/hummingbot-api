"""Пробы безопасной заморозки (план stop-freeze, раздел «Чем проверим»).

Пять проверок: метка «стоп запрошен» переживает штатный захват и перезагрузку;
`early_stop` не уходит до подтверждённой метки и идёт мимо отказа обёртки;
чужая склейка и неподтверждённая отправка держат с причиной; под меткой ни один
путь не чеканит и не шлёт, записи уровней не трогаются, цена тейка остаётся
занятой для соседней сетки; гашение и финализация не отдаются (brake-held).

Плюс пробы вердикта кода 20261012-212613-69817: гонка mutate (метка в теле CAS
после гейта — запись не проходит, цена тейка цела); перезагрузка после коммита
метки (RAM холодный — метка прогрета, обёртка не отказывает, отмены ушли);
чужая заявка на цене тейка НЕ держит команду (правило номера), нечитаемые
списки биржи ни на что не влияют, доводка сама доставляет отмену по каждому
нашему номеру (до `_place_cancel`/`strategy.cancel`, мимо защит) и кончается на
доказанных; задача канала закрытия уже в очереди, метка закоммичена —
`_create_order` не вызван.
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
        self.cancelled = []

    async def _create_order(self, **kwargs):
        self.created.append(kwargs)

    async def _place_cancel(self, order_id, tracked):
        self.cancelled.append(str(order_id))
        return True

    def buy(self, *args, **kwargs):
        return None

    def sell(self, *args, **kwargs):
        return None


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


def test_stop_does_not_send_early_stop_until_mark_confirmed(tmp_path, monkeypatch):
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(open_id="buy-1")), journal_lines=[], tmp_path=tmp_path,
    )
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


def test_stop_freezes_at_once_and_names_the_undelivered_cancels(tmp_path, monkeypatch):
    """Метка сразу, отмены сразу; ответ называет номера без доставки (план п.3)."""
    _journal(tmp_path, monkeypatch, [_row("sell-1", role="close", sent=True, price="101")])
    connector = _Connector()
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(open_id="buy-1", filled=True, close_id="sell-1")),
        connector=connector,
    )
    recorded = []
    executor = _stub_executor(connector, recorded)
    executor._close_holds = [{"order_id": "sell-1", "role": "close", "level_id": "L1", "price": "101"}]
    service._active_executors[EXECUTOR] = executor

    import asyncio

    async def call():
        return await service.stop_executor(EXECUTOR)

    answer = asyncio.run(call())
    # Заморозка и отмены сразу: отказа нет, повторного нажатия нет.
    assert answer["status"] == "stopping"
    assert "undelivered-cancel" in (answer.get("pending") or "")
    assert "sell-1"[-8:] in (answer.get("pending") or "")
    assert stop_mark_visible(_stored(repo))
    assert recorded == ["early_stop"]
    # Доставка пошла сразу: наши номера вне трекера отменяются по client id.
    assert connector.cancelled == ["buy-1", "sell-1"]


def test_cancel_delivery_reaches_place_cancel_for_untracked_number(tmp_path, monkeypatch):
    """Проба 6 плана: номер в журнале и в `_close_holds`, сидит в уровне, вне трекера.

    Доставкой считается только отмена, дошедшая до биржи: виден `_place_cancel`
    по номеру, а не stub `early_stop` и не пустой `_api_delete`; повторы
    кончаются, когда номер доказан снятым или мёртвым.
    """
    _journal(tmp_path, monkeypatch, [_row("sell-1", role="close", sent=True, price="101")])
    connector = _Connector()
    text = _marked(_checkpoint(_level(open_id="buy-1", filled=True, close_id="sell-1")))
    service, repo, _ = _service_with(monkeypatch, text, connector=connector)
    recorded = []
    executor = _stub_executor(connector, recorded)
    executor._close_holds = [{"order_id": "sell-1", "role": "close", "level_id": "L1", "price": "101"}]
    service._active_executors[EXECUTOR] = executor
    service.__dict__.pop("_stop_mark_ram", None)  # холодная RAM после перезагрузки

    import asyncio

    async def run():
        await service._stop_deliver_cancels(EXECUTOR, executor, connector, PAIR)

    asyncio.run(run())
    assert connector.cancelled == ["buy-1", "sell-1"]
    assert connector.created == []
    assert recorded == []

    # Повтор без кнопки, пока номера не доказаны.
    service.__dict__.setdefault("_stop_finish_last", {})[EXECUTOR] = 0.0
    asyncio.run(run())
    assert connector.cancelled == ["buy-1", "sell-1", "buy-1", "sell-1"]

    # Номера доказаны снятыми — повторы кончились.
    done = json.loads(text)
    done.setdefault("partial_ends", {})
    done["partial_ends"]["sell-1"] = {"kind": "cancelled", "order_ids": ["sell-1"]}
    done["partial_ends"]["buy-1"] = {"kind": "cancelled", "order_ids": ["buy-1"]}
    repo.rows[0]["checkpoint"] = json.dumps(done, sort_keys=True)
    service._set_hold_rows(repo.rows)
    service.__dict__.setdefault("_stop_finish_last", {})[EXECUTOR] = 0.0
    asyncio.run(run())
    assert connector.cancelled == ["buy-1", "sell-1", "buy-1", "sell-1"]


def test_cancel_delivery_passes_the_guard_for_tracked_number(tmp_path, monkeypatch):
    """Номер в трекере: штатная отмена проходит мимо защит только для доставки (п.2)."""
    connector = _Connector()
    connector.in_flight_orders["sell-1"] = SimpleNamespace(price="101", client_order_id="sell-1")
    service, repo, _ = _service_with(
        monkeypatch, _marked(_checkpoint(_level(open_id="buy-1", filled=True, close_id="sell-1"))),
        connector=connector, journal_lines=[], tmp_path=tmp_path,
    )
    recorded = []
    executor = _stub_executor(connector, recorded)
    executor._close_holds = [{"order_id": "sell-1", "role": "close", "level_id": "L1", "price": "101"}]
    sent = []

    def strategy_cancel(*args, **kwargs):
        order_id = kwargs.get("order_id") or (args[-1] if args else None)
        sent.append(str(order_id))
        return order_id

    executor._strategy = SimpleNamespace(cancel=strategy_cancel)
    service._active_executors[EXECUTOR] = executor
    service._install_partial_guards(EXECUTOR, executor)

    # Вне доставки обёртка гасит отмену: sell-1 в `_close_holds`.
    assert executor._strategy.cancel(order_id="sell-1") is None
    assert sent == []

    import asyncio
    assert asyncio.run(service._stop_deliver_cancel(executor, connector, "sell-1", PAIR)) is True
    assert sent == ["sell-1"]


def test_unreadable_reread_does_not_erase_the_mark(monkeypatch):
    """Непрочитанная перечитка не стирает метку из RAM (план п.1)."""
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor
    service.__dict__.setdefault("_stop_mark_ram", {})[EXECUTOR] = True

    async def unreadable(_executor_id):
        return False

    service._read_stop_mark_fresh = unreadable

    import asyncio
    assert asyncio.run(service._freeze_stop_mark(EXECUTOR)) is True
    assert service._stop_mark_now(EXECUTOR) is True
    assert asyncio.run(service._refresh_stop_mark(EXECUTOR)) is True


def test_failed_commit_publishes_no_mark_and_sends_no_cancels(tmp_path, monkeypatch):
    """Коммит метки не завершён: метка не видна никому, отмены не ушли (проба 9)."""
    from contextlib import asynccontextmanager
    connector = _Connector()
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(open_id="buy-1", filled=True, close_id="sell-1")),
        connector=connector, journal_lines=[], tmp_path=tmp_path,
    )
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor
    saved = dict(repo.rows[0])

    @asynccontextmanager
    async def commit_fails():
        try:
            yield SimpleNamespace()
        finally:
            # Коммит не вернулся: незакреплённая запись исчезает, как в транзакции.
            repo.rows[0].clear()
            repo.rows[0].update(saved)
            raise RuntimeError("commit failed")

    service.db_manager = SimpleNamespace(get_session_context=lambda: commit_fails())

    import asyncio

    async def call():
        return await service.stop_executor(EXECUTOR)

    answer = asyncio.run(call())
    assert answer["status"] == "not_accepted"
    assert answer.get("reason")
    # Отмены до возврата коммита не уходят и цену не освобождают.
    assert recorded == []
    assert connector.cancelled == []
    assert connector.created == []
    # Метка не опубликована: ни RAM, ни запись её не несут.
    assert service._stop_mark_now(EXECUTOR) is False
    # А непрочитаемость ДЕРЖИТ отправку, а не открывает её (п.2).
    assert asyncio.run(service._refresh_stop_mark(EXECUTOR)) is True


def test_commit_returned_holds_freeze_when_the_next_session_fails(tmp_path, monkeypatch):
    """Коммит вернулся, вторая сессия упала — заморозка держится (проба 11)."""
    from contextlib import asynccontextmanager
    connector = _Connector()
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(open_id="buy-1", filled=True, close_id="sell-1")),
        connector=connector, journal_lines=[], tmp_path=tmp_path,
    )
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor
    calls = {"n": 0}

    @asynccontextmanager
    async def third_fails():
        calls["n"] += 1
        try:
            yield SimpleNamespace()
        finally:
            if calls["n"] >= 3:
                raise RuntimeError("session failed")

    service.db_manager = SimpleNamespace(get_session_context=lambda: third_fails())

    import asyncio

    async def call():
        return await service.stop_executor(EXECUTOR)

    answer = asyncio.run(call())
    # Ответ честный: стоп принят, отмены и доставка пошли от опубликованного тела.
    assert answer["status"] == "stopping"
    assert stop_mark_visible(_stored(repo))
    assert service._stop_mark_now(EXECUTOR) is True
    assert recorded == ["early_stop"]
    assert connector.cancelled == ["buy-1", "sell-1"]

    # Упавшая перечитка не открывает отправку: гейт видит опубликованное тело.
    asyncio.run(
        service._partial_send_committed(EXECUTOR, executor, connector, PAIR, "sell-2", "L1", "101", Decimal("1"))
    )
    assert connector.created == []


def test_fill_down_writer_refuses_a_marked_body(monkeypatch):
    """Дверь постановки не пишет close_order с level_id на теле с меткой."""
    text = _marked(_checkpoint(_level(open_id="buy-1", filled=True)))
    service, repo, connector = _service_with(monkeypatch, text)
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor
    before = _stored(repo)

    import asyncio
    assert asyncio.run(
        service._write_fill_down_close(EXECUTOR, "L2", "buy-2", Decimal("100"), Decimal("1"), "sell-2", "101")
    ) is False
    assert _stored(repo) == before


def test_unreadable_confirmation_after_cas_still_freezes_and_delivers(tmp_path, monkeypatch):
    """CAS прошёл, новая сессия вернула None, RAM холодный — заморозка держится.

    Вердикт 20261013-075243-9283 и план п.1: гейты читают RAM ИЛИ запись —
    уже стоящая отправка не уходит (`_create_order` не вызван), доставка отмен
    по нашим номерам всё равно идёт.
    """
    connector = _Connector()
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(open_id="buy-1", filled=True, close_id="sell-1")),
        connector=connector, journal_lines=[], tmp_path=tmp_path,
    )
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor

    async def column_unreadable(_executor_id):
        raise RuntimeError("column unreadable")

    service._partial_column_text = column_unreadable

    import asyncio

    async def call():
        return await service.stop_executor(EXECUTOR)

    answer = asyncio.run(call())
    assert answer["status"] == "stopping"
    assert stop_mark_visible(_stored(repo))
    # Закоммиченное тело опубликовано синхронно: заморозка держится без перечитки.
    assert service._stop_mark_now(EXECUTOR) is True
    # Доставка отмен по нашим номерам пошла, несмотря на непрочитанную перечитку.
    assert recorded == ["early_stop"]
    assert connector.cancelled == ["buy-1", "sell-1"]

    # Уже стоящая отправка не уходит: гейт читает RAM ИЛИ запись.
    asyncio.run(
        service._partial_send_committed(EXECUTOR, executor, connector, PAIR, "sell-2", "L1", "101", Decimal("1"))
    )
    assert connector.created == []
    assert asyncio.run(service._refresh_stop_mark(EXECUTOR)) is True


def test_close_failure_after_commit_still_publishes_the_mark(tmp_path, monkeypatch):
    """Коммит вернулся, close сессии упал — метка опубликована ХУКОМ commit.

    Вердикт 20261013-095125-3865 (minor): сессия имеет `commit`, close падает
    после его возврата, строки `_hold_rows` не делят dict с репозиторием —
    публикация обязана пройти внутри commit, до close.
    """
    from contextlib import asynccontextmanager
    connector = _Connector()
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(open_id="buy-1", filled=True, close_id="sell-1")),
        connector=connector, journal_lines=[], tmp_path=tmp_path,
    )
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor
    # `_hold_rows` не делит dict с репозиторием: видимость — только от публикации.
    service._set_hold_rows([dict(row) for row in repo.rows])
    events = []
    original_publish = service._publish_committed_body

    def publish(executor_id, body):
        events.append("publish")
        original_publish(executor_id, body)

    service._publish_committed_body = publish

    async def session_commit():
        events.append("commit")

    @asynccontextmanager
    async def close_fails():
        session = SimpleNamespace(commit=session_commit)
        try:
            yield session
        finally:
            # Штатный контекст сперва коммитит, потом закрывает сессию.
            await session.commit()
            events.append("close")
            raise RuntimeError("close failed")

    service.db_manager = SimpleNamespace(get_session_context=lambda: close_fails())

    import asyncio

    async def call():
        return await service.stop_executor(EXECUTOR)

    answer = asyncio.run(call())
    assert answer["status"] == "stopping"
    assert stop_mark_visible(_stored(repo))
    # Публикация прошла ВНУТРИ commit — между коммитом и close.
    publish_at = events.index("publish")
    assert events[publish_at - 1] == "commit"
    assert events[publish_at + 1] == "close"
    assert service._stop_mark_now(EXECUTOR) is True
    assert recorded == ["early_stop"]
    assert connector.cancelled == ["buy-1", "sell-1"]


def test_close_channel_unreadable_check_holds_the_send(tmp_path, monkeypatch):
    """Канал с НАСТОЯЩЕЙ проверкой: непрочитанная колонка держит отправку."""
    connector = _Connector()
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(take="101")),
        connector=connector, journal_lines=[], tmp_path=tmp_path,
    )
    executor, _ = _channel_executor(connector)
    executor._next_close_id = "close-5"
    service._active_executors[EXECUTOR] = executor
    service._install_partial_guards(EXECUTOR, executor)
    queued = _queue_send(monkeypatch)
    _stub_trade_types(monkeypatch)

    async def column_unreadable(_executor_id):
        raise RuntimeError("column unreadable")

    service._partial_column_text = column_unreadable
    level = SimpleNamespace(
        id="L1",
        price=Decimal("101"),
        amount_quote=Decimal("1"),
        active_open_order=SimpleNamespace(order_id="buy-1"),
        active_close_order=None,
    )

    import asyncio

    async def run():
        assert ledger_mod.place_level_close(executor, EXECUTOR, level) == "close-5"
        assert len(queued) == 1
        await queued.pop()
        assert connector.created == []

    asyncio.run(run())


def test_foreign_order_by_price_does_not_hold(tmp_path, monkeypatch):
    """Чужая заявка на цене тейка — не помеха и не держатель (правило номера)."""
    connector = _Connector()
    connector.in_flight_orders["foreign-1"] = SimpleNamespace(price="101", client_order_id="foreign-1")
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(open_id="buy-1", close_id="sell-1")),
        connector=connector, journal_lines=[], tmp_path=tmp_path,
    )
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor

    async def fresh(_connector, _pair, _ts=None):
        return {
            "opens_read": True,
            "opens_done": True,
            "finished_read": True,
            "finished_done": True,
            "opens": [{"text": "foreign-2", "status": "open", "price": "101.0000"}],
            "finished": [],
        }

    service._fresh_raw_lists = fresh

    import asyncio

    async def call():
        return await service.stop_executor(EXECUTOR)

    answer = asyncio.run(call())
    assert answer["status"] == "stopping"
    assert "undelivered-cancel" in (answer.get("pending") or "")
    assert stop_mark_visible(_stored(repo))
    assert recorded == ["early_stop"]


def test_unreadable_opens_do_not_hold(tmp_path, monkeypatch):
    """Нечитаемые списки биржи ни на что не влияют: стоп принят, метка записана."""
    connector = _Connector()
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(open_id="buy-1", close_id="sell-1")),
        connector=connector, journal_lines=[], tmp_path=tmp_path,
    )
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor

    async def broken(_connector, _pair, _ts=None):
        raise OSError("lists unreadable")

    service._fresh_raw_lists = broken

    import asyncio

    async def call():
        return await service.stop_executor(EXECUTOR)

    answer = asyncio.run(call())
    assert answer["status"] == "stopping"
    assert stop_mark_visible(_stored(repo))
    assert recorded == ["early_stop"]


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


def test_stop_marks_nothing_but_the_mark(tmp_path, monkeypatch):
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(open_id="buy-1", filled=True, close_id="sell-1")),
        journal_lines=[], tmp_path=tmp_path,
    )
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


# ── гонка mutate: метка в теле CAS после гейта ──────────────────────────────


def _marked(text):
    body = json.loads(text)
    ends = dict(body.get("partial_ends") or {})
    ends[STOP_MARK_KEY] = {"kind": "stop_requested", "order_ids": []}
    body["partial_ends"] = ends
    return json.dumps(body, sort_keys=True)


def _gate_misses_mark(service):
    """Гейт до await метку не видел: его перечитка шла до её появления."""

    async def gate_misses(_executor_id):
        return False

    service._read_stop_mark_fresh = gate_misses


def test_mutate_race_keeps_the_take_price_on_a_marked_body(monkeypatch):
    """Метка в теле CAS, гейт её не видел: конец с level_id не проходит."""
    text = _checkpoint(_level(open_id="buy-1", filled=True, close_id="sell-1"))
    service, repo, connector = _service_with(monkeypatch, text)
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor
    _gate_misses_mark(service)
    repo.rows[0]["checkpoint"] = _marked(text)
    service._set_hold_rows(repo.rows)
    before = _stored(repo)

    import asyncio
    assert asyncio.run(
        service._partial_release(EXECUTOR, "buy-1", "L1", "stock_flat", connector, PAIR)
    ) is False

    after = _stored(repo)
    assert after == before
    assert (after["levels"][0] or {}).get("take_profit_price") == "101"
    assert ((after["levels"][0] or {}).get("close_order") or {}).get("order_id") == "sell-1"
    assert "level:L1" not in partial_price_released_ids(after)
    for mark in (after.get("partial_ends") or {}).values():
        assert "level_id" not in (mark or {})


def test_mutate_race_in_flat_reject_keeps_the_take_price(monkeypatch):
    """Та же гонка в flat-ветке _partial_reject: тело с меткой не меняется."""
    level = _level(open_id="buy-1", filled=True, close_id="sell-1")
    level["partial_bought"] = "1"
    level["partial_closes"] = []
    text = _checkpoint(level)
    service, repo, connector = _service_with(monkeypatch, text)
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor
    _gate_misses_mark(service)

    async def fresh(_connector, _pair, _ts=None):
        return {
            "opens_read": True,
            "opens_done": True,
            "finished_read": True,
            "finished_done": True,
            "opens": [],
            "finished": [{
                "text": "sell-1", "status": "closed", "finish_as": "cancelled",
                "amount": "1", "left": "0", "price": "101",
            }],
        }

    service._fresh_raw_lists = fresh
    repo.rows[0]["checkpoint"] = _marked(text)
    service._set_hold_rows(repo.rows)
    before = _stored(repo)

    import asyncio
    asyncio.run(service._partial_reject(
        EXECUTOR, executor, connector, PAIR, "sell-1", "L1", "101", Decimal("1"), RuntimeError("create failed"),
    ))

    after = _stored(repo)
    assert after == before
    assert (after["levels"][0] or {}).get("take_profit_price") == "101"
    assert "level:L1" not in partial_price_released_ids(after)


# ── перезагрузка после коммита метки: RAM холодный, отмены ушли ─────────────


def test_restart_after_mark_commit_warms_ram_and_sends_the_cancels(tmp_path, monkeypatch):
    """Обрыв между коммитом метки и early_stop: метка прогрета, обёртка не отказывает."""
    text = _marked(_checkpoint(_level(open_id="buy-1", filled=True, close_id="sell-1")))
    service, repo, connector = _service_with(monkeypatch, text, journal_lines=[], tmp_path=tmp_path)
    recorded = []
    executor = _stub_executor(connector, recorded)
    executor._status = "STOPPING"
    service._active_executors[EXECUTOR] = executor
    service._install_partial_guards(EXECUTOR, executor)
    service.__dict__.pop("_stop_mark_ram", None)  # RAM холодный после перезапуска

    import asyncio

    async def call():
        return await service.stop_executor(EXECUTOR)

    answer = asyncio.run(call())
    assert answer["status"] == "frozen"
    # Обёртка _wrap_stop не отказала: early_stop прошёл — отмены ушли штатно.
    assert recorded == ["early_stop"]
    # ... и не поставила RUNNING.
    assert executor._status == "STOPPING"
    # Метка прогрета до early_stop.
    assert (service.__dict__.get("_stop_mark_ram") or {}).get(EXECUTOR) is True

    # Холодная RAM — не отсутствие метки: гейт читает RAM ИЛИ перечитанную запись.
    service.__dict__.pop("_stop_mark_ram", None)
    assert service._stop_mark_now(EXECUTOR) is True

    # Подъём до start прогревает метку из записи строки.
    service.__dict__.pop("_stop_mark_ram", None)
    row = dict(repo.rows[0])
    row["config"] = ""
    assert asyncio.run(service._resume_one(row)) == "no stored config"
    assert (service.__dict__.get("_stop_mark_ram") or {}).get(EXECUTOR) is True


# ── чужая склейка по свежему opens ──────────────────────────────────────────


def test_stop_passes_when_the_take_price_in_opens_is_our_own(tmp_path, monkeypatch):
    """Своя заявка на цене тейка не держит: её снимает штатная отмена."""
    connector = _Connector()
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(open_id="buy-1", close_id="sell-1")),
        connector=connector, journal_lines=[], tmp_path=tmp_path,
    )
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor

    async def fresh(_connector, _pair, _ts=None):
        return {
            "opens_read": True,
            "opens_done": True,
            "finished_read": True,
            "finished_done": True,
            "opens": [{"text": "sell-1", "status": "open", "price": "101"}],
            "finished": [],
        }

    service._fresh_raw_lists = fresh

    import asyncio

    async def call():
        return await service.stop_executor(EXECUTOR)

    answer = asyncio.run(call())
    assert answer["status"] == "stopping"
    assert stop_mark_visible(_stored(repo))
    assert recorded == ["early_stop"]


def test_own_close_journal_line_with_level_id_does_not_hold(tmp_path, monkeypatch):
    """Свой close с журнальной строкой level_id (так пишет place_level_close) не держит.

    Воспроизведение вердикта 20261013-005043-97561: журнал sell-1/level_id=L1
    и sell-1 на цене тейка в opens/inflight — стоп обязан приняться.
    """
    connector = _Connector()
    connector.in_flight_orders["sell-1"] = SimpleNamespace(price="101", client_order_id="sell-1")
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(open_id="buy-1", close_id="sell-1")),
        connector=connector,
        journal_lines=[_row("sell-1", role="close", sent=True, price="101")],
        tmp_path=tmp_path,
    )
    recorded = []
    executor = _stub_executor(connector, recorded)
    service._active_executors[EXECUTOR] = executor

    async def fresh(_connector, _pair, _ts=None):
        return {
            "opens_read": True,
            "opens_done": True,
            "finished_read": True,
            "finished_done": True,
            "opens": [{"text": "sell-1", "status": "open", "price": "101"}],
            "finished": [],
        }

    service._fresh_raw_lists = fresh

    import asyncio

    async def call():
        return await service.stop_executor(EXECUTOR)

    answer = asyncio.run(call())
    assert answer["status"] == "stopping"
    assert stop_mark_visible(_stored(repo))
    assert recorded == ["early_stop"]


# ── отложенная отправка канала закрытия ─────────────────────────────────────


def _channel_executor(connector):
    recorded = []
    executor = _stub_executor(connector, recorded)
    executor._strategy = SimpleNamespace(connectors={"gate_io": connector})
    return executor, recorded


def _queue_send(monkeypatch):
    """Задачи канала копятся в очереди: проба гоняет их после записи метки."""
    queued = []
    import hummingbot.core.utils.async_utils as async_utils
    monkeypatch.setattr(async_utils, "safe_ensure_future", lambda coro: queued.append(coro))
    return queued


def _stub_trade_types(monkeypatch):
    """Мелочь для вызова вне коробки: enum биржи не влияет на выбор стороны."""
    import hummingbot.core.data_type.common as common
    monkeypatch.setattr(common, "TradeType", SimpleNamespace(SELL="SELL", BUY="BUY"), raising=False)
    monkeypatch.setattr(common, "OrderType", SimpleNamespace(LIMIT="LIMIT"), raising=False)


def test_close_channel_task_holds_the_send_under_the_mark(tmp_path, monkeypatch):
    """Задача канала уже в очереди, метка закоммичена: _create_order не вызван."""
    connector = _Connector()
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(take="101")),
        connector=connector, journal_lines=[], tmp_path=tmp_path,
    )
    executor, _ = _channel_executor(connector)
    executor._next_close_id = "close-2"
    service._active_executors[EXECUTOR] = executor
    service._install_partial_guards(EXECUTOR, executor)
    queued = _queue_send(monkeypatch)
    _stub_trade_types(monkeypatch)
    level = SimpleNamespace(
        id="L1",
        price=Decimal("101"),
        amount_quote=Decimal("1"),
        active_open_order=SimpleNamespace(order_id="buy-1"),
        active_close_order=None,
    )

    import asyncio

    async def run():
        assert ledger_mod.place_level_close(executor, EXECUTOR, level) == "close-2"
        assert len(queued) == 1
        assert await service._freeze_stop_mark(EXECUTOR) is True
        await queued.pop()
        assert connector.created == []

    asyncio.run(run())


def test_market_close_channel_task_holds_the_send_under_the_mark(tmp_path, monkeypatch):
    """Та же проба для очереди place_market_close."""
    connector = _Connector()
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(take="101")),
        connector=connector, journal_lines=[], tmp_path=tmp_path,
    )
    executor, _ = _channel_executor(connector)
    executor._next_close_id = "close-3"
    service._active_executors[EXECUTOR] = executor
    service._install_partial_guards(EXECUTOR, executor)
    queued = _queue_send(monkeypatch)
    _stub_trade_types(monkeypatch)

    import asyncio

    async def run():
        assert ledger_mod.place_market_close(executor, EXECUTOR) == "close-3"
        assert len(queued) == 1
        assert await service._freeze_stop_mark(EXECUTOR) is True
        await queued.pop()
        assert connector.created == []

    asyncio.run(run())


def test_close_channel_check_error_holds_the_send(tmp_path, monkeypatch):
    """Ошибка проверки метки в канале — заявка не уходит (проба 10)."""
    connector = _Connector()
    service, repo, _ = _service_with(
        monkeypatch, _checkpoint(_level(take="101")),
        connector=connector, journal_lines=[], tmp_path=tmp_path,
    )
    executor, _ = _channel_executor(connector)
    executor._next_close_id = "close-4"
    service._active_executors[EXECUTOR] = executor
    service._install_partial_guards(EXECUTOR, executor)
    queued = _queue_send(monkeypatch)
    _stub_trade_types(monkeypatch)

    async def broken():
        raise RuntimeError("mark check failed")

    executor._refuse_frozen_close = broken
    level = SimpleNamespace(
        id="L1",
        price=Decimal("101"),
        amount_quote=Decimal("1"),
        active_open_order=SimpleNamespace(order_id="buy-1"),
        active_close_order=None,
    )

    import asyncio

    async def run():
        assert ledger_mod.place_level_close(executor, EXECUTOR, level) == "close-4"
        assert len(queued) == 1
        await queued.pop()
        assert connector.created == []

    asyncio.run(run())
