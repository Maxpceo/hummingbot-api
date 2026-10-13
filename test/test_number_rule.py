"""Пробы «правило номера» (план number-rule, раздел «Чем проверим»).

Правило занятости quant для нового номера: наш номер из журнала, чекпоинта или
трекера держит, пока finished не показал именно эту заявку исполненной или
снятой; тейк открытой позиции держит, пока позиция не закрыта; голая цена книги
(номер не прочитан) держит и не присваивается; чужой текст с номером не держит
и не клеится. Повтор того же номера — не вторая заявка. Уровень со своим номером
продажи в журнале или чекпоинте второй номер не чеканит.
"""

from decimal import Decimal
from types import SimpleNamespace

from test.test_executor_checkpoint import _load_executor_service
from test.test_stop_freeze import (
    _checkpoint,
    _journal,
    _level,
    _row,
    _service_with,
    _stub_executor,
)

mod = _load_executor_service()
ExecutorService = mod.ExecutorService

import utils.executor_checkpoint as ledger_mod  # noqa: E402

EXECUTOR = "grid-a"
PAIR = "INJ-USDT"
TICK = Decimal("0.01")


def _fresh(opens=None, finished=None):
    return {
        "opens_read": True,
        "opens_done": True,
        "finished_read": True,
        "finished_done": True,
        "opens": opens or [],
        "finished": finished or [],
    }


def _held_prices(items):
    """Строки вида (номер, цена) и голые цены — одним списком цен."""
    found = []
    for item in items or []:
        if isinstance(item, tuple) and len(item) == 2:
            found.append(str(item[1]))
        elif item not in (None, ""):
            found.append(str(item))
    return found


def _level_stub(level_id="L1", price="100"):
    return SimpleNamespace(
        id=level_id,
        price=Decimal(price),
        side=SimpleNamespace(name="BUY"),
        amount_quote=Decimal("1"),
        active_open_order=object(),
    )


# ── 1. чужой текст не держит и не клеится ────────────────────────────────────


def test_foreign_text_does_not_hold_the_quant(monkeypatch):
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    fresh = _fresh(opens=[{"text": "foreign-bot-1", "price": "100"}])
    assert service._partial_tick_taken(connector, PAIR, "100", "sell-1", fresh, {}, {}) is False, (
        "чужой текст с номером не держит: своя заявка встаёт на ту же цену"
    )
    assert service._open_text_holds(fresh, "100", TICK, "sell-1") is False


def test_foreign_in_flight_does_not_glue_our_sell(monkeypatch, tmp_path):
    """Чужая заявка на цене тейка — не «наша продажа стоит» (склейки нет)."""
    _journal(tmp_path, monkeypatch, [_row("sell-x", level_id="", role="close", price="100", sent=True)])
    service, repo, connector = _service_with(
        monkeypatch, _checkpoint(_level(open_id="buy-1", filled=True, take="101")),
    )
    parsed = service._partial_parsed(EXECUTOR)
    connector.in_flight_orders["foreign-9"] = SimpleNamespace(price=Decimal("101"))
    assert service._partial_sell_is_live(EXECUTOR, connector, parsed, PAIR) is False, (
        "чужая на цене тейка не клеится к нашему слоту"
    )
    connector.in_flight_orders["sell-x"] = SimpleNamespace(price=Decimal("101"))
    assert service._partial_sell_is_live(EXECUTOR, connector, parsed, PAIR) is True, (
        "наш номер продажи — наша продажа стоит"
    )


# ── 2. наш номер держит, пока finished не доказал; сирота и история ─────────


def test_own_number_holds_until_finished_proves(monkeypatch, tmp_path):
    _journal(tmp_path, monkeypatch, [_row("sell-1", level_id="", role="close", price="100", sent=True)])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    service._remember_tick_fresh(PAIR, _fresh())
    assert service._ledger_close_price_taken("100", "", _fresh()) is True, (
        "наш журнальный close держит quant (в т.ч. после рестарта)"
    )
    proven = _fresh(finished=[{"text": "sell-1", "finish_as": "filled", "price": "100"}])
    assert service._ledger_close_price_taken("100", "", proven) is False, (
        "finished показал именно эту заявку исполненной — quant свободен"
    )


def test_orphan_of_a_dead_grid_holds_but_filled_history_does_not(monkeypatch, tmp_path):
    """Сирота мёртвой сетки жив на книге и держит; 0.09487 — история, не держит."""
    _journal(tmp_path, monkeypatch, [
        _row("orphan-1", level_id="L1", role="open", price="0.10", sent=True),
        _row("gone-1", level_id="L2", role="open", price="0.09487", sent=True),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    fresh = _fresh(
        opens=[{"text": "orphan-1", "price": "0.10"}],
        finished=[{"text": "gone-1", "finish_as": "filled", "price": "0.09487"}],
    )
    held = _held_prices(service._ban_pairs(PAIR, fresh))
    assert "0.10" in held, "живой номер мёртвой сетки-сироты — свой, quant держит"
    assert "0.09487" not in held, "исполненная строка без открытой позиции не держит"


def test_take_of_an_open_position_holds_the_tick(monkeypatch, tmp_path):
    """Покупка исполнена, продажа ещё не в книге — тик тейка держит вторую покупку."""
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", price="0.09487", take_profit_price="0.10", sent=True),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    fresh = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "price": "0.09487"}])
    held = _held_prices(service._ban_pairs(PAIR, fresh))
    assert "0.10" in held, "тейк открытой позиции держит quant, пока позиция не закрыта"
    assert "0.09487" not in held, "цена исполненной покупки — история, quant свободен"


# ── 3. повтор того же номера и голая цена книги ──────────────────────────────


def test_same_number_resend_is_not_a_second_order(monkeypatch):
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    fresh = _fresh(opens=[{"text": "sell-1", "price": "100"}])
    assert service._partial_tick_taken(connector, PAIR, "100", "sell-1", fresh, {}, {}) is False, (
        "повтор того же номера — не вторая заявка"
    )


def test_bare_book_price_holds_and_a_read_text_is_not_bare(monkeypatch):
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    bare = _fresh(opens=[{"price": "100"}])
    assert service._partial_tick_taken(connector, PAIR, "100", "sell-1", bare, {}, {}) is True, (
        "голая цена книги без номера держит и не выбрасывается"
    )
    items = service._ban_pairs(PAIR, bare)
    assert str("100") in _held_prices(items)
    foreign = _fresh(opens=[{"text": "foreign-2", "price": "101"}])
    assert "101" not in _held_prices(service._ban_pairs(PAIR, foreign)), (
        "строка с прочитанным чужим текстом не держит и не превращается в голую цену"
    )


# ── 4. сверка до чеканки: второй номер продажи не чеканится ──────────────────


def _prepare(monkeypatch, tmp_path, checkpoint, journal_lines):
    _journal(tmp_path, monkeypatch, journal_lines)
    service, repo, connector = _service_with(monkeypatch, checkpoint)
    recorded = []
    executor = _stub_executor(connector, recorded)
    executor._strategy = SimpleNamespace(connectors={"gate_io": connector})
    connector.trading_rules = {PAIR: SimpleNamespace(min_price_increment=TICK)}
    service._active_executors[EXECUTOR] = executor
    service._remember_tick_fresh(PAIR, _fresh())
    service._install_partial_guards(EXECUTOR, executor)
    return service, repo, connector, executor


def _ledger_rows_count():
    return len(ledger_mod.read_ledger_filtered() or [])


def test_same_level_recorded_close_blocks_the_second_mint(monkeypatch, tmp_path):
    """Уровень уже вычеканил номер продажи (чекпоинт) — второй не чеканится."""
    service, repo, connector, executor = _prepare(
        monkeypatch, tmp_path,
        _checkpoint(_level(level_id="L1", open_id="buy-1", filled=True, close_id="sell-1", take="101")),
        [],
    )
    level = _level_stub("L1", "100")
    assert executor._refuse_second_close(level) is True
    before = _ledger_rows_count()
    assert ledger_mod.place_level_close(executor, EXECUTOR, level) is None
    assert connector.created == []
    assert _ledger_rows_count() == before, "второй номер продажи не вычеканен"


def test_journal_close_after_restart_blocks_the_second_mint(monkeypatch, tmp_path):
    """Рестарт: close уровня A в журнале со sent, в opens/inflight/lost его нет,
    списки прочитаны, слот в RAM пуст — уровень B на тот же quant не чеканит и
    не шлёт второй номер."""
    service, repo, connector, executor = _prepare(
        monkeypatch, tmp_path,
        _checkpoint(
            _level(level_id="L1", open_id="buy-1", filled=True, take="101"),
            _level(level_id="L2", open_id="buy-2", filled=True, take="101"),
        ),
        [_row("sell-1", level_id="L1", role="close", price="100", sent=True)],
    )
    level_b = _level_stub("L2", "100")
    before = _ledger_rows_count()
    assert ledger_mod.place_level_close(executor, EXECUTOR, level_b) is None, (
        "журнальный close уровня A после рестарта держит quant для уровня B"
    )
    assert connector.created == [], "_create_order не уходит"
    assert _ledger_rows_count() == before, "второй номер продажи не вычеканен"


# ── 5. ворота покупки: чужая не держит, своя держит по номеру ────────────────


def test_buy_gate_follows_the_number_rule(monkeypatch, tmp_path):
    """`_partial_refuses_open`: чужая на цене не отказывает, своя запись держит."""
    service, repo, connector, executor = _prepare(
        monkeypatch, tmp_path,
        _checkpoint(_level(level_id="L1", take="101"), _level(level_id="L2", take="101")),
        [_row("buy-1", level_id="L2", role="open", price="100", sent=True)],
    )
    level = _level_stub("L1", "99")
    service._remember_tick_fresh(PAIR, _fresh(opens=[{"text": "foreign-1", "price": "100"}]))
    assert service._partial_refuses_open(EXECUTOR, executor, level, "100", TICK) is False, (
        "чужая заявка на цене не держит покупку"
    )
    service._remember_tick_fresh(PAIR, _fresh())
    assert service._partial_refuses_open(EXECUTOR, executor, _level_stub("L2", "100"), "100", TICK) is True, (
        "свой номер уровня держит слот (не ставить вторую покупку в ту же цену)"
    )


# ── 6. вердикт 20261013-151226-99598: тейк держит после фильтра, история нет ──


def test_take_of_open_position_survives_the_block_filter(monkeypatch, tmp_path):
    """Тейк исполненной покупки держит тик ДАЖЕ после `_prices_for_blocks` —
    тем списком, которым `recorded_price_blocks` режет новую покупку."""
    from utils.executor_checkpoint import recorded_price_blocks

    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", price="0.09487", take_profit_price="0.10", sent=True),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    fresh = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "price": "0.09487"}])
    blocked = service._prices_for_blocks(service._ban_pairs(PAIR, fresh), fresh)
    assert recorded_price_blocks(blocked, "0.10", TICK) is True, (
        "тейк открытой позиции держит тик: вторая покупка на него не уходит"
    )
    assert recorded_price_blocks(blocked, "0.09487", TICK) is False, (
        "цена самой исполненной покупки quant не держит"
    )


def test_cancelled_take_without_position_does_not_hold(monkeypatch, tmp_path):
    """Отмена без позиции: ни цена, ни тейк строки quant не держат."""
    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", price="0.09487", take_profit_price="0.10", sent=True),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    fresh = _fresh(finished=[{"text": "buy-1", "finish_as": "cancelled", "price": "0.09487"}])
    held = _held_prices(service._ban_pairs(PAIR, fresh))
    assert "0.09487" not in held
    assert "0.10" not in held, "тейк без открытой позиции quant не держит"


def test_tracker_only_number_holds_the_quant(monkeypatch):
    """Номер, который живёт только в трекере коннектора, — наш и держит quant."""
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    inflight = {"send-77": SimpleNamespace(price=Decimal("100"))}
    fresh = _fresh(opens=[{"text": "send-77", "price": "100"}])
    assert service._partial_tick_taken(connector, PAIR, "100", "sell-1", fresh, inflight, {}) is True, (
        "номер из трекера на книге держит quant"
    )
    assert service._partial_tick_taken(connector, PAIR, "100", "sell-1", _fresh(), inflight, {}) is True, (
        "сам трекер держит quant, пока finished не доказал номер"
    )


def test_filled_row_releases_the_price_on_the_sell_gate(monkeypatch, tmp_path):
    """Продажные ворота: исполненную строку finished доказал — quant свободен."""
    _journal(tmp_path, monkeypatch, [
        _row("gone-1", level_id="L2", role="close", price="0.09487", sent=True),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    assert service._partial_foreign_booked(EXECUTOR, connector, PAIR, "0.09487", "sell-1", _fresh()) is True, (
        "недоказанная запись держит quant"
    )
    proven = _fresh(finished=[{"text": "gone-1", "finish_as": "filled", "price": "0.09487"}])
    assert service._partial_foreign_booked(EXECUTOR, connector, PAIR, "0.09487", "sell-1", proven) is False, (
        "finished доказал строку — quant свободен"
    )


# ── 7. вердикт 20261013-151226-99598: сверка до чеканки в fill-down ──────────


def test_one_sell_after_fill_down_does_not_mint_on_a_held_quant(monkeypatch, tmp_path):
    """`_one_sell_after_fill_down` не чеканит второй номер, если quant держит
    наш живой номер соседа; на свободном quant — чеканит."""
    service, repo, connector, executor = _prepare(
        monkeypatch, tmp_path,
        _checkpoint(_level(level_id="L1", open_id="buy-1", filled=True, take="101")),
        [_row("sell-n", level_id="L9", role="close", price="101", sent=True)],
    )
    monkeypatch.setattr(mod, "partial_take_profit_quant", lambda *args, **kwargs: "101")
    monkeypatch.setattr(service, "_filled_down_open", lambda *args, **kwargs: "buy-1")
    minted = []

    async def fake_write(*args, **kwargs):
        minted.append(args[5] if len(args) > 5 else kwargs.get("close_id"))
        return True

    monkeypatch.setattr(service, "_write_fill_down_close", fake_write)
    level = SimpleNamespace(
        id="L1", price=Decimal("100"), take_profit=Decimal("0.01"),
        active_open_order=SimpleNamespace(order=SimpleNamespace(
            executed_amount_base=Decimal("1"), price=Decimal("100"),
        )),
    )
    before = _ledger_rows_count()

    import asyncio

    asyncio.run(service._one_sell_after_fill_down(EXECUTOR, executor, connector, level))
    assert minted == [], "quant занят нашим живым номером — второй номер не чеканится"
    assert _ledger_rows_count() == before
    assert connector.created == []

    service._remember_tick_fresh(PAIR, _fresh(finished=[{"text": "sell-n", "finish_as": "filled", "price": "101"}]))
    asyncio.run(service._one_sell_after_fill_down(EXECUTOR, executor, connector, level))
    assert len(minted) == 1, "quant свободен (finished доказал номер) — чеканка идёт"


# ── 8. вердикт 20261013-192315-55275: чеканка держится занятостью ворот ──────


def test_place_level_close_refuses_on_tracker_and_bare_book(monkeypatch, tmp_path):
    """`place_level_close` не чеканит и не шлёт, если quant держит номер трекера
    или голая цена книги — та же занятость, что у ворот отправки."""
    service, repo, connector, executor = _prepare(
        monkeypatch, tmp_path, _checkpoint(_level(level_id="L1", open_id="buy-1", filled=True, take="101")), [],
    )
    connector.in_flight_orders["send-77"] = SimpleNamespace(price=Decimal("100"))
    assert executor._refuse_close_price(Decimal("100")) is True
    before = _ledger_rows_count()
    assert ledger_mod.place_level_close(executor, EXECUTOR, _level_stub("L1", "100")) is None
    assert _ledger_rows_count() == before, "чеканки не было"
    assert connector.created == []
    connector.in_flight_orders.clear()
    connector.opens = [{"price": "100"}]
    assert executor._refuse_close_price(Decimal("100")) is True, "голая цена книги без номера держит quant"
    assert ledger_mod.place_level_close(executor, EXECUTOR, _level_stub("L1", "100")) is None
    assert _ledger_rows_count() == before


def test_place_level_close_refuses_on_the_take_of_an_open_position(monkeypatch, tmp_path):
    """Соседний `place_level_close` не чеканит продажу на тик тейка открытой
    позиции (вердикт: close_tick ложь при тейке 0.10 — худшее место чеканки)."""
    service, repo, connector, executor = _prepare(
        monkeypatch, tmp_path,
        _checkpoint(_level(level_id="L2", take="0.10")),
        [_row("buy-1", level_id="L1", role="open", price="0.09487", take_profit_price="0.10", sent=True)],
    )
    service._remember_tick_fresh(
        PAIR, _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "price": "0.09487"}]),
    )
    assert executor._refuse_close_price(Decimal("0.10")) is True, "тейк открытой позиции держит quant"
    before = _ledger_rows_count()
    assert ledger_mod.place_level_close(executor, EXECUTOR, _level_stub("L2", "0.10")) is None
    assert _ledger_rows_count() == before, "вторая продажа на тик тейка не чеканится"
    assert connector.created == []


def test_fill_down_does_not_mint_on_a_tracker_number(monkeypatch, tmp_path):
    """`_one_sell_after_fill_down` не чеканит второй номер, когда quant держит
    номер только из трекера коннектора."""
    service, repo, connector, executor = _prepare(
        monkeypatch, tmp_path, _checkpoint(_level(level_id="L1", open_id="buy-1", filled=True, take="101")), [],
    )
    monkeypatch.setattr(mod, "partial_take_profit_quant", lambda *args, **kwargs: "101")
    monkeypatch.setattr(service, "_filled_down_open", lambda *args, **kwargs: "buy-1")
    minted = []

    async def fake_write(*args, **kwargs):
        minted.append(True)
        return True

    monkeypatch.setattr(service, "_write_fill_down_close", fake_write)
    level = SimpleNamespace(
        id="L1", price=Decimal("100"), take_profit=Decimal("0.01"),
        active_open_order=SimpleNamespace(order=SimpleNamespace(
            executed_amount_base=Decimal("1"), price=Decimal("100"),
        )),
    )
    connector.in_flight_orders["send-77"] = SimpleNamespace(price=Decimal("101"))

    import asyncio

    asyncio.run(service._one_sell_after_fill_down(EXECUTOR, executor, connector, level))
    assert minted == [], "quant занят номером трекера — чеканки нет"


# ── 9. вердикт 20261013-195754-25863: голая строка снимка тика держит чеканку ─


def test_place_level_close_refuses_on_a_bare_tick_snapshot(monkeypatch, tmp_path):
    """Голая строка СНИМКА ТИКА (номер не прочитан) держит quant до чеканки:
    `get_new_client_order_id` не вызывается и запись журнала не происходит.
    Пустой текст строки — не «повтор своего номера» даже при пустом own_id."""
    service, repo, connector, executor = _prepare(
        monkeypatch, tmp_path, _checkpoint(_level(level_id="L1", take="101")), [],
    )
    executor._partial_owns_level = lambda level: False  # owns не отсекает — проверяем цену
    service._remember_tick_fresh(PAIR, _fresh(opens=[{"price": "100"}]))  # только снимок тика
    minted = []

    def no_mint(*args, **kwargs):
        minted.append(True)
        return "close-9"

    import hummingbot.connector.utils as connector_utils

    monkeypatch.setattr(connector_utils, "get_new_client_order_id", no_mint)
    before = _ledger_rows_count()
    assert executor._refuse_close_price(Decimal("100")) is True, (
        "голая цена снимка тика держит quant и при пустом своем номере"
    )
    assert ledger_mod.place_level_close(executor, EXECUTOR, _level_stub("L1", "100")) is None
    assert minted == [], "get_new_client_order_id не вызывался — чеканки не было"
    assert _ledger_rows_count() == before, "записи журнала не было"
    assert connector.created == []


# ── 10. вердикт 20261013-201526-2866: stored не держит цену исполненной ──────


def test_stored_block_prices_release_the_filled_buy_price(monkeypatch, tmp_path):
    """Список блокировки `_durable_open` (из `stored`) не держит цену
    исполненной покупки чужого исполнителя (0.09487), но держит тейк открытой
    позиции — через `recorded_price_blocks` после списка, не сырой `_ban_pairs`."""
    from utils.executor_checkpoint import recorded_price_blocks

    _journal(tmp_path, monkeypatch, [
        _row("gone-1", level_id="L1", role="open", price="0.09487", take_profit_price="0.10",
             sent=True, executor_id="other"),
    ])
    service, repo, connector = _service_with(monkeypatch, _checkpoint(_level()))
    fresh = _fresh(finished=[{"text": "gone-1", "finish_as": "filled", "price": "0.09487"}])
    stored = [("gone-1", "0.09487"), ("gone-1", "0.10")]
    blocked = service._prices_for_blocks(service._stored_block_prices(stored, fresh), fresh)
    assert recorded_price_blocks(blocked, "0.09487", TICK) is False, (
        "цена самой исполненной покупки чужого исполнителя quant не держит"
    )
    assert recorded_price_blocks(blocked, "0.10", TICK) is True, (
        "тейк открытой позиции держит quant"
    )


# ── 11. вердикт 20261013-221347-72133: тейк отпущен, когда продажа покрыла ───


def test_take_releases_when_the_sell_covered_the_buy(monkeypatch, tmp_path):
    """Тейк отпущен, только когда позиция доказанно закрыта: продажа уровня
    исполнена finished и её база покрыла покупку. Покупка без продажи — тейк
    держит (вердикт 20261013-221347-72133)."""
    from utils.executor_checkpoint import recorded_price_blocks

    _journal(tmp_path, monkeypatch, [
        _row("buy-1", level_id="L1", role="open", price="0.09487", take_profit_price="0.10", sent=True),
    ])
    covered = _level(level_id="L1", open_id="buy-1", close_id="sell-1", take="0.10", filled=True)
    covered["partial_bought"] = "1"
    service, repo, connector = _service_with(monkeypatch, _checkpoint(covered))
    sold = _fresh(finished=[
        {"text": "buy-1", "finish_as": "filled", "price": "0.09487", "amount": "1", "left": "0"},
        {"text": "sell-1", "finish_as": "filled", "price": "0.10", "amount": "1", "left": "0"},
    ])
    blocked = service._prices_for_blocks(service._ban_pairs(PAIR, sold), sold)
    assert recorded_price_blocks(blocked, "0.10", TICK) is False, (
        "продажа покрыла покупку — позиция закрыта, тейк quant не держит"
    )
    unsold = _fresh(finished=[{"text": "buy-1", "finish_as": "filled", "price": "0.09487"}])
    blocked_open = service._prices_for_blocks(service._ban_pairs(PAIR, unsold), unsold)
    assert recorded_price_blocks(blocked_open, "0.10", TICK) is True, (
        "покупка исполнена, продажи нет — тейк открытой позиции держит"
    )
