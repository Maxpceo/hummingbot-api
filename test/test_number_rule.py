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
