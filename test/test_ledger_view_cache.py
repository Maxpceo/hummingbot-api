"""Пробы снимка журнала номеров (план ledger-scan-cache).

Держат три вещи: журнал читается один раз на шаг, а ответы каждого читателя
повторяют прежнее чтение один в один; запись мимо кэша видна тому же шагу —
иначе цена номера выпадет из занятых и встанет вторая заявка на ту же цену;
сбой чтения не подменяется устаревшим снимком.
"""

import json
import os
import time

import pytest

import utils.executor_checkpoint as ledger_mod
from utils.executor_checkpoint import (
    _LedgerReadError,
    add_miss_order,
    add_sent_order,
    append_durable_row,
    ledger_cache_reset_stats,
    ledger_cache_stats,
    ledger_debt_marker,
    ledger_order_ids,
    partial_annotation_line,
    partial_exposure,
    partial_has_five,
    partial_hold_kinds,
    partial_holding_ids,
    partial_lines,
    partial_lone_five,
    partial_old_remember,
    partial_send_marked,
    partial_stop_forbidden,
    read_ledger_filtered,
    remove_miss_order,
    remove_sent_order,
    reset_ledger_process_state,
)


# ── прежнее чтение журнала: то, что читатели делали до правки ─────────────────


def _old_lines(rows, order_id):
    return [row for row in rows or [] if str(row.get("order_id") or "") == str(order_id)]


def _old_send_marked(rows, order_id):
    lines = _old_lines(rows, order_id)
    if len(lines) < 2:
        return False
    return any(partial_has_five(row) for row in lines[1:])


def _old_lone_five(rows, order_id):
    if not order_id or _old_send_marked(rows, order_id):
        return False
    lines = [row for row in rows or [] if str(row.get("order_id") or "") == str(order_id)]
    if any(partial_old_remember(row) for row in lines):
        return False
    return any(partial_has_five(row) for row in lines)


def _old_holding_ids(rows, executor_id):
    found = set()
    if rows is None:
        return found
    for row in rows:
        if row.get("executor_id") not in (None, "", executor_id):
            continue
        order_id = row.get("order_id")
        level_id = row.get("level_id")
        if not order_id or not level_id:
            continue
        if row.get("role") in ("close", "market_close"):
            continue
        if _old_send_marked(rows, str(order_id)):
            found.add(str(order_id))
            continue
        if not partial_old_remember(row):
            continue
        if partial_annotation_line(row) and any(
            partial_old_remember(item) and not partial_annotation_line(item)
            for item in _old_lines(rows, str(order_id))
            if item is not row
        ):
            continue
        found.add(str(order_id))
    return found


def _old_hold_kinds(rows, parsed, executor_id):
    from utils.executor_checkpoint import partial_end_visible

    kinds = {}
    if rows is None:
        return kinds
    for order_id in _old_holding_ids(rows, executor_id):
        if parsed is not None and partial_end_visible(parsed, str(order_id)):
            continue
        if _old_send_marked(rows, str(order_id)):
            kinds[str(order_id)] = "send-mark"
        elif any(
            partial_old_remember(item) and not partial_annotation_line(item)
            for item in _old_lines(rows, str(order_id))
        ):
            kinds[str(order_id)] = "remember"
        else:
            kinds[str(order_id)] = "annotation-only"
    return kinds


def _old_exposure(rows, parsed, executor_id):
    if rows is None:
        return True
    if _old_holding_ids(rows, executor_id):
        return True
    for level in (parsed or {}).get("levels") or []:
        slot = (level or {}).get("open_order") or {}
        if slot.get("order_id") and slot.get("level_id"):
            return True
    return False


def _old_stop_forbidden(rows, parsed, executor_id, *, proof_ready):
    from utils.executor_checkpoint import partial_end_visible, partial_level_released

    if rows is None:
        return _old_exposure(None, parsed, executor_id)
    holding = _old_holding_ids(rows, executor_id)
    column_holds = False
    for level in (parsed or {}).get("levels") or []:
        slot = (level or {}).get("open_order") or {}
        close = (level or {}).get("close_order") or {}
        if slot.get("order_id") and slot.get("level_id") and not partial_end_visible(parsed, str(slot.get("order_id"))):
            column_holds = True
        if close.get("order_id") and close.get("level_id") and not partial_level_released(parsed, str(level.get("id"))):
            column_holds = True
    if not holding and not column_holds:
        return False
    if not proof_ready:
        return True
    for order_id in holding:
        if not partial_end_visible(parsed, order_id):
            return True
    if column_holds:
        return True
    return False


# ── формы строк журнала ────────────────────────────────────────────────────────


def _five(order_id, **extra):
    row = {
        "order_id": order_id,
        "trading_pair": "DOGE-USDT",
        "order_type": "LIMIT_MAKER",
        "trade_type": "BUY",
        "leverage": 1,
        "position": "OPEN",
    }
    row.update(extra)
    return row


def _remember(order_id, level_id="L1", **extra):
    row = {
        "executor_id": "ex",
        "order_id": order_id,
        "level_id": level_id,
        "price": "0.1",
        "ts": 10.0,
    }
    row.update(extra)
    return row


def _annotation(order_id, level_id="L1", **extra):
    return _remember(order_id, level_id, price=None, take_profit_price="0.11", **extra)


def _close_send(order_id, level_id="L1", **extra):
    extra.setdefault("price", None)
    return _remember(order_id, level_id, **extra)


def _write_journal(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _boot(monkeypatch, path, rows):
    _write_journal(path, rows)
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    ledger_cache_reset_stats()


# ── проба 1: журнал читается один раз, проход не квадратичный ──────────────────


def test_journal_is_read_once_for_many_lookups(monkeypatch, tmp_path):
    rows = []
    for i in range(4377):
        order_id = "o%04d" % i
        if i % 3 == 0:
            rows.append(_annotation(order_id))
            rows.append(_five(order_id, level_id="L1"))
        elif i % 3 == 1:
            rows.append(_remember(order_id))
        else:
            rows.append(_five(order_id))
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", rows)

    seen = []
    for _ in range(5):
        journal = read_ledger_filtered()
        seen.append(partial_holding_ids(journal, "ex"))
        for order_id in ("o0000", "o0001", "o0002"):
            partial_send_marked(journal, order_id)
            partial_lines(journal, order_id)
    assert ledger_cache_stats()["reads"] == 1
    assert ledger_cache_stats()["lookups"] >= 30
    assert len(seen[0]) > 1000
    assert all(answer == seen[0] for answer in seen)

    journal = read_ledger_filtered()
    sample = [str(row.get("order_id") or "") for row in journal[:200]]
    for order_id in sample:
        assert partial_lines(journal, order_id) == _old_lines(journal, order_id)
        assert partial_send_marked(journal, order_id) == _old_send_marked(journal, order_id)
        assert partial_lone_five(journal, order_id) == _old_lone_five(journal, order_id)
    assert partial_holding_ids(journal, "ex") == _old_holding_ids(journal, "ex")


def test_journal_pass_is_not_quadratic(monkeypatch, tmp_path):
    rows = []
    for i in range(1200):
        order_id = "o%04d" % i
        if i % 2 == 0:
            rows.append(_annotation(order_id))
            rows.append(_five(order_id, level_id="L1"))
        else:
            rows.append(_remember(order_id))
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", rows)
    journal = read_ledger_filtered()

    started = time.monotonic()
    held = partial_holding_ids(journal, "ex")
    new_pass = []
    for row in journal:
        key = str(row.get("order_id") or "")
        if key in held:
            continue
        if _new_lone_five(journal, key):
            continue
        if row.get("price") not in (None, ""):
            new_pass.append((key, row.get("price")))
        if row.get("take_profit_price") not in (None, ""):
            new_pass.append((key, row.get("take_profit_price")))
    new_time = time.monotonic() - started

    started = time.monotonic()
    held = _old_holding_ids(journal, "ex")
    old_pass = []
    for row in journal:
        key = str(row.get("order_id") or "")
        if key in held:
            continue
        if _old_lone_five(journal, key):
            continue
        if row.get("price") not in (None, ""):
            old_pass.append((key, row.get("price")))
        if row.get("take_profit_price") not in (None, ""):
            old_pass.append((key, row.get("take_profit_price")))
    old_time = time.monotonic() - started

    assert new_pass == old_pass
    assert new_time < old_time


# ── проба 2: каждый читатель отвечает один в один ──────────────────────────────


def test_readers_match_the_old_journal_reading(monkeypatch, tmp_path):
    shapes = {
        "send-mark": [_annotation("n1"), _five("n1", level_id="L1")],
        "lone-five": [_five("n2")],
        "remember": [_remember("n3")],
        "annotation-only": [_annotation("n4")],
        "close-send": [_close_send("n5")],
        "foreign-executor": [_remember("n6", executor_id="other")],
        "no-level": [{"order_id": "n7", "price": "0.1"}],
        "close-role": [_remember("n8", role="close")],
        "market-close-role": [_remember("n9", role="market_close")],
        "annotation-plus-remember": [_annotation("n10"), _remember("n10")],
        "five-then-annotation": [_five("n11", level_id="L1"), _annotation("n11")],
    }
    rows = [row for shape in shapes.values() for row in shape]
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", rows)
    journal = read_ledger_filtered()

    parsed_forms = [
        None,
        {"partial_ends": {"n1": {"kind": "filled"}}},
        {"partial_ends": {"n1": {"kind": "filled", "price": "0.1"}}},
        {"levels": [{"id": "L1", "open_order": {"order_id": "n3", "level_id": "L1"}}]},
    ]
    for order_id in ["n%d" % i for i in range(1, 12)] + ["отсутствует"]:
        assert partial_lines(journal, order_id) == _old_lines(journal, order_id), order_id
        assert partial_send_marked(journal, order_id) == _old_send_marked(journal, order_id), order_id
        assert partial_lone_five(journal, order_id) == _old_lone_five(journal, order_id), order_id
    for executor_id in ("ex", "other", None, ""):
        assert partial_holding_ids(journal, executor_id) == _old_holding_ids(journal, executor_id)
        for parsed in parsed_forms:
            assert partial_hold_kinds(journal, parsed, executor_id) == _old_hold_kinds(
                journal, parsed, executor_id
            ), (executor_id, parsed)
            assert partial_exposure(journal, parsed, executor_id) == _old_exposure(
                journal, parsed, executor_id
            ), (executor_id, parsed)
            for proof_ready in (True, False):
                assert partial_stop_forbidden(
                    journal, parsed, executor_id, proof_ready=proof_ready
                ) == _old_stop_forbidden(
                    journal, parsed, executor_id, proof_ready=proof_ready
                ), (executor_id, parsed, proof_ready)
    for answer in (None, []):
        assert partial_lines(answer, "n1") == _old_lines(answer, "n1")
        assert partial_holding_ids(answer, "ex") == _old_holding_ids(answer, "ex")


# ── проба 3: писатель идёт мимо кэша ──────────────────────────────────────────


def test_writer_bypass_sees_the_fresh_send_mark(monkeypatch, tmp_path):
    path = tmp_path / "data" / "order-ledger.jsonl"
    _boot(monkeypatch, path, [_annotation("n1")])
    journal = read_ledger_filtered()
    assert not partial_send_marked(journal, "n1")

    assert append_durable_row(_five("n1", level_id="L1"))
    journal = read_ledger_filtered()
    assert partial_send_marked(journal, "n1"), "записанная метка отправки видна тому же шагу"
    assert partial_holding_ids(journal, "ex") == {"n1"}

    # запись во время сборки снимка: версия поднимается после чтения файла
    real_read = ledger_mod._read_ledger

    def read_and_append():
        rows = real_read()
        append_durable_row(_annotation("n2"))
        return rows

    monkeypatch.setattr(ledger_mod, "_read_ledger", read_and_append)
    monkeypatch.setattr(ledger_mod, "_ledger_file_key", lambda _path: ("frozen",))
    journal = read_ledger_filtered()
    assert all(row.get("order_id") != "n2" for row in journal)
    journal = read_ledger_filtered()
    assert any(row.get("order_id") == "n2" for row in journal), "следующий вызов пересобирает снимок по версии"


def test_version_beats_an_identical_stat(monkeypatch, tmp_path):
    path = tmp_path / "data" / "order-ledger.jsonl"
    _boot(monkeypatch, path, [_annotation("n1")])
    read_ledger_filtered()
    monkeypatch.setattr(ledger_mod, "_ledger_file_key", lambda _path: ("frozen",))
    assert ledger_mod._ledger_cache_key()[0] == (("frozen",),) * 3

    # снимок обязан построиться под замороженным ключом — дальше отличает только версия
    journal = read_ledger_filtered()
    assert not partial_send_marked(journal, "n1")
    assert append_durable_row(_five("n1", level_id="L1"))
    reads_before = ledger_cache_stats()["reads"]
    journal = read_ledger_filtered()
    assert ledger_cache_stats()["reads"] == reads_before + 1, "версия писателя перебивает неизменившийся stat"
    assert partial_send_marked(journal, "n1"), "записанная метка видна, хотя stat заморожен"


# ── проба 4: занятые цены не теряются ─────────────────────────────────────────


def test_occupied_prices_match_the_old_pass(monkeypatch, tmp_path):
    rows = [
        _annotation("n1"),
        _five("n1", level_id="L1", price="0.1"),
        _remember("n2", price="0.2"),
        _five("n3"),
        _remember("n4", price="0.4", take_profit_price="0.44"),
        _remember("n5", executor_id="other", price="0.5"),
        _close_send("n6", price="0.6"),
    ]
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", rows)
    journal = read_ledger_filtered()

    def collect(holding, lone_five):
        prices = []
        for row in journal:
            key = str(row.get("order_id") or "")
            if key in holding:
                continue
            if lone_five(journal, key):
                continue
            if row.get("price") not in (None, ""):
                prices.append((key, row.get("price")))
            if row.get("take_profit_price") not in (None, ""):
                prices.append((key, row.get("take_profit_price")))
        return prices

    for executor_id in ("ex", "other"):
        new_prices = collect(partial_holding_ids(journal, executor_id), partial_lone_five)
        old_prices = collect(_old_holding_ids(journal, executor_id), _old_lone_five)
        assert new_prices == old_prices
    ex_prices = collect(partial_holding_ids(journal, "ex"), partial_lone_five)
    assert ("n5", "0.5") in ex_prices, "чужой номер по цене остаётся занятой ценой для своих"
    assert ("n1", "0.1") not in ex_prices, "метка отправки держит свою цену"


# ── проба 5: гейты долга прежние, наборы применяются сразу ────────────────────


def test_debt_gates_unchanged_and_sets_apply_immediately(monkeypatch, tmp_path):
    path = tmp_path / "data" / "order-ledger.jsonl"
    _boot(monkeypatch, path, [_remember("n1"), _remember("n2")])
    journal = read_ledger_filtered()
    assert partial_holding_ids(journal, "ex") == {"n1", "n2"}

    add_miss_order("n1")
    journal = read_ledger_filtered()
    assert all(row.get("order_id") != "n1" for row in journal), "miss без sent скрывает строку"
    assert partial_holding_ids(journal, "ex") == {"n2"}

    add_sent_order("n1")
    journal = read_ledger_filtered()
    assert any(row.get("order_id") == "n1" for row in journal), "sent возвращает строку немедленно"
    remove_sent_order("n1")
    remove_miss_order("n1")
    assert partial_holding_ids(read_ledger_filtered(), "ex") == {"n1", "n2"}

    # долг маркера: id-гейт отдаёт None, как и до правки
    assert ledger_order_ids() is not None
    marker = ledger_debt_marker()
    os.makedirs(os.path.dirname(marker) or ".", exist_ok=True)
    with open(marker, "w", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "order_id": "owed",
                    "price": "0.1",
                    "executor_id": "ex",
                    "account_name": "a",
                    "connector": "c",
                    "trading_pair": "DOGE-USDT",
                    "ts": 10.0,
                }
            )
            + "\n"
        )
    assert ledger_order_ids() is None


# ── проба 6: сбой чтения не подменяется устаревшим снимком ────────────────────


def test_read_failure_does_not_serve_stale(monkeypatch, tmp_path):
    path = tmp_path / "data" / "order-ledger.jsonl"
    _boot(monkeypatch, path, [_remember("n1")])
    assert len(read_ledger_filtered()) == 1

    path.unlink()
    path.mkdir()
    for _ in range(2):
        with pytest.raises(_LedgerReadError):
            read_ledger_filtered()
    assert ledger_mod._LEDGER_SNAPSHOT is None, "снимок при сбое чтения не кэшируется"


def test_unreadable_set_keeps_the_line_and_the_stop(monkeypatch, tmp_path):
    path = tmp_path / "data" / "order-ledger.jsonl"
    _boot(monkeypatch, path, [_remember("n1"), _remember("n2")])
    add_miss_order("n1")

    sent = tmp_path / "data" / "order-sent.json"
    if sent.exists():
        sent.unlink()
    sent.mkdir()
    journal = read_ledger_filtered()
    assert any(row.get("order_id") == "n1" for row in journal), "строка номера не выкинута"
    assert partial_holding_ids(journal, "ex") == {"n1", "n2"}, "стоп держится"
    assert partial_stop_forbidden(journal, None, "ex", proof_ready=True)
    assert ledger_mod._LEDGER_SNAPSHOT is None, "снимок при сбое набора не кэшируется"
    reads_before = ledger_cache_stats()["reads"]
    read_ledger_filtered()
    assert ledger_cache_stats()["reads"] == reads_before + 1


def test_broken_stat_rebuilds_and_does_not_cache(monkeypatch, tmp_path):
    path = tmp_path / "data" / "order-ledger.jsonl"
    _boot(monkeypatch, path, [_remember("n1"), _remember("n2")])
    add_miss_order("n1")
    journal = read_ledger_filtered()
    assert all(row.get("order_id") != "n1" for row in journal)

    real_stat = os.stat

    def broken_stat(name, *args, **kwargs):
        if str(name).endswith("order-sent.json"):
            raise OSError("stat broken")
        return real_stat(name, *args, **kwargs)

    monkeypatch.setattr(os, "stat", broken_stat)
    reads_before = ledger_cache_stats()["reads"]
    journal = read_ledger_filtered()
    assert ledger_cache_stats()["reads"] == reads_before + 1
    assert all(row.get("order_id") != "n1" for row in journal), "фильтр прежний, ответ тот же"
    read_ledger_filtered()
    assert ledger_cache_stats()["reads"] == reads_before + 2
    assert ledger_mod._LEDGER_SNAPSHOT is None, "сломанный stat не равен отсутствующему файлу"


# ── проба 7: внешняя правка файлов подхватывается ─────────────────────────────


def test_external_change_is_picked_up(monkeypatch, tmp_path):
    path = tmp_path / "data" / "order-ledger.jsonl"
    _boot(monkeypatch, path, [_remember("n1")])
    assert len(read_ledger_filtered()) == 1

    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(_remember("n2")) + "\n")
    assert {row.get("order_id") for row in read_ledger_filtered()} == {"n1", "n2"}

    miss = tmp_path / "data" / "order-miss.json"
    miss.write_text(json.dumps(["n1"]), encoding="utf-8")
    assert {row.get("order_id") for row in read_ledger_filtered()} == {"n2"}
    miss.write_text(json.dumps([]), encoding="utf-8")
    assert {row.get("order_id") for row in read_ledger_filtered()} == {"n1", "n2"}


# ── проба 8: строки снимка неизменяемы, правка копии не видна ──────────────────


def test_snapshot_rows_are_read_only_for_copied_readers(monkeypatch, tmp_path):
    _boot(monkeypatch, tmp_path / "data" / "order-ledger.jsonl", [_annotation("n1"), _five("n1", level_id="L1")])
    journal = read_ledger_filtered()
    before = json.dumps(journal, sort_keys=True, default=str)

    partial_lines(journal, "n1")
    partial_send_marked(journal, "n1")
    partial_holding_ids(journal, "ex")
    partial_hold_kinds(journal, {"partial_ends": {"n1": {"kind": "filled"}}}, "ex")
    partial_exposure(journal, None, "ex")
    partial_stop_forbidden(journal, None, "ex", proof_ready=True)
    assert json.dumps(journal, sort_keys=True, default=str) == before, "читатели строки не правят"

    copied = partial_lines(journal, "n1")
    copied[0]["take_profit_price"] = "999"
    copied.pop()
    same = json.dumps(read_ledger_filtered(), sort_keys=True, default=str)
    assert same == before, "правка копии не видна следующему чтению"
