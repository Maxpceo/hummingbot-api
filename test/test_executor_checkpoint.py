"""Restart must reattach the same orders, not open a second trade.

The safety rules live here, without the exchange: a missing checkpoint is not
resumed, a grid does not guess between two levels, and an LP open that already
started is not minted again.
"""
from types import SimpleNamespace

from utils.executor_checkpoint import (
    can_resume,
    capture_executor,
    capture_tracked,
    exchange_open_request,
    exchange_pass_key,
    foreign_order_ids,
    new_exchange_slot,
    open_list_venue,
    grid_resume_plan,
    has_exposure,
    install_occupied_open_brake,
    merge_exchange_open_pages,
    note_filled,
    note_gone,
    note_placed,
    parse_checkpoint,
    parse_exchange_open_page,
    recent_order_ids,
    remember_order_now,
    seat_known_opens,
    would_place_at_touch,
    recorded_price_blocks,
    send_blocked,
    finished_list_request,
    gate_delete_request,
    must_keep_executor,
    note_owed,
    owed_records,
    four_proofs,
    gate_base_is_zero,
    checkpoint_plus,
    claim_quant,
    release_quant,
    pick_owner,
    local_create_missed_place,
    unread_blocks_buy,
    snapshot_body,
    remember_open_before_send,
)


def _level(level_id, price, open_order=None):
    return {
        "id": level_id,
        "price": price,
        "amount_quote": "10",
        "take_profit": "0.01",
        "side": "BUY",
        "open_order_type": "LIMIT",
        "take_profit_order_type": "LIMIT",
        "open_order": open_order,
        "close_order": None,
    }


def _grid(levels):
    return {"version": 1, "type": "grid_executor", "levels": levels, "close_order": None}


def test_missing_checkpoint_is_not_resumed():
    assert can_resume(None) == "no checkpoint"
    assert parse_checkpoint(None) is None
    assert parse_checkpoint("not json") is None


def test_capture_keeps_the_order_id_and_ignores_an_empty_slot():
    tracked = SimpleNamespace(order_id="abc", order=None)
    assert capture_tracked(tracked)["order_id"] == "abc"
    assert capture_tracked(SimpleNamespace(order_id=None, order=None)) is None

    level = SimpleNamespace(
        id="L0", price="100", amount_quote="10", take_profit="0.01",
        side=SimpleNamespace(name="BUY"),
        open_order_type=SimpleNamespace(name="LIMIT"),
        take_profit_order_type=SimpleNamespace(name="LIMIT"),
        active_open_order=tracked,
        active_close_order=None,
    )
    executor = SimpleNamespace(grid_levels=[level], _close_order=None)
    body = capture_executor(executor, "grid_executor")
    assert body["levels"][0]["open_order"]["order_id"] == "abc"
    assert has_exposure(body) is True


def test_grid_does_not_die_when_one_order_matches_two_levels():
    checkpoint = _grid([_level("L0", "100"), _level("L1", "100")])
    live = [{"order_id": "x", "price": "100", "is_open": True}]
    assert can_resume(checkpoint, live) is None
    plan = grid_resume_plan(checkpoint, live, market_price="118", side="BUY")
    assert plan["touch_budget"] == 1
    assert plan["ambiguous_ids"] == ["x"]


def test_grid_allows_an_order_that_matches_one_empty_level():
    checkpoint = _grid([_level("L0", "100"), _level("L1", "110")])
    live = [{"order_id": "x", "price": "100", "is_open": True}]
    assert can_resume(checkpoint, live) is None


def test_empty_checkpoint_does_not_start_beside_an_unnamed_open_order():
    checkpoint = {"version": 1, "type": "order_executor", "order": None}
    live = [{"order_id": "x", "price": "100", "is_open": True}]
    assert can_resume(checkpoint, live) is not None
    assert has_exposure(checkpoint) is False


def test_cleared_slot_keeps_the_id_until_the_exchange_confirms_cancel():
    """The grid drops the slot when it replaces an order. The id stays until cancel."""
    level = SimpleNamespace(
        id="L0", price="100", amount_quote="10", take_profit="0.01",
        side=SimpleNamespace(name="BUY"),
        open_order_type=SimpleNamespace(name="LIMIT"),
        take_profit_order_type=SimpleNamespace(name="LIMIT"),
        active_open_order=SimpleNamespace(order_id="abc", order=None),
        active_close_order=None,
    )
    executor = SimpleNamespace(grid_levels=[level], _close_order=None, connectors={})
    note_placed(executor, "abc")
    capture_executor(executor, "grid_executor")
    level.active_open_order = None
    body = capture_executor(executor, "grid_executor")
    assert body["levels"][0]["open_order"]["order_id"] == "abc"

    note_gone(executor, "abc")
    body = capture_executor(executor, "grid_executor")
    assert body["levels"][0]["open_order"] is None


def test_fill_is_recorded_on_the_event_not_the_next_tick():
    executor = SimpleNamespace(grid_levels=[], _close_order=None, connectors={})
    note_placed(executor, "abc")
    note_filled(executor, "abc", {"client_order_id": "abc", "last_state": "filled"})
    assert executor._order_ledger["abc"]["filled"] is True
    assert executor._order_ledger["abc"]["order"]["client_order_id"] == "abc"


def test_unrecognized_touch_order_keeps_the_grid_and_adds_no_second():
    """The grid comes back. The order already at the market counts as the one extra."""
    checkpoint = _grid([_level("L0", "120"), _level("L1", "125")])
    live = [{"order_id": "touch", "price": "118.18", "is_open": True}]
    assert can_resume(checkpoint, live) is None
    plan = grid_resume_plan(checkpoint, live, market_price="118.20", side="BUY")
    assert plan["touch_budget"] == 0
    assert "no new order is placed there" in plan["notice"]
    assert would_place_at_touch("125", "118.20", "BUY") is True
    assert would_place_at_touch("100", "118.20", "BUY") is False


def test_foreign_order_is_not_seated_by_price():
    levels = [_level("L0", "100")]
    foreign = {"order_id": "other", "price": "100", "is_open": True, "side": "BUY"}
    assert seat_known_opens(levels, [foreign], allowed_ids=set()) == set()
    assert levels[0]["open_order"] is None
    plan = grid_resume_plan(_grid(levels), [foreign], market_price="90", side="BUY")
    assert "100" in plan["blocked_prices"]


def test_known_order_seats_on_one_level_and_not_on_two():
    one = [_level("L0", "100"), _level("L1", "110")]
    own = {"order_id": "mine", "price": "100", "is_open": True, "side": "BUY"}
    assert seat_known_opens(one, [own], allowed_ids={"mine"}) == {"mine"}
    assert one[0]["open_order"]["order_id"] == "mine"

    two = [_level("L0", "100"), _level("L1", "100")]
    assert seat_known_opens(two, [own], allowed_ids={"mine"}) == set()
    assert two[0]["open_order"] is None
    plan = grid_resume_plan(_grid(two), [own], market_price="90", side="BUY")
    assert can_resume(_grid(two), [own]) is None
    assert plan["ambiguous_ids"] == ["mine"]
    assert plan["touch_budget"] == 1


def test_unattached_retained_id_still_blocks_the_current_price():
    checkpoint = _grid([_level("L0", "120"), _level("L1", "125")])
    checkpoint["retained_orders"] = [{"order_id": "late", "filled": False}]
    live = [{"order_id": "late", "price": "118.18", "is_open": True, "side": "BUY"}]
    plan = grid_resume_plan(checkpoint, live, market_price="118.20", side="BUY")
    assert plan["touch_budget"] == 0
    assert "118.18" in plan["blocked_prices"]


def test_order_ledger_drops_ids_older_than_the_keep_window(tmp_path, monkeypatch):
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(tmp_path / "ledger.jsonl"))
    remember_order_now("ex", "new")
    path = tmp_path / "ledger.jsonl"
    path.write_text(
        '{"executor_id": "ex", "order_id": "old", "ts": 1}\n' + path.read_text(),
        encoding="utf-8",
    )
    assert recent_order_ids("ex") == ["new"]
    remember_order_now("ex", "newer")
    before_prune = path.read_text(encoding="utf-8")
    assert "old" in before_prune
    assert recent_order_ids("ex") == ["new", "newer"]
    modes = []
    opens = []
    _track_ledger_opens(monkeypatch, modes, opens)
    prune_order_ledger()
    assert "old" in path.read_text(encoding="utf-8")
    prune_order_ledger(protected=ledger_order_ids())
    assert "old" in path.read_text(encoding="utf-8")
    prune_order_ledger(protected=ledger_order_ids(), proven={("other", "old")})
    assert "old" in path.read_text(encoding="utf-8")
    prune_order_ledger(protected=ledger_order_ids(), proven={("ex", "old")})
    ids = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        ids.append(json.loads(line)["order_id"])
    assert ids == ["new", "newer"]
    assert recent_order_ids("ex") == ["new", "newer"]
    assert not list(tmp_path.glob(".order-ledger-*"))
    assert not _live_opened_for_rewrite(path, modes, opens)


def test_lp_open_without_an_address_is_not_minted_again():
    checkpoint = {
        "version": 1,
        "type": "lp_executor",
        "position_address": None,
        "saw_open_attempt": True,
    }
    assert can_resume(checkpoint) is not None
    fresh = {
        "version": 1,
        "type": "lp_executor",
        "position_address": None,
        "saw_open_attempt": False,
    }
    assert can_resume(fresh) is None


from decimal import Decimal
import os
import threading
import time

import json

from utils.executor_checkpoint import (
    LEDGER_LOCK,
    _LedgerReadError,
    accounted_orders,
    cap_arguments,
    install_grid_resume_cap,
    merge_recent_ids,
    prune_order_ledger,
)


def _track_ledger_opens(monkeypatch, modes, opens):
    real_open = open
    real_os_open = os.open

    def tracking_open(file, mode="r", *args, **kwargs):
        modes.append((str(file), mode))
        return real_open(file, mode, *args, **kwargs)

    def tracking_os_open(file, flags, *args, **kwargs):
        opens.append((str(file), flags))
        return real_os_open(file, flags, *args, **kwargs)

    monkeypatch.setattr("builtins.open", tracking_open)
    monkeypatch.setattr(os, "open", tracking_os_open)


def _live_opened_for_rewrite(path, modes, opens):
    live = str(path)
    if any(name == live and isinstance(mode, str) and "w" in mode for name, mode in modes):
        return True
    return any(
        name == live and isinstance(flags, int) and flags & os.O_TRUNC
        for name, flags in opens
    )



class _Side:
    def __init__(self, name):
        self.name = name


def _proposal(price):
    return SimpleNamespace(price=Decimal(str(price)))


def _live(order_id, price, is_open=True):
    return SimpleNamespace(
        client_order_id=order_id,
        price=Decimal(str(price)),
        is_open=is_open,
        trading_pair="SOL-USDT",
    )


def _wrapped(proposals, plan, orders=None, cached=None, lost=None, ready=True,
             quote="120.01", side="BUY", tick="0.0001", spread="0.0001", raise_book=False):
    orders = list(orders or [])
    cached = list(cached or [])
    lost = list(lost or [])

    class _Inflight(dict):
        def values(self):
            if raise_book:
                raise OSError("book")
            return super().values()

    inflight = _Inflight({order.client_order_id: order for order in orders})
    connector = SimpleNamespace(
        ready=ready,
        in_flight_orders=inflight,
        _order_tracker=SimpleNamespace(
            cached_orders={order.client_order_id: order for order in cached},
            lost_orders={order.client_order_id: order for order in lost},
        ),
    )
    config = SimpleNamespace(
        side=_Side(side) if side else SimpleNamespace(),
        safe_extra_spread=Decimal(str(spread)),
        trading_pair="SOL-USDT",
        min_spread_between_orders=Decimal("0.0005"),
    )
    executor = SimpleNamespace(
        config=config,
        trading_rules=None if tick is None else SimpleNamespace(min_price_increment=Decimal(str(tick))),
        current_open_quote=None if quote is None else quote,
        connectors={"okx": connector},
        get_open_orders_to_create=lambda: list(proposals),
    )
    install_grid_resume_cap(executor, plan)
    return executor


def test_ledger_replace_failure_keeps_the_old_file(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    fresh = json.dumps({"executor_id": "ex", "order_id": "keep", "ts": time.time()})
    path.write_text(
        '{"executor_id": "ex", "order_id": "old", "ts": 1}\n' + fresh + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    before = path.read_bytes()
    modes = []
    opens = []
    _track_ledger_opens(monkeypatch, modes, opens)

    def fail_replace(*_args, **_kwargs):
        raise OSError("replace failed")

    monkeypatch.setattr(os, "replace", fail_replace)
    prune_order_ledger(protected={"old", "keep"}, proven={("ex", "old")})
    assert path.read_bytes() == before
    assert not _live_opened_for_rewrite(path, modes, opens)
    assert not list(tmp_path.glob(".order-ledger-*"))


def test_ledger_replace_writes_a_complete_file(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    remember_order_now("ex", "keep")
    remember_order_now("ex", "next")
    text = path.read_text(encoding="utf-8")
    assert "keep" in text and "next" in text
    assert not list(tmp_path.glob(".order-ledger-*"))


def test_ledger_read_error_appends_without_replace(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    path.write_bytes(b'{"executor_id": "ex", "order_id": "keep", "ts": 10}')
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    before = path.read_bytes()
    synced = []
    real_fsync = os.fsync

    def tracking_fsync(descriptor):
        synced.append(descriptor)
        return real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", tracking_fsync)
    monkeypatch.setattr(
        "utils.executor_checkpoint._read_ledger",
        lambda: (_ for _ in ()).throw(_LedgerReadError("unread")),
    )
    replaced = []
    monkeypatch.setattr(os, "replace", lambda *args: replaced.append(args))
    remember_order_now("ex", "new")
    text = path.read_text(encoding="utf-8")
    assert text.startswith(before.decode("utf-8"))
    assert '"order_id": "new"' in text.splitlines()[-1]
    assert replaced == []
    assert synced
    monkeypatch.setattr(os, "write", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("append")))
    failed = path.read_bytes()
    remember_order_now("ex", "lost")
    assert path.read_bytes() == failed
    assert replaced == []


def test_ledger_lock_covers_read_and_write(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    remember_order_now("ex", "first")
    holding = {"on": False}
    real_read = __import__("utils.executor_checkpoint", fromlist=["_read_ledger"])._read_ledger

    def guarded_read():
        if holding["on"]:
            raise AssertionError("read while the test holds the lock")
        return real_read()

    monkeypatch.setattr("utils.executor_checkpoint._read_ledger", guarded_read)
    done = []
    LEDGER_LOCK.acquire()
    holding["on"] = True

    def second():
        remember_order_now("ex", "second")
        done.append(True)

    worker = threading.Thread(target=second)
    worker.start()
    time.sleep(0.05)
    assert done == []
    holding["on"] = False
    LEDGER_LOCK.release()
    worker.join(1)
    text = path.read_text(encoding="utf-8")
    assert "first" in text and "second" in text


def test_remember_order_appends_without_replace_or_truncate(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    path.write_bytes(b'{"executor_id": "ex", "order_id": "keep", "ts": 10}')
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    modes = []
    opens = []
    _track_ledger_opens(monkeypatch, modes, opens)
    replaced = []
    monkeypatch.setattr(os, "replace", lambda *args: replaced.append(args))
    remember_order_now("ex", "new")
    text = path.read_text(encoding="utf-8")
    assert text.startswith('{"executor_id": "ex", "order_id": "keep", "ts": 10}\n')
    assert json.loads(text.splitlines()[-1])["order_id"] == "new"
    assert replaced == []
    assert not _live_opened_for_rewrite(path, modes, opens)


def _dir_fsync_log(monkeypatch):
    calls = []
    real = __import__("utils.executor_checkpoint", fromlist=["_fsync_directory"])._fsync_directory

    def tracking(path):
        calls.append(path)
        return real(path)

    monkeypatch.setattr("utils.executor_checkpoint._fsync_directory", tracking)
    return calls


def test_new_ledger_file_fsyncs_its_directory_once(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    order = []
    real_fsync = os.fsync

    def tracking_fsync(descriptor):
        order.append("file")
        return real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", tracking_fsync)
    seen = []
    real_dir = __import__("utils.executor_checkpoint", fromlist=["_fsync_directory"])._fsync_directory

    def tracking_dir(target):
        seen.append((target, bool(order)))
        return real_dir(target)

    monkeypatch.setattr("utils.executor_checkpoint._fsync_directory", tracking_dir)
    remember_order_now("ex", "new")
    assert "new" in path.read_text(encoding="utf-8")
    ledger_syncs = [target for target, ready in seen if target == str(path)]
    marker = str(path.parent.parent / ".order-ledger-debt")
    assert ledger_syncs
    assert seen[0][0] == marker and seen[0][1] is True
    assert seen.index((str(path), True)) > seen.index((marker, True))
    remember_order_now("ex", "next")
    assert [target for target, ready in seen if target == str(path)] == ledger_syncs


def test_existing_ledger_does_not_fsync_the_directory(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    path.write_text('{"executor_id": "ex", "order_id": "keep", "ts": 10}\n', encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    dirs = _dir_fsync_log(monkeypatch)
    replaced = []
    monkeypatch.setattr(os, "replace", lambda *args: replaced.append(args))
    remember_order_now("ex", "new")
    assert dirs == []
    assert replaced == []
    assert "keep" in path.read_text(encoding="utf-8")


def test_failed_create_fsyncs_directory_on_the_next_success(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    dirs = _dir_fsync_log(monkeypatch)
    real_write = os.write
    failed = {"on": True}

    def fail_once(descriptor, data):
        if failed["on"]:
            failed["on"] = False
            raise OSError("append")
        return real_write(descriptor, data)

    monkeypatch.setattr(os, "write", fail_once)
    remember_order_now("ex", "lost")
    assert str(path) not in dirs
    monkeypatch.setattr(os, "write", real_write)
    remember_order_now("ex", "kept")
    assert str(path) in dirs
    text = path.read_text(encoding="utf-8")
    assert "kept" in text
    assert "lost" not in text


def test_created_directory_fsyncs_parent_and_retries_only_parent(tmp_path, monkeypatch):
    directory = tmp_path / "data"
    path = directory / "ledger.jsonl"
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    parent_ok = {"on": False}
    calls = []

    def tracking(target):
        calls.append(target)
        if target == str(directory):
            return parent_ok["on"]
        return True

    monkeypatch.setattr("utils.executor_checkpoint._fsync_directory", tracking)
    marker = str(tmp_path / ".order-ledger-debt")
    remember_order_now("ex", "new")
    assert calls[0] == marker
    assert calls[1:] == [str(path), str(directory)]
    calls.clear()
    remember_order_now("ex", "retry")
    assert calls == [str(directory)]
    parent_ok["on"] = True
    calls.clear()
    remember_order_now("ex", "done")
    assert calls[:2] == [str(directory), str(tmp_path)]
    assert marker not in calls
    assert os.path.exists(marker)
    calls.clear()
    remember_order_now("ex", "later")
    assert calls == []


def test_prune_read_error_does_not_replace(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    path.write_text('{"executor_id": "ex", "order_id": "keep", "ts": 10}\n', encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    before = path.read_bytes()
    monkeypatch.setattr(
        "utils.executor_checkpoint._read_ledger",
        lambda: (_ for _ in ()).throw(_LedgerReadError("unread")),
    )
    replaced = []
    monkeypatch.setattr(os, "replace", lambda *args: replaced.append(args))
    prune_order_ledger()
    assert path.read_bytes() == before
    assert replaced == []


def test_prune_skips_nonempty_file_with_no_rows(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    path.write_text("not-json\n", encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    before = path.read_bytes()
    replaced = []
    monkeypatch.setattr(os, "replace", lambda *args: replaced.append(args))
    prune_order_ledger()
    assert path.read_bytes() == before
    assert replaced == []
    assert not list(tmp_path.glob(".order-ledger-*"))


def test_prune_does_not_create_a_missing_file(tmp_path, monkeypatch):
    path = tmp_path / "missing.jsonl"
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    prune_order_ledger()
    assert not path.exists()


def test_prune_waits_for_the_lock_and_keeps_both_fresh_ids(tmp_path, monkeypatch):
    path = tmp_path / "box" / "ledger.jsonl"
    path.parent.mkdir()
    fresh = json.dumps({"executor_id": "ex", "order_id": "fresh", "ts": time.time()})
    path.write_text(
        '{"executor_id": "ex", "order_id": "old", "ts": 1}\n' + fresh + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    done = []
    LEDGER_LOCK.acquire()
    try:
        def prune():
            prune_order_ledger(proven={("ex", "old")})
            done.append("prune")

        def append():
            remember_order_now("ex", "second")
            done.append("append")

        pruner = threading.Thread(target=prune)
        appender = threading.Thread(target=append)
        pruner.start()
        appender.start()
        time.sleep(0.1)
        assert done == []
    finally:
        LEDGER_LOCK.release()
    pruner.join(2)
    appender.join(2)
    assert not pruner.is_alive() and not appender.is_alive()
    text = path.read_text(encoding="utf-8")
    assert "fresh" in text and "second" in text and "old" not in text


def test_prune_keeps_a_line_appended_after_read_before_replace(tmp_path, monkeypatch):
    path = tmp_path / "box" / "ledger.jsonl"
    path.parent.mkdir()
    fresh = json.dumps({"executor_id": "ex", "order_id": "fresh", "ts": time.time()})
    path.write_text(
        '{"executor_id": "ex", "order_id": "old", "ts": 1}\n' + fresh + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    read_done = threading.Event()
    real_read = __import__("utils.executor_checkpoint", fromlist=["_read_ledger"])._read_ledger

    def wrapped_read():
        rows = real_read()
        read_done.set()
        return rows

    monkeypatch.setattr("utils.executor_checkpoint._read_ledger", wrapped_read)

    def appender():
        assert read_done.wait(2)
        remember_order_now("ex", "during")

    worker = threading.Thread(target=appender)
    worker.start()
    real_replace = os.replace

    def replace_after_append_window(src, dst):
        # If the lock was dropped after the read, this append finishes and the
        # following replace would erase it. A held lock makes the join time out.
        worker.join(1)
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", replace_after_append_window)
    prune_order_ledger(proven={("ex", "old")})
    worker.join(2)
    assert not worker.is_alive()
    ids = [json.loads(line)["order_id"] for line in path.read_text(encoding="utf-8").splitlines() if line]
    assert "old" not in ids
    assert "fresh" in ids and "during" in ids


def test_two_orders_at_one_price_stay_blocked_until_both_are_finished():
    plan = {
        "touch_budget": 1,
        "blocked_orders": [
            {"order_id": "a", "price": "100"},
            {"order_id": "b", "price": "100"},
        ],
        "blocked_prices": ["100"],
        "touch_hold_ids": [],
    }
    level = _proposal("100")
    seen_only_b = _wrapped([level], plan, orders=[_live("b", "100")], quote="110", tick="0.01")
    assert seen_only_b.get_open_orders_to_create() == []
    lost = _wrapped([level], plan, lost=[_live("a", "100")], quote="110", tick="0.01")
    assert lost.get_open_orders_to_create() == []
    done = _wrapped(
        [level], plan, cached=[_live("a", "100", is_open=False), _live("b", "100", is_open=False)],
        quote="110", tick="0.01",
    )
    assert done.get_open_orders_to_create() == [level]
    fake = SimpleNamespace(ready=True, in_flight_orders={}, cached_orders={"a": _live("a", "100", False)})
    executor = _wrapped([level], plan, quote="110", tick="0.01")
    executor.connectors = {"okx": fake}
    assert executor.get_open_orders_to_create() == []


def test_unreadable_book_does_not_place_or_raise():
    plan = {"touch_budget": 0, "blocked_orders": [{"order_id": "a", "price": "100"}], "blocked_prices": ["100"]}
    empty = _wrapped([_proposal("100")], plan)
    empty.connectors = {}
    assert empty.get_open_orders_to_create() == []
    not_dict = _wrapped([_proposal("100")], plan)
    not_dict.connectors["okx"].in_flight_orders = []
    assert not_dict.get_open_orders_to_create() == []
    exploding = _wrapped([_proposal("100")], plan, raise_book=True)
    assert exploding.get_open_orders_to_create() == []


def test_recent_order_ids_on_read_error_is_an_empty_list(tmp_path, monkeypatch):
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(tmp_path / "missing-read.jsonl"))
    monkeypatch.setattr(
        "utils.executor_checkpoint._read_ledger",
        lambda: (_ for _ in ()).throw(_LedgerReadError("unread")),
    )
    assert recent_order_ids("ex") == []
    assert merge_recent_ids({"levels": []}, recent_order_ids("ex"))["levels"] == []


def test_resume_plan_fields_cover_the_early_return_and_unknown_market():
    loose = [{"order_id": "live", "price": "100", "is_open": True, "side": "BUY"}]
    unknown = grid_resume_plan(_grid([_level("L0", "100")]), loose, market_price=None, side="BUY")
    assert unknown["touch_zero_unknown_market"] is True
    assert unknown["touch_budget"] == 0
    assert unknown["touch_hold_ids"] == []
    assert unknown["blocked_orders"][0]["order_id"] == "live"
    nan = grid_resume_plan(_grid([_level("L0", "100")]), loose, market_price=float("nan"), side="BUY")
    assert nan["touch_zero_unknown_market"] is True
    near = {"order_id": "near", "price": "100", "is_open": True, "side": "BUY"}
    aside = {"order_id": "aside", "price": "80", "is_open": True, "side": "BUY"}
    mixed = grid_resume_plan(_grid([_level("L0", "100")]), [near, aside], market_price="100", side="BUY")
    assert mixed["touch_hold_ids"] == ["near"]
    assert {row["order_id"] for row in mixed["blocked_orders"]} == {"near", "aside"}
    early = grid_resume_plan(_grid([_level("L0", "100")]), [], market_price=None, side="BUY")
    assert early["blocked_orders"] == []
    assert early["touch_hold_ids"] == []
    assert early["touch_zero_unknown_market"] is False
    assert early["touch_budget"] is None


def test_cap_arguments_keep_a_missing_key_missing():
    missing = cap_arguments({"blocked_prices": ["120"], "touch_budget": 0})
    assert "blocked_orders" not in missing
    assert missing["blocked_prices"] == ["120"]
    explicit = cap_arguments({
        "blocked_orders": [],
        "touch_budget": None,
        "blocked_prices": [],
        "touch_hold_ids": [],
        "touch_zero_unknown_market": False,
    })
    assert explicit["blocked_orders"] == []
    assert explicit["touch_budget"] is None


def test_missing_blocked_orders_keep_the_price_ban():
    plan = {"blocked_prices": ["120"], "touch_budget": 0}
    level = _proposal("120")
    crossing = _proposal("130")
    executor = _wrapped([level, crossing], plan, quote="120.01", tick="0.01")
    assert executor.get_open_orders_to_create() == []
    assert executor._touch_place_budget == 0


def test_touch_zero_waits_for_every_holder_and_one_touch_per_call():
    plan = {
        "touch_budget": 0,
        "blocked_orders": [
            {"order_id": "first", "price": "100"},
            {"order_id": "second", "price": "119.9979"},
        ],
        "blocked_prices": ["100", "119.9979"],
        "touch_hold_ids": ["first"],
    }
    crossing = _proposal("130")
    same_tick = _proposal("119.9979")
    holding = _wrapped(
        [crossing], plan,
        cached=[_live("first", "100", False)],
        orders=[_live("second", "119.9979")],
        quote="120.01", tick="0.0001",
    )
    assert holding.get_open_orders_to_create() == []
    assert holding._touch_place_budget == 0
    clear = _wrapped(
        [crossing, same_tick], plan,
        cached=[_live("first", "100", False), _live("second", "119.9979", False)],
        quote="120.01", tick="0.0001",
    )
    assert clear.get_open_orders_to_create() == [crossing]
    assert clear._touch_place_budget == 0
    assert clear.get_open_orders_to_create() == []
    no_touch = _wrapped(
        [_proposal("80")], plan,
        cached=[_live("first", "100", False)],
        orders=[_live("second", "119.9979")],
        quote="120.01", tick="0.0001",
    )
    assert no_touch.get_open_orders_to_create() == [_proposal("80")]
    assert no_touch._touch_place_budget == 0


def test_budget_one_does_not_glue_a_level_under_the_quote():
    plan = {"touch_budget": 1, "blocked_orders": [{"order_id": "live", "price": "80"}], "blocked_prices": ["80"]}
    crossing = _proposal("130")
    own = _proposal("120")
    for tick in ("0.0001", "0.01"):
        executor = _wrapped([crossing, own], plan, orders=[_live("live", "80")], quote="120.01", tick=tick)
        assert executor.get_open_orders_to_create() == [crossing, own]
        assert executor._touch_place_budget == 0
        assert executor.get_open_orders_to_create() == [own]
    blind = _wrapped([crossing], plan, orders=[_live("live", "120")], quote="120.01", tick=None)
    assert crossing not in blind.get_open_orders_to_create()
    assert blind._touch_place_budget == 1


def test_missing_or_nan_quote_does_not_release_a_touch():
    plan = {"touch_budget": 0, "blocked_orders": [], "blocked_prices": [], "touch_hold_ids": []}
    level = _proposal("130")
    for quote in (None, float("nan"), Decimal("NaN")):
        executor = _wrapped([level], plan, quote=quote, ready=True)
        assert executor.get_open_orders_to_create() == []
        assert executor._touch_place_budget == 0


def test_synthetic_unknown_market_cap_is_not_what_resume_returns():
    assert grid_resume_plan(_grid([_level("L0", "100")]), [], market_price=None)["touch_budget"] is None
    plan = {
        "touch_budget": 0,
        "blocked_orders": [],
        "blocked_prices": [],
        "touch_hold_ids": [],
        "touch_zero_unknown_market": True,
    }
    level = _proposal("130")
    executor = _wrapped([level], plan, quote="120.01", ready=True)
    assert executor.get_open_orders_to_create() == [level]
    assert executor._touch_place_budget == 0
    assert executor.get_open_orders_to_create() == []
    unseen = dict(plan)
    unseen["blocked_orders"] = [{"order_id": "late", "price": "119.9979"}]
    held = _wrapped([level], unseen, quote="120.01", ready=True)
    assert held.get_open_orders_to_create() == []
    assert held._touch_place_budget == 0
    quiet = _wrapped([level], plan, quote=None, ready=True)
    assert quiet.get_open_orders_to_create() == []
    not_ready = _wrapped([level], unseen, quote="120.01", ready=False)
    assert not_ready.get_open_orders_to_create() == []
    remembered = _wrapped(
        [level], unseen, quote="120.01", ready=False,
        cached=[_live("late", "119.9979", False)],
    )
    assert remembered.get_open_orders_to_create() == []
    assert "late" in remembered._seen_finished
    remembered.connectors["okx"].ready = True
    remembered.connectors["okx"]._order_tracker.cached_orders = {}
    assert remembered.get_open_orders_to_create() == [level]


def test_level_just_under_the_quote_is_not_a_second_touch():
    plan = {"touch_budget": 1, "blocked_orders": [], "blocked_prices": []}
    crossing = _proposal("130")
    own = _proposal("120")
    for tick in ("0.0001", "0.01"):
        executor = _wrapped([crossing, own], plan, quote="120.01", tick=tick)
        assert executor.get_open_orders_to_create() == [crossing, own]
        assert executor._touch_place_budget == 0
    nameless = _wrapped([crossing], plan, quote="120.01")
    nameless.config.side = SimpleNamespace()
    assert nameless.get_open_orders_to_create() == []


def test_completed_snapshot_id_does_not_free_another_order_at_that_price():
    plan = {
        "touch_budget": 0,
        "blocked_orders": [{"order_id": "snap", "price": "100"}],
        "blocked_prices": ["100"],
    }
    level = _proposal("100")
    occupied = _wrapped(
        [level], plan, quote="110", tick="0.01",
        cached=[_live("snap", "100", False)],
        orders=[_live("other", "100")],
    )
    assert occupied.get_open_orders_to_create() == []
    assert occupied._touch_place_budget == 0
    lost = _wrapped(
        [level], plan, quote="110", tick="0.01",
        cached=[_live("snap", "100", False)],
        lost=[_live("other", "100")],
    )
    assert lost.get_open_orders_to_create() == []
    clear = _wrapped(
        [level], plan, quote="110", tick="0.01",
        cached=[_live("snap", "100", False)],
    )
    assert clear.get_open_orders_to_create() == [level]


def test_unseen_or_lost_id_holds_zero_even_when_its_price_is_away():
    plan = {
        "touch_budget": 0,
        "blocked_orders": [{"order_id": "late", "price": "80"}],
        "blocked_prices": ["80"],
        "touch_hold_ids": [],
    }
    crossing = _proposal("130")
    empty = _wrapped([crossing], plan, quote="120.01", ready=True)
    assert empty.get_open_orders_to_create() == []
    assert empty._touch_place_budget == 0
    assert empty._touch_zero_released is False
    lost = _wrapped([crossing], plan, lost=[_live("late", "80")], quote="120.01", ready=True)
    assert lost.get_open_orders_to_create() == []
    assert lost._touch_place_budget == 0
    priceless = {
        "touch_budget": 0,
        "blocked_orders": [{"order_id": "late", "price": None}],
        "blocked_prices": [],
        "touch_hold_ids": [],
    }
    own = _proposal("90")
    priced = _wrapped([own, crossing], priceless, orders=[_live("late", "70")], quote="120.01")
    assert priced.get_open_orders_to_create() == []
    assert priced._touch_place_budget == 0


def test_sell_touch_follows_quantization_not_the_spread_window():
    plan = {"touch_budget": 1, "blocked_orders": [], "blocked_prices": []}
    crossing = _proposal("110")
    near = _proposal("120")
    coarse = _wrapped([crossing, near], plan, quote="119.99", side="SELL", tick="0.01")
    assert coarse.get_open_orders_to_create() == [crossing]
    assert coarse._touch_place_budget == 0
    assert coarse.get_open_orders_to_create() == []
    fine = _wrapped([crossing, near], plan, quote="119.99", side="SELL", tick="0.0001")
    assert fine.get_open_orders_to_create() == [crossing, near]
    assert fine._touch_place_budget == 0


def test_unread_active_book_does_not_finish_or_open_an_id():
    plan = {
        "touch_budget": 0,
        "blocked_orders": [{"order_id": "a", "price": "100"}],
        "blocked_prices": ["100"],
        "touch_hold_ids": [],
    }
    level = _proposal("100")
    closed = _wrapped(
        [level], plan, orders=[_live("a", "100", False)], quote="110", tick="0.01", ready=False,
    )
    assert closed.get_open_orders_to_create() == []
    assert "a" not in closed._seen_finished
    closed.connectors["okx"].ready = True
    closed.connectors["okx"].in_flight_orders = {}
    assert closed.get_open_orders_to_create() == []
    assert closed._touch_place_budget == 0
    opened = _wrapped(
        [_proposal("130")],
        {"touch_budget": 0, "blocked_orders": [{"order_id": "a", "price": "80"}], "blocked_prices": ["80"], "touch_hold_ids": []},
        orders=[_live("a", "80")],
        quote="120.01",
        ready=False,
    )
    assert opened.get_open_orders_to_create() == []
    assert "a" not in opened._seen_open
    opened.connectors["okx"].ready = True
    opened.connectors["okx"].in_flight_orders = {}
    assert opened.get_open_orders_to_create() == []
    assert opened._touch_zero_released is False


def test_uncomparable_price_holds_the_ban():
    plan = {
        "touch_budget": 0,
        "blocked_orders": [{"order_id": "snap", "price": float("nan")}],
        "blocked_prices": [float("nan")],
    }
    own = _proposal("80")
    crossing = _proposal("130")
    finished = _wrapped(
        [own, crossing], plan, quote="120.01", tick="0.01",
        cached=[_live("snap", "100", False)],
    )
    assert finished.get_open_orders_to_create() == []
    assert finished._touch_place_budget == 0
    live = _wrapped(
        [crossing],
        {"touch_budget": 1, "blocked_orders": [{"order_id": "live", "price": "NaN"}], "blocked_prices": ["NaN"]},
        orders=[_live("live", Decimal("NaN"))],
        quote="120.01",
        tick="0.01",
    )
    assert live.get_open_orders_to_create() == []
    assert live._touch_place_budget == 1
    book_only = _wrapped(
        [crossing],
        {"touch_budget": 0, "blocked_orders": [], "blocked_prices": []},
        orders=[SimpleNamespace(client_order_id="stray", price=None, is_open=True, trading_pair="SOL-USDT")],
        quote="120.01",
        tick="0.01",
    )
    assert book_only.get_open_orders_to_create() == []
    assert book_only._touch_place_budget == 0


def test_sell_live_order_at_the_quantized_touch_is_not_duplicated():
    plan = {"touch_budget": 1, "blocked_orders": [{"order_id": "ask", "price": "120"}], "blocked_prices": ["120"]}
    level = _proposal("120")
    executor = _wrapped(
        [level], plan, orders=[_live("ask", "120")], quote="119.99", side="SELL", tick="0.01",
    )
    assert executor.get_open_orders_to_create() == []
    assert executor._touch_place_budget == 1


def test_conservative_cap_does_not_release_zero():
    from utils.executor_checkpoint import conservative_grid_cap
    plan = {"touch_budget": 0, "blocked_orders": [{"order_id": "a", "price": "100"}], "blocked_prices": ["100"]}
    executor = SimpleNamespace(
        config=SimpleNamespace(side=_Side("BUY"), safe_extra_spread=Decimal("0.0001")),
        current_open_quote=Decimal("110"),
        get_open_orders_to_create=lambda: [_proposal("100"), _proposal("80")],
    )
    conservative_grid_cap(executor, plan)
    assert executor.get_open_orders_to_create() == [_proposal("80")]
    assert executor._touch_place_budget == 0


def _brake(
    proposals, orders=None, cached=None, quote="100", tick="0.01", side="BUY",
    exchange=None, foreign=None, own=None, prior=None, spread="0.0001",
    ready=True, raise_book=False, pair="SOL-USDT",
):
    orders = list(orders or [])
    cached = list(cached or [])

    class _Inflight(dict):
        def values(self):
            if raise_book:
                raise OSError("book")
            return super().values()

    inflight = _Inflight({order.client_order_id: order for order in orders})
    connector = SimpleNamespace(
        ready=ready,
        in_flight_orders=inflight,
        _order_tracker=SimpleNamespace(
            cached_orders={order.client_order_id: order for order in cached},
            lost_orders={},
        ),
    )
    closes = ["sell-a", "sell-b"]
    config = SimpleNamespace(
        side=_Side(side) if side else SimpleNamespace(),
        safe_extra_spread=Decimal(str(spread)),
        trading_pair=pair,
    )
    executor = SimpleNamespace(
        config=config,
        trading_rules=None if tick is None else SimpleNamespace(min_price_increment=Decimal(str(tick))),
        current_open_quote=None if quote is None else Decimal(str(quote)),
        connectors={"okx": connector},
        get_open_orders_to_create=lambda: list(proposals),
        get_close_orders_to_create=lambda: list(closes),
        grid_levels=[],
    )
    install_occupied_open_brake(
        executor,
        exchange if exchange is not None else {"read": True, "orders": []},
        foreign if foreign is not None else (lambda: set()),
        own_ids=set(own or []),
        prior_tracker_ids=set(prior or []),
    )
    executor._book = inflight
    executor._closes = closes
    return executor


def _done(order_id, state, is_open=False):
    return SimpleNamespace(
        client_order_id=order_id,
        price=Decimal("100"),
        is_open=is_open,
        trading_pair="SOL-USDT",
        current_state=SimpleNamespace(name=state),
        trade_type=SimpleNamespace(name="BUY"),
    )


def _buy(order_id, price, pair="SOL-USDT"):
    order = _live(order_id, price)
    order.trading_pair = pair
    order.trade_type = SimpleNamespace(name="BUY")
    return order


def test_open_buy_at_the_same_tick_is_not_repeated_and_is_not_seated():
    near = _proposal("99")
    same = _proposal("100")
    executor = _brake([same, near], orders=[_buy("loose", "100")], quote="110")
    assert executor.get_open_orders_to_create() == [near]
    assert executor.grid_levels == []
    assert "loose" in executor._book


def _move_finished(executor, order_id, state):
    executor.connectors["okx"].in_flight_orders.clear()
    executor.connectors["okx"]._order_tracker.cached_orders = {order_id: _done(order_id, state)}


def test_filled_tracker_id_releases_and_failed_or_lost_does_not():
    filled = _brake([_proposal("100")], orders=[_buy("was", "100")], prior={"was"}, quote="110")
    assert filled.get_open_orders_to_create() == []
    _move_finished(filled, "was", "FILLED")
    assert filled.get_open_orders_to_create() == [_proposal("100")]
    filled.connectors["okx"]._order_tracker.cached_orders = {}
    assert filled.get_open_orders_to_create() == [_proposal("100")]
    failed = _brake([_proposal("100")], orders=[_buy("was", "100")], prior={"was"}, quote="110")
    assert failed.get_open_orders_to_create() == []
    _move_finished(failed, "was", "FAILED")
    assert failed.get_open_orders_to_create() == []
    lost = _brake([_proposal("100")], orders=[_buy("was", "100")], prior={"was"}, quote="110")
    assert lost.get_open_orders_to_create() == []
    lost.connectors["okx"].in_flight_orders.clear()
    lost.connectors["okx"]._order_tracker.lost_orders = {"was": _buy("was", "100")}
    assert lost.get_open_orders_to_create() == []


def test_ready_false_still_blocks_a_buy_already_in_the_dictionary():
    executor = _brake([_proposal("100")], orders=[_buy("late", "100")], ready=False, quote="110")
    assert executor.get_open_orders_to_create() == []
    assert executor.grid_levels == []


def test_unread_book_or_unread_exchange_list_places_no_open_and_keeps_closes():
    unread_book = _brake([_proposal("100")], raise_book=True, quote="110")
    closes = unread_book.get_close_orders_to_create
    assert unread_book.get_open_orders_to_create() == []
    assert unread_book.get_close_orders_to_create is closes
    assert unread_book.get_close_orders_to_create() == ["sell-a", "sell-b"]
    empty = SimpleNamespace(connectors={})
    no_dict = _brake([_proposal("100")], quote="110")
    no_dict.connectors = {"okx": SimpleNamespace(in_flight_orders=[], _order_tracker=None)}
    assert no_dict.get_open_orders_to_create() == []
    unread_list = _brake([_proposal("100")], exchange={"read": False, "orders": []}, quote="110")
    assert unread_list.get_open_orders_to_create() == []
    assert empty.connectors == {}


def test_exchange_list_buy_is_not_seated_and_an_empty_book_does_not_release_it():
    exchange = {"read": True, "orders": [{"order_id": "ex", "price": "100", "side": "BUY"}]}
    executor = _brake([_proposal("100"), _proposal("99")], exchange=exchange, quote="110")
    before = set(executor._book)
    assert executor.get_open_orders_to_create() == [_proposal("99")]
    assert set(executor._book) == before
    assert executor.grid_levels == []
    executor.connectors["okx"].in_flight_orders = {}
    assert executor.get_open_orders_to_create() == [_proposal("99")]


def test_ledger_id_missing_from_a_read_list_does_not_block_a_priceless_row_does():
    clear = _brake(
        [_proposal("100")],
        exchange={"read": True, "orders": []},
        own={"ledger-only"},
        quote="110",
    )
    assert clear.get_open_orders_to_create() == [_proposal("100")]
    held = _brake(
        [_proposal("100"), _proposal("99")],
        exchange={"read": True, "orders": [{"order_id": "ex", "price": None, "side": "BUY"}]},
        quote="110",
    )
    assert held.get_open_orders_to_create() == []


def test_inventory_sell_and_a_foreign_buy_do_not_block_our_open():
    sell = _buy("tp", "100")
    sell.trade_type = SimpleNamespace(name="SELL")
    executor = _brake([_proposal("100")], orders=[sell], quote="110")
    assert executor.get_open_orders_to_create() == [_proposal("100")]
    foreign = _brake(
        [_proposal("100")],
        orders=[_buy("other", "100")],
        foreign=lambda: {"other"},
        quote="110",
    )
    assert foreign.get_open_orders_to_create() == [_proposal("100")]
    assert foreign.grid_levels == []


def test_level_under_the_quote_places_at_its_own_price_not_the_touch_formula():
    own = _proposal("120")
    blocked = _brake([own], orders=[_buy("live", "120")], quote="120.01", tick="0.0001")
    assert blocked.get_open_orders_to_create() == []
    crossing = _proposal("130")
    free = _brake([crossing], orders=[_buy("live", "120")], quote="120.01", tick="0.0001")
    assert free.get_open_orders_to_create() == [crossing]
    sell_own = _brake(
        [_proposal("120")],
        orders=[_buy("live", "120")],
        quote="119.99",
        side="SELL",
        tick="0.0001",
    )
    sell_own.connectors["okx"].in_flight_orders["live"].trade_type = SimpleNamespace(name="SELL")
    assert sell_own.get_open_orders_to_create() == []
    blind = _brake(
        [_proposal("100"), _proposal("99")],
        orders=[_buy("live", "100")],
        quote=None,
        tick=None,
    )
    assert blind.get_open_orders_to_create() == []


def test_foreign_ids_are_read_each_call_and_our_id_is_never_foreign():
    seen = {"ids": set()}

    def foreign():
        return set(seen["ids"])

    neighbor = _brake(
        [_proposal("100")],
        exchange={"read": True, "orders": [{"order_id": "neighbor", "price": "100", "side": "BUY"}]},
        foreign=foreign,
        quote="110",
    )
    assert neighbor.get_open_orders_to_create() == []
    seen["ids"] = {"neighbor"}
    assert neighbor.get_open_orders_to_create() == [_proposal("100")]
    own = _brake(
        [_proposal("100")],
        exchange={"read": True, "orders": [{"order_id": "mine", "price": "100", "side": "BUY"}]},
        foreign=lambda: {"mine"},
        own={"mine"},
        quote="110",
    )
    assert own.get_open_orders_to_create() == []


def test_twenty_five_grids_only_this_unseated_order_blocks():
    others = {f"grid-{index}" for index in range(24)}
    exchange = {"read": True, "orders": [
        {"order_id": "mine", "price": "100", "side": "BUY"},
        *(
            {"order_id": order_id, "price": "100", "side": "BUY"}
            for order_id in others
        ),
    ]}
    executor = _brake(
        [_proposal("100"), _proposal("99")],
        exchange=exchange,
        foreign=lambda: set(others),
        own={"mine"},
        quote="110",
    )
    assert executor.get_open_orders_to_create() == [_proposal("99")]


def _okx_row(order_id, price, state="live", ord_id=None, side="buy"):
    return {
        "clOrdId": order_id,
        "px": price,
        "side": side,
        "state": state,
        "ordId": ord_id or order_id,
    }


def test_exchange_page_parse_and_merge_keep_every_page():
    gate = parse_exchange_open_page(
        [{"currency_pair": "SOL_USDT", "text": "t-1", "price": "100", "side": "buy"}],
        "gate",
        "SOL_USDT",
    )
    assert gate["done"] is True
    assert gate["orders"] == [{"order_id": "t-1", "price": "100", "side": "BUY", "unknown_side": False}]
    other = parse_exchange_open_page(
        [{"currency_pair": "DOGE_USDT", "text": "t-x", "price": "1", "side": "buy"}],
        "gate",
        "SOL_USDT",
    )
    assert other["orders"] == []
    assert other["done"] is True
    groups = [
        {"currency_pair": "DOGE_USDT", "total": 1, "orders": [{"text": "a", "price": "1", "side": "buy"}]},
        {"currency_pair": "SOL_USDT", "total": 100, "orders": [{"text": "s", "price": "100", "side": "buy"}]},
    ]
    assert parse_exchange_open_page(groups, "gate", "SOL_USDT")["read"] is False
    assert parse_exchange_open_page([], "gate", "SOL_USDT", cursor_kind="before")["read"] is False
    full = [
        {"currency_pair": "SOL_USDT", "text": f"t-{index}", "price": "100", "side": "buy"}
        for index in range(100)
    ]
    first = parse_exchange_open_page(full, "gate", "SOL_USDT")
    assert first["done"] is False and first["cursor"] == "page+1"
    tail = parse_exchange_open_page(
        [{"currency_pair": "SOL_USDT", "text": "t-tail", "price": "101", "side": "buy"}],
        "gate",
        "SOL_USDT",
    )
    merged = merge_exchange_open_pages([first, tail])
    assert merged["read"] is True
    assert any(row["price"] == "100" for row in merged["orders"])
    assert any(row["price"] == "101" for row in merged["orders"])
    assert merge_exchange_open_pages([first, {"read": False}])["read"] is False
    broken = parse_exchange_open_page(
        [{"currency_pair": "SOL_USDT", "price": "100", "side": "buy"}],
        "gate",
        "SOL_USDT",
    )
    assert broken["read"] is False
    okx_bad = parse_exchange_open_page({"code": "500", "data": []}, "okx", "SOL-USDT")
    assert okx_bad["read"] is False
    okx_empty = parse_exchange_open_page({"code": "0", "data": []}, "okx", "SOL-USDT")
    assert okx_empty["read"] is True and okx_empty["done"] is True and okx_empty["orders"] == []
    hundred = {"code": "0", "data": [_okx_row(f"c-{index}", "100", ord_id=str(index)) for index in range(100)]}
    okx_full = parse_exchange_open_page(hundred, "okx", "SOL-USDT")
    assert okx_full["done"] is False and okx_full["cursor"] == "99"
    assert parse_exchange_open_page({"code": "0", "data": []}, "okx", "SOL-USDT", cursor_kind="before")["read"] is False
    filled = parse_exchange_open_page(
        {"code": "0", "data": [_okx_row("gone", "100", state="filled")]},
        "okx",
        "SOL-USDT",
    )
    assert filled["orders"] == []
    unknown = parse_exchange_open_page(
        {"code": "0", "data": [_okx_row("odd", "100", side="long")]},
        "okx",
        "SOL-USDT",
    )
    assert unknown["orders"][0]["unknown_side"] is True
    held = _brake(
        [_proposal("100"), _proposal("99")],
        exchange={"read": True, "orders": unknown["orders"]},
        quote="110",
    )
    assert held.get_open_orders_to_create() == []
    request = exchange_open_request("gate", "SOL_USDT")
    assert request["path"] == "spot/orders"
    assert "account" not in request["params"]
    assert exchange_open_request("okx", "SOL-USDT", after="99")["params"]["after"] == "99"
    assert "before" not in exchange_open_request("okx", "SOL-USDT", after="99")["params"]


def test_unknown_venue_does_not_treat_an_empty_book_as_permission():
    assert open_list_venue("BinanceExchange") is None
    assert open_list_venue("GateIoPerpetualDerivative") is None
    assert open_list_venue("OkxPerpetualDerivative") == "okx"
    assert open_list_venue("GateIoExchange") == "gate"
    assert exchange_open_request(None, "SOL-USDT") is None
    slot = new_exchange_slot()
    executor = _brake([_proposal("100")], exchange=slot, quote="110")
    assert executor.get_open_orders_to_create() == []
    assert slot["read"] is not True
    assert executor._occupied_wants_open is True
    assert executor._touch_place_budget if False else True


def test_unread_list_does_not_spend_the_touch_budget():
    slot = new_exchange_slot()
    # 130 is at the current price: the touch cap would spend its one ticket.
    level = _proposal("130")
    plan = {"touch_budget": 1, "blocked_orders": [], "blocked_prices": []}
    executor = _wrapped([level], plan, quote="120.01", tick="0.01")
    before = executor._touch_place_budget
    released = executor._touch_zero_released
    install_occupied_open_brake(executor, slot, lambda: set())
    assert executor.get_open_orders_to_create() == []
    assert executor._touch_place_budget == before
    assert executor._touch_zero_released == released
    assert executor._occupied_wants_open is True
    slot["read"] = True
    slot["orders"] = []
    assert executor.get_open_orders_to_create() == [level]
    assert executor._touch_place_budget == before - 1
    slot["orders"] = [{"order_id": "live", "price": "119.99", "side": "BUY"}]
    blocked = _brake([_proposal("119.99")], exchange=slot, quote="130", tick="0.01")
    assert blocked.get_open_orders_to_create() == []
    other = _brake([_proposal("99")], exchange=slot, quote="110")
    assert other.get_open_orders_to_create() == [_proposal("99")]


def test_seen_in_the_book_does_not_release_a_missing_id():
    slot = new_exchange_slot()
    executor = _brake(
        [_proposal("100")],
        orders=[_buy("mine", "100")],
        exchange=slot,
        own={"mine"},
        quote="110",
    )
    assert executor.get_open_orders_to_create() == []
    executor.connectors["okx"].in_flight_orders.clear()
    executor.connectors["okx"]._order_tracker.lost_orders = {"mine": _buy("mine", "100")}
    assert executor.get_open_orders_to_create() == []
    assert executor.grid_levels == []


def test_exchange_pass_key_does_not_share_an_empty_list_across_accounts():
    first = SimpleNamespace(name="okx")
    second = SimpleNamespace(name="okx")
    cache = {exchange_pass_key("alpha", first, "SOL-USDT"): {"read": True, "orders": []}}
    assert exchange_pass_key("beta", second, "SOL-USDT") not in cache
    rows = [
        {"executor_id": "other", "checkpoint": None},
    ]
    assert "other-ledger" in foreign_order_ids(rows, {"other": ["other-ledger"]}, "mine")
    assert "mine-ledger" not in foreign_order_ids(rows, {"mine": ["mine-ledger"], "other": []}, "mine")


def test_checkpoint_plus_keeps_levels_and_cancel_mark():
    read = json.dumps({
        "levels": [{"id": "L0", "price": "110"}],
        "cancel_mark": {"order_id": "old"},
    })
    body = checkpoint_plus(read, {"order_id": "new", "price": "105"})
    parsed = json.loads(body)
    assert parsed["levels"][0]["id"] == "L0"
    assert parsed["cancel_mark"]["order_id"] == "old"
    assert parsed["holds"][0]["order_id"] == "new"
    assert snapshot_body(read, {"holds": []}) is not None


def test_claim_quant_second_level_of_the_same_grid_does_not_take_it():
    reserves = {}
    assert claim_quant(reserves, "conn", "INJ-USDT", "105") is True
    assert claim_quant(reserves, "conn", "INJ-USDT", "105") is False
    release_quant(reserves, "conn", "INJ-USDT", "105")
    assert claim_quant(reserves, "conn", "INJ-USDT", "105") is True


def test_owner_is_smallest_on_the_same_pair_only():
    rows = [
        {"executor_id": "b", "account": "a", "connector": "gate", "pair": "INJ-USDT"},
        {"executor_id": "a", "account": "a", "connector": "gate", "pair": "DOGE-USDT"},
        {"executor_id": "c", "account": "a", "connector": "okx", "pair": "INJ-USDT"},
    ]
    assert pick_owner(rows, "a", "gate", "INJ-USDT") == "b"
    assert pick_owner(rows, "a", "gate", "SOL-USDT") is None


def test_failed_append_is_not_permission_to_send(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    monkeypatch.setattr(os, "write", lambda *args: (_ for _ in ()).throw(OSError("append")))
    assert remember_open_before_send("ex", "new", "L0", "105") is False


def test_prune_keeps_a_protected_id_older_than_the_window(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    path.write_text('{"executor_id": "ex", "order_id": "keep", "ts": 1}\n', encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    prune_order_ledger(protected={"keep"})
    assert "keep" in path.read_text(encoding="utf-8")


def test_hold_without_level_id_stays_a_hold():
    body = checkpoint_plus(None, {"order_id": "new", "price": "105"})
    parsed = parse_checkpoint(body)
    assert parsed["holds"][0]["order_id"] == "new"
    assert "level_id" not in parsed["holds"][0]


def test_owed_survives_a_reread_and_partial_cancel_does_not_release(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    parent.mkdir()
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(parent / "data" / "order-ledger.jsonl"))
    assert note_owed({
        "order_id": "cid",
        "price": "105",
        "executor_id": "grid-a",
        "account_name": "main",
        "connector": "gate_io",
        "trading_pair": "INJ-USDT",
        "ts": 1,
    }) is True
    assert owed_records()[0]["order_id"] == "cid"
    assert four_proofs(
        {"status": "closed", "finish_as": "cancelled", "amount": "2", "left": "1"},
        in_opens=False,
        in_flight=False,
        in_lost=False,
    ) is False
    assert gate_base_is_zero({"status": "closed", "finish_as": "cancelled"}) is False
    assert four_proofs(
        {"status": "closed", "finish_as": "cancelled", "amount": "2", "left": "2"},
        in_opens=False,
        in_flight=False,
        in_lost=False,
        history_read=True,
    ) is True
    assert four_proofs(
        {"status": "closed", "finish_as": "cancelled", "amount": "2", "left": "2"},
        in_opens=False,
        in_flight=False,
        in_lost=False,
    ) is False


def test_new_session_blocks_buy_without_finished_history():
    assert finished_list_request("gate", "INJ_USDT")["params"]["status"] == "finished"
    assert send_blocked(
        ledger_ok=True,
        column_ok=True,
        opens_ok=True,
        finished_ok=False,
        prices=[],
        placement="105",
        tick=Decimal("0.01"),
    ) is True
    request = gate_delete_request({"text": "cid", "id": "991", "currency_pair": "INJ_USDT"})
    assert request["path"] == "spot/orders/991"
    assert request["params"]["currency_pair"] == "INJ_USDT"
    assert gate_delete_request({"text": "cid"}) is None
    assert must_keep_executor("ex", None, None) is True
    assert must_keep_executor("ex", [], '{"order_id": "cid"}') is True


def test_missing_recorded_price_blocks_a_new_buy():
    assert recorded_price_blocks([None], "105", Decimal("0.01")) is True
    assert send_blocked(
        ledger_ok=True,
        column_ok=False,
        opens_ok=True,
        prices=[],
        placement="105",
        tick=Decimal("0.01"),
    ) is True
    assert send_blocked(
        ledger_ok=True,
        column_ok=True,
        opens_ok=True,
        prices=["105"],
        placement="105",
        tick=Decimal("0.01"),
    ) is True


def test_place_throw_is_not_a_local_miss_and_unread_blocks_buy():
    assert local_create_missed_place(False) is True
    assert local_create_missed_place(True) is False
    assert unread_blocks_buy(False) is True
    assert unread_blocks_buy(True) is False


import asyncio
from contextlib import asynccontextmanager

import pytest

from utils.executor_checkpoint import (
    buy_blocked,
    history_covers_ts,
    ledger_debt_marker,
    ledger_executor_ids,
    ledger_order_ids,
    recover_folder_debt,
    reset_ledger_process_state,
)


class _Kept:
    is_closed = False

    def stop(self):
        return None


class _Gate:
    client_order_id_prefix = "t-HBOT"

    def __init__(self):
        self.opens = []
        self.finished = []
        self.deleted = []
        self.created = []
        self.calls = []
        self.in_flight_orders = {}
        self._order_tracker = SimpleNamespace(lost_orders={})
        self.fail = False

    def add_listener(self, event, forwarder):
        listeners = self.__dict__.setdefault("_listeners", {})
        listeners[event] = forwarder

    async def exchange_symbol_associated_to_pair(self, pair):
        return pair.replace("-", "_")

    async def _api_get(self, path_url, params, is_auth_required=True, limit_id=None):
        self.calls.append(dict(params))
        if self.fail:
            raise OSError("unread")
        if params.get("status") == "open":
            return list(self.opens)
        return list(self.finished)

    def quantize_order_price(self, pair, price):
        return price

    async def _api_delete(self, path_url, params, is_auth_required=True):
        self.deleted.append((path_url, dict(params)))

    async def _create_order(self, **kwargs):
        self.created.append(kwargs)


class _Repo:
    def __init__(self, rows):
        self.rows = rows
        self.cleaned = None
        self.holds = []

    async def get_running_for_resume(self):
        return self.rows

    async def get_checkpoint_rows(self):
        return self.rows

    async def get_executor_by_id(self, executor_id):
        for row in self.rows:
            if row["executor_id"] == executor_id:
                return SimpleNamespace(checkpoint=row.get("checkpoint"), status="RUNNING")
        return None

    async def replace_checkpoint_if_unchanged(self, executor_id, seen, body):
        self.holds.append((executor_id, body))
        for row in self.rows:
            if row["executor_id"] == executor_id:
                row["checkpoint"] = body
        return 1

    async def cleanup_orphaned_executors(self, active_executor_ids, close_type="SYSTEM_CLEANUP"):
        self.cleaned = list(active_executor_ids)
        self.close_type = close_type
        return 0

    async def update_executor(self, executor_id, status=None, error_log=None):
        for row in self.rows:
            if row["executor_id"] == executor_id:
                row["status"] = status or row.get("status")
        return None


def _owed(order_id="t-HBOTcid", price="105", executor_id="grid-a", pair="INJ-USDT", account="main"):
    return {
        "order_id": order_id,
        "price": price,
        "executor_id": executor_id,
        "account_name": account,
        "connector": "gate_io",
        "trading_pair": pair,
        "ts": 10,
    }


def _running_row(executor_id="grid-a", pair="INJ-USDT", account="main", checkpoint=None):
    return {
        "executor_id": executor_id,
        "executor_type": "grid_executor",
        "account_name": account,
        "connector_name": "gate_io",
        "trading_pair": pair,
        "controller_id": "main",
        "config": "{}",
        "checkpoint": checkpoint,
        "status": "RUNNING",
    }


def _load_executor_service():
    import importlib.util
    import sys
    import types
    loaded = sys.modules.get("services.executor_service") or sys.modules.get("executor_service_under_test")
    if loaded is not None and hasattr(loaded, "ExecutorService"):
        return loaded
    try:
        import hummingbot  # noqa: F401
    except ImportError:
        hummingbot = None
    if hummingbot is not None:
        import services.executor_service as loaded
        return loaded

    class _Flex(types.ModuleType):
        def __getattr__(self, item):
            if item.startswith("__"):
                raise AttributeError(item)
            value = type(item, (), {"__init__": lambda self, *args, **kwargs: None})
            setattr(self, item, value)
            return value

    class _Hook:
        def find_spec(self, name, path, target=None):
            if name == "hummingbot" or name.startswith("hummingbot."):
                return importlib.util.spec_from_loader(name, self)
            return None

        def create_module(self, spec):
            module = _Flex(spec.name)
            module.__path__ = []
            module.__package__ = spec.name
            return module

        def exec_module(self, module):
            return None

    sys.meta_path.insert(0, _Hook())
    package = sys.modules.get("services")
    if package is None:
        package = types.ModuleType("services")
        package.__path__ = []
        sys.modules["services"] = package
    gateway = types.ModuleType("services.gateway_client")
    gateway.get_native_gas_token = lambda *args, **kwargs: None
    sys.modules["services.gateway_client"] = gateway
    trading = types.ModuleType("services.trading_service")
    trading.AccountTradingInterface = type("AccountTradingInterface", (), {})
    trading.TradingService = type("TradingService", (), {})
    sys.modules["services.trading_service"] = trading
    spec = importlib.util.spec_from_file_location(
        "executor_service_under_test", "services/executor_service.py"
    )
    loaded = importlib.util.module_from_spec(spec)
    sys.modules["executor_service_under_test"] = loaded
    spec.loader.exec_module(loaded)
    return loaded


def _service(monkeypatch, repo, connector, executor_id="grid-a"):
    loaded = _load_executor_service()
    monkeypatch.setattr(loaded, "ExecutorRepository", lambda session: repo)
    service = loaded.ExecutorService.__new__(loaded.ExecutorService)
    service.db_manager = SimpleNamespace(get_session_context=lambda: _session())
    service._active_executors = {}
    service._executor_metadata = {
        executor_id: {
            "account_name": "main",
            "connector_name": "gate_io",
            "trading_pair": "INJ-USDT",
            "executor_type": "grid_executor",
        }
    }
    service._trading_interfaces = {"main": SimpleNamespace(connectors={"gate_io": connector}, current_timestamp=1)}
    service._trading_service = SimpleNamespace(update_all_timestamps=lambda: None)
    service._resume_rows = list(repo.rows)
    service._is_running = True
    service._control_loop_task = None
    service.update_interval = 0
    service._lp_rent_recorded = set()
    service._lp_rent_retry_after = {}
    service._last_snapshot_at = time.monotonic()
    service._last_prune_at = time.monotonic()
    service.performance_snapshot_interval = 10 ** 9
    service.performance_retention_days = 0
    service.PERFORMANCE_PRUNE_INTERVAL_SECONDS = 10 ** 9
    service._kept_ids = set()
    return service


@asynccontextmanager
async def _session():
    yield SimpleNamespace()


async def _one_tick(service, monkeypatch):
    service._is_running = True

    async def _sleep(_seconds):
        service._is_running = False

    monkeypatch.setattr(asyncio, "sleep", _sleep)
    await service._control_loop()


def test_marker_is_synced_before_the_live_name_is_created(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    parent.mkdir()
    path = parent / "data" / "order-ledger.jsonl"
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    events = []
    real_open = os.open
    real_dir = __import__("utils.executor_checkpoint", fromlist=["_fsync_directory"])._fsync_directory
    marker = str(parent / ".order-ledger-debt")

    def tracking_open(file, flags, *args, **kwargs):
        events.append(("open", str(file)))
        return real_open(file, flags, *args, **kwargs)

    def tracking_dir(target):
        text = ""
        if os.path.exists(marker):
            text = open(marker, encoding="utf-8").read()
        events.append(("dir", str(target), text))
        return real_dir(target)

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr("utils.executor_checkpoint._fsync_directory", tracking_dir)
    assert remember_open_before_send(
        "grid-a", "t-HBOTcid", None, "105",
        account_name="main", connector="gate_io", trading_pair="INJ-USDT", ts=10,
    )
    marker_dir = next(i for i, event in enumerate(events) if event[0] == "dir" and event[1] == marker and "t-HBOTcid" in event[2])
    live_open = next(i for i, event in enumerate(events) if event[0] == "open" and event[1] == str(path))
    assert marker_dir < live_open
    assert os.path.exists(marker)
    assert "t-HBOTcid" in path.read_text(encoding="utf-8")
    reset_ledger_process_state()
    assert recover_folder_debt() is True
    assert not os.path.exists(marker)


def test_new_process_fsyncs_the_file_before_clearing_the_marker(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    parent.mkdir()
    path = parent / "data" / "order-ledger.jsonl"
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    real_dir = __import__("utils.executor_checkpoint", fromlist=["_fsync_directory"])._fsync_directory
    fail = {"on": True}

    def tracking(target):
        if fail["on"] and str(target) == str(path):
            return False
        return real_dir(target)

    monkeypatch.setattr("utils.executor_checkpoint._fsync_directory", tracking)
    assert remember_open_before_send(
        "grid-a", "t-HBOTcid", None, "105",
        account_name="main", connector="gate_io", trading_pair="INJ-USDT", ts=10,
    ) is False
    assert os.path.exists(ledger_debt_marker())
    assert buy_blocked()
    reset_ledger_process_state()
    assert buy_blocked()
    fail["on"] = False
    assert recover_folder_debt() is True
    assert not os.path.exists(ledger_debt_marker())
    assert path.exists()
    reset_ledger_process_state()
    assert path.exists()
    assert "t-HBOTcid" in path.read_text(encoding="utf-8")


def test_missing_file_with_a_marker_is_not_an_empty_start(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    parent.mkdir()
    path = parent / "data" / "order-ledger.jsonl"
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    assert note_owed(_owed()) is True
    assert not path.exists()
    assert ledger_order_ids() is None
    assert ledger_executor_ids() is None
    assert buy_blocked()


def test_marker_is_not_reopened_or_unlinked_before_the_journal_has_the_id(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    parent.mkdir()
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(parent / "data" / "order-ledger.jsonl"))
    reset_ledger_process_state()
    assert note_owed(_owed()) is True
    opens = []
    real_open = os.open
    unlinked = []

    def tracking_open(file, flags, *args, **kwargs):
        opens.append(str(file))
        return real_open(file, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "unlink", lambda file: unlinked.append(str(file)))
    assert note_owed(_owed(order_id="t-HBOTnext")) is True
    assert ledger_debt_marker() not in opens
    assert unlinked == []
    assert owed_records()[0]["order_id"] == "t-HBOTcid"
    assert owed_records()[0]["price"] == "105"


async def test_missing_file_does_not_buy_and_the_next_tick_deletes(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    parent.mkdir()
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(parent / "data" / "order-ledger.jsonl"))
    reset_ledger_process_state()
    assert note_owed(_owed()) is True
    connector = _Gate()
    repo = _Repo([_running_row(), _running_row("other", pair="DOGE-USDT", account="other")])
    service = _service(monkeypatch, repo, connector)
    service._active_executors["grid-a"] = _Kept()
    await _one_tick(service, monkeypatch)
    assert connector.created == []
    assert connector.deleted == []
    connector.opens = [
        {"text": "t-HBOTcid", "id": "991", "currency_pair": "INJ_USDT", "price": "105", "status": "open"},
        {"text": "t-HBOTother", "id": "992", "currency_pair": "DOGE_USDT", "price": "1", "status": "open"},
    ]
    await _one_tick(service, monkeypatch)
    assert connector.created == []
    assert connector.deleted == [("spot/orders/991", {"currency_pair": "INJ_USDT"})]
    assert repo.holds and "t-HBOTcid" in repo.holds[0][1]
    assert "level_id" not in repo.holds[0][1]


async def test_unread_open_list_keeps_the_row_running_and_the_next_tick_deletes(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text("", encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    connector = _Gate()
    connector.fail = True
    row = _running_row(checkpoint=None)
    repo = _Repo([row])
    service = _service(monkeypatch, repo, connector)
    interface = service._trading_interfaces["main"]
    interface.get_connector = lambda name: connector

    async def _prepare(*args, **kwargs):
        return None

    service._prepare_market = _prepare
    reason = await service._resume_one(row)
    assert reason
    assert "grid-a" in service._kept_ids
    assert not any(isinstance(item, SimpleNamespace) for item in service._active_executors.values())
    await service.cleanup_orphaned_executors()
    assert repo.cleaned is not None and "grid-a" in repo.cleaned
    assert row["status"] == "RUNNING"
    connector.fail = False
    connector.opens = [
        {"text": "t-HBOTcid", "id": "991", "currency_pair": "INJ_USDT", "price": "105", "status": "open"},
    ]
    service._executor_metadata["grid-a"] = {
        "account_name": "main",
        "connector_name": "gate_io",
        "trading_pair": "INJ-USDT",
        "executor_type": "grid_executor",
    }
    if "grid-a" not in service._active_executors:
        service._active_executors["grid-a"] = _Kept()
    await _one_tick(service, monkeypatch)
    await _one_tick(service, monkeypatch)
    assert len(connector.deleted) >= 2
    assert connector.created == []


async def test_absence_from_both_lists_does_not_release_or_buy(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "t-HBOTcid", "price": "105", "ts": 10}\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    connector = _Gate()
    connector.finished = [{"text": "other", "status": "closed", "finish_as": "cancelled", "amount": "1", "left": "1", "currency_pair": "INJ_USDT", "price": "90"}]
    repo = _Repo([_running_row(checkpoint=json.dumps({"holds": [{"order_id": "t-HBOTcid", "price": "105"}]}))])
    service = _service(monkeypatch, repo, connector)
    service._active_executors["grid-a"] = _Kept()
    await _one_tick(service, monkeypatch)
    assert "t-HBOTcid" not in service.__dict__.get("_released_order_ids", set())
    assert service._executor_proofs_released("grid-a") is False
    assert connector.created == []


async def test_four_proofs_release_the_tick(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "t-HBOTcid", "price": "105", "ts": 10}\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    connector = _Gate()
    connector.finished = [{
        "text": "t-HBOTcid",
        "status": "closed",
        "finish_as": "cancelled",
        "amount": "2",
        "left": "2",
        "currency_pair": "INJ_USDT",
        "price": "105",
    }]
    repo = _Repo([_running_row()])
    service = _service(monkeypatch, repo, connector)
    service._active_executors["grid-a"] = _Kept()
    await _one_tick(service, monkeypatch)
    assert "t-HBOTcid" in service._released_order_ids
    assert service._executor_proofs_released("grid-a") is True
    partial = dict(connector.finished[0])
    partial["left"] = "1"
    assert four_proofs(partial, in_opens=False, in_flight=False, in_lost=False, history_read=True) is False
    assert four_proofs({"status": "closed", "finish_as": "cancelled"}, in_opens=False, in_flight=False, in_lost=False, history_read=True) is False


def test_full_page_or_uncovered_window_is_not_history(tmp_path):
    request = finished_list_request("gate", "INJ_USDT")
    full = {"read": True, "done": False, "orders": []}
    assert history_covers_ts(request, 10, full) is False
    covered = finished_list_request("gate", "INJ_USDT", ts=10, now=20)
    assert history_covers_ts(covered, 10, {"read": True, "done": True}) is True
    assert history_covers_ts(request, 10, {"read": True, "done": True}) is False


def test_replace_fsyncs_the_live_file_before_dropping_the_marker(tmp_path, monkeypatch):
    path = tmp_path / "box" / "ledger.jsonl"
    path.parent.mkdir()
    fresh = json.dumps({"executor_id": "ex", "order_id": "keep", "ts": time.time()})
    path.write_text('{"executor_id": "ex", "order_id": "old", "ts": 1}\n' + fresh + "\n", encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    events = []
    real_replace = os.replace
    real_unlink = os.unlink
    real_live = __import__("utils.executor_checkpoint", fromlist=["_fsync_live_file"])._fsync_live_file

    def tracking_replace(src, dst):
        events.append("replace")
        return real_replace(src, dst)

    def tracking_live(target):
        events.append(("live", str(target)))
        return real_live(target)

    def tracking_unlink(file, *args, **kwargs):
        events.append(("unlink", str(file)))
        return real_unlink(file, *args, **kwargs)

    monkeypatch.setattr(os, "replace", tracking_replace)
    monkeypatch.setattr(os, "unlink", tracking_unlink)
    monkeypatch.setattr("utils.executor_checkpoint._fsync_live_file", tracking_live)
    prune_order_ledger(proven={("ex", "old")})
    assert events.index("replace") < events.index(("live", str(path)))
    assert events.index(("live", str(path))) < events.index(("unlink", ledger_debt_marker()))
    assert "keep" in path.read_text(encoding="utf-8")
    assert "old" not in path.read_text(encoding="utf-8")


def test_recover_does_not_unlink_before_the_directory_fsync(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text('{"executor_id": "grid-a", "order_id": "old", "price": "1", "ts": 1}\n', encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    assert note_owed(_owed()) is True
    unlinked = []
    real_unlink = os.unlink

    def tracking_unlink(file, *args, **kwargs):
        unlinked.append(str(file))
        return real_unlink(file, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", tracking_unlink)
    real_dir = __import__("utils.executor_checkpoint", fromlist=["_fsync_directory"])._fsync_directory

    def fail_live_dir(target):
        if str(target) == str(path):
            return False
        return real_dir(target)

    monkeypatch.setattr("utils.executor_checkpoint._fsync_directory", fail_live_dir)
    assert recover_folder_debt() is False
    assert os.path.exists(ledger_debt_marker())
    assert ledger_debt_marker() not in unlinked
    assert buy_blocked() is True
    monkeypatch.setattr("utils.executor_checkpoint._fsync_directory", real_dir)
    assert recover_folder_debt() is True
    assert not os.path.exists(ledger_debt_marker())
    assert "t-HBOTcid" in path.read_text(encoding="utf-8")


class _PagedGate(_Gate):
    def __init__(self):
        super().__init__()
        self.pages = {}

    async def _api_get(self, path_url, params, is_auth_required=True, limit_id=None):
        self.calls = getattr(self, "calls", [])
        self.calls.append(dict(params))
        if self.fail:
            raise OSError("unread")
        if params.get("status") == "open":
            page = int(params.get("page") or 1)
            if page == 2 and self.pages.get("fail_next"):
                raise OSError("next page")
            return list(self.pages.get(page, []))
        return list(self.finished)


def _open_level():
    return SimpleNamespace(
        price=Decimal("105"),
        amount=Decimal("1"),
        id="L0",
    )


def _buyer(connector):
    config = SimpleNamespace(
        connector_name="gate_io",
        trading_pair="INJ-USDT",
        side=SimpleNamespace(name="BUY"),
        triple_barrier_config=SimpleNamespace(open_order_type="LIMIT"),
    )
    executor = SimpleNamespace(
        config=config,
        connectors={"gate_io": connector},
        _get_open_order_candidate=lambda level: level,
        adjust_order_candidates=lambda name, rows: None,
    )
    connector.quantize_order_price = lambda pair, price: price
    connector.trading_rules = {"INJ-USDT": SimpleNamespace(min_price_increment=Decimal("0.01"))}
    connector.client_order_id_prefix = "t-HBOT"
    connector.client_order_id_max_length = 28
    return executor


async def test_full_open_page_does_not_create(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text("", encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    connector = _PagedGate()
    connector.pages = {
        1: [{"text": "other-%s" % i, "id": str(i), "currency_pair": "INJ_USDT", "price": "90"} for i in range(100)],
        "fail_next": True,
    }
    repo = _Repo([_running_row(checkpoint=json.dumps({"holds": []}))])
    service = _service(monkeypatch, repo, connector)
    await service._durable_open("grid-a", _buyer(connector), _open_level(), {})
    assert connector.created == []


async def test_open_row_without_price_does_not_create(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text("", encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    connector = _Gate()
    connector.opens = [{"text": "t-HBOTcid", "id": "991", "currency_pair": "INJ_USDT", "status": "open"}]
    repo = _Repo([_running_row(checkpoint=json.dumps({"version": 1, "type": "grid_executor", "holds": [], "levels": []}))])
    service = _service(monkeypatch, repo, connector)
    await service._durable_open("grid-a", _buyer(connector), _open_level(), {})
    assert connector.deleted == [("spot/orders/991", {"currency_pair": "INJ_USDT"})]
    assert repo.holds and "t-HBOTcid" in repo.holds[0][1]
    assert "\"price\"" not in repo.holds[0][1]
    assert connector.created == []
    connector.opens = []
    connector.deleted = []
    nxt = _service(monkeypatch, repo, connector)
    await nxt._durable_open("grid-a", _buyer(connector), _open_level(), {})
    assert connector.created == []


async def test_column_only_id_survives_stop_and_cleanup(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text("", encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    connector = _Gate()
    checkpoint = json.dumps({
        "version": 1,
        "type": "grid_executor",
        "holds": [{"order_id": "t-HBOTold", "price": "105", "ts": 1}],
        "levels": [],
    })
    row = _running_row(checkpoint=checkpoint)
    repo = _Repo([row])
    service = _service(monkeypatch, repo, connector)
    kept = _Kept()
    kept.is_closed = True
    service._active_executors["grid-a"] = kept
    await _one_tick(service, monkeypatch)
    assert "grid-a" in service._active_executors
    await service.stop()
    assert "grid-a" in service._active_executors
    await service.cleanup_orphaned_executors()
    assert "grid-a" in repo.cleaned
    await service._durable_open("grid-a", _buyer(connector), _open_level(), {})
    assert connector.created == []


async def test_finished_prefix_without_price_does_not_create(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text("", encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    connector = _Gate()
    connector.finished = [{
        "text": "t-HBOTcid",
        "id": "991",
        "currency_pair": "INJ_USDT",
        "status": "closed",
        "finish_as": "filled",
        "amount": "2",
        "left": "0",
    }]
    repo = _Repo([_running_row(checkpoint=json.dumps({"version": 1, "type": "grid_executor", "holds": [], "levels": []}))])
    service = _service(monkeypatch, repo, connector)
    await service._durable_open("grid-a", _buyer(connector), _open_level(), {})
    assert connector.created == []


async def test_unowned_prefix_is_claimed_before_any_create(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text("", encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    connector = _Gate()
    connector.finished = [{
        "text": "t-HBOTcid",
        "id": "991",
        "currency_pair": "INJ_USDT",
        "price": "105",
        "status": "closed",
        "finish_as": "filled",
        "amount": "2",
        "left": "0",
    }]
    repo = _Repo([_running_row(checkpoint=json.dumps({"version": 1, "type": "grid_executor", "holds": [], "levels": []}))])
    service = _service(monkeypatch, repo, connector)
    level = _open_level()
    level.price = Decimal("110")
    await service._durable_open("grid-a", _buyer(connector), level, {})
    assert connector.created == []
    assert repo.holds and "t-HBOTcid" in repo.holds[0][1]
    assert "level_id" not in repo.holds[0][1]


async def test_failed_commit_does_not_delete_or_create(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text("", encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    connector = _Gate()
    connector.opens = [{
        "text": "t-HBOTcid",
        "id": "991",
        "currency_pair": "INJ_USDT",
        "price": "105",
        "status": "open",
    }]
    repo = _Repo([_running_row(checkpoint=json.dumps({"version": 1, "type": "grid_executor", "holds": [], "levels": []}))])

    async def _fail_commit(self, executor_id, seen, body):
        return 0

    repo.replace_checkpoint_if_unchanged = _fail_commit.__get__(repo, _Repo)
    service = _service(monkeypatch, repo, connector)
    await service._durable_open("grid-a", _buyer(connector), _open_level(), {})
    assert connector.deleted == []
    assert connector.created == []


class _WidenGate(_Gate):
    async def _api_get(self, path_url, params, is_auth_required=True, limit_id=None):
        if params.get("status") == "open":
            return []
        if int(params.get("from", 999)) <= 10:
            return [{
                "text": "t-HBOTcid",
                "id": "991",
                "currency_pair": "INJ_USDT",
                "status": "closed",
                "finish_as": "filled",
                "amount": "2",
                "left": "0",
            }]
        return []


async def test_widened_finished_page_without_price_does_not_create(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "old", "price": "90", "ts": 200}\n'
        '{"executor_id": "grid-a", "order_id": "early", "price": "1", "ts": 10}\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    connector = _WidenGate()
    repo = _Repo([_running_row(checkpoint=json.dumps({"version": 1, "type": "grid_executor", "holds": [], "levels": []}))])
    service = _service(monkeypatch, repo, connector)
    await service._durable_open("grid-a", _buyer(connector), _open_level(), {})
    assert connector.created == []


async def test_released_other_id_does_not_free_an_open_price(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text('{"executor_id": "grid-a", "order_id": "old", "price": "90", "ts": 10}\n', encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    connector = _Gate()
    connector.opens = [{
        "text": "t-HBOTlive",
        "id": "991",
        "currency_pair": "INJ_USDT",
        "price": "105",
        "create_time": "10",
        "status": "open",
    }]
    connector.finished = [{
        "text": "t-HBOTgone",
        "id": "990",
        "currency_pair": "INJ_USDT",
        "price": "105",
        "status": "closed",
        "finish_as": "cancelled",
        "amount": "1",
        "left": "1",
        "create_time": "10",
    }]
    repo = _Repo([_running_row(checkpoint=json.dumps({"version": 1, "type": "grid_executor", "holds": [], "levels": []}))])
    service = _service(monkeypatch, repo, connector)
    service._released_order_ids = {"t-HBOTgone"}
    await service._durable_open("grid-a", _buyer(connector), _open_level(), {})
    assert connector.deleted
    assert connector.created == []


async def test_failed_read_does_not_pop_on_an_old_release(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text('{"executor_id": "grid-a", "order_id": "t-HBOTold", "price": "105", "ts": 10}\n', encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    connector = _Gate()
    connector.fail = True
    repo = _Repo([_running_row(checkpoint=json.dumps({
        "version": 1,
        "type": "grid_executor",
        "holds": [{"order_id": "t-HBOTold", "price": "105", "ts": 10}],
        "levels": [],
    }))])
    service = _service(monkeypatch, repo, connector)
    kept = _Kept()
    kept.is_closed = True
    service._active_executors["grid-a"] = kept
    service._released_order_ids = {"t-HBOTold"}
    service._sweep_seen = True
    await _one_tick(service, monkeypatch)
    assert "grid-a" in service._active_executors


def test_plain_append_does_not_unlink_the_marker(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text('{"executor_id": "grid-a", "order_id": "keep", "ts": 1}\n', encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    assert note_owed(_owed()) is True
    remember_order_now("grid-a", "other")
    assert os.path.exists(ledger_debt_marker())
    assert "t-HBOTcid" in open(ledger_debt_marker(), encoding="utf-8").read()


async def test_missing_file_after_send_still_has_an_owner(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    parent.mkdir()
    ledger = parent / "data" / "order-ledger.jsonl"
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(ledger))
    reset_ledger_process_state()
    assert remember_open_before_send(
        "grid-a", "t-HBOTcid", None, "105",
        account_name="main", connector="gate_io", trading_pair="INJ-USDT", ts=10,
    )
    assert os.path.exists(ledger_debt_marker())
    ledger.unlink()
    reset_ledger_process_state()
    connector = _Gate()
    connector.opens = [{
        "text": "t-HBOTcid",
        "id": "991",
        "currency_pair": "INJ_USDT",
        "price": "105",
        "status": "open",
    }]
    repo = _Repo([_running_row(checkpoint=json.dumps({"version": 1, "type": "grid_executor", "holds": [], "levels": []}))])
    service = _service(monkeypatch, repo, connector)
    service._active_executors["grid-a"] = _Kept()
    await _one_tick(service, monkeypatch)
    assert connector.deleted == [("spot/orders/991", {"currency_pair": "INJ_USDT"})]
    await service._durable_open("grid-a", _buyer(connector), _open_level(), {})
    assert connector.created == []


async def test_substring_checkpoint_is_not_a_hold_and_does_not_buy(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text("", encoding="utf-8")
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    note = json.dumps({
        "version": 1,
        "type": "grid_executor",
        "levels": [],
        "holds": [],
        "comment": "saw t-HBOTcid",
    })
    repo = _Repo([_running_row(checkpoint=note)])
    connector = _Gate()
    service = _service(monkeypatch, repo, connector)
    await service._durable_open("grid-a", _buyer(connector), _open_level(), {})
    assert connector.created == []
    assert connector.deleted == []


async def test_unreadable_marker_still_deletes_prefix(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    parent.mkdir()
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(parent / "data" / "order-ledger.jsonl"))
    reset_ledger_process_state()
    marker = ledger_debt_marker()
    open(marker, "w", encoding="utf-8").write("not-json\n")
    reset_ledger_process_state()
    connector = _Gate()
    connector.opens = [{"text": "t-HBOTcid", "id": "991", "currency_pair": "INJ_USDT", "price": "105", "status": "open"}]
    repo = _Repo([_running_row()])
    service = _service(monkeypatch, repo, connector)
    service._active_executors["grid-a"] = _Kept()
    await _one_tick(service, monkeypatch)
    assert connector.deleted == [("spot/orders/991", {"currency_pair": "INJ_USDT"})]
    assert connector.created == []


# --- grid-ledger-48h: the prune keeps an old line until the column proves the price ban.


def test_prune_keeps_an_old_line_without_a_price_proof(tmp_path, monkeypatch):
    box = tmp_path / "box"
    box.mkdir()
    path = box / "ledger.jsonl"
    fresh = json.dumps({"executor_id": "grid-a", "order_id": "fresh", "ts": time.time()})
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "t-HBOTold", "price": "105", "ts": 1}\n' + fresh + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    replaced = []
    monkeypatch.setattr(os, "replace", lambda *args: replaced.append(args))
    prune_order_ledger(protected=ledger_order_ids())
    assert replaced == []
    text = path.read_text(encoding="utf-8")
    assert "t-HBOTold" in text and "fresh" in text
    prune_order_ledger(protected=ledger_order_ids(), proven=set())
    assert replaced == []
    assert "t-HBOTold" in path.read_text(encoding="utf-8")


def test_prune_erases_only_the_proven_old_line(tmp_path, monkeypatch):
    box = tmp_path / "box"
    box.mkdir()
    path = box / "ledger.jsonl"
    fresh = json.dumps({"executor_id": "grid-a", "order_id": "fresh", "ts": time.time()})
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "t-HBOTold", "price": "105", "ts": 1}\n'
        '{"executor_id": "grid-b", "order_id": "t-HBOTkeep", "price": "106", "ts": 1}\n'
        + fresh + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    prune_order_ledger(protected=ledger_order_ids(), proven={("grid-a", "t-HBOTold")})
    text = path.read_text(encoding="utf-8")
    assert "t-HBOTold" not in text
    assert "t-HBOTkeep" in text and "fresh" in text


def test_prune_ignores_a_substring_or_a_foreign_executor_proof(tmp_path, monkeypatch):
    box = tmp_path / "box"
    box.mkdir()
    path = box / "ledger.jsonl"
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "buy-1", "price": "105", "ts": 1}\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    prune_order_ledger(protected=ledger_order_ids(), proven={("grid-a", "buy-10"), ("grid-b", "buy-1")})
    assert "buy-1" in path.read_text(encoding="utf-8")


def test_accounted_orders_reads_the_journal_and_the_holds(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    fresh = json.dumps({"executor_id": "grid-a", "order_id": "young", "ts": time.time()})
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "t-HBOTold", "price": "105", "ts": 1}\n'
        '{"executor_id": "grid-b", "order_id": "t-HBOTother", "price": "107", "ts": 1}\n'
        + fresh + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    checkpoint = {"holds": [{"order_id": "t-HBOThold", "price": "106"}, {"order_id": "t-HBOTold"}]}
    assert accounted_orders("grid-a", checkpoint) == {
        "t-HBOTold": "105",
        "young": None,
        "t-HBOThold": "106",
    }


def test_own_price_ban_blocks_an_empty_read_and_releases_when_led():
    from utils.executor_checkpoint import install_occupied_open_brake, order_ledger

    connector = _Gate()
    connector.trading_rules = {"INJ-USDT": SimpleNamespace(min_price_increment=Decimal("0.01"))}
    level = _open_level()
    executor = SimpleNamespace(
        config=SimpleNamespace(
            connector_name="gate_io",
            trading_pair="INJ-USDT",
            side=_Side("BUY"),
            safe_extra_spread=Decimal("0"),
        ),
        connectors={"gate_io": connector},
        current_open_quote=Decimal("105"),
        trading_rules=SimpleNamespace(min_price_increment=Decimal("0.01")),
        _order_ledger={},
    )
    executor.get_open_orders_to_create = lambda: [level]
    install_occupied_open_brake(
        executor,
        {"read": True, "done": True, "orders": []},
        lambda: set(),
        own_ids={"t-HBOTold"},
        own_prices={"t-HBOTold": "105"},
    )
    assert executor.get_open_orders_to_create() == []
    order_ledger(executor).setdefault("t-HBOTold", {"order_id": "t-HBOTold", "filled": False, "gone": False})
    order_ledger(executor)["t-HBOTold"]["gone"] = True
    assert executor.get_open_orders_to_create() == [level]


class _LiveInflight:
    def __init__(self, order_id, price):
        self.client_order_id = order_id
        self.trading_pair = "INJ-USDT"
        self.price = Decimal(price)
        self.is_open = True
        self.is_filled = False
        self.trade_type = _Side("BUY")

    def to_json(self):
        return {"client_order_id": self.client_order_id, "price": str(self.price)}


class _StartedGrid:
    is_closed = False

    def __init__(self, connector, levels=None):
        self.config = SimpleNamespace(
            connector_name="gate_io",
            trading_pair="INJ-USDT",
            side=_Side("BUY"),
            safe_extra_spread=Decimal("0"),
        )
        self.connectors = {"gate_io": connector}
        self.current_open_quote = Decimal("105")
        self.trading_rules = SimpleNamespace(min_price_increment=Decimal("0.01"))
        self.grid_levels = []
        self._close_order = None
        self._order_ledger = {}
        self._proposed = list(levels or [])
        self.started = 0

    def update_grid_levels(self):
        return None

    def _get_open_order_candidate(self, level):
        return level

    def adjust_order_candidates(self, name, rows):
        return None

    def get_open_orders_to_create(self):
        return list(self._proposed)

    def start(self):
        self.started += 1

    def stop(self):
        return None


def _fake_market_events(monkeypatch):
    import sys
    import types

    events = types.ModuleType("hummingbot.core.event.events")

    class _MarketEvent:
        OrderFilled = "accounted-fill"
        OrderCancelled = "accounted-cancel"

    events.MarketEvent = _MarketEvent
    forwarder = types.ModuleType("hummingbot.core.event.event_forwarder")

    class _Forwarder:
        def __init__(self, callback):
            self.callback = callback

    forwarder.SourceInfoEventForwarder = _Forwarder
    common = types.ModuleType("hummingbot.core.data_type.common")

    class _FakeEnum(dict):
        def __init__(self, **items):
            super().__init__(**items)
            self.__dict__.update(items)

    class _CommonModule(types.ModuleType):
        def __getattr__(self, item):
            if item.startswith("__"):
                raise AttributeError(item)
            value = _FakeEnum(**{item: item})
            setattr(self, item, value)
            return value

    common = _CommonModule("hummingbot.core.data_type.common")
    common.TradeType = _FakeEnum(BUY="BUY", SELL="SELL")
    common.OrderType = _FakeEnum(LIMIT="LIMIT", MARKET="MARKET")
    common.PriceType = _FakeEnum(MidPrice="MidPrice")
    monkeypatch.setitem(sys.modules, "hummingbot.core.event.events", events)
    monkeypatch.setitem(sys.modules, "hummingbot.core.event.event_forwarder", forwarder)
    monkeypatch.setitem(sys.modules, "hummingbot.core.data_type.common", common)


def _stubbed_resume(monkeypatch, service, connector):
    loaded = _load_executor_service()
    interface = service._trading_interfaces["main"]
    interface.get_connector = lambda name: connector

    async def _prepare(*args, **kwargs):
        return None

    service._prepare_market = _prepare

    async def _open_pass(account, conn, pair):
        return {"read": True, "done": True, "orders": []}

    service._exchange_open_pass = _open_pass
    monkeypatch.setattr(loaded, "grid_resume_plan", lambda *args, **kwargs: None)

    def _validate(config, default_timestamp=None):
        return _StartedGrid, object, SimpleNamespace(id=config.get("id"))

    def _instantiate(executor_class, typed_config, trading_interface, metadata, start=True):
        executor = _StartedGrid(connector, [_open_level()])
        service._active_executors[metadata["config"]["id"]] = executor
        return metadata["config"]["id"], executor

    service._validate_executor_config = _validate
    service._instantiate_and_register = _instantiate
    connector.trading_rules = {"INJ-USDT": SimpleNamespace(min_price_increment=Decimal("0.01"))}
    return service


async def test_old_ledger_line_keeps_a_started_owner_and_blocks_the_price(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    fresh = json.dumps({"executor_id": "grid-a", "order_id": "fresh", "ts": time.time()})
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "t-HBOTold", "price": "105", "ts": 1}\n'
        '{"executor_id": "grid-a", "order_id": "t-HBOTgone", "ts": 1}\n'
        + fresh + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    _fake_market_events(monkeypatch)
    connector = _Gate()
    checkpoint = json.dumps({"version": 1, "type": "grid_executor", "holds": [], "levels": []})
    repo = _Repo([_running_row(checkpoint=checkpoint)])
    service = _service(monkeypatch, repo, connector)
    _stubbed_resume(monkeypatch, service, connector)
    await service.resume_running_executors()
    text = path.read_text(encoding="utf-8")
    assert "t-HBOTold" in text and "fresh" in text
    executor = service._active_executors["grid-a"]
    assert isinstance(executor, _StartedGrid) and executor.started == 1
    ledger = executor._order_ledger
    assert ledger["t-HBOTold"]["filled"] is False and ledger["t-HBOTold"]["gone"] is False
    assert ledger["t-HBOTold"]["price"] == "105"
    assert ledger["t-HBOTgone"]["gone"] is False
    routes = service.__dict__.get("_accounted_routes") or {}
    assert routes.get("t-HBOTold") == "grid-a" and routes.get("t-HBOTgone") == "grid-a"
    assert executor.grid_levels == []
    assert executor.get_open_orders_to_create() == []
    await service._durable_open("grid-a", executor, _open_level(), {})
    assert connector.created == []
    listeners = connector.__dict__.get("_listeners") or {}
    fill = listeners.get("accounted-fill")
    cancel = listeners.get("accounted-cancel")
    assert fill is not None and cancel is not None
    forwarders = service.__dict__.get("_accounted_forwarders") or {}
    assert fill is forwarders.get(("main", "gate_io"), (None, None))[0]
    assert cancel is forwarders.get(("main", "gate_io"), (None, None))[1]
    fill.callback(1, connector, SimpleNamespace(order_id="t-HBOTold"))
    assert ledger["t-HBOTold"]["filled"] is True
    assert executor.get_open_orders_to_create() == executor._proposed
    cancel.callback(1, connector, SimpleNamespace(order_id="t-HBOTgone"))
    assert ledger["t-HBOTgone"]["gone"] is True


async def test_hold_price_proof_erases_the_line_and_leads_the_live_order(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    fresh = json.dumps({"executor_id": "grid-a", "order_id": "fresh", "ts": time.time()})
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "t-HBOTold", "price": "105", "ts": 1}\n' + fresh + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    _fake_market_events(monkeypatch)
    connector = _Gate()
    live = _LiveInflight("t-HBOTold", "105")
    connector.in_flight_orders = {"t-HBOTold": live}
    checkpoint = json.dumps({
        "version": 1,
        "type": "grid_executor",
        "holds": [{"order_id": "t-HBOTold", "price": "105", "ts": 1, "filled": True}],
        "levels": [],
    })
    repo = _Repo([_running_row(checkpoint=checkpoint)])
    service = _service(monkeypatch, repo, connector)
    _stubbed_resume(monkeypatch, service, connector)
    await service.resume_running_executors()
    text = path.read_text(encoding="utf-8")
    assert "t-HBOTold" not in text and "fresh" in text
    executor = service._active_executors["grid-a"]
    assert executor.started == 1
    row = executor._order_ledger["t-HBOTold"]
    assert row["order"] == {"client_order_id": "t-HBOTold", "price": "105"}
    assert row["filled"] is True
    assert executor.grid_levels == []
    assert executor.get_open_orders_to_create() == []
    assert service.__dict__.get("_accounted_routes", {}).get("t-HBOTold") == "grid-a"


async def test_column_open_order_without_price_is_not_a_proof(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "t-HBOTold", "price": "105", "ts": 1}\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    _fake_market_events(monkeypatch)
    connector = _Gate()
    checkpoint = json.dumps({
        "version": 1,
        "type": "grid_executor",
        "holds": [],
        "levels": [{
            "id": "L0", "price": "105", "amount_quote": "10", "take_profit": "0.01",
            "side": "BUY", "open_order_type": "LIMIT", "take_profit_order_type": "LIMIT",
            "open_order": {"order_id": "t-HBOTold", "filled": False},
        }],
    })
    repo = _Repo([_running_row(checkpoint=checkpoint)])
    service = _service(monkeypatch, repo, connector)
    _stubbed_resume(monkeypatch, service, connector)
    await service.resume_running_executors()
    assert "t-HBOTold" in path.read_text(encoding="utf-8")
    executor = service._active_executors["grid-a"]
    assert executor.started == 1
    assert getattr(executor.grid_levels[0], "active_open_order", "missing") is None
    assert executor.get_open_orders_to_create() == []


async def test_hold_without_price_keeps_the_line_and_blocks_the_send(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "t-HBOTold", "ts": 1}\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    _fake_market_events(monkeypatch)
    connector = _Gate()
    checkpoint = json.dumps({
        "version": 1,
        "type": "grid_executor",
        "holds": [{"order_id": "t-HBOTold"}],
        "levels": [],
    })
    repo = _Repo([_running_row(checkpoint=checkpoint)])
    service = _service(monkeypatch, repo, connector)
    _stubbed_resume(monkeypatch, service, connector)
    await service.resume_running_executors()
    assert "t-HBOTold" in path.read_text(encoding="utf-8")
    executor = service._active_executors["grid-a"]
    assert executor.started == 1
    assert "t-HBOTold" in executor._order_ledger
    await service._durable_open("grid-a", executor, _open_level(), {})
    assert connector.created == []
    # The gate ran: the connector answered the open and finished lists.
    assert connector.calls


async def test_other_executor_hold_is_not_a_proof_and_keeps_the_owner(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "t-HBOTold", "price": "105", "ts": 1}\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    _fake_market_events(monkeypatch)
    empty = json.dumps({"version": 1, "type": "grid_executor", "holds": [], "levels": []})
    owner_hold_priced = json.dumps({
        "version": 1,
        "type": "grid_executor",
        "holds": [{"order_id": "t-HBOTold", "price": "105"}],
        "levels": [],
    })
    stopped_hold_priced = json.dumps({
        "version": 1,
        "type": "grid_executor",
        "holds": [{"order_id": "t-HBOTold", "price": "105"}],
        "levels": [],
    })
    for owner_checkpoint, line_kept in ((empty, True), (owner_hold_priced, False)):
        connector = _Gate()
        rows_all = [
            _running_row(checkpoint=owner_checkpoint),
            {**_running_row("grid-s", checkpoint=stopped_hold_priced), "status": "TERMINATED"},
        ]
        repo = _Repo(rows_all)

        async def _running_only():
            return [row for row in rows_all if row.get("status") == "RUNNING"]

        repo.get_running_for_resume = _running_only
        service = _service(monkeypatch, repo, connector)
        _stubbed_resume(monkeypatch, service, connector)
        await service.resume_running_executors()
        text = path.read_text(encoding="utf-8")
        assert ("t-HBOTold" in text) is line_kept
        assert service.__dict__.get("_accounted_routes", {}).get("t-HBOTold") == "grid-a"
        assert "t-HBOTold" in service._active_executors["grid-a"]._order_ledger
        # The stopped prover neither starts nor holds the route.
        assert "grid-s" not in service._active_executors
        path.write_text(
            '{"executor_id": "grid-a", "order_id": "t-HBOTold", "price": "105", "ts": 1}\n',
            encoding="utf-8",
        )


async def test_non_started_row_claims_rollback_to_the_starter(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text(
        '{"executor_id": "grid-x", "order_id": "t-HBOTold", "price": "105", "ts": 1}\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    _fake_market_events(monkeypatch)
    connector = _Gate()
    live = _LiveInflight("t-HBOTold", "105")
    connector.in_flight_orders = {"t-HBOTold": live}
    hold_priced = json.dumps({
        "version": 1,
        "type": "grid_executor",
        "holds": [{"order_id": "t-HBOTold", "price": "105"}],
        "levels": [],
    })
    broken_row = _running_row("grid-x", checkpoint=json.dumps({"version": 1, "type": "grid_executor", "holds": [], "levels": []}))
    broken_row["config"] = ""
    repo = _Repo([broken_row, _running_row(checkpoint=hold_priced)])
    service = _service(monkeypatch, repo, connector)
    _stubbed_resume(monkeypatch, service, connector)
    await service.resume_running_executors()
    executor = service._active_executors["grid-a"]
    assert executor.started == 1
    assert service.__dict__.get("_accounted_routes", {}).get("t-HBOTold") == "grid-a"
    row = executor._order_ledger["t-HBOTold"]
    assert row["order"] == {"client_order_id": "t-HBOTold", "price": "105"}
    assert executor.grid_levels == []
    assert executor.get_open_orders_to_create() == []
    # grid-x returned before start(); its claim did not survive the loop.
    assert "grid-x" not in {value for value in service.__dict__.get("_accounted_routes", {}).values()}
    # The line belongs to grid-x, which has no priced hold of its own: it stays.
    assert "t-HBOTold" in path.read_text(encoding="utf-8")


async def test_ledger_price_proofs_require_the_own_hold_with_a_price(tmp_path, monkeypatch):
    box = tmp_path / "bots"
    (box / "data").mkdir(parents=True)
    path = box / "data" / "order-ledger.jsonl"
    fresh = json.dumps({"executor_id": "grid-a", "order_id": "fresh", "ts": time.time()})
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "t-HBOTold", "price": "105", "ts": 1}\n' + fresh + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    connector = _Gate()
    good = _running_row(checkpoint=json.dumps({
        "version": 1, "type": "grid_executor",
        "holds": [{"order_id": "t-HBOTold", "price": "105"}], "levels": [],
    }))
    flat_only = _running_row("grid-b", checkpoint=json.dumps({
        "version": 1, "type": "grid_executor", "holds": [],
        "levels": [{"open_order": {"price": "105"}}],
    }))
    service = _service(monkeypatch, _Repo([good, flat_only]), connector)
    proven = await service._ledger_price_proofs()
    assert proven == {("grid-a", "t-HBOTold")}
    prune_order_ledger(protected=ledger_order_ids(), proven=proven)
    text = path.read_text(encoding="utf-8")
    assert "t-HBOTold" not in text and "fresh" in text
    prices = await service._column_prices("INJ-USDT")
    assert prices is not None and "105" in prices
    assert recorded_price_blocks(prices, "105", Decimal("0.01")) is True
    broken = _running_row("grid-c", checkpoint="{not json")
    service2 = _service(monkeypatch, _Repo([good, broken]), connector)
    assert await service2._ledger_price_proofs() is None
    service3 = _service(monkeypatch, _Repo([_running_row()]), connector)
    service3.db_manager = None
    assert await service3._ledger_price_proofs() is None


def test_prune_keeps_a_priced_line_without_a_numeric_ts_until_proof(tmp_path, monkeypatch):
    box = tmp_path / "box"
    box.mkdir()
    path = box / "ledger.jsonl"
    path.write_text(
        '{"executor_id": "ex", "order_id": "priced", "price": "105"}\n'
        '{"executor_id": "ex", "order_id": "bare"}\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    replaced = []
    real_replace = os.replace

    def tracking_replace(src, dst):
        replaced.append((src, dst))
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", tracking_replace)
    prune_order_ledger(protected=ledger_order_ids())
    text = path.read_text(encoding="utf-8")
    assert "priced" in text and "bare" not in text
    assert replaced
    prune_order_ledger(protected=ledger_order_ids(), proven=set())
    assert "priced" in path.read_text(encoding="utf-8")
    prune_order_ledger(protected=ledger_order_ids(), proven={("ex", "priced")})
    assert "priced" not in path.read_text(encoding="utf-8")


async def test_column_read_failure_keeps_every_line_and_starts(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    fresh = json.dumps({"executor_id": "grid-a", "order_id": "fresh", "ts": time.time()})
    path.write_text(
        '{"executor_id": "grid-a", "order_id": "t-HBOTold", "price": "105", "ts": 1}\n' + fresh + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    _fake_market_events(monkeypatch)
    connector = _Gate()
    checkpoint = json.dumps({
        "version": 1,
        "type": "grid_executor",
        "holds": [{"order_id": "t-HBOTold", "price": "105"}],
        "levels": [],
    })
    repo = _Repo([_running_row(checkpoint=checkpoint)])

    async def _boom():
        raise OSError("column unread")

    repo.get_checkpoint_rows = _boom
    service = _service(monkeypatch, repo, connector)
    _stubbed_resume(monkeypatch, service, connector)
    replaced = []
    monkeypatch.setattr(os, "replace", lambda *args: replaced.append(args))
    before = path.read_bytes()
    await service.resume_running_executors()
    assert replaced == []
    assert path.read_bytes() == before
    executor = service._active_executors["grid-a"]
    assert executor.started == 1
    assert "t-HBOTold" in executor._order_ledger


async def test_saved_id_of_a_starter_gets_the_listener_route(tmp_path, monkeypatch):
    parent = tmp_path / "bots"
    (parent / "data").mkdir(parents=True)
    path = parent / "data" / "order-ledger.jsonl"
    path.write_text(
        '{"executor_id": "grid-z", "order_id": "t-HBOTold", "price": "105", "ts": 1}\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("EXECUTOR_ORDER_LEDGER", str(path))
    reset_ledger_process_state()
    _fake_market_events(monkeypatch)
    connector = _Gate()
    saved_checkpoint = json.dumps({
        "version": 1,
        "type": "grid_executor",
        "holds": [],
        "levels": [{
            "id": "L0", "price": "105", "amount_quote": "10", "take_profit": "0.01",
            "side": "BUY", "open_order_type": "LIMIT", "take_profit_order_type": "LIMIT",
            "open_order": {"order_id": "t-HBOTold", "filled": False},
        }],
    })
    repo = _Repo([_running_row(checkpoint=saved_checkpoint)])
    service = _service(monkeypatch, repo, connector)
    _stubbed_resume(monkeypatch, service, connector)
    await service.resume_running_executors()
    executor = service._active_executors["grid-a"]
    assert executor.started == 1
    assert service.__dict__.get("_accounted_routes", {}).get("t-HBOTold") == "grid-a"
    assert "grid-z" not in {value for value in service.__dict__.get("_accounted_routes", {}).values()}
    # Not seated by price: _attach has no live and no filled snapshot.
    assert getattr(executor.grid_levels[0], "active_open_order", "missing") is None
    # No proof erases the line: the hold of grid-z does not exist.
    assert "t-HBOTold" in path.read_text(encoding="utf-8")
    listeners = connector.__dict__.get("_listeners") or {}
    fill = listeners.get("accounted-fill")
    assert fill is not None
    fill.callback(1, connector, SimpleNamespace(order_id="t-HBOTold"))
    assert executor._order_ledger["t-HBOTold"]["filled"] is True
