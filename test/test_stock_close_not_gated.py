"""Штатный конец пишется без нашего разрешения. План stock-close-not-gated. Без биржи.

Замок доказательств номеров больше не стоит перед штатной записью конца сетки
в базу: закрытый исполнитель садится фактическим типом конца (TAKE_PROFIT и
другие), даже когда какой-то номер ещё не доказан биржей.
"""

import asyncio

from types import SimpleNamespace

from test.test_stop_freeze import _checkpoint, _service_with
from test.test_executor_checkpoint import _one_tick
from test.test_stop_position_close import EXECUTOR


class _Row:
    def __init__(self, status="RUNNING", close_type=None):
        self.status = status
        self.close_type = close_type


class _UpsertRepo:
    def __init__(self, row):
        self.row = row
        self.writes = []

    async def upsert_executor_completion(self, executor_id, status=None, close_type=None, **kwargs):
        self.writes.append((status, close_type))
        self.row.status = status
        self.row.close_type = close_type
        return self.row, False


class _Db:
    def __init__(self, repo):
        self.repo = repo

    def get_session_context(self):
        return self

    async def __aenter__(self):
        return self.repo

    async def __aexit__(self, *args):
        return False


def _bind_upsert_db(monkeypatch, service, row):
    repo = _UpsertRepo(row)
    service.db_manager = _Db(repo)
    import services.executor_service as svc_mod
    monkeypatch.setattr(svc_mod, "ExecutorRepository", lambda session: session)
    return repo


def _closed_executor(close_name):
    return SimpleNamespace(
        is_closed=True,
        status=SimpleNamespace(name="TERMINATED"),
        close_type=SimpleNamespace(name=close_name),
        get_custom_info=lambda: {},
    )


def _quiet_completion(service):
    service._lp_position_addresses = {}
    service._lp_rent_recorded = set()
    service._lp_rent_retry_after = {}
    service._log_capture = SimpleNamespace(
        clear=lambda executor_id: None,
        get_error_count=lambda executor_id: 0,
    )

    async def quiet(*args, **kwargs):
        return None

    service._record_executor_swap = quiet


def test_take_profit_end_writes_even_with_an_unproven_number(tmp_path, monkeypatch):
    """Номер не доказан — штатная запись конца всё равно села. TAKE_PROFIT, не только ранняя остановка."""
    service, _repo, _connector = _service_with(monkeypatch, _checkpoint())
    executor = _closed_executor("TAKE_PROFIT")
    service._active_executors[EXECUTOR] = executor
    row = _Row(status="RUNNING")
    repo = _bind_upsert_db(monkeypatch, service, row)
    service._executor_proofs_released = lambda executor_id: False
    _quiet_completion(service)
    asyncio.run(service._handle_executor_completion(EXECUTOR))
    assert repo.writes == [("TERMINATED", "TAKE_PROFIT")]
    assert row.status == "TERMINATED" and row.close_type == "TAKE_PROFIT"
    assert EXECUTOR not in service._active_executors


def test_early_stop_end_still_writes_with_proven_numbers(tmp_path, monkeypatch):
    """Регресс: доказанная ранняя остановка садится как раньше."""
    service, _repo, _connector = _service_with(monkeypatch, _checkpoint())
    executor = _closed_executor("EARLY_STOP")
    service._active_executors[EXECUTOR] = executor
    row = _Row(status="RUNNING")
    repo = _bind_upsert_db(monkeypatch, service, row)
    service._executor_proofs_released = lambda executor_id: True
    _quiet_completion(service)
    asyncio.run(service._handle_executor_completion(EXECUTOR))
    assert repo.writes == [("TERMINATED", "EARLY_STOP")]
    assert EXECUTOR not in service._active_executors


def test_failed_write_keeps_the_executor_for_the_next_tick(tmp_path, monkeypatch):
    """Запись конца упала — исполнитель остаётся в памяти и строка RUNNING;"
    следующий вызов с успешной записью сажает конец и только тогда вынимает."""
    service, _repo, _connector = _service_with(monkeypatch, _checkpoint())
    executor = _closed_executor("TAKE_PROFIT")
    service._active_executors[EXECUTOR] = executor
    row = _Row(status="RUNNING")
    repo = _bind_upsert_db(monkeypatch, service, row)

    async def failing(*args, **kwargs):
        raise RuntimeError("write failed")

    repo.upsert_executor_completion = failing
    service._executor_proofs_released = lambda executor_id: False
    _quiet_completion(service)
    asyncio.run(service._handle_executor_completion(EXECUTOR))
    assert EXECUTOR in service._active_executors
    assert row.status == "RUNNING"

    asyncio.run(service._handle_executor_completion(EXECUTOR))
    assert EXECUTOR in service._active_executors

    del repo.upsert_executor_completion
    asyncio.run(service._handle_executor_completion(EXECUTOR))
    assert row.status == "TERMINATED" and row.close_type == "TAKE_PROFIT"
    assert EXECUTOR not in service._active_executors


def test_control_tick_sends_closed_take_profit_to_stock_completion(tmp_path, monkeypatch):
    """Один тик цикла: закрытый TAKE_PROFIT с недоказанным номером идёт в штатную запись,
    наш посадчик ранней остановки из цикла не зовётся."""
    service, _repo, _connector = _service_with(monkeypatch, _checkpoint())
    executor = _closed_executor("TAKE_PROFIT")
    service._active_executors[EXECUTOR] = executor
    called = []

    async def sweep():
        pass

    async def retry(now):
        pass

    async def checkpoint():
        pass

    async def completion(executor_id):
        called.append(("stock", executor_id))

    async def land(executor_id, executor):
        called.append(("land", executor_id))

    async def no_owner(executor_id, executor):
        return False

    service._sweep_unproven_orders = sweep
    service._retry_unread_open_lists = retry
    service._checkpoint_active_executors = checkpoint
    service._handle_executor_completion = completion
    service._land_closed_early_stop = land
    service._flat_stop_needs_owner = no_owner
    service._executor_proofs_released = lambda executor_id: False
    asyncio.run(_one_tick(service, monkeypatch))
    assert ("land", EXECUTOR) not in called
    assert called == [("stock", EXECUTOR)]
