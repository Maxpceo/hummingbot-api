"""После штатного конца строку не открываем снова. Подъём закрытую раннюю остановку не поднимает.

План stop-finished-not-reopened. Без биржи.
"""

import asyncio
import json

from types import SimpleNamespace

from test.test_stop_freeze import PAIR, _checkpoint, _service_with, _stub_executor
from test.test_stop_position_close import EXECUTOR, _FlatRow, _bind_flat_db


def _quiet_completion(service):
    service._lp_position_addresses = {}
    service._lp_rent_recorded = set()
    service._lp_rent_retry_after = {}
    service._log_capture = SimpleNamespace(clear=lambda executor_id: None)

    async def quiet(*args, **kwargs):
        return None

    service._record_executor_swap = quiet


def test_finished_stop_is_not_reopened_when_lists_are_empty(monkeypatch):
    """Исполнитель уже остановлен, номеров нет. Конец не пишет строку в работу."""
    service, _repo, connector = _service_with(monkeypatch, _checkpoint())
    executor = _stub_executor(connector, [])
    executor.is_closed = True
    executor._status = type("Status", (), {"name": "TERMINATED"})()
    executor.close_type = type("Close", (), {"name": "EARLY_STOP"})()
    service._active_executors[EXECUTOR] = executor
    row = _FlatRow(status="TERMINATED", close_type="EARLY_STOP")
    _bind_flat_db(monkeypatch, service, row)
    called = []

    async def restore(executor_id):
        called.append("restore")
        return True

    def reopen(executor, executor_id=""):
        called.append("reopen")

    async def persist(executor_id, executor):
        called.append(("persist", executor._status.name, executor.close_type.name))
        return True

    service._executor_proofs_released = lambda executor_id: True
    service._restore_running_after_flat = restore
    service._reopen_closed_executor = reopen
    service._persist_executor_completed = persist
    _quiet_completion(service)
    service._flat_checkpoint_number_live = lambda executor_id, executor: True
    asyncio.run(service._handle_executor_completion(EXECUTOR))
    assert "restore" not in called
    assert "reopen" not in called
    assert called == [("persist", "TERMINATED", "EARLY_STOP")]
    assert row.status == "TERMINATED"
    assert EXECUTOR not in service._active_executors


def test_finished_stop_is_not_reopened_when_lists_are_unread(monkeypatch):
    """Непрочитанные списки после штатного конца строку в работу не возвращают."""
    service, _repo, connector = _service_with(monkeypatch, _checkpoint())
    executor = _stub_executor(connector, [])
    executor.is_closed = True
    executor._status = type("Status", (), {"name": "TERMINATED"})()
    executor.close_type = type("Close", (), {"name": "EARLY_STOP"})()
    service._active_executors[EXECUTOR] = executor
    called = []
    service._executor_proofs_released = lambda executor_id: True
    service._restore_running_after_flat = lambda executor_id: called.append("restore")
    service._reopen_closed_executor = lambda *args, **kwargs: called.append("reopen")
    async def persist(executor_id, executor):
        called.append("persist")
        return True

    service._persist_executor_completed = persist
    _quiet_completion(service)
    monkeypatch.setattr("services.executor_service.order_miss_unreadable", lambda: True)
    asyncio.run(service._handle_executor_completion(EXECUTOR))
    assert called == ["persist"]


def test_resume_does_not_reopen_a_closed_early_stop(monkeypatch):
    """Подъём не пишет закрытую раннюю остановку в работу и читает строку, которая в работе."""
    service, _repo, connector = _service_with(monkeypatch, _checkpoint())
    closed = _FlatRow(status="TERMINATED", close_type="EARLY_STOP")
    running = {
        "executor_id": "grid-run",
        "executor_type": "grid_executor",
        "account_name": "main",
        "connector_name": "gate_io",
        "trading_pair": PAIR,
        "controller_id": "main",
        "config": json.dumps({"id": "grid-run", "type": "grid_executor"}),
        "checkpoint": json.dumps({"version": 1, "type": "grid_executor", "levels": [], "holds": []}),
        "status": "RUNNING",
        "close_type": "",
    }
    seen = []

    class _Repo:
        async def get_early_stop_rows(self):
            seen.append("early")
            return [{
                "executor_id": EXECUTOR,
                "status": "TERMINATED",
                "close_type": "EARLY_STOP",
                "checkpoint": "{}",
            }]

        async def get_running_for_resume(self):
            seen.append("running")
            return [running]

        async def update_executor(self, executor_id, status=None, close_type=None, **kwargs):
            seen.append(("write", executor_id, status, close_type))
            if executor_id == EXECUTOR:
                closed.status = status
                closed.close_type = close_type
            return closed

    class _Db:
        def __init__(self, repo):
            self.repo = repo

        def get_session_context(self):
            return self

        async def __aenter__(self):
            return self.repo

        async def __aexit__(self, *args):
            return False

    service.db_manager = _Db(_Repo())
    monkeypatch.setattr("services.executor_service.ExecutorRepository", lambda session: session)
    service._active_executors["grid-run"] = _stub_executor(connector, [])
    service._remember_journal_prefix_ids = lambda: None

    async def prefix(rows):
        seen.append(("prefix", [row["executor_id"] for row in rows]))

    service._remember_open_prefix_ids = prefix

    async def quiet(*args, **kwargs):
        return None

    service._commit_close_holds_before_prune = quiet
    asyncio.run(service.resume_running_executors())
    assert "early" not in seen
    assert ("write", EXECUTOR, "RUNNING", "") not in seen
    assert closed.status == "TERMINATED"
    assert closed.close_type == "EARLY_STOP"
    assert "running" in seen
    assert ("prefix", ["grid-run"]) in seen
