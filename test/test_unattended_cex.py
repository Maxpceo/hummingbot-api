"""CEX restart leftovers are named, not closed.

cleanup_orphaned_executors marks every leftover RUNNING row SYSTEM_CLEANUP and
does not touch the exchange. The unattended listing must show the CEX types and
must not include lp_executor, which already has its own close-oriented orphan list.
"""
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from database.models import ExecutorRecord
from database.repositories.executor_repository import ExecutorRepository
from utils.unattended_cex import (
    CEX_UNATTENDED_TYPES,
    UNATTENDED_WARNING,
    is_unattended_cex,
    unattended_view,
)


class _AsyncSessionAdapter:
    def __init__(self, session: Session):
        self._session = session

    async def execute(self, statement):
        return self._session.execute(statement)

    async def commit(self):
        self._session.commit()

    async def close(self):
        self._session.close()


@pytest.fixture
def db():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    ExecutorRecord.__table__.create(engine)

    @asynccontextmanager
    async def session_context():
        session = Session(engine)
        adapter = _AsyncSessionAdapter(session)
        try:
            yield adapter
            await adapter.commit()
        finally:
            await adapter.close()

    def insert(rows):
        with Session(engine) as session:
            for i, row in enumerate(rows):
                fields = {
                    "executor_type": "grid_executor",
                    "connector_name": "okx_perpetual",
                    "status": "TERMINATED",
                    "close_type": "SYSTEM_CLEANUP",
                    "net_pnl_quote": Decimal("0"),
                    "filled_amount_quote": Decimal("0"),
                    **row,
                }
                session.add(ExecutorRecord(
                    executor_id=fields.pop("executor_id", f"e-{i}"),
                    account_name="master",
                    trading_pair=fields.pop("trading_pair", "SOL-USDT"),
                    controller_id="main",
                    net_pnl_pct=Decimal("0"),
                    cum_fees_quote=Decimal("0"),
                    closed_at=datetime(2026, 9, 21, 12, 48, tzinfo=timezone.utc),
                    **fields,
                ))
            session.commit()

    try:
        yield SimpleNamespace(session_context=session_context, insert=insert)
    finally:
        engine.dispose()


def test_warning_does_not_tell_anyone_to_close_or_resume():
    lowered = UNATTENDED_WARNING.lower()
    assert "does not close" in lowered
    assert "does not resume" in lowered
    assert "manage_clmm" not in lowered
    assert "market" not in lowered


def test_lp_and_clean_closes_are_not_unattended():
    assert is_unattended_cex("lp_executor", "SYSTEM_CLEANUP", {}) is False
    assert is_unattended_cex("grid_executor", "TAKE_PROFIT", {}) is False
    assert is_unattended_cex("grid_executor", "FAILED", {}) is False
    assert is_unattended_cex("grid_executor", "SYSTEM_CLEANUP", {"orphan_resolved": True}) is False
    assert is_unattended_cex("dca_executor", "SYSTEM_CLEANUP", {}) is True


def test_every_cex_registry_type_is_listed_and_lp_is_not():
    assert "lp_executor" not in CEX_UNATTENDED_TYPES
    assert CEX_UNATTENDED_TYPES == {
        "position_executor",
        "grid_executor",
        "dca_executor",
        "twap_executor",
        "arbitrage_executor",
        "xemm_executor",
        "order_executor",
    }


@pytest.mark.asyncio
async def test_query_returns_cex_cleanup_only(db):
    db.insert([
        {"executor_id": "grid", "executor_type": "grid_executor"},
        {"executor_id": "dca", "executor_type": "dca_executor"},
        {"executor_id": "order", "executor_type": "order_executor"},
        {"executor_id": "lp", "executor_type": "lp_executor"},
        {"executor_id": "done", "executor_type": "position_executor", "close_type": "TAKE_PROFIT"},
        {
            "executor_id": "seen",
            "executor_type": "twap_executor",
            "final_state": '{"orphan_resolved": true}',
        },
    ])

    async with db.session_context() as session:
        records = await ExecutorRepository(session).get_executors_by_close_types(
            ["SYSTEM_CLEANUP"],
            executor_types=sorted(CEX_UNATTENDED_TYPES),
        )
        views = [view for view in (unattended_view(record) for record in records) if view]
    assert {view["executor_id"] for view in views} == {"grid", "dca", "order"}
    assert all(view["warning"] == UNATTENDED_WARNING for view in views)
    assert all(view["close_type"] == "SYSTEM_CLEANUP" for view in views)


@pytest.mark.asyncio
async def test_empty_type_list_matches_nothing(db):
    """An empty executor_types filter must not fall through to every row."""
    db.insert([{"executor_id": "grid", "executor_type": "grid_executor"}])

    async with db.session_context() as session:
        records = await ExecutorRepository(session).get_executors_by_close_types(
            ["SYSTEM_CLEANUP"],
            executor_types=[],
        )

    assert records == []
