"""
ExecutorService manages executor lifecycle and orchestration.
This service enables running Hummingbot executors directly via API
without Docker containers or full strategy setup.
"""
import asyncio
import contextvars

_placing_order_id = contextvars.ContextVar("placing_order_id", default=None)
# Доставка отмены этим номером: блокировки её не прячут (план stop-freeze п.2).
_cancel_delivery_id = contextvars.ContextVar("cancel_delivery_id", default=None)


def _delivery_owns(text: str) -> bool:
    current = _cancel_delivery_id.get()
    return current not in (None, "") and str(text) == str(current)
_own_fresh_send = contextvars.ContextVar("own_fresh_send", default="")
# Уровень, который живой шаг уже держит, пока идёт его штатная постановка.
_placing_level = contextvars.ContextVar("placing_level", default=None)
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, Tuple, Type

from fastapi import HTTPException
from hummingbot.strategy_v2.executors.arbitrage_executor.arbitrage_executor import ArbitrageExecutor
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase
from hummingbot.strategy_v2.executors.dca_executor.data_types import DCAExecutorConfig
from hummingbot.strategy_v2.executors.dca_executor.dca_executor import DCAExecutor
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.grid_executor.data_types import GridExecutorConfig
from hummingbot.strategy_v2.executors.grid_executor.grid_executor import GridExecutor
from hummingbot.strategy_v2.executors.lp_executor.data_types import LPExecutorConfig
from hummingbot.strategy_v2.executors.lp_executor.lp_executor import LPExecutor
from hummingbot.strategy_v2.executors.order_executor.data_types import OrderExecutorConfig
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.executors.twap_executor.data_types import TWAPExecutorConfig
from hummingbot.strategy_v2.executors.twap_executor.twap_executor import TWAPExecutor
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.executors.xemm_executor.xemm_executor import XEMMExecutor
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder
from sqlalchemy.exc import IntegrityError

from database import (
    AsyncDatabaseManager,
    ExecutorPerformanceRepository,
    ExecutorRepository,
    GatewayCLMMRepository,
    GatewaySwapRepository,
)
from models.executors import PositionHold
from services.gateway_client import get_native_gas_token
from services.trading_service import AccountTradingInterface, TradingService
from utils.executor_checkpoint import (
    accounted_orders,
    COMPLETED_CYCLE_KEY,
    completed_cycle_levels,
    resume_complete_mutate,
    resume_complete_visible,
    apply_checkpoint,
    can_resume,
    capture_executor,
    grid_resume_plan,
    has_exposure,
    live_order_view,
    merge_recent_ids,
    note_filled,
    note_gone,
    note_placed,
    parse_checkpoint,
    recent_order_ids,
    seed_accounted_orders,
    blank_grid_cap,
    conservative_grid_cap,
    install_grid_resume_cap,
    exchange_open_request,
    exchange_pass_key,
    foreign_order_ids,
    new_exchange_slot,
    open_list_venue,
    hold_new_opens,
    install_occupied_open_brake,
    merge_exchange_open_pages,
    parse_exchange_open_page,
    remember_order_now,
    remember_open_before_send,
    claim_quant,
    release_quant,
    prune_order_ledger,
    ledger_order_ids,
    ledger_executor_ids,
    recorded_price_blocks,
    send_blocked,
    finished_list_request,
    okx_closed_request,
    OKX_ARCHIVE_MS,
    OKX_HISTORY_MS,
    parse_okx_closed_page,
    gate_delete_request,
    must_keep_executor,
    pick_owner,
    saved_order_ids,
    checkpoint_plus,
    checkpoint_keeping_holds,
    owed_records,
    buy_blocked,
    recover_folder_debt,
    four_proofs,
    history_covers_ts,
    gate_base_is_zero,
    raw_list_page,
    CLOSE_HOLD_KEY,
    CLOSE_BAN_KEY,
    install_close_without_level,
    append_level_place_line,
    seat_recorded_level_opens,
    stop_mark_visible,
    STOP_MARK_KEY,
    read_merged_ledger,
    strip_close_holds_from_saved,
    is_close_ledger_row,
    checkpoint_master,
    checkpoint_cache_prune,
    checkpoint_cache_reset_stats,
    checkpoint_cache_stats,
    checkpoint_view,
    ledger_close_price_ids,
    ledger_journal_mark,
    ledger_min_ts,
    ledger_price_known,
    ledger_raw_rows,
    ledger_unowned_ids,
    note_checkpoint_copy,
    parse_checkpoint_cached,
    append_durable_row,
    record_returned_close,
    append_published_close,
    published_close_rows,
    seat_live_returned_close,
    seat_role_close_before_next,
    recent_ids_kept_as_open,
    step_close_witness,
    okx_blank_px_fills,
    recorded_close_holds_retry,
    resolve_close_bans,
    restore_close_memory,
    note_owed,
    note_partial_absent,
    partial_column_prices,
    partial_column_hold_levels,
    partial_decimal,
    partial_commit_body,
    partial_close_send_line,
    partial_end_visible,
    partial_exposure,
    partial_feed_prices,
    partial_fill_base,
    partial_find_text,
    partial_full_fill,
    partial_has_five,
    partial_held_level_ids,
    partial_hold_kinds,
    partial_holding_ids,
    partial_level_released,
    partial_lines,
    partial_lone_five,
    partial_lists_cover,
    partial_old_remember,
    partial_order_body,
    partial_raw_base,
    partial_base_zero,
    partial_released_ids,
    partial_price_released_ids,
    partial_seating_visible,
    partial_send_marked,
    partial_stop_forbidden,
    partial_take_profit_quant,
    partial_text_in,
    _append_ledger_line,
    _read_ledger,
    _same_tick,
    LEDGER_LOCK,
    add_miss_order,
    add_sent_order,
    ledger_cache_stats,
    order_is_unsent,
    order_miss_unreadable,
    order_send_unknown,
    read_ledger_filtered,
    remove_miss_order,
    remove_sent_order,
)
from utils.executor_log_capture import ExecutorLogCapture, current_executor_id
from utils.unattended_cex import CEX_UNATTENDED_TYPES, UNATTENDED_WARNING, unattended_view
from utils.trading_pair import InvalidTradingPair, split_trading_pair

logger = logging.getLogger(__name__)
_OKX_JOURNAL_UNSET = object()

# Доводка заморозки: пауза между досылками отмен (план stop-freeze п.3).
STOP_FINISH_RETRY_SECONDS = 30.0
_OKX_TERMINAL_STATES = {"filled", "canceled", "mmp_canceled"}


def _parsed_hold_matches(parsed: Optional[Dict[str, Any]], order_id: str, price: Any) -> bool:
    """Exact hold or saved order. A substring of the raw column is not a hold."""
    if not parsed or not order_id:
        return False
    for hold in parsed.get("holds") or []:
        if str(hold.get("order_id") or "") != str(order_id):
            continue
        if price in (None, ""):
            return True
        return str(hold.get("price")) == str(price)
    if str(order_id) not in {str(item) for item in saved_order_ids(parsed)}:
        return False
    if price in (None, ""):
        return True
    for item in (parsed.get("retained_orders") or []):
        if str(item.get("order_id") or "") == str(order_id):
            return str(item.get("price")) == str(price)
    return False


def _json_default(obj):
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, Enum):
        return obj.name
    if isinstance(obj, TrackedOrder):
        return {
            "order_id": obj.order_id,
            "price": float(obj.price) if obj.price else None,
            "executed_amount_base": float(obj.executed_amount_base) if obj.executed_amount_base else 0.0,
            "executed_amount_quote": float(obj.executed_amount_quote) if obj.executed_amount_quote else 0.0,
            "is_filled": obj.is_filled if hasattr(obj, 'is_filled') else False,
            "is_open": obj.is_open if hasattr(obj, 'is_open') else False,
        }
    # Handle Pydantic models
    if hasattr(obj, 'model_dump'):
        return obj.model_dump(mode='json')
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _coerce_json_compatible(obj):
    """Recursively coerce a value into JSON-compatible primitives.

    Mirrors the result of ``json.loads(json.dumps(obj, default=_json_default))``
    without the string round-trip: containers are walked recursively and any
    object handled by ``_json_default`` is coerced to the same output type.
    """
    # JSON-native primitives are returned as-is.
    if obj is None or isinstance(obj, (str, bool, int, float)):
        return obj
    if isinstance(obj, dict):
        # json.dumps coerces non-string scalar keys (int/float/bool/None) to
        # strings; replicate that so the output shape is identical.
        coerced = {}
        for key, value in obj.items():
            if isinstance(key, str):
                str_key = key
            elif isinstance(key, bool):
                str_key = "true" if key else "false"
            elif key is None:
                str_key = "null"
            elif isinstance(key, (int, float)):
                str_key = json.dumps(key)
            else:
                raise TypeError(
                    f"keys must be str, int, float, bool or None, not {type(key).__name__}"
                )
            coerced[str_key] = _coerce_json_compatible(value)
        return coerced
    if isinstance(obj, (list, tuple)):
        # json.dumps serializes tuples as JSON arrays (-> lists on decode).
        return [_coerce_json_compatible(item) for item in obj]
    # Non-native types: route through the same coercion as the JSON encoder,
    # then recurse into the (possibly nested) replacement value.
    return _coerce_json_compatible(_json_default(obj))


class ExecutorService:
    """
    Service for managing trading executors without Docker containers.

    This service provides:
    - Dynamic executor creation for any market/connector
    - Executor lifecycle management (start, stop, cleanup)
    - Real-time executor status monitoring
    - Database persistence of executor state and history
    """

    # How long to wait before looking again for a position row that was not there. The
    # poller creates it on discovery, which is far slower than the control loop's tick.
    LP_RENT_RETRY_SECONDS = 30.0

    # How often the retention sweep runs, once it is enabled at all. Deleting rows older
    # than N days does not get more correct for being done every minute, and the sweep is
    # a DELETE over two tables sharing the control loop's tick.
    PERFORMANCE_PRUNE_INTERVAL_SECONDS = 3600.0
    OPEN_LIST_RETRY_SECONDS = 30.0
    OPEN_LIST_REQUEST_TIMEOUT = 30.0

    # Mapping of executor type strings to (executor_class, config_class)
    EXECUTOR_REGISTRY: Dict[str, tuple[Type[ExecutorBase], Type[ExecutorConfigBase]]] = {
        "position_executor": (PositionExecutor, PositionExecutorConfig),
        "grid_executor": (GridExecutor, GridExecutorConfig),
        "dca_executor": (DCAExecutor, DCAExecutorConfig),
        "arbitrage_executor": (ArbitrageExecutor, ArbitrageExecutorConfig),
        "twap_executor": (TWAPExecutor, TWAPExecutorConfig),
        "xemm_executor": (XEMMExecutor, XEMMExecutorConfig),
        "order_executor": (OrderExecutor, OrderExecutorConfig),
        "lp_executor": (LPExecutor, LPExecutorConfig),
    }

    def __init__(
        self,
        trading_service: TradingService,
        db_manager: AsyncDatabaseManager,
        default_account: str = "master_account",
        update_interval: float = 1.0,
        max_retries: int = 10,
        performance_snapshot_interval: float = 60.0,
        performance_retention_days: int = 0
    ):
        """
        Initialize ExecutorService.

        Args:
            trading_service: TradingService for trading operations and interfaces
            db_manager: AsyncDatabaseManager for persistence
            default_account: Default account to use
            update_interval: Executor update interval in seconds
            max_retries: Maximum retries for executor operations
            performance_snapshot_interval: How often a live executor's performance is
                written to executor_performance_snapshots, in seconds
            performance_retention_days: Delete performance snapshots older than this many
                days; 0 (the default) keeps everything
        """
        self._trading_service = trading_service
        self.db_manager = db_manager
        self.default_account = default_account
        self.update_interval = update_interval
        self.max_retries = max_retries
        self.performance_snapshot_interval = performance_snapshot_interval
        self.performance_retention_days = performance_retention_days

        # Trading interfaces per account (lazy initialized via TradingService)
        self._trading_interfaces: Dict[str, AccountTradingInterface] = {}

        # Active executors: executor_id -> executor instance
        self._active_executors: Dict[str, ExecutorBase] = {}

        # Executor metadata: executor_id -> metadata dict
        self._executor_metadata: Dict[str, Dict[str, Any]] = {}

        # Position holds: key = "account_name|connector_name|trading_pair"
        # Tracks aggregated positions from executors stopped with keep_position=True
        self._positions_held: Dict[str, PositionHold] = {}

        # Executor log capture
        self._log_capture = ExecutorLogCapture()
        self._log_capture.install()

        # An LP executor's position address, learned while the executor is live. A
        # successful close clears it from custom_info before the executor terminates, so
        # by completion — which is when the rent refund is finally known — there is
        # nothing left to file it under. See _record_lp_position_rent.
        self._lp_position_addresses: Dict[str, str] = {}
        # Executors whose locked rent is already stored, so the control loop stops
        # re-reading and re-writing a figure that cannot change.
        self._lp_rent_recorded: set = set()
        # Earliest monotonic time to retry an executor whose position row did not exist
        # yet. Discovery runs on its own schedule, so retrying at the control loop's 1 Hz
        # would be a query a second against a row that appears about once a minute.
        self._lp_rent_retry_after: Dict[str, float] = {}

        # Performance snapshot cadence, on the control loop's own clock (monotonic, so a
        # system clock step cannot stall the series or flood it).
        self._last_snapshot_at: float = 0.0
        self._last_prune_at: float = 0.0

        # Control loop task
        self._control_loop_task: Optional[asyncio.Task] = None
        self._is_running = False
        self._checkpoint_written: Dict[str, str] = {}

    def start(self):
        """Start the executor service control loop."""
        if not self._is_running:
            self._is_running = True
            self._control_loop_task = asyncio.create_task(self._control_loop())
            logger.info("ExecutorService started")

    async def recover_positions_from_db(self):
        """
        Recover position holds from the dedicated position_holds table on startup.
        """
        if not self.db_manager:
            return

        try:
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)

                records = await repo.get_active_position_holds()

                for record in records:
                    controller_id = record.controller_id or "main"
                    position_key = self._get_position_key(
                        record.account_name,
                        record.connector_name,
                        record.trading_pair,
                        controller_id
                    )

                    executor_ids = []
                    if record.executor_ids:
                        try:
                            executor_ids = json.loads(record.executor_ids)
                        except (json.JSONDecodeError, TypeError):
                            pass

                    position = PositionHold(
                        trading_pair=record.trading_pair,
                        connector_name=record.connector_name,
                        account_name=record.account_name,
                        controller_id=controller_id,
                        buy_amount_base=Decimal(str(record.buy_amount_base or 0)),
                        buy_amount_quote=Decimal(str(record.buy_amount_quote or 0)),
                        sell_amount_base=Decimal(str(record.sell_amount_base or 0)),
                        sell_amount_quote=Decimal(str(record.sell_amount_quote or 0)),
                        realized_pnl_quote=Decimal(str(record.realized_pnl_quote or 0)),
                        cum_fees_quote=Decimal(str(record.cum_fees_quote or 0)),
                        executor_ids=executor_ids,
                        last_updated=record.last_updated,
                    )
                    # Settle any matched volume from legacy unsettled data
                    position._calculate_realized_pnl()
                    self._positions_held[position_key] = position

                if self._positions_held:
                    logger.info(f"Recovered {len(self._positions_held)} position holds from database")

        except Exception as e:
            logger.error(f"Error recovering positions from database: {e}", exc_info=True)

    async def cleanup_orphaned_executors(self):
        """
        Clean up orphaned executors from database on startup.

        Identifies executors marked as RUNNING in the database but not present
        in memory (i.e., from previous API sessions that were terminated).
        """
        if not self.db_manager:
            logger.debug("No database manager available, skipping orphaned executor cleanup")
            return
        if ledger_executor_ids() is None or buy_blocked():
            logger.warning("order ledger was not read; not marking SYSTEM_CLEANUP")
            await self._warn_unattended_cex()
            return
        fresh = await self._fresh_pair_reads()
        if fresh is None:
            logger.warning("open and finished lists were not read; not marking SYSTEM_CLEANUP")
            await self._warn_unattended_cex()
            return

        try:
            # Get list of currently active executor IDs in memory
            active_executor_ids = list(self._active_executors.keys())
            active_executor_ids.extend(self._executors_with_unproven_orders())
            active_executor_ids.extend(self.__dict__.get("_kept_ids", set()))
            rows = await self._running_rows()
            if rows is None:
                await self._warn_unattended_cex()
                return
            for row in rows:
                key = (row.get("account_name"), row.get("connector_name"), row.get("trading_pair"))
                slot = fresh.get(key) or {}
                if not slot.get("opens_read") or not slot.get("finished_read"):
                    active_executor_ids.append(row["executor_id"])
                    continue
                if not self._row_proofs_released(row, slot) or self._partial_must_keep(row):
                    active_executor_ids.append(row["executor_id"])

            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)

                # Clean up orphaned executors
                cleaned_count = await repo.cleanup_orphaned_executors(
                    active_executor_ids=active_executor_ids,
                    close_type="SYSTEM_CLEANUP"
                )

                if cleaned_count > 0:
                    logger.info(f"Cleaned up {cleaned_count} orphaned executors from database")
                else:
                    logger.debug("No orphaned executors found in database")

            # Outside the session context: the marks above are committed now, and
            # the listing reads through its own fresh session.
            await self._warn_unattended_cex()

        except Exception as e:
            logger.error(f"Error cleaning up orphaned executors: {e}", exc_info=True)
            # The session above rolled back on the way here; rows marked dead by
            # earlier restarts are still listed.
            await self._warn_unattended_cex()

    async def get_unattended_cex(self) -> List[Dict[str, Any]]:
        """CEX executors marked SYSTEM_CLEANUP that may still have exchange state.

        Read-only. Does not cancel orders, flatten positions, or resume executors.
        ``lp_executor`` is not included; use get_orphaned_positions() for those.
        The repository takes one executor type per call, so the service walks the
        CEX types and merges by executor_id. Views are built inside the session:
        reading the ORM row after close raises DetachedInstanceError and the
        startup warning would vanish on the exact restart this listing is for.
        """
        if not self.db_manager:
            raise RuntimeError("Unattended listing unavailable: no database configured")

        merged: Dict[str, Dict[str, Any]] = {}
        async with self.db_manager.get_session_context() as session:
            repo = ExecutorRepository(session)
            for executor_type in sorted(CEX_UNATTENDED_TYPES):
                records = await repo.get_executors_by_close_types(
                    ["SYSTEM_CLEANUP"],
                    executor_type=executor_type,
                )
                for record in records:
                    view = unattended_view(record)
                    if view:
                        merged.setdefault(str(view["executor_id"]), view)
        return list(merged.values())

    async def _warn_unattended_cex(self) -> None:
        """Log the restart leftover. Must not raise into startup cleanup."""
        try:
            unattended = await self.get_unattended_cex()
        except Exception as e:
            logger.error(f"Error listing unattended CEX executors: {e}", exc_info=True)
            return
        if not unattended:
            return
        names = ", ".join(
            f"{row['executor_id']} ({row['executor_type']} {row['connector_name']} {row['trading_pair']})"
            for row in unattended
        )
        logger.warning(
            f"Unattended CEX executors after restart: {len(unattended)}. "
            f"{UNATTENDED_WARNING} {names}"
        )

    async def stop(self):
        """Stop the executor service and all active executors."""
        self._is_running = False

        if self._control_loop_task:
            self._control_loop_task.cancel()
            try:
                await self._control_loop_task
            except asyncio.CancelledError:
                pass
            self._control_loop_task = None

        try:
            await self._checkpoint_active_executors()
        except Exception as e:
            logger.error(f"Error checkpointing executors on shutdown: {e}", exc_info=True)

        # A cancel that is not proven is not a stop. Do not drop the row or call stock stop.
        for executor_id in list(self._active_executors.keys()):
            if not self._executor_proofs_released(executor_id):
                continue
            try:
                executor = self._active_executors.get(executor_id)
                if executor:
                    executor.stop()
            except Exception as e:
                logger.error(f"Error stopping executor {executor_id}: {e}")
            self._active_executors.pop(executor_id, None)
            self._executor_metadata.pop(executor_id, None)

        if self._active_executors:
            logger.warning("order cancel is not proven; not clearing executors or trading interfaces")
            return

        self._executor_metadata.clear()

        # Cleanup trading interfaces
        for trading_interface in self._trading_interfaces.values():
            await trading_interface.cleanup()
        self._trading_interfaces.clear()

        logger.info("ExecutorService stopped")

    async def _control_loop(self):
        """Main control loop that updates all active executors."""
        while self._is_running:
            step_started = time.monotonic()
            step_stats = checkpoint_cache_stats()
            step_ledger = ledger_cache_stats()
            try:
                # Update timestamps for all trading interfaces via TradingService
                self._trading_service.update_all_timestamps()
                await self._sweep_unproven_orders()

                # Check for completed executors. Iterate a snapshot: the rent recording
                # below awaits, and create_executor runs in a request task that can add to
                # or remove from _active_executors while this loop is suspended.
                completed_ids = []
                for executor_id, executor in list(self._active_executors.items()):
                    if getattr(executor, "is_closed", False):
                        if executor_id in (self.__dict__.get("_flat_stopped") or set()):
                            if await self._flat_checkpoint_number_live(executor_id, executor):
                                await self._restore_running_after_flat(executor_id)
                                self._reopen_closed_executor(executor, executor_id)
                                continue
                        if await self._flat_stop_needs_owner(executor_id, executor):
                            await self._restore_running_after_flat(executor_id)
                            self._reopen_closed_executor(executor, executor_id)
                            continue
                        completed_ids.append(executor_id)
                    elif executor_id in (self.__dict__.get("_flat_stopped") or set()):
                        await self._restart_flat_owner_if_needed(executor_id, executor)
                    elif (
                        self._executor_metadata.get(executor_id, {}).get("executor_type") == "lp_executor"
                        and executor_id not in self._lp_rent_recorded
                        and time.monotonic() >= self._lp_rent_retry_after.get(executor_id, 0.0)
                    ):
                        # Locked rent, stored while the position is still held rather than
                        # at completion — a position open for days should be answerable
                        # for the whole time it is open. Drops out of this branch as soon
                        # as it lands, so it is one read per executor, not one per tick.
                        await self._record_lp_position_rent(executor_id, executor)

                # Handle completed executors
                for executor_id in completed_ids:
                    await self._handle_executor_completion(executor_id)

                # Performance snapshots, after completion handling so a just-closed
                # executor is already out of _active_executors and gets only its terminal
                # row, not a duplicate periodic one. This is not a separate task on
                # purpose: the loop already ticks at 1 Hz and already awaits the database
                # inside the tick (see _record_lp_position_rent), so the cadence is one
                # guard rather than another thing to schedule and shut down.
                now = time.monotonic()
                await self._retry_unread_open_lists(now)
                await self._checkpoint_active_executors()
                if now - self._last_snapshot_at >= self.performance_snapshot_interval:
                    self._last_snapshot_at = now
                    await self._dump_executor_performance()
                    if (
                        self.performance_retention_days > 0
                        and now - self._last_prune_at >= self.PERFORMANCE_PRUNE_INTERVAL_SECONDS
                    ):
                        self._last_prune_at = now
                        await self._prune_performance_snapshots()

            except Exception as e:
                logger.error(f"Error in executor control loop: {e}", exc_info=True)

            # Свидетель медленного шага: в журнал процесса, не в журнал номеров,
            # и без номеров и цен — эти поля в журнале номеров значат владение.
            step_took = time.monotonic() - step_started
            if step_took > 5.0:
                stats = checkpoint_cache_stats()
                ledger = ledger_cache_stats()
                logger.warning(
                    "slow control step: %.1fs hold_rows=%d parses=%d copies=%d ledger_reads=%d ledger_lookups=%d",
                    step_took,
                    len(getattr(self, "_hold_rows", None) or []),
                    stats.get("parses", 0) - step_stats.get("parses", 0),
                    stats.get("copies", 0) - step_stats.get("copies", 0),
                    ledger.get("reads", 0) - step_ledger.get("reads", 0),
                    ledger.get("lookups", 0) - step_ledger.get("lookups", 0),
                )

            await asyncio.sleep(self.update_interval)

    @staticmethod
    def _measured_rent(custom_info: Dict[str, Any], key: str) -> Optional[Decimal]:
        """A rent figure the executor actually observed, or None.

        The LP executor reports these as plain floats and defaults them to 0.0, so zero
        means "never measured" far more often than it means "measured and empty": a
        position still open has no refund yet, and an EVM CLMM has no rent at all. Rent
        that was genuinely locked is never zero. Storing the 0.0 would put a figure in
        the table that reads as an observation and is not one — the mistake GW-18 made,
        where a hardcoded 0 refund was indistinguishable from a position that earned
        nothing.
        """
        value = custom_info.get(key)
        if value is None:
            return None
        try:
            amount = Decimal(str(value))
        except (ArithmeticError, ValueError):
            return None
        return amount if amount > 0 else None

    async def _record_lp_position_rent(
        self, executor_id: str, executor: ExecutorBase, *, final: bool = False
    ) -> None:
        """Carry an LP executor's rent figures into the CLMM position table.

        ``final`` marks the call made from executor completion, which is the last one
        this executor will ever get: its retry state is torn down immediately after,
        so there is no later tick to hand the work to.

        `gateway_clmm_positions.position_rent` is written by hummingbot-api's OPEN route
        and `position_rent_refunded` by its CLOSE route. An executor holds its position
        through the wheel, talking to Gateway directly, so neither route runs: the poller
        discovers the position and files it with both columns NULL, and the close leaves
        no refund behind either. That is the recommended way to hold a CLMM position and
        the one path whose rent went unrecorded — ~0.0100572 SOL on Orca, more than the
        liquidity in a small position.

        The executor knows both figures; nothing was asking it. Locked rent is written as
        soon as the position row exists, rather than at completion, so a position held for
        days is answerable while it is held. The row may not exist yet on an early tick —
        discovery runs on its own schedule — in which case this is a no-op and the next
        tick tries again.
        """
        try:
            custom_info = executor.get_custom_info()
        except Exception as e:
            logger.debug(f"Could not read custom_info for {executor_id} while recording rent: {e}")
            return

        position_address = custom_info.get("position_address")
        if position_address:
            self._lp_position_addresses[executor_id] = position_address
        else:
            # Cleared by a successful close. The refund is only known now, so fall back to
            # the address this executor was holding.
            position_address = self._lp_position_addresses.get(executor_id)
        if not position_address:
            return

        position_rent = self._measured_rent(custom_info, "position_rent")
        position_rent_refunded = self._measured_rent(custom_info, "position_rent_refunded")
        if position_rent is None and position_rent_refunded is None:
            return

        try:
            async with self.db_manager.get_session_context() as session:
                position = await GatewayCLMMRepository(session).record_position_rent(
                    position_address,
                    position_rent=position_rent,
                    position_rent_refunded=position_rent_refunded,
                )
        except Exception as e:
            logger.error(f"Error recording rent for LP position {position_address}: {e}", exc_info=True)
            return

        if position is None:
            if not final:
                # The poller has simply not discovered the position yet; back off and let a
                # later tick try again. Not worth a warning while the executor still runs.
                self._lp_rent_retry_after[executor_id] = time.monotonic() + self.LP_RENT_RETRY_SECONDS
                return
            # Last call: scheduling a retry here would be theatre, since the caller clears
            # this executor's retry state on the next line. Say plainly what was lost and
            # what it was worth, so the figures can be recovered from the log.
            logger.error(
                f"LP executor {executor_id} finished holding position {position_address} "
                f"(rent={position_rent}, refund={position_rent_refunded}), but no row exists "
                "for it: the position was opened and closed inside a single discovery "
                "sweep, so it was never filed and these figures are not recorded anywhere "
                "but this log line."
            )
            return

        if position_rent is not None:
            self._lp_rent_recorded.add(executor_id)
        logger.debug(
            f"Recorded rent for LP position {position_address}: locked={position_rent}, "
            f"refunded={position_rent_refunded}"
        )

    async def _record_executor_swap(self, executor_id: str, executor: ExecutorBase) -> None:
        """Record a swap an executor made, so the swap history covers the recommended path.

        `gateway_swaps` is written by hummingbot-api's own /gateway/swap/* routes. An
        executor holds its connector through the wheel and talks to Gateway directly, so
        hummingbot-api never sees the call and had nothing to record. The table was not
        wrong, it was silently partial: POST /gateway/swaps/search and the swap summary
        described only swaps made by hand, and a caller reading "9 SOL-USDC swaps" had no
        way to learn that the most recent ones were missing.

        Same shape as _record_lp_position_rent: the executor knows what it did, so ask it
        at completion rather than waiting for a route that will never be called. Keyed on
        the transaction hash, which is what GW-43 added to custom_info — `order_id` is
        internal (buy-SOL-USDC-1787271213996599) and appears nowhere on chain, so before
        that there was nothing to key a row on.

        Skipped silently when there is no hash: a Gateway swap that never reached the
        chain has nothing to record, and an executor on a CEX is not a Gateway swap at all.
        """
        try:
            custom_info = executor.get_custom_info()
        except Exception as e:
            logger.debug(f"Could not read custom_info for {executor_id} while recording its swap: {e}")
            return

        transaction_hash = custom_info.get("transaction_hash")
        swap_provider = custom_info.get("swap_provider")
        if not transaction_hash or not swap_provider:
            return

        metadata = self._executor_metadata.get(executor_id, {})
        network = metadata.get("connector_name") or ""
        trading_pair = metadata.get("trading_pair") or ""
        if not network or "-" not in trading_pair:
            logger.warning(
                f"Executor {executor_id} swapped in {transaction_hash} but reports "
                f"network={network!r} pair={trading_pair!r}; not recorded."
            )
            return

        base_token, quote_token = split_trading_pair(trading_pair)
        side = str(custom_info.get("side") or "").upper()
        side = "BUY" if "BUY" in side else "SELL"

        amount_base = Decimal(str(custom_info.get("executed_amount_base") or 0))
        price = Decimal(str(custom_info.get("average_executed_price") or 0))
        if amount_base <= 0 or price <= 0:
            logger.warning(
                f"Executor {executor_id} swapped in {transaction_hash} but reports no "
                f"realized amounts (base={amount_base}, price={price}); not recorded."
            )
            return
        amount_quote = amount_base * price

        # A BUY spends quote to receive base; a SELL is the mirror image.
        input_amount, output_amount = (
            (amount_quote, amount_base) if side == "BUY" else (amount_base, amount_quote)
        )

        chain = network.split("-", 1)[0]
        # The provider travels as "jupiter/router"; the table stores the bare DEX name,
        # which is what the /gateway/swap routes write.
        connector = swap_provider.split("/", 1)[0]

        try:
            async with self.db_manager.get_session_context() as session:
                repo = GatewaySwapRepository(session)
                if await repo.get_swap_by_tx_hash(transaction_hash):
                    return
                await repo.create_swap({
                    "transaction_hash": transaction_hash,
                    "network": network,
                    "connector": connector,
                    "wallet_address": custom_info.get("wallet_address") or "",
                    "trading_pair": trading_pair,
                    "base_token": base_token,
                    "quote_token": quote_token,
                    "side": side,
                    "input_amount": input_amount,
                    "output_amount": output_amount,
                    "price": price,
                    # The LIVE tolerance the swap went out with, which is not
                    # config.slippage_pct when earlier attempts failed and widened it.
                    "slippage_pct": (Decimal(str(custom_info["slippage_pct"]))
                                     if custom_info.get("slippage_pct") is not None else None),
                    "gas_token": get_native_gas_token(chain),
                    "status": "CONFIRMED",
                })
            logger.info(
                f"Recorded executor swap {transaction_hash}: {side} {amount_base} "
                f"{base_token} @ {price} on {connector}/{network}"
            )
        except Exception as e:
            logger.error(f"Error recording executor swap {transaction_hash}: {e}", exc_info=True)

    # ========================================
    # Performance Snapshots
    # ========================================

    def _build_snapshot_row(
        self,
        executor_id: str,
        executor: ExecutorBase,
        *,
        is_terminal: bool,
        metrics: Optional[Dict[str, Any]] = None,
        status: Optional[str] = None,
        close_type: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """One executor_performance_snapshots row, from the two sources _format_executor_info reads.

        `metrics`, `status` and `close_type` let the terminal row reuse the figures
        _persist_executor_completed already computed rather than reading executor_info a
        second time -- the read that can raise and get silently substituted with zeros.

        Returns None when the metrics cannot be read at all: a row of fabricated zeros in
        the middle of a series is worse than a gap, because a reader cannot tell it from
        an executor that genuinely made nothing.
        """
        metadata = self._executor_metadata.get(executor_id, {})

        if metrics is None:
            try:
                info = executor.executor_info
                metrics = {
                    "net_pnl_quote": info.net_pnl_quote,
                    "net_pnl_pct": info.net_pnl_pct,
                    "cum_fees_quote": info.cum_fees_quote,
                    "filled_amount_quote": info.filled_amount_quote,
                }
            except Exception as e:
                logger.debug(f"Could not read executor_info for {executor_id} while snapshotting: {e}")
                return None

        if status is None:
            try:
                status = executor.status.name
            except Exception as e:
                logger.debug(f"Could not read status for {executor_id} while snapshotting: {e}")
                return None

        return {
            "executor_id": executor_id,
            "executor_type": metadata.get("executor_type") or "unknown",
            "account_name": metadata.get("account_name") or self.default_account,
            "connector_name": metadata.get("connector_name") or "",
            "trading_pair": metadata.get("trading_pair") or "",
            "controller_id": metadata.get("controller_id", "main"),
            "status": status,
            "close_type": close_type,
            "is_terminal": is_terminal,
            **metrics,
        }

    async def _dump_executor_performance(self):
        """Write one snapshot row per live executor.

        Only live executors are sampled because _active_executors is, by construction,
        the only place a running executor's performance exists -- its database row still
        carries the creation-time zeros until it completes. A closed executor is not
        missing from here: it got its terminal row at completion.

        Never lets a database failure out: this shares the control loop's tick, and a
        dropped snapshot must not stop executors from being updated.
        """
        if not self.db_manager or not self._active_executors:
            return

        snapshot_timestamp = datetime.now(timezone.utc)
        rows = []
        for executor_id, executor in list(self._active_executors.items()):
            row = self._build_snapshot_row(executor_id, executor, is_terminal=False)
            if row is not None:
                row["snapshot_timestamp"] = snapshot_timestamp
                rows.append(row)

        if not rows:
            return

        try:
            async with self.db_manager.get_session_context() as session:
                await ExecutorPerformanceRepository(session).save_snapshots(rows)
            logger.debug(f"Dumped {len(rows)} executor performance snapshots")
        except Exception as e:
            logger.error(f"Error saving executor performance snapshots: {e}", exc_info=True)

    async def _prune_performance_snapshots(self):
        """Delete snapshots older than the configured retention, from both snapshot tables.

        Gated on performance_retention_days > 0 by the caller: the default keeps
        everything, so an upgrade never starts deleting an operator's history.
        """
        if not self.db_manager or self.performance_retention_days <= 0:
            return

        cutoff = datetime.now(timezone.utc) - timedelta(days=self.performance_retention_days)
        try:
            async with self.db_manager.get_session_context() as session:
                executor_rows, controller_rows = await ExecutorPerformanceRepository(
                    session
                ).prune_older_than(cutoff)
            if executor_rows or controller_rows:
                logger.info(
                    f"Pruned performance snapshots older than {cutoff.isoformat()}: "
                    f"{executor_rows} executor, {controller_rows} controller"
                )
        except Exception as e:
            logger.error(f"Error pruning performance snapshots: {e}", exc_info=True)

    async def get_executor_performance_history(
        self,
        executor_id: Optional[str] = None,
        executor_type: Optional[str] = None,
        controller_id: Optional[str] = None,
        account_name: Optional[str] = None,
        connector_name: Optional[str] = None,
        trading_pair: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        interval: str = "5m"
    ):
        """Read an executor snapshot series. Mirrors BotsOrchestrator.get_controller_performance_history."""
        async with self.db_manager.get_session_context() as session:
            repo = ExecutorPerformanceRepository(
                session, grain_minutes=self.performance_snapshot_interval / 60.0
            )
            return await repo.get_performance_history(
                executor_id=executor_id,
                executor_type=executor_type,
                controller_id=controller_id,
                account_name=account_name,
                connector_name=connector_name,
                trading_pair=trading_pair,
                limit=limit,
                cursor=cursor,
                start_time=start_time,
                end_time=end_time,
                interval=interval,
            )

    async def get_latest_executor_performance(
        self,
        executor_id: Optional[str] = None,
        executor_type: Optional[str] = None,
        controller_id: Optional[str] = None,
        account_name: Optional[str] = None,
        connector_name: Optional[str] = None,
        trading_pair: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        """The last snapshot of each matching executor. Mirrors
        BotsOrchestrator.get_latest_controller_performance."""
        async with self.db_manager.get_session_context() as session:
            repo = ExecutorPerformanceRepository(
                session, grain_minutes=self.performance_snapshot_interval / 60.0
            )
            return await repo.get_latest(
                executor_id=executor_id,
                executor_type=executor_type,
                controller_id=controller_id,
                account_name=account_name,
                connector_name=connector_name,
                trading_pair=trading_pair,
                limit=limit,
            )

    def _get_trading_interface(self, account_name: str) -> AccountTradingInterface:
        """Get or create an AccountTradingInterface for the account."""
        if account_name not in self._trading_interfaces:
            self._trading_interfaces[account_name] = self._trading_service.get_trading_interface(account_name)
        return self._trading_interfaces[account_name]

    def _validate_executor_config(
        self,
        executor_config: Dict[str, Any],
        default_timestamp: Optional[float] = None
    ) -> tuple[Type[ExecutorBase], Type[ExecutorConfigBase], ExecutorConfigBase]:
        """
        Validate the executor type and build the typed executor config.

        Pure validation step: no IO, no executor started, no DB access.

        Args:
            executor_config: Executor configuration dictionary (must include 'type')
            default_timestamp: Timestamp to set on the config if not provided
                (required for time-based features like time_limit)

        Returns:
            Tuple of (executor_class, config_class, typed_config)

        Raises:
            HTTPException: 400 if the type is missing/invalid or the config is invalid
        """
        executor_type = executor_config.get("type")
        if not executor_type:
            raise HTTPException(
                status_code=400,
                detail="executor_config must include 'type' field"
            )

        if executor_type not in self.EXECUTOR_REGISTRY:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid executor type '{executor_type}'. Valid types: {list(self.EXECUTOR_REGISTRY.keys())}"
            )

        if "timestamp" not in executor_config or executor_config["timestamp"] is None:
            executor_config["timestamp"] = default_timestamp

        executor_class, config_class = self.EXECUTOR_REGISTRY[executor_type]
        try:
            typed_config = config_class(**executor_config)
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid executor config: {str(e)}"
            )

        return executor_class, config_class, typed_config

    async def _prepare_market(self, account: str, connector_name: Optional[str], trading_pair: Optional[str]):
        """Ensure the connector and market for the executor are ready on the account's trading interface."""
        trading_interface = self._get_trading_interface(account)
        if connector_name:
            if trading_pair:
                await trading_interface.add_market(connector_name, trading_pair)
            else:
                await trading_interface.ensure_connector(connector_name)

    def _instantiate_and_register(
        self,
        executor_class: Type[ExecutorBase],
        typed_config: ExecutorConfigBase,
        trading_interface: AccountTradingInterface,
        metadata: Dict[str, Any],
        start: bool = True,
    ) -> tuple[str, ExecutorBase]:
        """
        Instantiate the executor, register it in memory and start it.

        Args:
            executor_class: Executor class to instantiate
            typed_config: Validated typed executor config
            trading_interface: Trading interface acting as the executor's strategy
            metadata: Metadata dict to register for the executor

        Returns:
            Tuple of (executor_id, executor)

        Raises:
            HTTPException: 400 if the executor fails to instantiate
        """
        try:
            executor = executor_class(
                strategy=trading_interface,
                config=typed_config,
                update_interval=self.update_interval,
                max_retries=self.max_retries
            )
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to create executor: {str(e)}"
            )

        executor_id = typed_config.id
        self._active_executors[executor_id] = executor
        self._executor_metadata[executor_id] = metadata
        self._install_checkpoint_hook(executor_id, executor)
        if metadata.get("executor_type") == "grid_executor":
            install_close_without_level(
                executor, executor_id,
                frozen=lambda eid=str(executor_id): self._stop_mark_now(eid),
            )
            self._open_fill_down_close_door(executor_id, executor)
            self._install_close_record(executor_id, executor)
        if start:
            self._start_registered_executor(executor_id, executor)
        return executor_id, executor

    def _open_fill_down_close_door(self, executor_id: str, executor: Any) -> None:
        """The live brake stays shut. One filled-down buy may ask for its own sell."""
        if not getattr(executor, "_close_brake", False) or getattr(executor, "_fill_down_door", False):
            return
        brake_get = getattr(executor, "get_close_orders_to_create", None)
        brake_adjust = getattr(executor, "adjust_and_place_close_order", None)
        if brake_get is None or brake_adjust is None:
            return
        service = self

        def get_close(*args, **kwargs):

            if not service._stop_mark_now(executor_id):

                return brake_get(*args, **kwargs)

            found = list(brake_get(*args, **kwargs) or [])
            seen = {id(item) for item in found}
            pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
            connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
            connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
            fresh = service._tick_fresh(pair)
            for level in list(getattr(executor, "grid_levels", None) or []):
                if id(level) in seen or not service._filled_down_open(executor_id, executor, level):
                    continue
                level_id = str(getattr(level, "id", "") or "")
                close_id = service._recorded_close_id(executor_id, level_id)
                if close_id and service._close_text_seen(fresh, connector, close_id) is True:
                    continue
                found.append(level)
            return found

        def adjust(level, *args, **kwargs):
            # Исполненная покупка не переводится в нашу продажу. Их тейк не глушим.
            return brake_adjust(level, *args, **kwargs)

        executor.get_close_orders_to_create = get_close
        executor.adjust_and_place_close_order = adjust
        executor._fill_down_door = True

    def _install_close_record(self, executor_id: str, executor: Any) -> None:

        """Их продажа не уходит, пока строка с уровнем не сброшена. Свой номер не чеканим."""

        if getattr(executor, "_close_record_wrapped", False):

            return

        original = getattr(executor, "adjust_and_place_close_order", None)

        if original is None:

            return

        service = self



        def wrapped(level, *args, **kwargs):
            if service._stop_mark_now(executor_id):
                return original(level, *args, **kwargs)
            import hummingbot.connector.exchange_py_base as exchange_base
            from hummingbot.core.utils.async_utils import safe_ensure_future
            held = []
            real_ensure = exchange_base.safe_ensure_future

            def capture(coro, *coro_args, **coro_kwargs):
                held.append(coro)
                return None

            exchange_base.safe_ensure_future = capture
            try:
                result = original(level, *args, **kwargs)
            finally:
                exchange_base.safe_ensure_future = real_ensure
            order_id = str(getattr(getattr(level, "active_close_order", None), "order_id", "") or "")
            if not order_id:
                for coro in held:
                    coro.close()
                level.active_close_order = None
                return result
            kept = append_published_close(
                executor_id, order_id, str(getattr(level, "id", "") or ""), getattr(level, "price", None),
            )
            if not kept:
                for coro in held:
                    coro.close()
                rows = published_close_rows(executor_id)
                level_id = str(getattr(level, "id", "") or "")
                still = rows is None or any(
                    str(item.get("order_id") or "") == order_id and str(item.get("level_id") or "") == level_id
                    for item in rows
                )
                if not still:
                    level.active_close_order = None
                return result
            for coro in held:
                safe_ensure_future(coro)
            return result




        wrapped._close_record_wrapped = True

        executor.adjust_and_place_close_order = wrapped

        executor._close_record_wrapped = True



    def _restart_seat_due(self, executor_id: str, executor: Any) -> bool:
        return bool(getattr(executor, "_notebook_resumed", False)) and not self._stop_mark_now(executor_id)


    def _finish_restart_seat(self, executor_id: str, executor: Any, fresh: Optional[Dict[str, Any]]) -> bool:
        """Ложь — журнал не прочитан. Их шаг не звать и флаг подъёма не снимать."""
        if not self._seat_restart_closes(executor_id, executor, fresh):
            return False
        if self._lists_ready(fresh):
            executor._notebook_resumed = False
        return True

    def _seat_restart_closes(self, executor_id: str, executor: Any, fresh: Optional[Dict[str, Any]]) -> bool:

        """После восстановления уровней: поздняя строка закрытия в пустой слот. По цене не сажаем."""

        if self._stop_mark_now(executor_id):

            return True

        rows = published_close_rows(executor_id)
        if rows is None:
            return False

        by_level: Dict[str, list] = {}

        for row in rows:

            by_level.setdefault(str(row.get("level_id") or ""), []).append(row)

        opens_read = bool((fresh or {}).get("opens_read") and (fresh or {}).get("opens_done"))

        finished_read = bool((fresh or {}).get("finished_read") and (fresh or {}).get("finished_done"))

        half = not opens_read or not finished_read

        shown_lists = []

        if opens_read:

            shown_lists.extend((fresh or {}).get("opens") or [])

        if finished_read:

            shown_lists.extend((fresh or {}).get("finished") or [])

            shown_lists.extend((fresh or {}).get("finished_blank_px") or [])



        def shown(order_id: str):

            if half:

                return True

            return self._shown_fill_row(fresh, order_id, "SELL") is not None or any(

                self._row_order_id(item) == str(order_id) for item in shown_lists if isinstance(item, dict)

            )



        def live(order_id: str) -> bool:

            if not opens_read:

                return False

            return any(

                self._row_order_id(item) == str(order_id) for item in ((fresh or {}).get("opens") or []) if isinstance(item, dict)

            )



        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""

        side = getattr(getattr(getattr(executor, "config", None), "side", None), "name", "BUY")
        close_side = "SELL" if side == "BUY" else "BUY" if side == "SELL" else side

        leverage = getattr(getattr(executor, "config", None), "leverage", None) or 1

        from hummingbot.strategy_v2.models.executors import TrackedOrder

        for level in list(getattr(executor, "grid_levels", None) or []):

            level_id = str(getattr(level, "id", "") or "")

            group = sorted(by_level.get(level_id) or [], key=lambda item: float(item.get("ts") or 0))

            occupied = str(getattr(getattr(level, "active_close_order", None), "order_id", "") or "")
            if not group:
                continue


            later = group[-1]

            earlier = group[:-1]

            chosen = None

            filled = False

            if half:

                chosen = later

            elif live(str(later.get("order_id") or "")) or self._shown_fill_row(fresh, str(later.get("order_id") or ""), close_side) is not None:
                chosen = later
                filled = self._shown_fill_row(fresh, str(later.get("order_id") or ""), close_side) is not None

            else:

                for row in reversed(earlier):

                    order_id = str(row.get("order_id") or "")

                    if self._shown_fill_row(fresh, order_id, close_side) is not None:

                        chosen = row

                        filled = True

                        break

                    if live(order_id):

                        chosen = row

                        filled = False

                        break

            if chosen is None:
                if not half and occupied and self._shown_fill_row(fresh, occupied, close_side) is None and not live(occupied):
                    level.active_close_order = None
                continue


            order_id = str(chosen.get("order_id") or "")

            tracked = TrackedOrder(order_id=order_id)

            if filled:

                row = self._shown_fill_row(fresh, order_id, close_side)

                opened = self._stock_filled_order(

                    client_id=order_id,

                    trading_pair=pair,

                    trade_name="SELL" if side == "BUY" else "BUY",

                    position_name="CLOSE",

                    amount=self._row_fill_size(row) if row else None,

                    price=self._row_fill_price(row) if row else None,

                    leverage=int(leverage),

                )

                if opened is not None:

                    tracked.order = opened

            level.active_close_order = tracked

        return True
    def _start_registered_executor(self, executor_id: str, executor: ExecutorBase) -> None:
        token = current_executor_id.set(executor_id)
        try:
            executor.start()
        finally:
            current_executor_id.reset(token)

    def _install_accounted_listener(self, connector, account: str = "", connector_name: str = "") -> None:
        """Record fill and cancel for accounted ids the level slots do not hold.

        The instance wrappers of process_order_* never see these events: the
        base executor binds its forwarders in __init__ and register_events()
        plants them at start(). This listener hangs on the connector itself,
        the way services/orders_recorder.py does, and only writes the accounted
        rows. note_gone is not called. PubSub keeps listeners by weak reference,
        so the forwarders live on the service, or they die on return.
        """
        if connector is None or getattr(connector, "_accounted_listener_installed", False):
            return
        if not hasattr(connector, "add_listener"):
            return
        try:
            from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
            from hummingbot.core.event.events import MarketEvent
        except Exception:
            logger.warning("accounted order listener was not installed", exc_info=True)
            return
        fill_event = getattr(MarketEvent, "OrderFilled", None)
        cancel_event = getattr(MarketEvent, "OrderCancelled", None)
        if fill_event is None or cancel_event is None:
            return
        service = self
        routes = self.__dict__.setdefault("_accounted_routes", {})

        def _did_fill(event_tag: int, market: Any, event: Any) -> None:
            order_id = getattr(event, "order_id", None)
            executor = service._active_executors.get(routes.get(order_id) or "")
            if executor is None:
                return
            note_filled(executor, order_id, None)

        def _did_cancel(event_tag: int, market: Any, event: Any) -> None:
            order_id = getattr(event, "order_id", None)
            executor = service._active_executors.get(routes.get(order_id) or "")
            if executor is None:
                return
            from utils.executor_checkpoint import _order_still_open, order_ledger
            row = order_ledger(executor).get(order_id)
            if row is None or row.get("filled"):
                return
            if service._is_held_close(executor, order_id):
                return
            owner_id = next(
                (key for key, live in service._active_executors.items() if live is executor),
                "",
            )
            if service._partial_gone_blocked(owner_id, order_id):
                return
            if _order_still_open(executor, order_id):
                return
            row["gone"] = True

        fill_forwarder = SourceInfoEventForwarder(_did_fill)
        cancel_forwarder = SourceInfoEventForwarder(_did_cancel)
        # A strong reference on the service: add_listener alone keeps a weakref.
        forwarders = self.__dict__.setdefault("_accounted_forwarders", {})
        forwarders[(account or "", connector_name or "")] = (fill_forwarder, cancel_forwarder)
        try:
            connector.add_listener(fill_event, fill_forwarder)
            connector.add_listener(cancel_event, cancel_forwarder)
        except Exception:
            logger.warning("accounted order listener was not added", exc_info=True)
            return
        connector._accounted_listener_installed = True

    def _install_checkpoint_hook(self, executor_id: str, executor: ExecutorBase) -> None:
        """Write the id when the order is placed, and the fill when the fill event arrives."""
        if getattr(executor, "_checkpoint_hooked", False):
            return
        service = self
        original_place = executor.place_order

        def place_wrapped(*args, **kwargs):
            # Сторож отхода не отвергает штатную постановку и не требует номер
            # до возврата. Свой номер до вызова не чеканим.
            level = _placing_level.get()
            order_type = kwargs.get("order_type")
            side = kwargs.get("side")
            if (
                getattr(order_type, "name", str(order_type or "")) == "MARKET"
                and getattr(side, "name", str(side or "")) == "BUY"
            ):
                try:
                    from hummingbot.core.data_type.common import PriceType
                    ask = executor.get_price(
                        kwargs.get("connector_name"), kwargs.get("trading_pair"), PriceType.BestAsk,
                    )
                    price = kwargs.get("price")
                    if ask is not None and price is not None and price < ask:
                        kwargs["price"] = ask
                except Exception:
                    pass
            result = original_place(*args, **kwargs)
            order_id = result if isinstance(result, str) else None
            if level is not None:
                level_id = str(getattr(level, "id", "") or "")
                if order_id and level_id:
                    # Строка с id уровня до возврата. Голую строку не пишем:
                    # иначе старая посадка снимет колпак по цене.
                    append_level_place_line(
                        executor_id, order_id, level_id, kwargs.get("price"),
                    )
                service._schedule_checkpoint(executor_id)
                return result
            note_placed(executor, order_id)
            remember_order_now(executor_id, order_id)
            action = kwargs.get("position_action")
            is_close = getattr(action, "name", str(action or "")) == "CLOSE"
            if order_id and is_close:
                record_returned_close(executor_id, order_id)
                seat_live_returned_close(executor, order_id)
            service._schedule_checkpoint(executor_id)
            return result

        executor.place_order = place_wrapped
        self._install_open_shell(executor_id, executor)
        self._install_partial_guards(executor_id, executor)
        for name, marker in (
            ("process_order_filled_event", "filled"),
            ("process_order_completed_event", "filled"),
            ("process_order_canceled_event", "gone"),
            ("process_order_failed_event", "gone"),
        ):
            original = getattr(executor, name, None)
            if original is None:
                continue
            setattr(executor, name, self._event_wrapper(executor_id, executor, original, marker))
        executor._checkpoint_hooked = True

    def _ledger_ids_without_column(self):
        return ledger_order_ids()

    def _executors_with_unproven_orders(self) -> list:
        found = ledger_executor_ids()
        if found is None:
            return ["__ledger_unread__"]
        return found

    async def _finished_pass(self, connector, pair: str) -> dict:
        venue = open_list_venue(type(connector).__name__ if connector is not None else "")
        request = finished_list_request(venue or "", pair)
        if request is None or connector is None or not hasattr(connector, "_api_get"):
            return {"read": False, "done": False, "orders": []}
        try:
            body = await connector._api_get(
                path_url=request["path"],
                params=request["params"],
                is_auth_required=True,
            )
        except Exception:
            return {"read": False, "done": False, "orders": []}
        page = parse_exchange_open_page(body, venue, pair)
        return {
            "read": bool(page.get("read")),
            "done": bool(page.get("done")),
            "orders": page.get("orders") or [],
        }

    async def _gate_symbol(self, connector, pair: str):
        method = getattr(connector, "exchange_symbol_associated_to_pair", None)
        if method is None:
            return pair
        try:
            symbol = await method(pair)
        except Exception:
            return None
        return symbol or None

    def _unread_raw_lists(self) -> Dict[str, Any]:
        return {
            "opens_read": False,
            "opens_done": False,
            "finished_read": False,
            "finished_done": False,
            "covers_ts": False,
            "opens": [],
            "finished": [],
            "finished_blank_px": [],
        }

    async def _okx_closed_pages(
        self, connector, symbol: str, pair: str, inst_type: str, archive: bool,
        begin: Optional[str] = None, end: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        first = okx_closed_request(symbol, begin=begin, end=end, inst_type=inst_type, archive=archive)
        if first is None:
            return None
        if begin is None:
            begin = first["params"]["begin"]
        if end is None:
            end = first["params"]["end"]
        orders = []
        blank_fills = []
        after = None
        try:
            for _ in range(1000):
                request = okx_closed_request(
                    symbol, after=after, begin=begin, end=end, inst_type=inst_type, archive=archive,
                )
                if request is None:
                    return None
                body = await connector._api_get(
                    path_url=request["path"],
                    params=request["params"],
                    is_auth_required=True,
                    limit_id=self._open_limit_id(connector, "okx"),
                )
                parsed = parse_okx_closed_page(body, int(begin), int(end))
                if not parsed.get("read"):
                    return None
                orders.extend(parsed.get("orders") or [])
                blank_fills.extend(okx_blank_px_fills(body))
                if parsed.get("done"):
                    return {
                        "read": True,
                        "done": True,
                        "orders": orders,
                        "blank_px_fills": blank_fills,
                        "request": request,
                    }
                after = parsed.get("cursor")
                if not after:
                    return None
            self._log_silent_exit(
                "okx-closed-page-limit", pair or symbol,
                f"symbol={symbol} pages=1000 done=false",
            )
            return None
        except Exception as exc:
            self._log_silent_exit("okx-closed-exception", pair or symbol, f"symbol={symbol} error={exc!r}")
            return None

    async def _fresh_okx_closed(
        self, connector, symbol: str, pair: str = "", ts: Any = None,
    ) -> Optional[Dict[str, Any]]:
        """Perpetual and spot closed lists. A narrowed begin is not an empty book."""
        inst_type = "SWAP" if "perpetual" in type(connector).__name__.lower() else "SPOT"
        mark = None
        if ts not in (None, ""):
            try:
                mark = float(ts)
                if mark > 1_000_000_000_000:
                    mark = mark / 1000
            except (TypeError, ValueError):
                mark = None
        now_sec = time.time()
        if (
            mark is not None
            and now_sec - mark > OKX_HISTORY_MS / 1000
            and mark >= now_sec - OKX_ARCHIVE_MS / 1000
        ):
            begin = str(int((mark - 300) * 1000))
            end = str(int(now_sec * 1000))
            archived = await self._okx_closed_pages(
                connector, symbol, pair, inst_type, True, begin, end,
            )
            if archived is not None and archived.get("orders"):
                live = await self._okx_closed_pages(connector, symbol, pair, inst_type, False)
                if live is None:
                    return None
                return {
                    "read": True,
                    "done": True,
                    "orders": archived["orders"] + live["orders"],
                    "request": archived["request"],
                }
            return await self._okx_closed_pages(connector, symbol, pair, inst_type, False)
        return await self._okx_closed_pages(connector, symbol, pair, inst_type, False)

    async def _fresh_okx_opens(self, connector, pair: str, ts: Any = None) -> Dict[str, Any]:
        """Open OKX orders only. A failed page is not an empty book and not a closed list."""
        empty = self._unread_raw_lists()
        symbol = await self._gate_symbol(connector, pair)
        if not symbol:
            return empty
        opens = []
        after = None
        try:
            for _ in range(1000):
                request = exchange_open_request("okx", symbol, after=after)
                if request is None:
                    return empty
                body = await connector._api_get(
                    path_url=request["path"],
                    params=request["params"],
                    is_auth_required=True,
                    limit_id=self._open_limit_id(connector, "okx"),
                )
                parsed = parse_exchange_open_page(body, "okx", symbol)
                if not parsed.get("read"):
                    return empty
                for row in parsed.get("orders") or []:
                    if not isinstance(row, dict):
                        return empty
                    copied = dict(row)
                    order_id = copied.get("order_id")
                    if order_id and not copied.get("text"):
                        copied["text"] = str(order_id)
                    opens.append(copied)
                if parsed.get("done"):
                    closed = await self._fresh_okx_closed(connector, symbol, pair=pair, ts=ts)
                    finished = closed or {"read": False, "done": False, "orders": []}
                    return {
                        "opens_read": True,
                        "opens_done": True,
                        "finished_read": bool(finished.get("read")),
                        "finished_done": bool(finished.get("done")),
                        "covers_ts": False,
                        "opens": opens,
                        "finished": finished.get("orders") or [],
                        "finished_blank_px": finished.get("blank_px_fills") or [],
                        "symbol": symbol,
                        "finished_request": finished.get("request"),
                    }
                after = parsed.get("cursor")
                if not after:
                    return empty
            return empty
        except Exception:
            return empty

    async def _fresh_raw_lists(self, connector, pair: str, ts: Any = None) -> Dict[str, Any]:
        """This tick's lists. A cached slot with read=True is not a proof."""
        empty = self._unread_raw_lists()
        venue = open_list_venue(type(connector).__name__ if connector is not None else "")
        if connector is None or not hasattr(connector, "_api_get"):
            return empty
        if venue == "okx":
            return await self._fresh_okx_opens(connector, pair, ts)
        if venue != "gate":
            return empty
        symbol = await self._gate_symbol(connector, pair)
        if not symbol:
            return empty
        open_request = exchange_open_request(venue, symbol)
        finished_request = finished_list_request(venue, symbol, ts=ts)
        if open_request is None or finished_request is None:
            return empty
        opens = []
        opens_done = False
        page = 1
        try:
            for _ in range(1000):
                open_request = exchange_open_request(venue, symbol, page=page)
                if open_request is None:
                    return empty
                open_body = await connector._api_get(
                    path_url=open_request["path"],
                    params=open_request["params"],
                    is_auth_required=True,
                )
                parsed = raw_list_page(open_body, symbol)
                if not parsed.get("read"):
                    return empty
                opens.extend(parsed.get("orders") or [])
                if parsed.get("done"):
                    opens_done = True
                    break
                page += 1
            else:
                return empty
            finished_body = await connector._api_get(
                path_url=finished_request["path"],
                params=finished_request["params"],
                is_auth_required=True,
            )
        except Exception:
            return empty
        finished = raw_list_page(finished_body, symbol)
        return {
            "opens_read": True,
            "opens_done": opens_done,
            "finished_read": bool(finished.get("read")),
            "finished_done": bool(finished.get("done")),
            "covers_ts": history_covers_ts(finished_request, ts, finished),
            "opens": opens,
            "finished": finished.get("orders") or [],
            "symbol": symbol,
            "finished_request": finished_request,
        }

    def _sweep_targets(self) -> List[Dict[str, Any]]:
        targets = []
        seen = set()
        for row in self._owner_rows():
            key = (row.get("account"), row.get("connector"), row.get("pair"))
            if not all(key) or key in seen:
                continue
            seen.add(key)
            targets.append({"account": key[0], "connector": key[1], "pair": key[2]})
        for item in owed_records() or []:
            key = (item.get("account_name"), item.get("connector"), item.get("trading_pair"))
            if not all(key) or key in seen:
                continue
            seen.add(key)
            targets.append({"account": key[0], "connector": key[1], "pair": key[2]})
        for row in getattr(self, "_hold_rows", None) or []:
            key = (row.get("account_name"), row.get("connector_name"), row.get("trading_pair"))
            if not all(key) or key in seen:
                continue
            seen.add(key)
            targets.append({"account": key[0], "connector": key[1], "pair": key[2]})
        return targets

    def _connector_for(self, account: str, connector_name: str):
        for executor_id, executor in self._active_executors.items():
            meta = self._executor_metadata.get(executor_id) or {}
            if meta.get("account_name") != account or meta.get("connector_name") != connector_name:
                continue
            found = (getattr(executor, "connectors", None) or {}).get(connector_name)
            if found is not None:
                return found
        interface = self._trading_interfaces.get(account)
        connectors = getattr(interface, "connectors", None) or {}
        if isinstance(connectors, dict):
            return connectors.get(connector_name)
        return None

    def _client_in_book(self, connector, client_id: str, name: str) -> bool:
        if connector is None:
            return True
        if name == "in_flight":
            book = getattr(connector, "in_flight_orders", None)
        else:
            tracker = getattr(connector, "_order_tracker", None)
            book = getattr(tracker, "lost_orders", None) if tracker is not None else None
        if not isinstance(book, dict):
            return True
        return str(client_id) in {str(key) for key in book}

    def _proof_for_raw(self, client_id: str, fresh: Dict[str, Any], connector, ts: Any) -> bool:
        opens = fresh.get("opens") or []
        finished = fresh.get("finished") or []
        in_opens = any(str(row.get("text") or "") == str(client_id) for row in opens)
        status = next((row for row in finished if str(row.get("text") or "") == str(client_id)), {})
        return four_proofs(
            status,
            in_opens=in_opens,
            in_flight=self._client_in_book(connector, client_id, "in_flight"),
            in_lost=self._client_in_book(connector, client_id, "lost"),
            history_read=bool(fresh.get("opens_read") and fresh.get("covers_ts") and not in_opens),
        ) and gate_base_is_zero(status)

    def _raw_prefix_without_hold(self, pair: str, prefix: Optional[str]) -> bool:
        """A client id only inside the raw text is not a committed hold and not an empty book."""
        if not prefix:
            return False
        for row in getattr(self, "_hold_rows", None) or []:
            if row.get("trading_pair") != pair:
                continue
            text = row.get("checkpoint") or ""
            parsed = checkpoint_master(text)
            held = {
                str(item.get("order_id"))
                for item in (parsed or {}).get("holds") or []
                if item.get("order_id")
            }
            held.update(str(item) for item in saved_order_ids(parsed))
            held.update(self._checkpoint_close_ids(parsed, past=True))
            held.update(self._ram_close_ids())
            if str(prefix) in text and not any(item.startswith(str(prefix)) for item in held):
                leftover = text
                for item in partial_released_ids(parsed):
                    leftover = leftover.replace(str(item), "")
                if str(prefix) not in leftover:
                    continue
                return True
        return False

    async def _commit_price_hold(self, executor_id: str, order_id: str, price: Any, extra: Optional[Dict[str, Any]] = None) -> bool:
        if not order_id or not self.db_manager:
            return False
        addition = {"order_id": order_id}
        if price not in (None, ""):
            addition["price"] = str(price)
        if extra:
            addition.update(extra)
            addition.pop("level_id", None)
            if price in (None, ""):
                addition.pop("price", None)
        try:
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)
                row = await repo.get_executor_by_id(executor_id)
                seen = row.checkpoint if row is not None else None
                body = checkpoint_plus(seen, addition)
                if body is None:
                    return False
                if _parsed_hold_matches(parse_checkpoint(seen), order_id, price):
                    return True
                changed = await repo.replace_checkpoint_if_unchanged(executor_id, seen, body)
                if not changed:
                    return False
                fresh = await repo.get_executor_by_id(executor_id)
                stored = fresh.checkpoint if fresh is not None else None
                return _parsed_hold_matches(parse_checkpoint(stored), order_id, price)
        except Exception:
            logger.exception("price hold was not committed")
            return False

    async def _sweep_unproven_orders(self) -> None:
        """One control-loop tick. Membership in memory is not a cancel."""
        recover_folder_debt()
        self.__dict__["_released_order_ids"] = set()
        self.__dict__["_sweep_failed"] = False
        self._set_hold_rows(await self._checkpoint_rows())
        if self._hold_rows is None:
            self.__dict__["_sweep_failed"] = True
            self._log_silent_exit(
                "sweep-hold-rows", "", "checkpoint rows unread",
                interval=300.0, label="silent sweep exit:", key="hold-rows",
            )
        for target in self._sweep_targets():
            connector = self._connector_for(target["account"], target["connector"])
            if connector is None:
                self.__dict__["_sweep_failed"] = True
                self._log_silent_exit(
                    "sweep-no-connector", target["pair"],
                    f"account={target['account']} connector={target['connector']}",
                    interval=300.0, label="silent sweep exit:",
                    key=f"{target['account']}/{target['connector']}/{target['pair']}",
                )
                continue
            ts = self._oldest_ts(target)
            fresh = await self._fresh_raw_lists(connector, target["pair"], ts)
            self.__dict__.setdefault("_fresh_reads", {})[
                (target["account"], target["connector"], target["pair"])
            ] = fresh
            if not fresh.get("opens_read") or not fresh.get("finished_read") or not fresh.get("opens_done"):
                self.__dict__["_sweep_failed"] = True
                unread = " ".join(
                    name for name in ("opens_read", "finished_read", "opens_done") if not fresh.get(name)
                )
                self._log_silent_exit(
                    "sweep-lists-unread", target["pair"],
                    f"account={target['account']} connector={target['connector']} unread={unread}",
                    interval=300.0, label="silent sweep exit:",
                    key=f"{target['account']}/{target['connector']}/{target['pair']}",
                )
                continue
            await self._delete_owned_texts(connector, target, fresh)
        skipped = self._close_price_skip_ids()
        for order_id, item in list((self.__dict__.get("_hold_pending") or {}).items()):
            if str(order_id) in skipped:
                self.__dict__["_hold_pending"].pop(order_id, None)
                self._clear_unresolved(order_id)
                continue
            if await self._commit_price_hold(item.get("owner"), order_id, item.get("price")):
                self.__dict__["_hold_pending"].pop(order_id, None)
                self._clear_unresolved(order_id)
        try:
            ledger = ledger_raw_rows()
        except Exception:
            ledger = []
        for row in ledger or []:
            order_id = str(row.get("order_id") or "")
            owner = row.get("executor_id")
            if not order_id or not owner or row.get("price") in (None, "") or row.get("ts") is None:
                continue
            if order_is_unsent(order_id) or order_id in skipped:
                continue
            if await self._commit_price_hold(owner, order_id, row.get("price")):
                self._clear_unresolved(order_id)
        self.__dict__["_sweep_seen"] = not self.__dict__.get("_sweep_failed")

    def _oldest_ts(self, target: Dict[str, Any]):
        found = []
        for item in owed_records() or []:
            if item.get("trading_pair") == target["pair"] and item.get("ts") is not None:
                found.append(item.get("ts"))
        try:
            ledger_ts = ledger_min_ts()
        except Exception:
            return None
        if ledger_ts is not None:
            found.append(ledger_ts)
        best = None
        for value in found:
            if best is None:
                best = value
                continue
            try:
                if value < best:
                    best = value
            except TypeError:
                # Несопоставимые типы ts не обрывают шаг sweep (та же защита,
                # что в сборке снимка — minor вердикта 20261010-232605-72948).
                pass
        return best

    def _ids_of(self, items) -> set:
        found = set()
        for item in items or []:
            order_id = item.get("order_id") if isinstance(item, dict) else item
            if order_id:
                found.add(str(order_id))
        return found

    def _checkpoint_close_ids(self, parsed: Optional[Dict[str, Any]], *, ends: bool = False, past: bool = False) -> set:
        found = set()
        if not isinstance(parsed, dict):
            return found
        top = parsed.get("close_order") or {}
        if isinstance(top, dict) and top.get("order_id"):
            found.add(str(top.get("order_id")))
        found.update(self._ids_of(parsed.get("close_holds")))
        for level in parsed.get("levels") or []:
            if not isinstance(level, dict):
                continue
            close = level.get("close_order") or {}
            if isinstance(close, dict) and close.get("order_id"):
                found.add(str(close.get("order_id")))
            if past:
                found.update(self._ids_of(level.get("partial_closes")))
        if ends:
            for mark in (parsed.get("partial_ends") or {}).values():
                if isinstance(mark, dict):
                    found.update(str(item) for item in (mark.get("order_ids") or []) if item)
            found.update(str(item) for item in partial_released_ids(parsed) if item and not str(item).startswith("level:"))
        return found

    def _parsed_hold_rows(self):
        for row in getattr(self, "_hold_rows", None) or []:
            raw = row.get("checkpoint")
            parsed = raw if isinstance(raw, dict) else checkpoint_view(raw)
            if isinstance(parsed, dict):
                yield parsed

    # ── Наборы из тела строк ───────────────────────────────────────────
    #
    # Каждый набор повторяет прежнее чтение тела один в один (пробы сверки
    # в test/ держат это). Наборы считаются один раз на поколение строк и
    # хранят только то, что лежит в теле: order_is_unsent и отборы по паре
    # читатель применяет в момент чтения. Писатели идут мимо кэша: тело,
    # только что записанное в базу, разбирается напрямую.

    def _set_hold_rows(self, rows) -> None:
        self._hold_rows = rows
        self.__dict__["_hold_index_cache"] = None
        try:
            checkpoint_cache_prune(
                row.get("checkpoint") for row in (rows or []) if isinstance(row, dict)
            )
        except Exception:
            logger.debug("Could not prune the checkpoint cache.", exc_info=True)

    def _hold_index(self):
        index = self.__dict__.get("_hold_index_cache")
        if index is None:
            index = [self._row_extract(row) for row in (getattr(self, "_hold_rows", None) or [])]
            self.__dict__["_hold_index_cache"] = index
        return index

    def _row_extract(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Извлечение из тела одной строки — ровно то, что читатели брали
        из тела сами, только один раз на поколение строк."""
        text = row.get("checkpoint") or ""
        parsed = checkpoint_master(text)
        body = parsed if isinstance(parsed, dict) else None

        owner_ids: set = set(saved_order_ids(body)) if body is not None else set()
        column_ids: set = set(owner_ids)
        seated_ids: set = set()
        released_ids: set = set()
        price_released_ids: set = set()
        released_texts: set = set()
        level_close_ids: set = set()
        level_close_prices: list = []
        close_price_map: dict = {}
        close_hold_price_map: dict = {}
        protected_ids: set = set()
        close_ids: set = self._checkpoint_close_ids(body, past=True, ends=True)
        ban_holds: list = []

        if body is not None:
            for hold in body.get("holds") or []:
                if not isinstance(hold, dict):
                    continue
                if hold.get("order_id"):
                    owner_ids.add(str(hold.get("order_id")))
                    seated_ids.add(str(hold.get("order_id")))
                    column_ids.add(hold.get("order_id"))
            # Цены держателей берутся из close_holds — ровно как читал прежний
            # _close_price_known. Источник holds сюда не подмешивается: запись
            # holds с ценой — это запрет цены, а не «сохранённая цена закрытия».
            for item in body.get("close_holds") or []:
                if not isinstance(item, dict):
                    continue
                if item.get("price") not in (None, ""):
                    close_hold_price_map[str(item.get("order_id") or "")] = item.get("price")
            for level in body.get("levels") or []:
                if not isinstance(level, dict):
                    continue
                for key in ("open_order", "close_order"):
                    slot = level.get(key) or {}
                    if isinstance(slot, dict) and slot.get("order_id"):
                        owner_ids.add(str(slot.get("order_id")))
                open_slot = level.get("open_order") or {}
                if isinstance(open_slot, dict) and open_slot.get("order_id"):
                    seated_ids.add(str(open_slot.get("order_id")))
                close = level.get("close_order") or {}
                if isinstance(close, dict) and close.get("order_id"):
                    close_id = str(close.get("order_id"))
                    level_close_ids.add(close_id)
                    if close.get("price") not in (None, ""):
                        close_price_map[close_id] = close.get("price")
                        level_close_prices.append((close_id, close.get("price")))
                if not partial_level_released(body, str(level.get("id"))):
                    ids = [
                        str((level.get("open_order") or {}).get("order_id") or ""),
                        str((level.get("close_order") or {}).get("order_id") or ""),
                    ]
                    ids.extend(
                        str(item.get("order_id") or "")
                        for item in level.get("partial_closes") or []
                        if isinstance(item, dict)
                    )
                    protected_ids.update(item for item in ids if item)
            released_ids = set(partial_released_ids(body))
            price_released_ids = set(partial_price_released_ids(body))
            released_texts = {
                str(item) for item in partial_released_ids(body)
                if item and not str(item).startswith("level:")
            }

        price_body = row.get("price_checkpoint")
        if not isinstance(price_body, dict):
            price_body = body
        for hold in (price_body or {}).get("holds") or []:
            if not isinstance(hold, dict):
                continue
            if hold.get("price") not in (None, ""):
                ban_holds.append((str(hold.get("order_id") or ""), hold.get("price")))

        return {
            "pair": row.get("trading_pair"),
            "identity": self._row_identity(row),
            "unread": bool(text) and body is None,
            "owner_ids": owner_ids,
            "column_ids": column_ids,
            "seated_ids": seated_ids,
            "released_ids": released_ids,
            "price_released_ids": price_released_ids,
            "released_texts": released_texts,
            "level_close_ids": level_close_ids,
            "level_close_prices": level_close_prices,
            "close_price_map": close_price_map,
            "close_hold_price_map": close_hold_price_map,
            "protected_ids": protected_ids,
            "close_ids": close_ids,
            "ban_holds": ban_holds,
        }

    def _ram_close_ids(self) -> set:
        found = set()
        for executor in getattr(self, "_active_executors", {}).values():
            found.update(self._ids_of(getattr(executor, "_close_holds", []) or []))
        return found

    def _close_cancel_ids(self) -> set:
        """Do not cancel a close slot, a past close, or an id already ended.

        RAM may be empty after a restart. partial_ends still names the open id,
        and that id may still be on the book. Cancelling it would free the tick
        for a second order at the same price.
        """
        found = self._ram_close_ids()
        for entry in self._hold_index():
            found.update(entry["close_ids"])
        return found

    def _close_price_skip_ids(self) -> set:
        """Ids whose price must not be written into holds, bans, or the journal."""
        found = self._close_cancel_ids()
        for entry in self._hold_index():
            found.update(entry["close_ids"])
        return found

    def _close_recorded_ids(self) -> set:
        """A close the seen-prefix bag must not treat as unrecorded."""
        found = set()
        for entry in self._hold_index():
            found.update(entry["close_ids"])
        return found

    def _seat_close_hold(self, executor: Any, order_id: str) -> None:
        holds = list(getattr(executor, "_close_holds", []) or [])
        if str(order_id) in self._ids_of(holds):
            executor._close_holds = holds
            return
        holds.append({"order_id": order_id, "role": "close"})
        executor._close_holds = holds

    def _forget_close_memory(self, executor: Any, order_ids) -> None:
        if executor is None:
            return
        gone = {str(item) for item in order_ids if item}
        executor._close_holds = [
            item for item in getattr(executor, "_close_holds", []) or []
            if str((item.get("order_id") if isinstance(item, dict) else item) or "") not in gone
        ]
        executor._close_ban_ids = {
            str(item) for item in getattr(executor, "_close_ban_ids", set()) or set()
            if str(item) not in gone
        }
        prices = dict(getattr(executor, "_close_ban_prices", None) or {})
        for item in gone:
            prices.pop(item, None)
        executor._close_ban_prices = prices
        executor._column_close_ids = [
            item for item in getattr(executor, "_column_close_ids", []) or []
            if str(item) not in gone
        ]

    def _strip_recorded(self, body: Dict[str, Any], order_ids) -> None:
        gone = {str(item) for item in order_ids if item}

        def keep(item) -> bool:
            token = item.get("order_id") if isinstance(item, dict) else item
            return str(token or "") not in gone

        body["close_holds"] = [item for item in body.get("close_holds") or [] if keep(item)]
        body["holds"] = [item for item in body.get("holds") or [] if keep(item)]
        body["close_bans"] = [item for item in body.get("close_bans") or [] if keep(item)]

    def _release_close_reserve(self, connector: Any, pair: str, quant: str, order_id: str) -> None:
        reserves = self.__dict__.get("_quant_reserves") or {}
        key = (str(id(connector)), pair, str(quant))
        if reserves.get(key) == order_id:
            release_quant(reserves, str(id(connector)), pair, str(quant))

    def _drop_dead_close_records(self, body: Optional[str]) -> Optional[str]:
        """A price-less hold or a dead ban must not be resumed as a live close."""
        if not body:
            return body
        try:
            parsed = json.loads(body)
        except (json.JSONDecodeError, TypeError):
            return body
        if not isinstance(parsed, dict):
            return body
        slots = set()
        top = parsed.get("close_order") or {}
        if isinstance(top, dict) and top.get("order_id"):
            slots.add(str(top.get("order_id")))
        for level in parsed.get("levels") or []:
            if not isinstance(level, dict):
                continue
            close = level.get("close_order") or {}
            if isinstance(close, dict) and close.get("order_id"):
                slots.add(str(close.get("order_id")))
        released = self._checkpoint_close_ids(parsed, past=True, ends=True)
        current_holds = list(parsed.get("close_holds") or [])
        kept_holds = []
        for item in current_holds:
            if isinstance(item, dict):
                order_id = str(item.get("order_id") or "")
                if item.get("price") not in (None, "") or order_id in slots:
                    kept_holds.append(item)
            elif item and str(item) in slots:
                kept_holds.append(item)
        current_bans = list(parsed.get("close_bans") or [])
        kept_bans = []
        for item in current_bans:
            token = str(item.get("order_id") if isinstance(item, dict) else item or "")
            if token and token not in slots and token in released:
                continue
            kept_bans.append(item)
        if kept_holds == current_holds and kept_bans == current_bans:
            return body
        parsed["close_holds"] = kept_holds
        parsed["close_bans"] = kept_bans
        return json.dumps(parsed, sort_keys=True)

    def _seated_open_id(self, text: str, pair: str) -> bool:
        """A hold or an open slot already in the readable column. A close slot is not this."""
        if not text:
            return False
        for entry in self._hold_index():
            if entry["pair"] not in (None, "", pair):
                continue
            if text in entry["seated_ids"]:
                return True
        return False

    def _keeps_fresh_open(self, text: str, pair: str, known_commits) -> bool:
        """Do not cancel a blocked id, a seated open, or a hold just written for a known owner."""
        if not text:
            return False
        if self._cancel_blocked(text):
            return True
        if text in (known_commits or ()):
            return True
        return self._seated_open_id(text, pair)

    async def _delete_owned_texts(self, connector, target: Dict[str, Any], fresh: Dict[str, Any]) -> None:
        if not fresh.get("opens_read"):
            return
        prefix = getattr(connector, "client_order_id_prefix", None)
        journal = ledger_order_ids()
        column = await self._column_order_ids(target["pair"])
        if column is None or self._checkpoint_unread(target["pair"]):
            return
        owed_ids = {
            str(item.get("order_id"))
            for item in (owed_records() or [])
            if item.get("order_id") and item.get("trading_pair") == target["pair"]
            and item.get("account_name") == target["account"]
            and item.get("connector") == target["connector"]
        }
        skipped = self._close_price_skip_ids()
        protected = self._close_cancel_ids()
        known_commits = set()
        for raw in list(fresh.get("opens") or []) + list(fresh.get("finished") or []):
            text = str(raw.get("text") or raw.get("order_id") or "")
            if not text:
                continue
            owned_by_prefix = bool(prefix) and text.startswith(str(prefix))
            in_journal = journal is not None and text in journal
            in_column = text in column
            in_owed = text in owed_ids
            mark = raw.get("currency_pair")
            if mark not in (None, "") and mark != fresh.get("symbol"):
                continue
            if not owned_by_prefix and not in_journal and not in_column and not in_owed:
                continue
            if self._checkpoint_unread(target["pair"]):
                self._mark_unresolved(text)
                continue
            known = self._known_owner(text, target["account"], target["connector"], target["pair"])
            owner = known
            if owner is None and not self._checkpoint_unread(target["pair"]):
                owner = self._sole_live_owner(text, target["account"], target["connector"], target["pair"])
            sole = known is None and owner is not None
            self.__dict__.setdefault("_seen_client_ids", {}).setdefault(target["pair"], set()).add(text)
            price = raw.get("price")
            if text in skipped:
                self.__dict__.setdefault("_hold_pending", {}).pop(text, None)
            elif sole or self._keeps_fresh_open(text, target["pair"], known_commits):
                pass
            elif owner and not in_column:
                wrote = await self._commit_price_hold(owner, text, price)
                if not wrote:
                    self.__dict__.setdefault("_hold_pending", {})[text] = {"owner": owner, "price": price}
                    self._remember_unwritten_hold(owner, text, price)
                    self._mark_unresolved(text)
                    continue
                known_commits.add(text)
            if raw in (fresh.get("opens") or []) and (
                self._close_seat_blocks(text)
                or text in protected
                or self._keeps_fresh_open(text, target["pair"], known_commits)
            ):
                self._clear_unresolved(text)
                continue
            if raw in (fresh.get("opens") or []):
                cancelled = await self._cancel_copied_open(connector, raw, target["pair"])
                if cancelled:
                    self._clear_unresolved(text)
                elif text in protected:
                    self._clear_unresolved(text)
                else:
                    self._mark_unresolved(text)

    async def _running_rows(self):
        if not self.db_manager:
            return None
        try:
            async with self.db_manager.get_session_context() as session:
                return await ExecutorRepository(session).get_running_for_resume()
        except Exception:
            return None

    async def _checkpoint_rows(self):
        if not self.db_manager:
            return []
        try:
            async with self.db_manager.get_session_context() as session:
                return await ExecutorRepository(session).get_checkpoint_rows()
        except Exception:
            return None

    async def _column_order_ids(self, pair: str):
        rows = getattr(self, "_hold_rows", None)
        if rows is None:
            rows = await self._checkpoint_rows()
        if rows is None:
            return None
        if rows is getattr(self, "_hold_rows", None):
            entries = self._hold_index()
        else:
            entries = [self._row_extract(row) for row in rows]
        found = set()
        for entry, row in zip(entries, rows):
            if row.get("trading_pair") != pair:
                continue
            if entry["unread"]:
                return None
            found.update(entry["column_ids"])
        return found

    async def _fresh_pair_reads(self):
        rows = await self._running_rows()
        if rows is None:
            return None
        fresh = {}
        for row in rows:
            key = (row.get("account_name"), row.get("connector_name"), row.get("trading_pair"))
            if key in fresh or not all(key):
                continue
            connector = self._connector_for(key[0], key[1])
            if connector is None:
                fresh[key] = {"opens_read": False, "finished_read": False}
                continue
            slot = await self._fresh_raw_lists(connector, key[2], self._oldest_ts({
                "account": key[0], "connector": key[1], "pair": key[2],
            }))
            fresh[key] = slot
        return fresh

    def _row_proofs_released(self, row: Dict[str, Any], slot: Dict[str, Any]) -> bool:
        parsed = checkpoint_master(row.get("checkpoint"))
        ids = set(saved_order_ids(parsed))
        for hold in (parsed or {}).get("holds") or []:
            if hold.get("order_id"):
                ids.add(hold["order_id"])
        if not ids:
            return False
        connector = self._connector_for(row.get("account_name"), row.get("connector_name"))
        return all(self._proof_for_raw(order_id, slot, connector, None) for order_id in ids)

    def _column_ids_for_executor(self, executor_id: str):
        rows = getattr(self, "_hold_rows", None)
        if rows is None:
            return None
        found = set()
        seen = False
        for row in rows:
            if row.get("executor_id") != executor_id:
                continue
            seen = True
            text = row.get("checkpoint") or ""
            parsed = checkpoint_master(text)
            if text and parsed is None:
                return None
            found.update(saved_order_ids(parsed))
            for hold in (parsed or {}).get("holds") or []:
                if hold.get("order_id"):
                    found.add(str(hold["order_id"]))
        found = {item for item in found if not order_is_unsent(item)}
        return found if seen or rows is not None else None

    def _executor_proofs_released(self, executor_id: str) -> bool:
        gates = self.__dict__.setdefault("_stop_gate", {})
        if buy_blocked():
            gates[executor_id] = "buy-blocked"
            return False
        if ledger_executor_ids() is None:
            gates[executor_id] = "ledger-unread"
            return False
        if not self.__dict__.get("_sweep_seen"):
            gates[executor_id] = "sweep-not-seen"
            return False
        if self.__dict__.get("_sweep_failed"):
            gates[executor_id] = "sweep-failed"
            return False
        column_ids = self._column_ids_for_executor(executor_id)
        if column_ids is None:
            gates[executor_id] = "column-unread"
            return False
        try:
            from utils.executor_checkpoint import read_ledger_filtered
            rows = read_ledger_filtered()
        except Exception:
            gates[executor_id] = "ledger-reread"
            return False
        ids = {
            str(row.get("order_id"))
            for row in rows
            if row.get("executor_id") == executor_id and row.get("order_id")
        }
        ids.update(column_ids)
        # Видимый конец в partial_ends — штатная запись «номер отпущен»:
        # оперативный список стирается каждым проходом обхода и один не держит
        # гашение. Концы без level_id (отдельные номера) тоже отпускают номер.
        released = set(self.__dict__.get("_released_order_ids", set()))
        released.update(partial_released_ids(self._partial_parsed(executor_id)))
        released.difference_update(self._retained_51400_unproven(executor_id))
        held_ids = [order_id for order_id in sorted(ids) if order_id not in released]
        if held_ids:
            gates[executor_id] = "unreleased-ids ids=" + ",".join(held_ids)
            return False
        gates.pop(executor_id, None)
        return True

    async def _land_closed_early_stop(self, executor_id: str, executor: Any) -> bool:
        """Сажает уже закрытую раннюю остановку уже существующей записью.
        Новый статус не пишем. Недоказанное не сажаем и из памяти не вынимаем.
        """
        if not await self._closed_early_stop_ready(executor_id, executor):
            return False
        await self._persist_executor_completed(executor_id, executor)
        if not await self._closed_row_landed(executor_id):
            return False
        self._active_executors.pop(executor_id, None)
        self._executor_metadata.pop(executor_id, None)
        return True

    async def _closed_early_stop_ready(self, executor_id: str, executor: Any) -> bool:
        """Каждый свой номер доказан, тело прочитано и плоско.

        Номера без finished и без открытых доказывает архив: сырые строки
        страницы (пустой px строку не выкидывает), состояние «исполнена» и
        монетный размер больше нуля. Открытые, полёт, потерянные, неизвестная
        отправка и непрочитанное архив не заменяют. После тела списки
        читаются заново.
        """
        close_name = getattr(getattr(executor, "close_type", None), "name", None)
        status_name = getattr(getattr(executor, "status", None), "name", None)
        if not getattr(executor, "is_closed", False):
            self._log_silent_exit(
                "closed-stop-proof", getattr(getattr(executor, "config", None), "trading_pair", None) or "",
                "not-closed", executor_id, interval=60.0, label="silent stop exit:", key=executor_id,
            )
            return False
        if close_name != "EARLY_STOP" or status_name != "TERMINATED":
            self._log_silent_exit(
                "closed-stop-proof", getattr(getattr(executor, "config", None), "trading_pair", None) or "",
                f"memory-state {close_name}/{status_name}", executor_id,
                interval=60.0, label="silent stop exit:", key=executor_id,
            )
            return False
        ids = self._closed_early_stop_ids(executor_id, executor)
        if ids is None:
            self._log_silent_exit(
                "closed-stop-proof", getattr(getattr(executor, "config", None), "trading_pair", None) or "",
                "ids-unread", executor_id, interval=60.0, label="silent stop exit:", key=executor_id,
            )
            return False
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        if connector is None or not pair or not hasattr(connector, "_api_get"):
            return False

        def proof_log(reason):
            self._log_silent_exit(
                "closed-stop-proof", pair or "", reason, executor_id,
                interval=60.0, label="silent stop exit:", key=executor_id,
            )

        def lists_state(fresh):
            if not self._lists_ready(fresh):
                proof_log("lists-unread")
                return None
            inflight = getattr(getattr(connector, "in_flight_orders", None), "copy", lambda: {})()
            lost = getattr(getattr(getattr(connector, "_order_tracker", None), "lost_orders", None), "copy", lambda: {})()
            missing = []
            for text in sorted(ids):
                if order_send_unknown(text) or text in inflight or text in lost:
                    proof_log("number-live " + text[-8:])
                    return None
                if self._shutdown_text_present((fresh or {}).get("opens"), text):
                    proof_log("number-open " + text[-8:])
                    return None
                row = self._finished_row(text, fresh)
                if row is not None:
                    if not self._closed_row_filled(row):
                        proof_log("finished-not-filled " + text[-8:])
                        return None
                    continue
                missing.append(text)
            return missing

        fresh = await self._fresh_raw_lists(connector, pair, None)
        missing = lists_state(fresh)
        if missing is None:
            return False
        archive_deaths = {}
        need = []
        for text in missing:
            refusal = await self._shutdown_close_refusal(text)
            if refusal is True:
                continue
            need.append(text)
        if need:
            archive_deaths = await self._archive_death_bases(connector, pair, need)
            if archive_deaths is None:
                proof_log("archive-unproven " + ",".join(text[-8:] for text in need))
                return False
        own_size = getattr(executor, "position_size_base", None)
        try:
            own_flat = own_size is not None and Decimal(str(own_size)) == 0
        except Exception:
            own_flat = False
        if not own_flat:
            proof_log("own-size-not-zero")
            return False
        fresh = await self._fresh_raw_lists(connector, pair, None)
        missing_after = lists_state(fresh)
        if missing_after is None:
            return False
        for text in missing_after:
            if await self._shutdown_close_refusal(text) is True:
                continue
            if text not in archive_deaths:
                proof_log("archive-death-dropped " + text[-8:])
                return False
        return True

    async def _archive_death_bases(self, connector, pair: str, numbers) -> Optional[Dict[str, Decimal]]:
        """Сырые страницы (свежая 7-дневная и 90-дневный архив) по каждому номеру:

        исполнен и монетный размер больше нуля. Пустой px строку не выкидываем.
        Нечитаемое, незнайденное, отмена и нулевой размер — None, не ноль.
        """
        from utils.executor_checkpoint import OPEN_PAGE_LIMIT
        symbol = await self._gate_symbol(connector, pair)
        if not symbol:
            return None
        inst_type = "SWAP" if "perpetual" in type(connector).__name__.lower() else "SPOT"
        window = self._retained_archive_window()
        rows: List[Dict[str, Any]] = []
        for use_archive, begin, end in (
            (False, None, None),
            (True, window["begin"], window["end"]),
        ):
            after = None
            try:
                for _ in range(1000):
                    request = okx_closed_request(
                        symbol, after=after, begin=begin, end=end,
                        inst_type=inst_type, archive=use_archive,
                    )
                    if request is None:
                        return None
                    body = await connector._api_get(
                        path_url=request["path"],
                        params=request["params"],
                        is_auth_required=True,
                        limit_id=self._open_limit_id(connector, "okx"),
                    )
                    if not isinstance(body, dict) or str(body.get("code")) != "0" or not isinstance(body.get("data"), list):
                        return None
                    data = body["data"]
                    for raw in data:
                        if isinstance(raw, dict):
                            if use_archive:
                                try:
                                    created = int(str(raw.get("cTime")))
                                except (TypeError, ValueError):
                                    return None
                                if created < int(window["begin"]) or created > int(window["end"]):
                                    continue
                            rows.append(raw)
                    if len(data) < OPEN_PAGE_LIMIT:
                        break
                    cursor = data[-1].get("ordId") if isinstance(data[-1], dict) else None
                    if not cursor:
                        return None
                    after = str(cursor)
            except Exception:
                return None
        deaths: Dict[str, Decimal] = {}
        for text in [str(item) for item in numbers]:
            base = None
            for raw in rows:
                name = str(raw.get("clOrdId") or raw.get("text") or raw.get("order_id") or "")
                if name != text:
                    continue
                if str(raw.get("state") or "") != "filled":
                    continue
                size = partial_decimal(raw.get("accFillSz"))
                coin = self._contract_base(connector, pair, size) if size is not None and size > 0 else None
                if coin is not None and coin > 0:
                    base = coin
                    break
            if base is None:
                self._log_silent_exit(
                    "archive-proof", pair or "",
                    f"no-death {text[-8:]} rows={len(rows)}",
                    key=pair or "archive",
                    label="silent stop exit:",
                )
                return None
            deaths[text] = base
        return deaths

    def _closed_early_stop_ids(self, executor_id: str, executor: Any):
        """Журнал, колонка и close_holds, включая не ушедшую отправку.
        Отфильтрованный журнал такую строку прячет. Непрочитанное — не пустой набор.
        """
        ids = self._own_stop_ids(executor_id)
        if ids is None:
            return None
        from utils.executor_checkpoint import parse_checkpoint, _read_ledger, saved_order_ids
        found = set(ids)
        try:
            raw_rows = _read_ledger()
        except Exception:
            import traceback; traceback.print_exc()
            return None
        found.update(
            str(row.get("order_id"))
            for row in raw_rows or []
            if row.get("executor_id") == executor_id and row.get("order_id")
        )
        for row in getattr(self, "_hold_rows", None) or []:
            key = str(row.get("executor_id") or row.get("row_executor_id") or "")
            if key != str(executor_id):
                continue
            text = row.get("checkpoint") or ""
            parsed = parse_checkpoint(text)
            if text and parsed is None:
                return None
            found.update(saved_order_ids(parsed))
            found.update(self._ids_of((parsed or {}).get("holds")))
            found.update(self._ids_of((parsed or {}).get("close_holds")))
            found.update(self._ids_of((parsed or {}).get("retained_orders")))
            found.update(self._ban_ids(parsed))
            found.update(self._partial_end_order_ids(parsed))
            found.update(self._checkpoint_close_ids(parsed, past=True))
        found.update(self._ids_of(getattr(executor, "_close_holds", []) or []))
        found.update(self._executor_close_slot_ids(executor))
        return found

    @staticmethod
    def _ban_ids(parsed) -> set:
        found = set()
        for ban in (parsed or {}).get("close_bans") or []:
            if isinstance(ban, dict) and ban.get("order_id"):
                found.add(str(ban["order_id"]))
            elif isinstance(ban, str) and ban:
                found.add(ban)
        return found

    @staticmethod
    def _partial_end_order_ids(parsed) -> set:
        ends = (parsed or {}).get("partial_ends") or {}
        if not isinstance(ends, dict):
            return set()
        found = set()
        for key, mark in ends.items():
            text = str(key or "")
            if text and text != "stop-requested" and not text.startswith("level:"):
                found.add(text)
            if isinstance(mark, dict):
                found.update(str(item) for item in (mark.get("order_ids") or []) if item)
        return found

    @staticmethod
    def _executor_close_slot_ids(executor) -> set:
        found = {str(item) for item in getattr(executor, "_close_ban_ids", set()) or set() if item}
        close = getattr(executor, "_close_order", None)
        if getattr(close, "order_id", None):
            found.add(str(close.order_id))
        pending = getattr(executor, "_shutdown_close_id", None)
        if pending:
            found.add(str(pending))
        for level in getattr(executor, "grid_levels", []) or []:
            seated = getattr(level, "active_close_order", None)
            if getattr(seated, "order_id", None):
                found.add(str(seated.order_id))
        return found

    async def _numbers_dead_on_fresh_lists(self, executor: Any, ids) -> bool:
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        if connector is None or not pair:
            return False
        try:
            fresh = await self._fresh_raw_lists(connector, pair, None)
        except Exception:
            return False
        if not self._lists_ready(fresh):
            return False
        inflight = getattr(connector, "in_flight_orders", None)
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None)
        if not isinstance(inflight, dict) or not isinstance(lost, dict):
            return False
        return all(
            self._sent_number_proven_dead(text, fresh, inflight, lost)
            for text in ids
        )

    def _sent_number_proven_dead(
        self, order_id: str, fresh: Optional[Dict[str, Any]], inflight: Dict[str, Any], lost: Dict[str, Any],
    ) -> bool:
        text = str(order_id or "")
        if (
            not text
            or order_send_unknown(text)
            or text in inflight
            or text in lost
            or self._shutdown_text_present((fresh or {}).get("opens"), text)
        ):
            return False
        if order_is_unsent(text):
            return True
        return self._finished_row(text, fresh) is not None

    async def _closed_row_landed(self, executor_id: str) -> bool:
        if not self.db_manager:
            return False
        try:
            async with self.db_manager.get_session_context() as session:
                current = await ExecutorRepository(session).get_executor_by_id(executor_id)
        except Exception:
            return False
        if current is None:
            return False
        return (
            str(getattr(current, "status", None) or "") == "TERMINATED"
            and str(getattr(current, "close_type", None) or "") == "EARLY_STOP"
        )

    def _stop_held_may_finish(self, executor_id: str, executor: Any) -> bool:
        """Внутреннее завершение только если каждый номер мёртв доказанно.

        Нет в открытых — не смерть. Непрочитанная книга, трекер, lost и
        неизвестная отправка пропуск не дают.
        """
        gates = self.__dict__.setdefault("_stop_gate", {})
        if buy_blocked() or ledger_executor_ids() is None or order_miss_unreadable():
            gates[executor_id] = "unread"
            return False
        if not self.__dict__.get("_sweep_seen") or self.__dict__.get("_sweep_failed"):
            gates[executor_id] = "sweep"
            return False
        column_ids = self._column_ids_for_executor(executor_id)
        if column_ids is None:
            gates[executor_id] = "column-unread"
            return False
        try:
            from utils.executor_checkpoint import read_ledger_filtered
            rows = read_ledger_filtered()
        except Exception:
            gates[executor_id] = "ledger-reread"
            return False
        ids = {
            str(row.get("order_id"))
            for row in rows or []
            if row.get("executor_id") == executor_id and row.get("order_id")
        }
        ids.update(column_ids)
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        fresh = self._pair_book(pair) if pair else None
        if not self._lists_ready(fresh):
            gates[executor_id] = "book-unread"
            return False
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        parsed = self._partial_parsed(executor_id)
        released = partial_released_ids(parsed)
        for text in sorted(ids):
            if order_send_unknown(text) or text in inflight or text in lost:
                gates[executor_id] = "still-live"
                return False
            if self._shutdown_text_present((fresh or {}).get("opens"), text):
                gates[executor_id] = "still-open"
                return False
            if text in released or partial_end_visible(parsed, text):
                if text in self._retained_51400_unproven(executor_id, fresh):
                    gates[executor_id] = "retained-unproven"
                    return False
                continue
            if self._finished_row(text, fresh) is not None:
                continue
            gates[executor_id] = "unreleased-ids"
            return False
        gates.pop(executor_id, None)
        return True

    def _own_stop_ids(self, executor_id: str) -> Optional[set]:
        """Свои номера шага остановки: журнал этого исполнителя и колонка.

        close_bans чужого журнала сюда не входят. Непрочитанное — не пустой набор.
        """
        if buy_blocked() or ledger_executor_ids() is None or order_miss_unreadable():
            return None
        column_ids = self._column_ids_for_executor(executor_id)
        if column_ids is None:
            return None
        try:
            from utils.executor_checkpoint import read_ledger_filtered
            rows = read_ledger_filtered()
        except Exception:
            return None
        ids = {
            str(row.get("order_id"))
            for row in rows or []
            if row.get("executor_id") == executor_id and row.get("order_id")
        }
        ids.update(column_ids)
        return ids

    def _dead_end_visible(self, executor_id: str, order_id: str) -> bool:
        """Конец номера без price и без level_id. Метка с ценой цену не отпускает."""
        text = str(order_id or "")
        mark = ((self._partial_parsed(executor_id) or {}).get("partial_ends") or {}).get(text)
        return self._flat_dead_mark(mark, text)

    @staticmethod
    def _flat_dead_mark(mark: Any, order_id: str) -> bool:
        text = str(order_id or "")
        return (
            bool(text)
            and isinstance(mark, dict)
            and mark.get("kind") == "flat"
            and "price" not in mark
            and "level_id" not in mark
            and list(mark.get("order_ids") or []) == [text]
        )

    def _own_number_still_dead(self, executor: Any, order_id: str) -> bool:
        """После записи конец годится, только если книга заново показывает смерть.

        Непрочитанная книга, открытый список, трекер, lost и неизвестная
        отправка — не смерть. Вид метки сам по себе тоже не смерть.
        """
        text = str(order_id or "")
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        fresh = self._pair_book(pair) if pair else None
        if not text or not self._lists_ready(fresh):
            return False
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        inflight = getattr(connector, "in_flight_orders", None)
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None)
        if not isinstance(inflight, dict) or not isinstance(lost, dict):
            return False
        if order_send_unknown(text) or text in inflight or text in lost:
            return False
        if self._shutdown_text_present((fresh or {}).get("opens"), text):
            return False
        return self._finished_row(text, fresh) is not None

    async def _drop_flat_ends(self, executor_id: str, order_ids: List[str]) -> bool:
        """Снять только что записанный flat-конец. Чужой и nothing-to-close не трогать."""
        pending = [str(item) for item in order_ids if item]
        if not pending:
            return True

        def mutate(body: Dict[str, Any]) -> None:
            ends = dict(body.get("partial_ends") or {})
            for text in pending:
                if self._flat_dead_mark(ends.get(text), text):
                    ends.pop(text, None)
            body["partial_ends"] = ends

        def visible(stored: Optional[Dict[str, Any]]) -> bool:
            ends = (stored or {}).get("partial_ends") or {}
            return all(text not in ends for text in pending)

        if not await self._partial_cas(executor_id, mutate, visible):
            return False
        parsed = self._partial_parsed(executor_id) or {}
        ends = parsed.get("partial_ends") or {}
        return all(text not in ends for text in pending)

    async def _retract_unconfirmed_dead_ends(self, executor_id: str, executor: Any) -> bool:
        """Номер ожил после записи — снять конец, чтобы отмена снова его видела."""
        bag = self.__dict__.setdefault("_unconfirmed_dead_ends", {}).setdefault(executor_id, set())
        if not bag:
            return True
        live = [text for text in sorted(bag) if not self._own_number_still_dead(executor, text)]
        if not live:
            return True
        if not await self._drop_flat_ends(executor_id, live):
            return False
        bag.difference_update(live)
        return True

    async def _remember_own_dead_ends(self, executor_id: str, executor: Any) -> bool:
        """Своему номеру со строкой finished — конец до stop(). Чужой ban не пишем.

        Пустой открытый список и таблица orders концом не являются. После записи
        книга читается заново: номер ожил — конец снимается, stop() не зовётся.
        """
        if not await self._retract_unconfirmed_dead_ends(executor_id, executor):
            return False
        ids = self._own_stop_ids(executor_id)
        if ids is None:
            return False
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        fresh = self._pair_book(pair) if pair else None
        if not self._lists_ready(fresh):
            return False
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        inflight = getattr(connector, "in_flight_orders", None)
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None)
        if not isinstance(inflight, dict) or not isinstance(lost, dict):
            return False
        pending: List[str] = []
        parsed = self._partial_parsed(executor_id) or {}
        already = set((parsed.get("partial_ends") or {}))
        for text in sorted(ids):
            if text in already:
                continue
            if order_send_unknown(text) or text in inflight or text in lost:
                return False
            if self._shutdown_text_present((fresh or {}).get("opens"), text):
                return False
            if self._finished_row(text, fresh) is None:
                continue
            pending.append(text)
        if not pending:
            return True

        def mutate(body: Dict[str, Any]) -> None:
            ends = dict(body.get("partial_ends") or {})
            for text in pending:
                if text in ends:
                    continue
                self._shutdown_end_mark(ends, text)
            body["partial_ends"] = ends

        def visible(stored: Optional[Dict[str, Any]]) -> bool:
            ends = (stored or {}).get("partial_ends") or {}
            for text in pending:
                mark = ends.get(text) or {}
                if not self._flat_dead_mark(mark, text):
                    return False
            return True

        if not await self._partial_cas(executor_id, mutate, visible):
            return False
        if not all(self._dead_end_visible(executor_id, text) for text in pending):
            return False
        bag = self.__dict__.setdefault("_unconfirmed_dead_ends", {}).setdefault(executor_id, set())
        bag.update(pending)
        if not await self._retract_unconfirmed_dead_ends(executor_id, executor):
            return False
        return all(
            self._own_number_still_dead(executor, text) and self._dead_end_visible(executor_id, text)
            for text in pending
        )

    def _maybe_enter_shutdown(self, executor_id: str, executor: Any) -> bool:
        """Метка и доказанная продажа гашения: один раз войти в завершение."""
        if getattr(executor, "_stop_entered", False):
            return False
        if not self._stop_mark_now(executor_id):
            return False
        if not getattr(executor, "_shutdown_close_proven", False):
            return False
        status = getattr(executor, "status", None)
        if getattr(status, "name", None) != "RUNNING":
            return False
        early = getattr(executor, "early_stop", None)
        if not callable(early):
            return False
        keep = bool((self.__dict__.get("_stop_keep_position") or {}).get(str(executor_id)))
        early(keep_position=keep)
        executor._stop_entered = True
        return True

    async def _cancel_owned_rows(self, connector, row: Dict[str, Any]) -> None:
        venue = open_list_venue(type(connector).__name__ if connector is not None else "")
        request = exchange_open_request(venue or "", row.get("trading_pair") or "")
        if request is None or connector is None or not hasattr(connector, "_api_get"):
            return
        try:
            body = await connector._api_get(
                path_url=request["path"],
                params=request["params"],
                is_auth_required=True,
            )
        except Exception:
            return
        owned = set(ledger_order_ids() or [])
        checkpoint = row.get("checkpoint") or ""
        if not isinstance(body, list):
            return
        for raw in body:
            if not isinstance(raw, dict):
                continue
            text = str(raw.get("text") or "")
            if text and (text in owned or text in checkpoint):
                await self._delete_exchange_row(connector, raw)

    def _has_owner_id(self, order_id: str) -> bool:
        mark, _owner = self._journal_mark(order_id)
        if mark == "owned":
            return True
        if mark == "unread":
            return True
        if self._checkpoint_order_owner(order_id):
            return True
        return False

    def _close_seat_blocks(self, text: str) -> bool:
        """A held close blocks cancel. A ban without an owner id does not."""
        if not text:
            return False
        for executor in getattr(self, "_active_executors", {}).values():
            holds = {
                str(item.get("order_id"))
                for item in getattr(executor, "_close_holds", []) or []
                if isinstance(item, dict) and item.get("order_id")
            }
            if text in holds:
                return True
            bans = {str(item) for item in getattr(executor, "_close_ban_ids", set()) or set()}
            if text in bans and self._has_owner_id(text):
                return True
        return False

    def _close_delete_blocked(self, text: str) -> bool:
        """A held close, a ban, or an unproven prefix id must not be cancelled."""
        if not text:
            return False
        if _delivery_owns(text):
            return False
        for executor in self._active_executors.values():
            holds = {
                str(item.get("order_id"))
                for item in getattr(executor, "_close_holds", []) or []
                if isinstance(item, dict) and item.get("order_id")
            }
            bans = {str(item) for item in getattr(executor, "_close_ban_ids", set()) or set()}
            if text in holds or text in bans:
                return True
        if text in {str(item) for item in getattr(self, "_seen_prefix_ids", []) or []}:
            return True
        return False

    async def _delete_exchange_row(self, connector, raw: dict) -> bool:
        text = str(raw.get("text") or raw.get("order_id") or raw.get("client_order_id") or "")
        if self._prefix_delete_held(connector, text) or self._partial_text_protected(text):
            return False
        request = gate_delete_request(raw)
        if request is None or connector is None or not hasattr(connector, "_api_delete"):
            return False
        await connector._api_delete(
            path_url=request["path"],
            params=request["params"],
            is_auth_required=True,
        )
        return True

    async def _ledger_price_proofs(self):
        """(executor_id, order_id) pairs whose price ban survives without the file.

        Proof: the committed checkpoint of this very executor holds the order id
        with a price, and that price is in the pair's column price list. A flat
        price of another executor, a hold without a price, and an unreadable
        column are not proofs: the caller then keeps every old line.
        """
        if not self.db_manager:
            return None
        try:
            async with self.db_manager.get_session_context() as session:
                rows = await ExecutorRepository(session).get_checkpoint_rows()
        except Exception:
            return None
        parsed_rows = []
        pair_prices: Dict[str, set] = {}
        for row in rows or []:
            text = row.get("checkpoint") or ""
            parsed = checkpoint_master(text)
            if text and parsed is None:
                return None
            parsed_rows.append((row, parsed))
            prices = pair_prices.setdefault(row.get("trading_pair"), set())
            for hold in (parsed or {}).get("holds") or []:
                if hold.get("price") not in (None, ""):
                    prices.add(str(hold.get("price")))
            for saved in (parsed or {}).get("retained_orders") or []:
                if saved.get("price") not in (None, ""):
                    prices.add(str(saved.get("price")))
            for level in (parsed or {}).get("levels") or []:
                for key in ("open_order", "close_order"):
                    price = (level.get(key) or {}).get("price")
                    if price not in (None, ""):
                        prices.add(str(price))
        proofs = set()
        for row, parsed in parsed_rows:
            prices = pair_prices.get(row.get("trading_pair")) or set()
            for hold in (parsed or {}).get("holds") or []:
                order_id = hold.get("order_id")
                price = hold.get("price")
                if not order_id or price in (None, ""):
                    continue
                if str(price) in prices:
                    proofs.add((row.get("executor_id") or row.get("row_executor_id"), str(order_id)))
        return proofs

    async def _column_prices(self, pair: str):
        if not self.db_manager:
            return None
        try:
            async with self.db_manager.get_session_context() as session:
                rows = await ExecutorRepository(session).get_checkpoint_rows()
        except Exception:
            return None
        prices = []
        for row in rows:
            if row.get("trading_pair") != pair:
                continue
            if "price_checkpoint" not in row:
                return None
            if row.get("checkpoint") and checkpoint_master(row.get("checkpoint")) is None:
                return None
            parsed = row.get("price_checkpoint")
            if isinstance(parsed, str):
                parsed = checkpoint_master(parsed)
            fresh = self._tick_fresh(pair)
            feed = self._parsed_without_dead(parsed, fresh) if self._lists_ready(fresh) else parsed
            for hold in (feed or {}).get("holds") or []:
                if hold.get("price") in (None, ""):
                    continue
                hold_id = str(hold.get("order_id") or "")
                if hold_id and order_is_unsent(hold_id):
                    continue
                prices.append((hold_id, hold.get("price")))
            for saved in (feed or {}).get("retained_orders") or []:
                if saved.get("price") in (None, ""):
                    continue
                saved_id = str(saved.get("order_id") or "")
                if saved_id and order_is_unsent(saved_id):
                    continue
                prices.append((saved_id, saved.get("price")))
            for item in partial_column_prices(feed, None):
                if item not in (None, ""):
                    prices.append(item)
        return prices

    def _owner_rows(self) -> List[Dict[str, Any]]:
        """Each row keeps its own account. The caller's account is not stamped on the others."""
        rows = []
        seen = set()
        for executor_id, executor in self._active_executors.items():
            meta = self._executor_metadata.get(executor_id) or {}
            config = getattr(executor, "config", None)
            row = {
                "executor_id": executor_id,
                "account": meta.get("account_name"),
                "connector": meta.get("connector_name") or getattr(config, "connector_name", None),
                "pair": meta.get("trading_pair") or getattr(config, "trading_pair", None),
            }
            rows.append(row)
            seen.add(executor_id)
        for source in list(getattr(self, "_resume_rows", []) or []):
            executor_id = source.get("executor_id")
            if not executor_id or executor_id in seen:
                continue
            rows.append({
                "executor_id": executor_id,
                "account": source.get("account_name"),
                "connector": source.get("connector_name"),
                "pair": source.get("trading_pair"),
            })
            seen.add(executor_id)
        owed = owed_records() or []
        for item in owed:
            executor_id = item.get("executor_id")
            if not executor_id or executor_id in seen:
                continue
            rows.append({
                "executor_id": executor_id,
                "account": item.get("account_name"),
                "connector": item.get("connector"),
                "pair": item.get("trading_pair"),
            })
        return rows

    def _owner_for_client(self, client_id: str, account: str, connector: str, pair: str):
        owed = owed_records()
        if owed is not None:
            for item in owed:
                if str(item.get("order_id") or "") != str(client_id):
                    continue
                if (
                    item.get("account_name") != account
                    or item.get("connector") != connector
                    or item.get("trading_pair") != pair
                ):
                    return None
                return item.get("executor_id")
        return pick_owner(self._owner_rows(), account, connector, pair)

    def _owner_for_pair(self, account: str, connector: str, pair: str):
        return pick_owner(self._owner_rows(), account, connector, pair)

    def _row_identity(self, row: Dict[str, Any]):
        if row.get("executor_id"):
            return row.get("executor_id")
        return row.get("row_executor_id")

    def _checkpoint_unread(self, pair: Optional[str] = None) -> bool:
        rows = getattr(self, "_hold_rows", None)
        if rows is None:
            return True
        for entry in self._hold_index():
            if pair and entry["pair"] not in (None, pair):
                continue
            if entry["unread"]:
                return True
        return False

    def _checkpoint_order_owner(self, order_id: str):
        """Owner id from a saved checkpoint number, before the one-live rule."""
        if self._checkpoint_unread():
            return None
        wanted = str(order_id)
        for entry in self._hold_index():
            if entry["unread"]:
                continue
            if wanted not in entry["owner_ids"]:
                continue
            identity = entry["identity"]
            if identity:
                return identity
        return None

    def _pending_ticks_live(self) -> list:
        """Pending take-profit ticks whose number is not a durable miss."""
        items = []
        for item in (self.__dict__.get("_partial_pending_ticks") or set()):
            if isinstance(item, tuple) and len(item) == 2 and order_is_unsent(str(item[0] or "")):
                continue
            items.append(item)
        return items

    def _ban_pairs(self, pair: str, fresh: Optional[Dict[str, Any]]):
        """Занятые цены этого тика с их номерами (правило номера: чужой текст и
        доказанная история не держат, голая цена и наш живой номер — держат)."""
        items = []
        for raw in list((fresh or {}).get("opens") or []) + list((fresh or {}).get("finished") or []):
            text = str(raw.get("text") or raw.get("order_id") or "")
            price = raw.get("price")
            if price in (None, ""):
                continue
            if not self._text_holds(text, fresh):
                continue
            items.append((text, price) if text else str(price))
        try:
            from utils.executor_checkpoint import read_ledger_filtered
            ledger = read_ledger_filtered()
        except Exception:
            ledger = None
        if ledger is not None:
            for row in ledger:
                order_id = str(row.get("order_id") or "")
                if not order_id:
                    continue
                if row.get("price") not in (None, "") and self._text_holds(order_id, fresh):
                    items.append((order_id, row.get("price")))
                if row.get("take_profit_price") not in (None, "") and self._take_holds(order_id, fresh):
                    items.append((order_id, row.get("take_profit_price")))
        for item in owed_records() or []:
            if order_is_unsent(str(item.get("order_id") or "")):
                continue
            if item.get("trading_pair") not in (None, "", pair):
                continue
            if item.get("price") not in (None, "") and self._text_holds(item.get("order_id"), fresh):
                items.append((str(item.get("order_id") or ""), item.get("price")))
        for entry in self._hold_index():
            if entry["pair"] not in (None, "", pair):
                continue
            for hold_id, price in entry["ban_holds"]:
                if order_is_unsent(hold_id):
                    continue
                if not self._text_holds(hold_id, fresh):
                    continue
                items.append((hold_id, price))
        return items

    def _feed_prices(self, rows, parsed, fresh):
        """Live numbers stay out of both the row list and the released set."""
        dead = [
            row for row in rows or []
            if row.get("order_id") and self._proven_dead(str(row.get("order_id")), fresh)
        ]
        dead_ids = {str(row.get("order_id")) for row in dead}
        return partial_feed_prices(dead, self._parsed_without_dead(parsed, fresh), dead_ids)

    def _remember_unwritten_hold(self, owner, order_id, price) -> bool:
        if not owner or not order_id or price in (None, ""):
            return False
        wrote = append_durable_row({
            "order_id": str(order_id),
            "executor_id": owner,
            "price": price,
            "ts": time.time(),
        })
        return wrote is True

    def _raw_unowned_ids(self):
        try:
            return ledger_unowned_ids()
        except Exception:
            return set()

    def _journal_mark(self, order_id: str):
        """owned, unowned, absent, or unread. A line without executor_id is not an owner."""
        try:
            return ledger_journal_mark(order_id)
        except Exception:
            return "unread", None

    def _owed_owner(self, order_id: str, account: str, connector: str, pair: str):
        owed = owed_records()
        if owed is None:
            return "unread", None
        for item in owed:
            if str(item.get("order_id") or "") != str(order_id):
                continue
            if (
                item.get("account_name") not in (None, account)
                or item.get("connector") not in (None, connector)
                or item.get("trading_pair") not in (None, pair)
            ):
                continue
            if item.get("executor_id"):
                return "owned", item.get("executor_id")
            return "unowned", None
        return "absent", None

    def _known_owner(self, order_id: str, account: str, connector: str, pair: str):
        """Id only. A journal line without executor_id does not close the miss."""
        column = self._checkpoint_order_owner(order_id)
        if column:
            return column
        mark, owner = self._journal_mark(order_id)
        if mark == "owned":
            return owner
        mark, owner = self._owed_owner(order_id, account, connector, pair)
        if mark == "owned":
            return owner
        return None

    def _live_ids(self, account: str, connector: str, pair: str) -> List[str]:
        found = []
        for executor_id, executor in self._active_executors.items():
            meta = self._executor_metadata.get(executor_id) or {}
            config = getattr(executor, "config", None)
            acc = meta.get("account_name")
            conn = meta.get("connector_name") or getattr(config, "connector_name", None)
            pr = meta.get("trading_pair") or getattr(config, "trading_pair", None)
            if acc == account and conn == connector and pr == pair:
                found.append(executor_id)
        return found

    def _sole_live_owner(self, order_id: str, account: str, connector: str, pair: str):
        """One live executor only when the number is in neither journal, checkpoint, nor owed."""
        if self._checkpoint_unread(pair):
            return None
        if self._checkpoint_order_owner(order_id):
            return None
        mark, _owner = self._journal_mark(order_id)
        if mark != "absent":
            return None
        owed_mark, _owed = self._owed_owner(order_id, account, connector, pair)
        if owed_mark != "absent":
            return None
        lives = self._live_ids(account, connector, pair)
        if len(lives) == 1:
            return lives[0]
        return None

    def _tick_fresh(self, pair: str):
        slot = (self.__dict__.get("_tick_reads") or {}).get(pair)
        if not slot or slot.get("tick") != self.__dict__.get("_price_tick"):
            return None
        return slot.get("fresh")

    def _pair_book(self, pair: str) -> Optional[Dict[str, Any]]:
        """Книга в слоте пары: концу 51400 и продаже остатка. Не через общий номер шага.

        Отмена или ошибка чтения старый слот не отдаёт. Идущее чтение той же
        пары готовой книге в слоте не помеха (факт плана okx-closed-list-ready):
        с сетками одной пары чтения идут без перерыва, и отказ из-за идущего
        чтения не даёт записаться никогда. Готовность книги проверяет зовущий.
        """
        if not pair or pair in (self.__dict__.get("_fresh_blocked") or set()):
            return None
        slot = (self.__dict__.get("_tick_reads") or {}).get(pair)
        fresh = slot.get("fresh") if isinstance(slot, dict) else None
        return fresh if isinstance(fresh, dict) else None

    def _remember_tick_fresh(self, pair: str, fresh: Dict[str, Any]) -> None:
        tick = self.__dict__.get("_price_tick")
        if tick is None:
            tick = 1
            self.__dict__["_price_tick"] = tick
        self.__dict__.setdefault("_tick_reads", {})[pair] = {"tick": tick, "fresh": fresh}

    async def _store_tick_fresh(self, connector, pair: str):
        self.__dict__["_price_tick"] = self.__dict__.get("_price_tick", 0) + 1
        stored = False
        try:
            fresh = await self._fresh_raw_lists(connector, pair, None)
            self._remember_tick_fresh(pair, fresh)
            stored = True
            self.__dict__.setdefault("_fresh_blocked", set()).discard(pair)
            return fresh
        finally:
            if not stored:
                self.__dict__.setdefault("_fresh_blocked", set()).add(pair)

    def _lists_ready(self, fresh: Optional[Dict[str, Any]]) -> bool:
        if not fresh:
            return False
        return bool(fresh.get("opens_read") and fresh.get("opens_done") and fresh.get("finished_read") and fresh.get("finished_done"))

    def _silent_exit_flags(self, fresh: Optional[Dict[str, Any]]) -> str:
        """False list flags of this tick, or the missing tick read itself."""
        if not isinstance(fresh, dict):
            return "tick read missing"
        unread = [name for name in ("opens_read", "opens_done", "finished_read", "finished_done") if not fresh.get(name)]
        return "unread " + " ".join(unread) if unread else "unread"

    def _log_silent_exit(self, place: str, pair: str, detail: str, executor_id: str = "", *, interval: float = 5.0, label: str = "silent buy exit:", key: Optional[str] = None) -> None:
        """One throttled WARNING per exit place and key; a stopped buy names itself."""
        try:
            now = time.monotonic()
            slot = (place, str(key if key is not None else pair or ""))
            last = self.__dict__.get("_silent_exit_logged") or {}
            if now - float(last.get(slot, 0.0)) < float(interval):
                return
            self.__dict__["_silent_exit_logged"] = {**last, slot: now}
            logger.warning(
                "%s %s pair=%s%s %s",
                label, place, pair or "",
                f" executor={executor_id}" if executor_id else "",
                detail,
            )
        except Exception:
            return

    def _proven_dead(self, order_id: str, fresh: Optional[Dict[str, Any]]) -> bool:
        if not self._lists_ready(fresh):
            return False
        text = str(order_id)
        for raw in fresh.get("opens") or []:
            if str(raw.get("text") or raw.get("order_id") or "") == text:
                return False
        for raw in fresh.get("finished") or []:
            if str(raw.get("text") or raw.get("order_id") or "") == text:
                return True
        return False

    def _own_text(self, text: str) -> bool:
        """Наш номер (правило номера, план number-rule): журнал, чекпоинт или
        трекер коннектора его чеканили. Неважно, жив ли его исполнитель."""
        name = str(text or "")
        if not name:
            return False
        try:
            from utils.executor_checkpoint import ledger_order_ids
            ids = ledger_order_ids()
        except Exception:
            ids = None
        if ids and name in ids:
            return True
        for entry in self._hold_index():
            if name in entry["owner_ids"] or name in entry["level_close_ids"]:
                return True
            if name in entry["close_price_map"] or name in entry["close_hold_price_map"]:
                return True
            for hold_id, _price in entry["ban_holds"]:
                if str(hold_id) == name:
                    return True
        return False

    def _take_holds(self, order_id: str, fresh: Optional[Dict[str, Any]]) -> bool:
        """Тейк держит quant, пока позиция не закрыта (план number-rule, п.2 и п.7):
        живой или неизвестный номер ещё не кончил держание; отмена без позиции
        не держит. Исполненный держит, только пока позиция номера открыта. Продажа уровня,
        доказанная finished и покрывшая покупку, отпускает тейк и когда слот уже снят:
        считается строка журнала того же уровня, не только номер в слоте. Пока такой продажи
        в finished нет, тейк держит (вердикт 20261014-031325-47150)."""
        name = str(order_id or "")
        if not name:
            return False
        if not self._proven_dead(name, fresh):
            return True
        if not self._proven_dead_filled(name, fresh):
            return False
        return self._number_position_open(name, fresh)

    def _number_position_open(self, order_id: str, fresh: Optional[Dict[str, Any]]) -> bool:
        """Позиция номера открыта, пока продажа её уровня не доказана. Слот — как
        `_level_position_open`. Слот уже снят гашением — продажа того же уровня из журнала. Уровень не
        найден или продажа не доказана — позиция считается открытой."""
        level, body = self._level_of_number(order_id)
        if level is None:
            return True
        if not self._level_position_open(level, body, fresh):
            return False
        return not self._journal_sell_covered(order_id, level, fresh)

    def _journal_sell_covered(self, order_id: str, level: Dict[str, Any], fresh: Optional[Dict[str, Any]]) -> bool:
        """Продажа этой покупки без слота: строка журнала role=close или market_close того же
        level_id и того же исполнителя, не старше строки покупки. Продажа прошлого круга и чужого
        исполнителя покупку не покрывает. Finished доказал исполнение, база покрыла partial_bought."""
        level_id = str(level.get("id") or "")
        bought = partial_decimal(level.get("partial_bought"))
        if not level_id or bought is None or bought <= 0:
            return False
        rows = self._partial_rows()
        if not rows:
            return False
        buy = None
        for row in rows:
            if str(row.get("order_id") or "") == str(order_id or ""):
                buy = row
                break
        owner = str((buy or {}).get("executor_id") or "")
        buy_ts = partial_decimal((buy or {}).get("ts"))
        if not owner or buy_ts is None:
            return False
        sold = Decimal("0")
        seen = set()
        for row in rows:
            if str(row.get("level_id") or "") != level_id:
                continue
            if str(row.get("executor_id") or "") != owner:
                continue
            if str(row.get("role") or "") not in ("close", "market_close"):
                continue
            sell_ts = partial_decimal(row.get("ts"))
            if sell_ts is None or sell_ts < buy_ts:
                continue
            name = str(row.get("order_id") or "")
            if not name or name in seen:
                continue
            seen.add(name)
            finished = self._finished_row(name, fresh)
            if finished is None or not self._closed_row_filled(finished):
                continue
            sold += self._shutdown_sold_base(finished)
        return sold >= bought

    def _level_of_number(self, order_id: str):
        """Уровень чекпоинта, которому принадлежит номер: по слоту уровня, иначе по
        level_id журнальной строки. Не нашли — (None, None)."""
        name = str(order_id or "")
        bodies = []
        for row in getattr(self, "_hold_rows", None) or []:
            raw = row.get("checkpoint")
            body = raw if isinstance(raw, dict) else checkpoint_view(raw)
            if isinstance(body, dict):
                bodies.append(body)
        for body in bodies:
            for level in body.get("levels") or []:
                if not isinstance(level, dict):
                    continue
                ids = [
                    str((level.get("open_order") or {}).get("order_id") or ""),
                    str((level.get("close_order") or {}).get("order_id") or ""),
                ]
                ids.extend(
                    str(item.get("order_id") or "")
                    for item in level.get("partial_closes") or []
                    if isinstance(item, dict)
                )
                if name and name in ids:
                    return level, body
        level_id = ""
        for row in self._partial_rows() or []:
            if str(row.get("order_id") or "") == name and row.get("level_id"):
                level_id = str(row.get("level_id"))
                break
        if level_id:
            for body in bodies:
                for level in body.get("levels") or []:
                    if isinstance(level, dict) and str(level.get("id") or "") == level_id:
                        return level, body
        return None, None

    def _quant_held(
        self, executor_id: str, executor: Any, connector: Any, pair: str, quant: Any, own_id: str,
        fresh: Optional[Dict[str, Any]] = None, inflight: Any = None, lost: Any = None,
    ) -> bool:
        """Quant занят (правило номера) — ОДНА сверка для ворот отправки и для
        чеканки/отправки закрытия: голая цена книги, номер трекера, тейк
        открытой позиции и наш живой номер держат; чужой текст с номером и
        доказанная история — нет (вердикт 20261013-192315-55275)."""
        if fresh is None:
            fresh = self._tick_fresh(pair or "")
        fresh = fresh or {}
        if inflight is None:
            inflight = getattr(connector, "in_flight_orders", None) or {}
        if lost is None:
            lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        if self._partial_tick_taken(connector, pair, quant, own_id, fresh, inflight, lost):
            return True
        if self._partial_foreign_booked(executor_id, connector, pair, quant, own_id, fresh):
            return True
        if self._close_tick_taken(executor, quant, own_id):
            return True
        return self._ledger_close_price_taken(quant, own_id, fresh)

    def _close_quant_held(self, executor_id: str, executor: Any, price: Any, own_id: Any = "") -> bool:
        """Та же занятость для хука `_refuse_close_price` (чеканка закрытия)."""
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        return self._quant_held(
            executor_id, executor, connector, pair, price, str(own_id or ""), self._tick_fresh(pair),
        )

    def _take_map(self) -> dict:
        """Тейки номеров из журнала: пара (номер, цена) — либо цена номера, либо
        его тейк, и держат они по разным правилам."""
        found = {}
        for row in self._partial_rows() or []:
            order_id = str(row.get("order_id") or "")
            if order_id and order_id not in found and row.get("take_profit_price") not in (None, ""):
                found[order_id] = row.get("take_profit_price")
        return found

    def _pair_holds(self, order_id: Any, price: Any, fresh: Optional[Dict[str, Any]], takes: Any = None) -> bool:
        """Пара (номер, цена) держит quant: голая пара без номера — как голая
        цена; чужой текст — нет. Цена номера держит, пока `_text_holds`; его
        тейк — пока `_take_holds`: цена самой исполненной покупки — история и
        quant не держит, её тейк держит позицию (вердикт 20261013-201526-2866)."""
        name = str(order_id or "")
        if not name:
            return True
        if not self._own_text(name):
            return False
        if takes is None:
            takes = self._take_map()
        take = str(takes.get(name) or "")
        if take and str(price) == take:
            return self._take_holds(name, fresh)
        return self._text_holds(name, fresh)

    def _stored_block_prices(self, stored: Any, fresh: Optional[Dict[str, Any]]) -> list:
        """Список блокировки из `stored`: пара проходит, пока её держит
        `_pair_holds`; отпущенная `_text_holds` пара в блокировку не попадает."""
        takes = self._take_map()
        kept = []
        for item in stored or []:
            if isinstance(item, tuple) and len(item) == 2:
                if not self._pair_holds(item[0], item[1], fresh, takes):
                    continue
            kept.append(item)
        return kept

    def _text_holds(self, text: Any, fresh: Optional[Dict[str, Any]], extra_own: Any = None) -> bool:
        """Держит ли запись с этим текстом цену для нового номера (правило
        номера): голый текст (номер не прочитан) — держит и не присваивается;
        наш номер — пока finished не доказал именно эту заявку исполненной или
        снятой; чужой номер с прочитанным текстом — не держит и не клеится."""
        name = str(text or "")
        if not name:
            return True
        if self._proven_dead(name, fresh):
            return False
        if extra_own is not None and name in extra_own:
            return True
        return self._own_text(name)

    def _finished_row(self, order_id: str, fresh: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Строка finished этого номера. Живой и неизвестный номер её не имеют."""
        if not self._lists_ready(fresh):
            return None
        text = str(order_id)
        for raw in fresh.get("opens") or []:
            if str(raw.get("text") or raw.get("order_id") or "") == text:
                return None
        for raw in fresh.get("finished") or []:
            if str(raw.get("text") or raw.get("order_id") or "") == text:
                return raw
        return None

    def _proven_dead_filled(self, order_id: str, fresh: Optional[Dict[str, Any]]) -> bool:
        """Доказанно мёртв и исполнен: у уровня открыта позиция, тейк жив."""
        row = self._finished_row(order_id, fresh)
        return row is not None and self._closed_row_filled(row)

    def _proven_dead_zero(self, order_id: str, fresh: Optional[Dict[str, Any]]) -> bool:
        """Отмена с нулевой базой. Пустые amount/left — не ноль."""
        row = self._finished_row(order_id, fresh)
        return row is not None and gate_base_is_zero(row)

    def _parsed_without_dead(self, parsed, fresh):
        import copy
        if not isinstance(parsed, dict) or not self._lists_ready(fresh):
            return parsed
        dead = False
        for level in parsed.get("levels") or []:
            if not isinstance(level, dict):
                continue
            for key in ("open_order", "close_order"):
                slot = level.get(key) or {}
                if isinstance(slot, dict) and slot.get("order_id") and self._proven_dead(str(slot.get("order_id")), fresh):
                    dead = True
                    break
            if dead:
                break
        if not dead:
            for item in parsed.get("holds") or []:
                if isinstance(item, dict) and self._proven_dead(str(item.get("order_id") or ""), fresh):
                    dead = True
                    break
        if not dead:
            return parsed
        note_checkpoint_copy()
        body = copy.deepcopy(parsed)
        levels = []
        for level in body.get("levels") or []:
            if not isinstance(level, dict):
                levels.append(level)
                continue
            copy_level = dict(level)
            for key in ("open_order", "close_order"):
                slot = copy_level.get(key) or {}
                if isinstance(slot, dict) and slot.get("order_id") and self._proven_dead(str(slot.get("order_id")), fresh):
                    copy_level[key] = {}
                    # Исполненная покупка держит тейк-тик уровня (позиция открыта):
                    # цена take-profit не отпущена и остаётся в фиде постановки.
                    if (
                        key == "open_order"
                        and not self._proven_dead_filled(str(slot.get("order_id")), fresh)
                        and not self._level_position_open(copy_level, body, fresh)
                    ):
                        copy_level["take_profit_price"] = None
            levels.append(copy_level)
        body["levels"] = levels
        body["holds"] = [
            item for item in (body.get("holds") or [])
            if not (isinstance(item, dict) and self._proven_dead(str(item.get("order_id") or ""), fresh))
        ]
        return body

    def _drop_dead_pending(self, pair: str) -> None:
        fresh = self._tick_fresh(pair)
        if not self._lists_ready(fresh):
            return
        pending = self.__dict__.get("_partial_pending_ticks")
        if not pending:
            return
        kept = set()
        held = set()
        for item in list(pending):
            if isinstance(item, tuple) and len(item) == 2:
                order_id, price = item
                if self._proven_dead(order_id, fresh):
                    continue
                held.add(str(price))
                kept.add(item)
            else:
                kept.add(item)
                held.add(str(item))
        dead_prices = set()
        for item in list(pending):
            if isinstance(item, tuple) and len(item) == 2 and self._proven_dead(item[0], fresh):
                if str(item[1]) not in held:
                    dead_prices.add(str(item[1]))
        pending.clear()
        pending.update(kept)
        for price in dead_prices:
            pending.discard(price)

    def _closed_row_filled(self, raw: Dict[str, Any]) -> bool:
        """A closed row is an execution: a filled state or a non-zero fill."""
        if str(raw.get("state") or "") == "filled":
            return True
        if str(raw.get("finish_as") or "").lower() == "filled":
            return True
        try:
            if float(str(raw.get("fill_size") or "0")) > 0:
                return True
        except (TypeError, ValueError):
            pass
        try:
            amount = float(str(raw.get("amount") or ""))
            left = float(str(raw.get("left") or ""))
            if amount - left > 0:
                return True
        except (TypeError, ValueError):
            pass
        return False

    def _prices_for_blocks(self, items, fresh) -> List[Any]:
        """Price strings only. Пара (номер, цена) держит, пока держит `_pair_holds`:
        цена номера — пока `_text_holds`, его тейк — пока `_take_holds`; чужой
        текст и голая пара без номера — как раньше."""
        takes = self._take_map()
        live_prices = set()
        for item in items or []:
            if isinstance(item, tuple) and len(item) == 2:
                order_id, price = item
                if price in (None, "") or isinstance(price, (tuple, dict)):
                    continue
                if self._pair_holds(order_id, price, fresh, takes):
                    live_prices.add(str(price))
            elif not isinstance(item, (tuple, dict)) and item not in (None, ""):
                live_prices.add(str(item))
        out = []
        seen = set()
        for item in items or []:
            if isinstance(item, tuple) and len(item) == 2:
                order_id, price = item
                if price in (None, "") or isinstance(price, (tuple, dict)):
                    continue
                if not self._pair_holds(order_id, price, fresh, takes) and str(price) not in live_prices:
                    continue
                value = price
            elif isinstance(item, dict) or item in (None, ""):
                continue
            else:
                value = item
            key = str(value)
            if key in seen:
                continue
            seen.add(key)
            out.append(value)
        return out

    def _price_only(self, items) -> List[Any]:
        prices = []
        for item in items or []:
            if isinstance(item, tuple):
                if len(item) != 2 or item[1] in (None, ""):
                    continue
                prices.append(item[1])
            elif isinstance(item, dict) or item in (None, ""):
                continue
            else:
                prices.append(item)
        return prices

    async def _cancel_copied_open(self, connector, raw: dict, pair: str) -> bool:
        """True only from the existing cancel. An empty Gate request is not success."""
        request = gate_delete_request(raw)
        if request is not None:
            try:
                deleted = await self._delete_exchange_row(connector, raw)
            except Exception:
                return False
            return deleted is True
        text = str(raw.get("text") or raw.get("order_id") or raw.get("client_order_id") or "")
        if text and text in self._close_cancel_ids():
            return False
        if not text or connector is None or not hasattr(connector, "_place_cancel"):
            return False
        tracked = type("TrackedCancel", (), {})()
        tracked.exchange_order_id = ""
        tracked.client_order_id = text
        tracked.trading_pair = pair
        try:
            result = await connector._place_cancel(text, tracked)
        except Exception:
            return False
        return result is True

    def _mark_unresolved(self, order_id: str) -> None:
        self.__dict__.setdefault("_unresolved_opens", set()).add(str(order_id))

    def _clear_unresolved(self, order_id: str) -> None:
        self.__dict__.setdefault("_unresolved_opens", set()).discard(str(order_id))

    def _buy_waiting(self) -> bool:
        return bool(self.__dict__.get("_unresolved_opens"))

    def _release_gone(self, fresh: Optional[Dict[str, Any]]) -> None:
        """A number that left the open list is not an unfinished cancel."""
        if not self._lists_ready(fresh):
            return
        opens = {
            str(raw.get("text") or raw.get("order_id") or "")
            for raw in (fresh.get("opens") or [])
        }
        bag = self.__dict__.get("_unresolved_opens")
        if not bag:
            return
        waiting_hold = self.__dict__.get("_hold_pending") or {}
        for order_id in list(bag):
            if order_id in waiting_hold:
                continue
            if order_id not in opens:
                bag.discard(order_id)

    def _text_is_owned(self, text: str, connector, column_ids) -> bool:
        if not text:
            return False
        prefix = getattr(connector, "client_order_id_prefix", None)
        journal = ledger_order_ids()
        column = set() if column_ids is None else {str(item) for item in column_ids}
        owed_ids = {str(item.get("order_id")) for item in (owed_records() or []) if item.get("order_id")}
        if prefix and text.startswith(str(prefix)):
            return True
        if journal is not None and text in journal:
            return True
        return text in column or text in owed_ids

    def _owned_row_lacks_price(self, connector, fresh: Dict[str, Any], column_ids) -> bool:
        rows = list(fresh.get("opens") or []) + list(fresh.get("finished") or [])
        for raw in rows:
            text = str(raw.get("text") or "")
            if self._text_is_owned(text, connector, column_ids) and raw.get("price") in (None, ""):
                return True
        return False

    def _has_owned_text(self, connector, fresh: Dict[str, Any], column_ids) -> bool:
        rows = list(fresh.get("opens") or []) + list(fresh.get("finished") or [])
        return any(self._text_is_owned(str(raw.get("text") or ""), connector, column_ids) for raw in rows)

    def _column_hold_lacks_price(self, pair: str) -> bool:
        rows = getattr(self, "_hold_rows", None)
        if rows is None:
            return True
        for row in rows:
            if row.get("trading_pair") != pair:
                continue
            parsed = checkpoint_master(row.get("checkpoint"))
            if row.get("checkpoint") and parsed is None:
                return True
            fresh = self._tick_fresh(pair)
            for hold in (parsed or {}).get("holds") or []:
                if not hold.get("order_id") or hold.get("price") not in (None, ""):
                    continue
                if not self._lists_ready(fresh) or not self._proven_dead(str(hold.get("order_id")), fresh):
                    return True
        return False

    def _remember_seen_texts(self, connector, fresh: Dict[str, Any], column_ids, pair: str) -> None:
        seen = self.__dict__.setdefault("_seen_client_ids", {}).setdefault(pair, set())
        for raw in list(fresh.get("opens") or []) + list(fresh.get("finished") or []):
            text = str(raw.get("text") or "")
            if self._text_is_owned(text, connector, column_ids):
                seen.add(text)

    def _seen_prefix_unrecorded(self, connector, fresh: Dict[str, Any], column_ids, pair: str):
        """A prefix we already saw, with no hold and no fresh row, is not an empty book.

        The same number that recorded it excuses it: text, or order_id when text is
        empty. A number in durable miss never left; a number this tick proved dead
        stops waiting. Returns the offending number, or a falsy value.
        """
        journal = ledger_order_ids() or set()
        column = set() if column_ids is None else {str(item) for item in column_ids}
        visible = {
            str(raw.get("text") or raw.get("order_id") or "")
            for raw in list(fresh.get("opens") or []) + list(fresh.get("finished") or [])
        }
        seen_bag = self.__dict__.setdefault("_seen_client_ids", {}).get(pair) or set()
        if self._lists_ready(fresh):
            for text in list(seen_bag):
                if self._proven_dead(text, fresh):
                    seen_bag.discard(text)
        recorded = self._close_recorded_ids()
        for text in sorted(seen_bag):
            if order_is_unsent(text) or text in recorded:
                continue
            if text in journal or text in column or text in visible:
                continue
            return text
        return None

    async def _claim_visible_texts(self, executor_id, connector, pair, fresh, column_ids) -> bool:
        """A map miss does not return False. A buy waits until hold or cancel True."""
        account = (self._executor_metadata.get(executor_id) or {}).get("account_name")
        connector_name = (self._executor_metadata.get(executor_id) or {}).get("connector_name")
        claimed = self.__dict__.setdefault("_claimed_prices", [])
        seen = set(column_ids or [])
        known_commits = set()
        symbol = fresh.get("symbol")
        for raw in list(fresh.get("opens") or []) + list(fresh.get("finished") or []):
            text = str(raw.get("text") or raw.get("order_id") or "")
            if not text:
                continue
            mark = raw.get("currency_pair")
            if mark not in (None, "") and mark != symbol:
                continue
            if text in self._close_price_skip_ids() or text in self._close_cancel_ids():
                self.__dict__.setdefault("_seen_client_ids", {}).setdefault(pair, set()).add(text)
                self.__dict__.setdefault("_hold_pending", {}).pop(text, None)
                self._clear_unresolved(text)
                continue
            if not self._text_is_owned(text, connector, column_ids):
                continue
            if self._partial_text_protected(text):
                self._clear_unresolved(text)
                continue
            self.__dict__.setdefault("_seen_client_ids", {}).setdefault(pair, set()).add(text)
            price = raw.get("price")
            if self._checkpoint_unread(pair):
                self._mark_unresolved(text)
                continue
            known = self._known_owner(text, account, connector_name, pair)
            owner = known
            if owner is None:
                owner = self._sole_live_owner(text, account, connector_name, pair)
            sole = known is None and owner is not None
            if self._keeps_fresh_open(text, pair, known_commits):
                self._clear_unresolved(text)
                continue
            if owner and not sole and text not in seen:
                hold = {"order_id": text}
                if price not in (None, ""):
                    hold["price"] = str(price)
                wrote = await self._commit_price_hold(owner, text, price, extra=hold)
                if not wrote:
                    self.__dict__.setdefault("_hold_pending", {})[text] = {"owner": owner, "price": price}
                    self._remember_unwritten_hold(owner, text, price)
                    self._mark_unresolved(text)
                    if price not in (None, ""):
                        claimed.append((text, str(price)))
                    continue
                known_commits.add(text)
                seen.add(text)
                self.__dict__.setdefault("_hold_pending", {}).pop(text, None)
                self._clear_unresolved(text)
            elif owner is None and raw in (fresh.get("opens") or []):
                self._mark_unresolved(text)
            if price not in (None, "") and not self._proven_dead(text, fresh):
                claimed.append((text, str(price)))
            if raw not in (fresh.get("opens") or []):
                continue
            if owner and text not in seen and not sole:
                continue
            if self._keeps_fresh_open(text, pair, known_commits):
                self._clear_unresolved(text)
                continue
            if owner and text in seen:
                cancelled = await self._cancel_copied_open(connector, raw, pair)
                if not cancelled:
                    self._mark_unresolved(text)
                else:
                    self._clear_unresolved(text)
                continue
            cancelled = await self._cancel_copied_open(connector, raw, pair)
            if cancelled:
                self._clear_unresolved(text)
            else:
                self._mark_unresolved(text)
        return True

    def _known_acceptance_times(self, pair: str, fresh: Dict[str, Any]) -> List[float]:
        found = []

        def add(value):
            if value in (None, ""):
                return
            try:
                moment = float(value)
            except (TypeError, ValueError):
                return
            if moment > 1_000_000_000_000:
                moment = moment / 1000
            found.append(moment)

        for item in owed_records() or []:
            if item.get("trading_pair") in (None, pair):
                add(item.get("ts"))
        try:
            from utils.executor_checkpoint import read_ledger_filtered
            for row in read_ledger_filtered():
                add(row.get("ts"))
        except Exception:
            return []
        for row in getattr(self, "_hold_rows", None) or []:
            if row.get("trading_pair") not in (None, pair):
                continue
            parsed = checkpoint_master(row.get("checkpoint"))
            for hold in (parsed or {}).get("holds") or []:
                add(hold.get("ts"))
        for raw in list(fresh.get("opens") or []) + list(fresh.get("finished") or []):
            for key in ("create_time", "update_time", "ts"):
                add(raw.get(key))
        return found

    def _install_open_shell(self, executor_id: str, executor: ExecutorBase) -> None:
        """Живой шаг зовёт штатную постановку. Свой номер до возврата не чеканим."""
        if not hasattr(executor, "adjust_and_place_open_order"):
            return
        service = self

        original_adjust = executor.adjust_and_place_open_order

        def wrapped(level):
            # Живой шаг зовёт штатную постановку этого уровня. Уровни, которые
            # она сама ставит на одну цену, не удерживаем. Свой номер не чеканим.
            token = _placing_level.set(level)
            try:
                return original_adjust(level)
            finally:
                _placing_level.reset(token)

        executor.adjust_and_place_open_order = wrapped
        install_partial = getattr(self, "_install_partial_guards", None)
        if install_partial is not None:
            install_partial(executor_id, executor)
        original_stop = getattr(executor, "control_shutdown_process", None)

        async def stop_held(*args, **kwargs):
            # Метка кнопки не пропускает штатное гашение. Нулевая ликвидность
            # без метки по-прежнему не входит: ячейку не стираем.
            if service._stop_mark_now(executor_id):
                if original_stop is None:
                    return None
                return await original_stop(*args, **kwargs)
            if service._live_delivery_reads_zero_liquidity(executor_id, executor):
                return None
            if original_stop is None:
                return None
            return await original_stop(*args, **kwargs)

        if original_stop is not None:
            executor.control_shutdown_process = stop_held
        current_stop = getattr(executor, "stop", None)
        if current_stop is not None and not getattr(current_stop, "_delivery_stop_guard", False):

            if not getattr(executor, "_author_stop", None):
                executor._author_stop = current_stop
            def delivery_stop(*args, **kwargs):
                # Уже стоящий stop() при тормозе не ставит «закончено», пока доставка жива.
                if service._live_delivery_reads_zero_liquidity(executor_id, executor):
                    return None
                if (
                    service._stop_mark_now(executor_id)
                    and service._unproven_delivery_ids(executor_id, executor)
                ):
                    return None
                return current_stop(*args, **kwargs)

            delivery_stop._delivery_stop_guard = True
            executor.stop = delivery_stop
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        if connector_name != "okx_perpetual":
            return
        from utils.executor_checkpoint import install_unhold_guard
        texts = service.__dict__.setdefault("_unhold_text", {})
        install_unhold_guard(executor, lambda executor_id=executor_id: texts.get(executor_id))
        if hasattr(executor, "control_task"):
            original_task = executor.control_task

            async def task_after_visible_mark(*args, **kwargs):
                try:
                    await service._refresh_unhold(executor_id, executor)
                except Exception:
                    logger.exception("visible cancel mark was not read")
                try:
                    return await original_task(*args, **kwargs)
                finally:
                    texts.pop(executor_id, None)

            executor.control_task = task_after_visible_mark

    async def _refresh_unhold(self, executor_id: str, executor) -> None:
        """Read a new session, then release only a matching marked id."""
        from utils.executor_checkpoint import visible_cancel_ids, write_unhold
        if not getattr(self, "db_manager", None):
            return
        async with self.db_manager.get_session_context() as session:
            reader = session if hasattr(session, "get_executor_by_id") else ExecutorRepository(session)
            row = await reader.get_executor_by_id(executor_id)
        text = None if row is None else row.checkpoint
        self.__dict__.setdefault("_unhold_text", {})[executor_id] = text
        for level_id, _order_id in visible_cancel_ids(text):
            for level in getattr(executor, "grid_levels", []) or []:
                if str(getattr(level, "id", "") or "") == level_id:
                    await write_unhold(self.db_manager, executor_id, level_id, level)
        async with self.db_manager.get_session_context() as session:
            reader = session if hasattr(session, "get_executor_by_id") else ExecutorRepository(session)
            fresh = await reader.get_executor_by_id(executor_id)
        self.__dict__["_unhold_text"][executor_id] = None if fresh is None else fresh.checkpoint

    def _readable_sent(self):
        """None means the sent file cannot be read. An empty set is not that."""
        from utils.executor_checkpoint import _load_set_file, _sent_path
        return _load_set_file(_sent_path())

    def _stepped_aside(self, executor_id: str) -> bool:
        return str(executor_id) in (self.__dict__.get("_stepped_aside_ids") or set())

    def _seated_ids(self, executor) -> set:
        found = set()
        for level in list(getattr(executor, "grid_levels", None) or []):
            for slot in (
                getattr(level, "active_open_order", None),
                getattr(level, "active_close_order", None),
            ):
                order_id = str(getattr(slot, "order_id", "") or "")
                if order_id:
                    found.add(order_id)
        return found

    def _own_ids(self, executor_id: str) -> set:
        """Our journal and checkpoint texts. A foreign book row is not here."""
        found = set()
        rows = self._partial_rows()
        if rows is None:
            return found
        for row in rows:
            if str(row.get("executor_id") or "") not in ("", str(executor_id)):
                continue
            order_id = str(row.get("order_id") or "")
            if order_id and row.get("level_id"):
                found.add(order_id)
        for level in (self._partial_parsed(executor_id) or {}).get("levels") or []:
            if not isinstance(level, dict):
                continue
            for key in ("open_order", "close_order"):
                slot = level.get(key) or {}
                order_id = str(slot.get("order_id") or "")
                if order_id:
                    found.add(order_id)
        return found

    def _visible_own_unseated(self, executor_id, executor, connector, fresh) -> bool:
        """Confirmed own number is not in its slot. Do not hand the wheel."""
        if self._partial_rows() is None:
            return True
        seated = self._seated_ids(executor)
        for order_id in self._own_ids(executor_id):
            if self._id_still_live(fresh, connector, order_id) and order_id not in seated:
                return True
        return False

    def _level_visible_own(self, executor_id, level, connector, fresh) -> bool:
        level_id = str(getattr(level, "id", "") or "")
        if not level_id or self._partial_rows() is None:
            return False
        for row in self._partial_rows() or []:
            if str(row.get("executor_id") or "") != str(executor_id):
                continue
            if str(row.get("level_id") or "") != level_id:
                continue
            order_id = str(row.get("order_id") or "")
            if order_id and self._id_still_live(fresh, connector, order_id):
                return True
        return False

    def _unanswered_send_ids(self, executor_id, level, rows, fresh, connector) -> list:
        """Send marks the exchange has not shown. Price and tick are not required."""
        level_id = str(getattr(level, "id", "") or "")
        if not level_id or not rows:
            return []
        book = fresh if isinstance(fresh, dict) else {}
        visible = set()
        for raw in list(book.get("opens") or []) + list(book.get("finished") or []):
            visible.add(str(raw.get("text") or raw.get("order_id") or ""))
        visible.discard("")
        inflight = {str(item) for item in (getattr(connector, "in_flight_orders", None) or {})}
        lost = {
            str(item)
            for item in (getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {})
        }
        found = []
        for row in rows:
            order_id = str(row.get("order_id") or "")
            if not order_id or order_id in found:
                continue
            if str(row.get("executor_id") or "") != str(executor_id):
                continue
            if str(row.get("level_id") or "") != level_id:
                continue
            if not partial_send_marked(rows, order_id):
                continue
            if order_id in visible or order_id in inflight or order_id in lost:
                continue
            if self._proven_dead(order_id, book):
                continue
            found.append(order_id)
        return found

    def _own_level_id(self, executor_id: str, order_id: str) -> str:
        for row in self._partial_rows() or []:
            if str(row.get("order_id") or "") != str(order_id) or not row.get("level_id"):
                continue
            if str(row.get("executor_id") or "") not in ("", str(executor_id)):
                continue
            return str(row.get("level_id"))
        for level in (self._partial_parsed(executor_id) or {}).get("levels") or []:
            if not isinstance(level, dict):
                continue
            for key in ("open_order", "close_order"):
                slot = level.get(key) or {}
                if str(slot.get("order_id") or "") == str(order_id):
                    return str(level.get("id") or "")
        return ""

    def _visible_body(self, fresh, connector, order_id: str):
        for raw in list((fresh or {}).get("opens") or []):
            text = str(raw.get("text") or raw.get("order_id") or "")
            if text == str(order_id):
                return raw
        inflight = (getattr(connector, "in_flight_orders", None) or {}).get(order_id)
        if inflight is not None:
            return inflight
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        return lost.get(order_id)

    def _seat_visible_own(self, executor_id, executor, connector, fresh) -> None:
        """Seat our visible text by its side. A foreign text is not in this loop."""
        from utils.executor_checkpoint import _order_side
        if self._partial_rows() is None:
            return
        grid_side = getattr(getattr(getattr(executor, "config", None), "side", None), "name", "")
        close_side = "SELL" if grid_side == "BUY" else "BUY" if grid_side == "SELL" else ""
        for order_id in self._own_ids(executor_id):
            if order_id in self._seated_ids(executor) or not self._id_still_live(fresh, connector, order_id):
                continue
            level_id = self._own_level_id(executor_id, order_id)
            side = _order_side(self._visible_body(fresh, connector, order_id))
            if not level_id or side not in ("BUY", "SELL") or side not in (grid_side, close_side):
                continue
            close = side == close_side
            if self._slot_has_other(executor, level_id, close, order_id):
                continue
            if self._shown_fill_row(fresh, order_id, grid_side) is not None:
                continue
            self._seat_not_filled(executor, level_id, order_id, close)

    def _seat_filled_while_down(self, executor_id, executor, connector, fresh) -> set:
        """Seat our filled text before the stock step. A foreign price is not a seat."""
        blocked = set()
        if self._partial_rows() is None or not self._lists_ready(fresh):
            return blocked
        grid_side = getattr(getattr(getattr(executor, "config", None), "side", None), "name", "")
        if grid_side not in ("BUY", "SELL"):
            return blocked
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        leverage = getattr(getattr(executor, "config", None), "leverage", None) or 1
        parsed = self._partial_parsed(executor_id)
        released = set(partial_released_ids(parsed))
        released.update(self._partial_released_ids_for_pair(pair))
        from hummingbot.strategy_v2.models.executors import TrackedOrder
        for order_id in self._own_ids(executor_id):
            if not order_id or order_id in released or partial_end_visible(parsed, order_id):
                continue
            level_id = self._own_level_id(executor_id, order_id)
            if not level_id or partial_level_released(parsed, level_id):
                continue
            if self._id_still_live(fresh, connector, order_id):
                continue
            row = partial_find_text((fresh or {}).get("finished"), order_id)
            if not isinstance(row, dict) or str(row.get("text") or "") != str(order_id):
                continue
            if str(row.get("state") or "") != "filled":
                continue
            fill_size = partial_decimal(row.get("fill_size"))
            if fill_size is None or fill_size <= 0:
                continue
            if str(row.get("side") or "").upper() != grid_side:
                continue
            blocked.add(level_id)
            if self._slot_has_other(executor, level_id, False, order_id):
                continue
            price = partial_decimal(row.get("price"))
            if price is None or price <= 0 or not pair:
                continue
            opened = self._stock_filled_order(
                client_id=order_id,
                trading_pair=pair,
                trade_name=grid_side,
                position_name="OPEN",
                amount=fill_size,
                price=price,
                leverage=int(leverage),
            )
            if opened is None:
                continue
            for level in list(getattr(executor, "grid_levels", None) or []):
                if str(getattr(level, "id", "") or "") != level_id:
                    continue
                tracked = TrackedOrder(order_id=str(order_id))
                tracked.order = opened
                level.active_open_order = tracked
        return blocked

    def _checkpoint_open_ids(self, executor_id: str) -> list:
        """Свои покупки из записи. Журнал не нужен, чужая цена не входит."""
        found = []
        for level in (self._partial_parsed(executor_id) or {}).get("levels") or []:
            if not isinstance(level, dict):
                continue
            slot = level.get("open_order") or {}
            order_id = str(slot.get("order_id") or "")
            level_id = str(level.get("id") or "")
            if order_id and level_id:
                found.append((order_id, level_id))
        return found

    def _row_order_id(self, row: Dict[str, Any]) -> str:
        return str(row.get("text") or row.get("clOrdId") or row.get("order_id") or "")

    def _row_fill_size(self, row: Dict[str, Any]):
        return partial_decimal(row.get("fill_size") if row.get("fill_size") not in (None, "") else row.get("accFillSz"))

    def _row_fill_price(self, row: Dict[str, Any]):
        for key in ("price", "fillPx", "avgPx", "px"):
            if row.get(key) not in (None, ""):
                return partial_decimal(row.get(key))
        return None

    def _shown_fill_row(self, fresh: Optional[Dict[str, Any]], order_id: str, side: str) -> Optional[Dict[str, Any]]:
        """Строка исполнения этого номера. Непрочитанный список исполнением не становится."""
        if not isinstance(fresh, dict) or not fresh.get("finished_read") or not fresh.get("finished_done"):
            return None
        row = None
        lists = list(fresh.get("finished") or [])
        lists.extend(fresh.get("finished_blank_px") or [])
        for item in lists:
            if isinstance(item, dict) and self._row_order_id(item) == str(order_id):
                row = item
                break
        if not isinstance(row, dict) or str(row.get("state") or "") != "filled":
            return None
        shown_side = str(row.get("side") or "").upper()
        if shown_side and shown_side != side:
            return None
        size = self._row_fill_size(row)
        price = self._row_fill_price(row)
        if size is None or size <= 0 or price is None or price <= 0:
            return None
        return row

    async def _note_shown_buy(self, executor_id: str, level_id: str, order_id: str, row: Dict[str, Any]) -> bool:
        """Исполнение без своего номера продажи. Метка кнопки запись не запрещает."""
        size = self._row_fill_size(row)
        price = self._row_fill_price(row)

        def mutate(body: Dict[str, Any]) -> None:
            target = None
            for level in body.get("levels") or []:
                if isinstance(level, dict) and str(level.get("id") or "") == str(level_id):
                    target = level
                    break
            if target is None:
                raise RuntimeError("level missing")
            opened = dict(target.get("open_order") or {})
            if str(opened.get("order_id") or "") != str(order_id):
                raise RuntimeError("not our open")
            opened["filled"] = True
            opened["price"] = str(price)
            opened["order"] = {"executed_amount_base": str(size)}
            target["open_order"] = opened

        def visible(parsed: Optional[Dict[str, Any]]) -> bool:
            for level in (parsed or {}).get("levels") or []:
                if str(level.get("id") or "") != str(level_id):
                    continue
                opened = level.get("open_order") or {}
                body = opened.get("order") or {}
                return (
                    str(opened.get("order_id") or "") == str(order_id)
                    and opened.get("filled") is True
                    and str(body.get("executed_amount_base") or "") == str(size)
                    and not (level.get("close_order") or {}).get("order_id")
                )
            return False

        return await self._partial_cas(executor_id, mutate, visible, level_id)

    async def _note_shown_buys(self, executor_id: str, fresh: Optional[Dict[str, Any]]) -> None:
        side = "BUY"
        executor = (getattr(self, "_active_executors", None) or {}).get(executor_id)
        named = getattr(getattr(getattr(executor, "config", None), "side", None), "name", "")
        if named in ("BUY", "SELL"):
            side = named
        for order_id, level_id in self._checkpoint_open_ids(executor_id):
            row = self._shown_fill_row(fresh, order_id, side)
            if row is None:
                continue
            await self._note_shown_buy(executor_id, level_id, order_id, row)

    def _seat_shown_buys(self, executor_id: str, executor: Any, fresh: Optional[Dict[str, Any]]) -> None:
        """До их шага: исполнение на уровне, даже если номер ещё в полёте. Размер позиции не пишем."""
        side = getattr(getattr(getattr(executor, "config", None), "side", None), "name", "")
        if side not in ("BUY", "SELL"):
            return
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        leverage = getattr(getattr(executor, "config", None), "leverage", None) or 1
        if not pair:
            return
        from hummingbot.strategy_v2.models.executors import TrackedOrder
        for order_id, level_id in self._checkpoint_open_ids(executor_id):
            row = self._shown_fill_row(fresh, order_id, side)
            if row is None:
                continue
            opened = self._stock_filled_order(
                client_id=order_id,
                trading_pair=pair,
                trade_name=side,
                position_name="OPEN",
                amount=self._row_fill_size(row),
                price=self._row_fill_price(row),
                leverage=int(leverage),
            )
            if opened is None:
                continue
            for level in list(getattr(executor, "grid_levels", None) or []):
                if str(getattr(level, "id", "") or "") != level_id:
                    continue
                tracked = TrackedOrder(order_id=str(order_id))
                tracked.order = opened
                level.active_open_order = tracked

    def _filled_down_open(self, executor_id, executor, level) -> str:
        """Our filled open text in this slot. A foreign id is not this."""
        slot = getattr(level, "active_open_order", None)
        order_id = str(getattr(slot, "order_id", "") or "")
        order = getattr(slot, "order", None)
        if not order_id or not getattr(order, "is_filled", False):
            return ""
        if order_id not in self._own_ids(executor_id):
            return ""
        return order_id

    def _recorded_close_id(self, executor_id: str, level_id: str) -> str:
        for level in (self._partial_parsed(executor_id) or {}).get("levels") or []:
            if str(level.get("id") or "") != str(level_id):
                continue
            return str((level.get("close_order") or {}).get("order_id") or "")
        return ""

    def _level_close_recorded(self, executor_id: str, level: Any) -> str:
        """Номер продажи уровня в чекпоинте ИЛИ в журнале (правило номера,
        п.6): второй номер не чеканится — слать тот же или стоять. Журнал
        ловит и случай после рестарта, когда чекпоинт ещё не успел запись."""
        level_id = str(getattr(level, "id", "") or "")
        if not level_id:
            return ""
        recorded = self._recorded_close_id(executor_id, level_id)
        if recorded:
            return recorded
        for row in self._partial_rows() or []:
            if str(row.get("executor_id") or "") != str(executor_id):
                continue
            if str(row.get("level_id") or "") != level_id:
                continue
            if str(row.get("role") or "") not in ("close", "market_close"):
                continue
            order_id = str(row.get("order_id") or "")
            if order_id:
                return order_id
        return ""

    def _close_text_seen(self, fresh, connector, order_id: str):
        """True if the close is already on the book. None means the closed list is not proof."""
        if not self._lists_ready(fresh) or not fresh.get("finished_read") or not fresh.get("finished_done"):
            return None
        if self._id_still_live(fresh, connector, order_id):
            return True
        if partial_find_text((fresh or {}).get("finished"), order_id) is not None:
            return True
        return False

    async def _write_fill_down_close(
        self,
        executor_id: str,
        level_id: str,
        buy_id: str,
        buy_price,
        fill_size,
        close_id: str,
        quant: str,
    ) -> bool:
        """One compare-and-write. A failed write does not mean the id was kept."""
        def mutate(body: Dict[str, Any]) -> None:
            # Гонка CAS: метка в прочитанном теле — запись не меняется (п.2).
            if stop_mark_visible(body):
                raise RuntimeError("stop mark")
            levels = body.setdefault("levels", [])
            target = None
            for level in levels:
                if isinstance(level, dict) and str(level.get("id") or "") == str(level_id):
                    target = level
                    break
            if target is None:
                target = {"id": level_id}
                levels.append(target)
            existing = target.get("close_order") or {}
            if existing.get("order_id") and str(existing.get("order_id")) != str(close_id):
                raise RuntimeError("close exists")
            target["open_order"] = {
                "order_id": buy_id,
                "level_id": level_id,
                "filled": True,
                "price": str(buy_price),
                "order": {"executed_amount_base": str(fill_size)},
            }
            target["close_order"] = {
                "order_id": close_id,
                "level_id": level_id,
                "price": str(quant),
            }

        def visible(parsed) -> bool:
            for level in (parsed or {}).get("levels") or []:
                if str(level.get("id") or "") != str(level_id):
                    continue
                opened = level.get("open_order") or {}
                close = level.get("close_order") or {}
                return (
                    str(opened.get("order_id") or "") == str(buy_id)
                    and str(close.get("order_id") or "") == str(close_id)
                    and close.get("level_id")
                    and str(close.get("price") or "") == str(quant)
                )
            return False

        return await self._partial_cas(executor_id, mutate, visible, level_id)

    async def _one_sell_after_fill_down(self, executor_id, executor, connector, level) -> None:
        """Stock asked for the close. Record the id, then send that id once."""
        if await self._refresh_stop_mark(executor_id):
            return
        recorded = self._recorded_market_close(executor_id)
        if recorded is None or recorded:
            return
        level_id = str(getattr(level, "id", "") or "")
        buy_id = self._filled_down_open(executor_id, executor, level)
        if not level_id or not buy_id or connector is None:
            return
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        side = getattr(getattr(getattr(executor, "config", None), "side", None), "name", "")
        if side not in ("BUY", "SELL") or not pair:
            return
        fresh = self._tick_fresh(pair)
        # Правило номера, п.6: своя продажа уровня уже вычеканена (журнал или
        # чекпоинт) — второй номер не чеканить, слать тот же или стоять.
        close_id = self._level_close_recorded(executor_id, level)
        if close_id:
            if self._close_text_seen(fresh, connector, close_id) is not False:
                return
            quant = ""
            for item in (self._partial_parsed(executor_id) or {}).get("levels") or []:
                if str(item.get("id") or "") != level_id:
                    continue
                quant = str((item.get("close_order") or {}).get("price") or "")
            amount = getattr(getattr(getattr(level, "active_open_order", None), "order", None), "executed_amount_base", None)
            if not quant or amount in (None, "") or Decimal(str(amount)) <= 0:
                return
            order = getattr(getattr(level, "active_open_order", None), "order", None)
            send_amount = self._amount_from_contracts(connector, pair, order, amount)
            if send_amount is None:
                return
            if await self._refresh_stop_mark(executor_id):
                return
            await self._partial_send_committed(
                executor_id, executor, connector, pair, close_id, level_id, quant, send_amount,
                journal_ts=self._buy_journal_ts(buy_id),
            )
            return
        once = self.__dict__.setdefault("_fill_down_close_once", set())
        key = (str(executor_id), level_id)
        if key in once:
            return
        quant = partial_take_profit_quant(
            connector, pair, getattr(level, "price", None), getattr(level, "take_profit", None), side,
        )
        amount = getattr(getattr(getattr(level, "active_open_order", None), "order", None), "executed_amount_base", None)
        price = getattr(getattr(getattr(level, "active_open_order", None), "order", None), "price", None)
        if not quant or amount in (None, "") or Decimal(str(amount)) <= 0 or price in (None, ""):
            return
        if await self._refresh_stop_mark(executor_id):
            return
        # Правило номера: занятость — та же, что у ворот отправки (вердикт
        # 20261013-192315-55275): quant занят — второй номер не чеканить.
        if self._quant_held(executor_id, executor, connector, pair, str(quant), buy_id, fresh):
            return
        from hummingbot.connector.utils import get_new_client_order_id
        close_id = get_new_client_order_id(
            is_buy=side == "SELL",
            trading_pair=pair,
            hbot_order_id_prefix=getattr(connector, "client_order_id_prefix", ""),
            max_id_len=getattr(connector, "client_order_id_max_length", 32),
        )
        once.add(key)
        kept = await self._write_fill_down_close(
            executor_id, level_id, buy_id, price, amount, close_id, str(quant),
        )
        if not kept or self._recorded_close_id(executor_id, level_id) != close_id:
            return
        if self._close_text_seen(self._tick_fresh(pair), connector, close_id) is not False:
            return
        order = getattr(getattr(level, "active_open_order", None), "order", None)
        send_amount = self._amount_from_contracts(connector, pair, order, amount)
        if send_amount is None:
            return
        if await self._refresh_stop_mark(executor_id):
            return
        await self._partial_send_committed(
            executor_id, executor, connector, pair, close_id, level_id, str(quant), send_amount,
            journal_ts=self._buy_journal_ts(buy_id),
        )

    def _unseated_level_ids(self, executor_id, executor, connector, fresh) -> set:
        seated = self._seated_ids(executor)
        found = set()
        for order_id in self._own_ids(executor_id):
            if order_id in seated or not self._id_still_live(fresh, connector, order_id):
                continue
            level_id = self._own_level_id(executor_id, order_id)
            if level_id:
                found.add(level_id)
        return found

    def _open_text_holds(self, fresh, placement, tick, own_id: str) -> bool:
        """Наш живой номер или голая цена книги держат постановку (правило
        номера). Чужой текст с номером не держит — своя заявка встаёт на ту же
        цену, где стоит чужая."""
        from utils.executor_checkpoint import _same_tick
        for raw in list((fresh or {}).get("opens") or []):
            text = str(raw.get("text") or raw.get("order_id") or "")
            if text and text == str(own_id or ""):
                continue
            if not self._text_holds(text, fresh):
                continue
            price = raw.get("price")
            if price in (None, ""):
                continue
            if tick is None:
                if str(price) == str(placement):
                    return True
                continue
            if _same_tick(price, placement, tick) is True:
                return True
        return False

    def _price_of(self, order) -> str:
        if isinstance(order, dict):
            price = order.get("price")
        else:
            price = getattr(order, "price", None)
        return "" if price in (None, "") else str(price)

    def _placement_hold_prices(self, fresh, connector, placement, own_id: str, rows=None) -> list:
        """Наш живой номер (не этот) с той же строкой цены или голая цена книги.
        Наш тейк-профит и чужой текст с номером — не из них (правило номера)."""
        found = []
        own = str(own_id or "")

        def add(price, text):
            if not price or (text and str(text) == own) or str(price) != str(placement):
                return
            found.append(str(price))

        book = fresh if isinstance(fresh, dict) else {}
        tracker_texts = set(getattr(connector, "in_flight_orders", None) or {}) | set(
            getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        )
        for raw in list(book.get("opens") or []) + list(book.get("finished") or []):
            text = str(raw.get("text") or raw.get("order_id") or "")
            if not self._text_holds(text, book, tracker_texts):
                continue
            add(raw.get("price"), text)
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        for order_id, order in list(inflight.items()) + list(lost.items()):
            if not self._text_holds(order_id, book, tracker_texts):
                continue
            add(self._price_of(order), order_id)
        for row in rows or []:
            if str(row.get("order_id") or "") == own:
                continue
            if not self._text_holds(row.get("order_id"), book, tracker_texts):
                continue
            add(row.get("price"), row.get("order_id"))
        return found

    def _slot_has_other(self, executor, level_id: str, close: bool, order_id: str) -> bool:
        for level in list(getattr(executor, "grid_levels", None) or []):
            if str(getattr(level, "id", "") or "") != str(level_id):
                continue
            slot = getattr(level, "active_close_order" if close else "active_open_order", None)
            sitting = str(getattr(slot, "order_id", "") or "")
            return bool(sitting) and sitting != str(order_id)
        return False

    def _prices_without_own_mark(self, prices, order_id, rows, fresh=None):
        """Drop only this mark's (id, price) pair. A bare foreign price stays."""
        return self._without_own_placement(prices, order_id, rows)

    def _own_fresh_send_id(self, executor_id, level, price_text, tick, rows, fresh, connector):
        """One send-marked id of this level whose placement is this tick and is not on the book."""
        level_id = str(getattr(level, "id", "") or "")
        if not level_id or not rows or tick is None or price_text in (None, ""):
            return None
        book = fresh if isinstance(fresh, dict) else {}
        visible = set()
        for raw in list(book.get("opens") or []) + list(book.get("finished") or []):
            visible.add(str(raw.get("text") or raw.get("order_id") or ""))
        visible.discard("")
        inflight = {str(item) for item in (getattr(connector, "in_flight_orders", None) or {})}
        lost = {
            str(item)
            for item in (getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {})
        }
        found = []
        seen = set()
        for row in rows:
            order_id = str(row.get("order_id") or "")
            if not order_id or order_id in seen:
                continue
            if str(row.get("executor_id") or "") != str(executor_id):
                continue
            if str(row.get("level_id") or "") != level_id:
                continue
            if not partial_send_marked(rows, order_id):
                continue
            if order_id in visible or order_id in inflight or order_id in lost:
                continue
            if self._proven_dead(order_id, book):
                continue
            same = False
            for item in rows:
                if str(item.get("order_id") or "") != order_id or item.get("price") in (None, ""):
                    continue
                matched = _same_tick(item.get("price"), price_text, tick)
                if matched is True:
                    same = True
            if not same:
                continue
            seen.add(order_id)
            found.append(order_id)
        if len(found) != 1:
            return None
        return found[0]

    def _without_own_placement(self, prices, order_id, rows):
        """Drop (id, placement) pairs only. A bare string and the take-profit pair stay."""
        own = {
            str(row.get("price"))
            for row in rows or []
            if str(row.get("order_id") or "") == str(order_id) and row.get("price") not in (None, "")
        }
        kept = []
        for item in prices or []:
            if (
                isinstance(item, tuple)
                and len(item) == 2
                and str(item[0]) == str(order_id)
                and str(item[1]) in own
            ):
                continue
            kept.append(item)
        return kept

    def _held_level_is_this_send(self, rows, parsed, executor_id, level_id, own_id) -> bool:
        """True only when this level is held by this one send-marked id."""
        if not own_id or not partial_send_marked(rows, str(own_id)):
            return False
        for order_id in partial_holding_ids(rows, executor_id):
            if str(order_id) == str(own_id):
                continue
            for row in rows or []:
                if str(row.get("order_id") or "") == str(order_id) and str(row.get("level_id") or "") == str(level_id):
                    return False
        for level in (parsed or {}).get("levels") or []:
            if str(level.get("id") or "") != str(level_id):
                continue
            for key in ("open_order", "close_order"):
                slot_id = str((level.get(key) or {}).get("order_id") or "")
                if slot_id and slot_id != str(own_id):
                    return False
        return True

    async def _durable_open(self, executor_id: str, executor, level, reserves: dict) -> None:
        """Journal the client id, then send. Do not call stock adjust, buy, or sell."""
        if await self._refresh_stop_mark(executor_id):
            return
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        pair = getattr(getattr(executor, "config", None), "trading_pair", None)
        if connector is None or not pair or not hasattr(connector, "quantize_order_price"):
            return
        account = (self._executor_metadata.get(executor_id) or {}).get("account_name")
        if not account or self.db_manager is None:
            return
        recover_folder_debt()
        if buy_blocked() or ledger_order_ids() is None:
            return
        candidate = executor._get_open_order_candidate(level)
        executor.adjust_order_candidates(connector_name, [candidate])
        if getattr(candidate, "amount", 0) <= 0:
            return
        stored = []
        journal_ts = []
        try:
            from utils.executor_checkpoint import read_ledger_filtered
            ledger_rows = read_ledger_filtered()
            parsed_now = self._partial_parsed(executor_id)
            held_now = partial_holding_ids(ledger_rows, executor_id)
            for row in ledger_rows:
                order_key = str(row.get("order_id") or "")
                if row.get("ts") is not None:
                    journal_ts.append(row.get("ts"))
                if order_key in held_now:
                    continue
                if self._partial_lone_five(ledger_rows, order_key):
                    continue
                if row.get("price") not in (None, ""):
                    stored.append((order_key, row.get("price")))
                if row.get("take_profit_price") not in (None, ""):
                    stored.append((order_key, row.get("take_profit_price")))
            stored.extend(self._pending_ticks_live())
        except Exception:
            return
        for item in owed_records() or []:
            if order_is_unsent(str(item.get("order_id") or "")):
                continue
            if item.get("trading_pair") == pair and item.get("price"):
                stored.append((str(item.get("order_id") or ""), item.get("price")))
                if item.get("ts") is not None:
                    journal_ts.append(item.get("ts"))
        rules = getattr(connector, "trading_rules", None) or {}
        tick = getattr(rules.get(pair) if isinstance(rules, dict) else None, "min_price_increment", None)
        fresh = await self._fresh_raw_lists(connector, pair, journal_ts[0] if journal_ts else None)
        self.__dict__["_price_tick"] = self.__dict__.get("_price_tick", 0) + 1
        self._remember_tick_fresh(pair, fresh)
        self._drop_dead_pending(pair)
        self._partial_prepare_sync(executor_id, executor, connector, pair)
        if not fresh.get("opens_read") or not fresh.get("opens_done"):
            self._log_silent_exit("durable-open-first", pair, self._silent_exit_flags(fresh), executor_id)
            return
        self._set_hold_rows(await self._checkpoint_rows())
        column_prices = await self._column_prices(pair)
        if column_prices is None:
            self._log_silent_exit("durable-open-column-prices", pair, "column prices unread", executor_id)
            return
        column_ids = await self._column_order_ids(pair)
        if column_ids is None:
            self._log_silent_exit("durable-open-column-ids", pair, "column ids unread", executor_id)
            return
        if self._raw_prefix_without_hold(pair, getattr(connector, "client_order_id_prefix", None)):
            self._log_silent_exit("durable-open-raw-prefix", pair, "prefix without hold", executor_id)
            return
        if not await self._claim_visible_texts(executor_id, connector, pair, fresh, column_ids):
            return
        if self._owned_row_lacks_price(connector, fresh, column_ids):
            self._log_silent_exit("durable-open-owned-row-first", pair, "owned row lacks price", executor_id)
            return
        self._set_hold_rows(await self._checkpoint_rows())
        column_ids = await self._column_order_ids(pair)
        if column_ids is None or self._column_hold_lacks_price(pair):
            self._log_silent_exit("durable-open-column-hold-first", pair, "column hold unread or lacks price", executor_id)
            return
        moments = self._known_acceptance_times(pair, fresh)
        earliest = min(moments) if moments else None
        request = fresh.get("finished_request") or {}
        if earliest is not None and not history_covers_ts(request, earliest, {
            "read": fresh.get("finished_read"),
            "done": fresh.get("finished_done"),
        }):
            fresh = await self._fresh_raw_lists(connector, pair, earliest)
            self.__dict__["_price_tick"] = self.__dict__.get("_price_tick", 0) + 1
            self._remember_tick_fresh(pair, fresh)
            self._drop_dead_pending(pair)
            request = fresh.get("finished_request") or {}
            if not fresh.get("opens_read") or not fresh.get("opens_done"):
                self._log_silent_exit("durable-open-window", pair, self._silent_exit_flags(fresh), executor_id)
                return
            if not await self._claim_visible_texts(executor_id, connector, pair, fresh, column_ids):
                return
            if self._owned_row_lacks_price(connector, fresh, column_ids):
                self._log_silent_exit("durable-open-owned-row-window", pair, "owned row lacks price in window", executor_id)
                return
            self._set_hold_rows(await self._checkpoint_rows())
            column_ids = await self._column_order_ids(pair)
            if column_ids is None or self._column_hold_lacks_price(pair):
                self._log_silent_exit("durable-open-column-hold-window", pair, "column hold unread or lacks price in window", executor_id)
                return
        self._remember_seen_texts(connector, fresh, column_ids, pair)
        blocked_seen = self._seen_prefix_unrecorded(connector, fresh, column_ids, pair)
        if blocked_seen:
            detail = "seen prefix unrecorded"
            if isinstance(blocked_seen, str) and blocked_seen:
                detail = f"seen prefix unrecorded id={blocked_seen}"
            self._log_silent_exit("durable-open-seen-prefix", pair, detail, executor_id)
            return
        owned_visible = (
            self._has_owned_text(connector, fresh, column_ids)
            or bool(journal_ts)
            or bool(column_ids)
            or bool([
                text for text in ((self.__dict__.get("_seen_client_ids") or {}).get(pair) or set())
                if not order_is_unsent(text)
            ])
        )
        covered = history_covers_ts(request, earliest, {
            "read": fresh.get("finished_read"),
            "done": fresh.get("finished_done"),
        })
        params = request.get("params") or {}
        gate_window = "from" in params
        okx_window = covered and "begin" in params and "end" in params
        window_ok = (not owned_visible) or (
            earliest is not None and covered and (gate_window or okx_window)
        )
        if not window_ok:
            self._log_silent_exit("durable-open-window-cover", pair, f"earliest={earliest}", executor_id)
            return
        released = self._partial_released_ids_for_pair(pair)
        released.update(self.__dict__.get("_released_order_ids", set()))
        prices = self._stored_block_prices(stored, fresh)
        prices.extend(column_prices)
        prices.extend(self.__dict__.get("_claimed_prices", []))
        for raw in list(fresh.get("opens") or []) + list(fresh.get("finished") or []):
            text = str(raw.get("text") or raw.get("order_id") or "")
            price = raw.get("price")
            if price in (None, ""):
                continue
            # Правило номера: чужой текст и доказанная история не держат;
            # голая цена (номер не прочитан) и наш живой номер — держат.
            if not self._text_holds(text, fresh):
                continue
            prices.append((text, price) if text else str(price))
        prices.extend(self._partial_flight_prices(executor))
        prices.extend(self._partial_missing_take_prices(ledger_rows, executor, pair))
        prices.extend(self._partial_live_sell_prices(executor_id, connector, self._partial_parsed(executor_id), pair))
        self._set_hold_rows(await self._checkpoint_rows())
        if self._hold_rows is None:
            self._log_silent_exit("durable-open-hold-rows", pair, "late checkpoint rows unread", executor_id)
            return
        try:
            from utils.executor_checkpoint import read_ledger_filtered
            ledger_rows = read_ledger_filtered()
        except Exception:
            self._log_silent_exit("durable-open-ledger-reread", pair, "ledger reread failed", executor_id)
            return
        released = self._partial_released_ids_for_pair(pair)
        journal_prices = []
        for row in ledger_rows:
            order_key = str(row.get("order_id") or "")
            if not order_key:
                continue
            if self._partial_lone_five(ledger_rows, order_key):
                continue
            if row.get("price") not in (None, "") and self._text_holds(order_key, fresh):
                journal_prices.append((order_key, row.get("price")))
            if row.get("take_profit_price") not in (None, "") and self._take_holds(order_key, fresh):
                journal_prices.append((order_key, row.get("take_profit_price")))
        released_text = set()
        for row in ledger_rows:
            if str(row.get("order_id") or "") not in released:
                continue
            if row.get("price") not in (None, ""):
                released_text.add(str(row.get("price")))
            if row.get("take_profit_price") not in (None, ""):
                released_text.add(str(row.get("take_profit_price")))
        occupied = self._occupied_close_prices(executor)
        prices.extend(journal_prices)
        prices.extend(occupied)
        prices.extend(column_prices)
        prices.extend(self.__dict__.get("_claimed_prices", []))
        for raw in list(fresh.get("opens") or []) + list(fresh.get("finished") or []):
            text = str(raw.get("text") or raw.get("order_id") or "")
            price = raw.get("price")
            if price in (None, ""):
                continue
            # Правило номера: чужой текст и доказанная история не держат;
            # голая цена (номер не прочитан) и наш живой номер — держат.
            if not self._text_holds(text, fresh):
                continue
            prices.append((text, price) if text else str(price))
        feed_rows = [
            row for row in (self._partial_rows_with_owner_take(ledger_rows) or [])
            if not self._proven_dead(str(row.get("order_id") or ""), self._tick_fresh(pair))
        ]
        prices.extend(self._feed_prices(feed_rows, self._partial_parsed(executor_id), self._tick_fresh(pair)))
        prices.extend(self._partial_pair_seat_prices(pair, set()))
        prices.extend(self._live_open_prices(executor))
        prices.extend(self._partial_missing_take_prices(ledger_rows, executor, pair))
        prices.extend(self._partial_flight_prices(executor))
        prices.extend(self._partial_live_sell_prices(executor_id, connector, None, pair))
        price = connector.quantize_order_price(pair, candidate.price)
        price_text = str(price)
        fresh_now = self._tick_fresh(pair)
        if self._level_visible_own(executor_id, level, connector, fresh_now):
            self._log_silent_exit(
                "durable-open-visible-own", pair, f"level_id={getattr(level, 'id', '')}", executor_id,
            )
            return
        reuse_id = self._own_fresh_send_id(
            executor_id, level, price_text, tick, ledger_rows, fresh_now, connector,
        )
        pending = self._unanswered_send_ids(executor_id, level, ledger_rows, fresh_now, connector)
        if len(pending) == 1:
            reuse_id = pending[0]
        elif pending:
            self._log_silent_exit(
                "durable-open-pending-many", pair, f"level_id={getattr(level, 'id', '')}", executor_id,
            )
            return
        hold_prices = []
        if reuse_id and tick is None:
            hold_prices = self._placement_hold_prices(
                fresh_now, connector, price_text, reuse_id, ledger_rows,
            )
        if reuse_id and (
            self._open_text_holds(fresh_now, price_text, tick, reuse_id)
            or hold_prices
        ):
            self._log_silent_exit(
                "durable-open-foreign-holds", pair, f"placement={price_text}", executor_id,
            )
            return
        if reuse_id:
            prices = self._prices_without_own_mark(prices, reuse_id, ledger_rows, fresh_now)
        prices = self._prices_for_blocks(prices, fresh_now)
        if not (reuse_id and tick is None) and recorded_price_blocks(prices, price_text, tick):
            self._log_silent_exit("durable-open-price-blocked", pair, f"placement={price_text}", executor_id)
            return
        send_prices = hold_prices if reuse_id and tick is None else prices
        if send_blocked(
            ledger_ok=ledger_order_ids() is not None,
            column_ok=True,
            opens_ok=bool(fresh.get("opens_read")) and bool(fresh.get("opens_done")),
            finished_ok=bool(fresh.get("finished_read")) and bool(fresh.get("finished_done")),
            history_covers=window_ok and bool(fresh.get("finished_read")) and bool(fresh.get("finished_done")),
            prices=send_prices,
            placement=price_text,
            tick=tick,
        ):
            self._log_silent_exit("durable-open-send-blocked", pair, f"placement={price_text}", executor_id)
            return
        if not (reuse_id and tick is None) and recorded_price_blocks(prices, price_text, tick):
            return
        if self._owner_for_pair(account, connector_name, pair) is None and stored:
            self._log_silent_exit("durable-open-owner", pair, "stored non-empty, no owner", executor_id)
            return
        connector_key = str(id(connector))
        fresh_token = _own_fresh_send.set(reuse_id or "")
        try:
            refused = self._partial_refuses_open(executor_id, executor, level, price_text, tick)
        finally:
            _own_fresh_send.reset(fresh_token)
        if refused:
            self._log_silent_exit("durable-open-partial-refuses", pair, f"placement={price_text}", executor_id)
            return
        if self._unpriced_close_blocks(executor):
            self._log_silent_exit("durable-open-unpriced-close", pair, "unpriced close blocks", executor_id)
            return
        self._release_gone(self._tick_fresh(pair))
        if self._buy_waiting():
            self._log_silent_exit("durable-open-buy-waiting", pair, "buy waiting", executor_id)
            return
        if not claim_quant(reserves, connector_key, pair, price_text):
            self._log_silent_exit("durable-open-claim-quant", pair, f"placement={price_text} key={connector_key}/{pair}", executor_id)
            return
        level_id = getattr(level, "id", None)
        if reuse_id:
            # Already journaled. Do not mint, do not miss, do not rewrite the marker.
            order_id = reuse_id
            sent_now = self._readable_sent()
            if sent_now is None:
                release_quant(reserves, connector_key, pair, price_text)
                return
            if str(order_id) not in sent_now:
                try:
                    sent_marked = add_sent_order(order_id)
                except Exception:
                    logger.exception("durable sent mark failed")
                    sent_marked = False
                if not sent_marked:
                    release_quant(reserves, connector_key, pair, price_text)
                    return
        else:
            if await self._refresh_stop_mark(executor_id):
                release_quant(reserves, connector_key, pair, price_text)
                return
            from hummingbot.connector.utils import get_new_client_order_id
            order_id = get_new_client_order_id(
                is_buy=getattr(getattr(executor.config, "side", None), "name", "BUY") != "SELL",
                trading_pair=pair,
                hbot_order_id_prefix=connector.client_order_id_prefix,
                max_id_len=connector.client_order_id_max_length,
            )
            # Durable miss first: the refusal outlives the process before the journal
            # line becomes visible. A failed write aborts before remember, so the
            # kill window between the line and the miss cannot open.
            try:
                miss_marked = add_miss_order(order_id)
            except Exception:
                logger.exception("durable miss mark failed")
                miss_marked = False
            if not miss_marked:
                release_quant(reserves, connector_key, pair, price_text)
                return
            remembered = remember_open_before_send(
                executor_id,
                order_id,
                level_id,
                price_text,
                account_name=account,
                connector=connector_name,
                trading_pair=pair,
            )
            if not remembered:
                release_quant(reserves, connector_key, pair, price_text)
                return
            if not self._partial_mark_send(
                executor_id, executor, connector, pair, order_id, level_id, price_text, account, connector_name,
            ):
                release_quant(reserves, connector_key, pair, price_text)
                return
            if self._raw_prefix_without_hold(pair, getattr(connector, "client_order_id_prefix", None)):
                release_quant(reserves, connector_key, pair, price_text)
                return
            # Durable sent, replace and directory fsync, before the original place
            # call runs. A crash after this point keeps the line as an owner; a crash
            # before it leaves the number in miss only, and readers drop the line.
            try:
                sent_marked = add_sent_order(order_id)
            except Exception:
                logger.exception("durable sent mark failed")
                sent_marked = False
            if not sent_marked:
                release_quant(reserves, connector_key, pair, price_text)
                return
        from hummingbot.strategy_v2.models.executors import TrackedOrder
        level.active_open_order = TrackedOrder(order_id=order_id)
        reached = self.__dict__.setdefault("_reached_place", set())
        token = _placing_order_id.set(order_id)
        self._chain_place_once(connector, reached)
        from hummingbot.core.data_type.common import PositionAction
        try:
            if await self._refresh_stop_mark(executor_id):
                release_quant(reserves, connector_key, pair, price_text)
                level.active_open_order = None
                return
            await connector._create_order(
                trade_type=executor.config.side,
                order_id=order_id,
                trading_pair=pair,
                amount=candidate.amount,
                order_type=executor.config.triple_barrier_config.open_order_type,
                price=price,
                position_action=PositionAction.OPEN,
            )
        except Exception:
            logger.exception("durable open create failed")
        finally:
            _placing_order_id.reset(token)
        if order_id not in reached:
            release_quant(reserves, connector_key, pair, price_text)
            level.active_open_order = None
            if reuse_id:
                # Sent and the journal stay. The next entry sends this id, not a new one.
                return
            tail = str(order_id)[-8:]
            if remove_sent_order(order_id):
                self._log_silent_exit(
                    "send-never-left", pair,
                    f"order_id=…{tail} level_id={level_id} writer=durable-open", executor_id,
                    interval=300.0, key=f"unsend:{tail}",
                )
            else:
                self._log_silent_exit(
                    "send-unmark-failed", pair,
                    f"order_id=…{tail} level_id={level_id} writer=durable-open", executor_id,
                    interval=300.0, key=f"unsend:{tail}",
                )
            return
        # Reached: sent is already durable, so dropping the miss entry cannot
        # free this price to a second level. The reserve is not released.
        if not reuse_id:
            remove_miss_order(order_id)
        await self._commit_open_hold(executor_id, order_id, level_id, price_text, connector)

    async def _commit_open_hold(self, executor_id, order_id, level_id, price_text, connector) -> None:
        """Write the hold after the exchange id exists. Do not seat a fill."""
        live = (getattr(connector, "in_flight_orders", None) or {}).get(order_id)
        exchange_id = getattr(live, "exchange_order_id", None) if live is not None else None
        state = getattr(live, "current_state", None) if live is not None else None
        name = getattr(state, "name", str(state or ""))
        if not exchange_id or name not in ("OPEN", "FILLED", "PARTIALLY_FILLED"):
            return
        if not order_id or not level_id or not self.db_manager:
            return
        snapshot = (self.__dict__.get("_partial_presend") or {}).get(order_id) or {}
        addition = {"order_id": order_id, "level_id": level_id, "filled": False}
        if price_text not in (None, ""):
            addition["price"] = price_text
        for key in ("trading_pair", "order_type", "trade_type", "leverage", "position", "take_profit_price"):
            if snapshot.get(key) not in (None, ""):
                addition[key] = snapshot.get(key)
        from utils.executor_checkpoint import checkpoint_plus
        try:
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)
                row = await repo.get_executor_by_id(executor_id)
                seen = row.checkpoint if row is not None else None
                body = checkpoint_plus(seen, addition)
                if body is None:
                    return
                changed = await repo.replace_checkpoint_if_unchanged(executor_id, seen, body)
                if changed:
                    return
                fresh = await repo.get_executor_by_id(executor_id)
                seen = fresh.checkpoint if fresh is not None else None
                if seen and order_id in seen:
                    return
                body = checkpoint_plus(seen, addition)
                if body is None:
                    return
                await repo.replace_checkpoint_if_unchanged(executor_id, seen, body)
        except Exception:
            logger.exception("open hold was not committed")

    def _chain_place_once(self, connector, reached: set) -> None:
        if getattr(connector, "_accept_place_chained", False):
            return
        original = connector._place_order

        async def chained(*args, **kwargs):
            current = _placing_order_id.get()
            if current:
                reached.add(current)
            return await original(*args, **kwargs)

        connector._place_order = chained
        connector._accept_place_chained = True

    def _event_wrapper(self, executor_id, executor, original, marker):
        service = self

        def wrapped(*args, **kwargs):
            order_id = service._event_order_id(args)
            if marker == "gone" and (
                service._is_held_close(executor, order_id)
                or service._partial_gone_blocked(executor_id, order_id)
            ):
                return None
            result = original(*args, **kwargs)
            order_id = service._event_order_id(args)
            if marker == "filled":
                note_filled(executor, order_id, service._event_order_json(executor, order_id))
            elif not service._is_held_close(executor, order_id) and not service._partial_gone_blocked(
                executor_id, order_id
            ):
                note_gone(executor, order_id)
            service._schedule_checkpoint(executor_id)
            return result

        return wrapped

    @staticmethod
    def _event_order_id(args) -> Optional[str]:
        for arg in args:
            order_id = getattr(arg, "order_id", None)
            if order_id:
                return order_id
        return None

    @staticmethod
    def _event_order_json(executor, order_id: Optional[str]):
        if not order_id:
            return None
        for connector in (getattr(executor, "connectors", None) or {}).values():
            live = (getattr(connector, "in_flight_orders", None) or {}).get(order_id)
            if live is not None and hasattr(live, "to_json"):
                return live.to_json()
        return None

    def _schedule_checkpoint(self, executor_id: str) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._checkpoint_executor(executor_id))

    def _initial_checkpoint(self, executor_id: str, executor: ExecutorBase) -> Optional[str]:
        """Ownership at insert time. Empty is still a checkpoint: resume may start it.

        A missing checkpoint means "we do not know", and resume refuses those.
        """
        metadata = self._executor_metadata.get(executor_id) or {}
        executor_type = metadata.get("executor_type")
        if not executor_type:
            return None
        try:
            body = json.dumps(
                capture_executor(executor, executor_type), default=_json_default, sort_keys=True
            )
        except Exception as e:
            logger.error(f"Error capturing initial checkpoint for {executor_id}: {e}", exc_info=True)
            return None
        self._checkpoint_written[executor_id] = body
        return body

    async def _checkpoint_executor(self, executor_id: str) -> None:
        """Persist ownership if it changed. Never raises into the control loop."""
        if not self.db_manager:
            return
        executor = self._active_executors.get(executor_id)
        metadata = self._executor_metadata.get(executor_id) or {}
        executor_type = metadata.get("executor_type")
        if executor is None or not executor_type or executor.is_closed:
            return
        if self._partial_column_pending(executor_id) and not getattr(executor, "_partial_seated", False):
            return
        hold = getattr(self, "_resume_column_hold", None) or {}
        if executor_id in hold and not hold.get(executor_id):
            # Подъём держит колонку: метка конца могла уже погасить
            # _partial_column_pending, а посадка ещё не прошла (план
            # okx-resume-complete-level, мина 3).
            return
        try:
            captured = capture_executor(executor, executor_type)
        except Exception as e:
            logger.error(f"Error capturing checkpoint for {executor_id}: {e}", exc_info=True)
            return
        try:
            async with self.db_manager.get_session_context() as session:
                row = await ExecutorRepository(session).get_executor_by_id(executor_id)
                seen = row.checkpoint if row is not None else None
                wait_cycles = (getattr(self, "_resume_column_hold", None) or {}).get(executor_id)
                if wait_cycles:
                    # Метка стопа: запись закрыта, пока в колонке не виден конец
                    # именно номеров этого круга — старая метка уровня не годится
                    # (код-ревью 20261110-072441).
                    parsed_seen = parse_checkpoint(seen)
                    released_ids = partial_released_ids(parsed_seen)
                    if not all(
                        open_id in released_ids and close_id in released_ids
                        for (_level_id, open_id, close_id) in wait_cycles
                    ):
                        return
                    (getattr(self, "_resume_column_hold", None) or {}).pop(executor_id, None)
                body = checkpoint_keeping_holds(seen, captured)
                body = self._drop_dead_close_records(body)
                if body is None:
                    return
                changed = await ExecutorRepository(session).replace_checkpoint_if_unchanged(
                    executor_id=executor_id,
                    seen_text=seen,
                    new_text=body,
                )
            if changed:
                self._checkpoint_written[executor_id] = body
        except Exception as e:
            logger.error(f"Error writing checkpoint for {executor_id}: {e}", exc_info=True)

    async def _checkpoint_active_executors(self) -> None:
        for executor_id in list(self._active_executors):
            await self._checkpoint_executor(executor_id)

    @staticmethod
    def _is_held_close(executor, order_id: Optional[str]) -> bool:
        if not order_id:
            return False
        if order_id in {
            item.get("order_id") for item in getattr(executor, "_close_holds", []) or []
        }:
            return True
        if str(order_id) in {str(item) for item in getattr(executor, "_close_ban_ids", set()) or set()}:
            return True
        close = getattr(executor, "_close_order", None)
        if getattr(close, "order_id", None) == order_id:
            return True
        for level in getattr(executor, "grid_levels", []) or []:
            seated = getattr(level, "active_close_order", None)
            if getattr(seated, "order_id", None) == order_id:
                return True
        return False

    @staticmethod
    def _copy_column_closes_into_holds(executor, checkpoint) -> None:
        holds = list(getattr(executor, "_close_holds", []) or [])
        known = {item.get("order_id") for item in holds}
        for item in (checkpoint or {}).get(CLOSE_HOLD_KEY) or []:
            order_id = item.get("order_id") if isinstance(item, dict) else None
            if order_id and order_id not in known:
                holds.append(item if isinstance(item, dict) else {"order_id": order_id})
                known.add(order_id)
        close = (checkpoint or {}).get("close_order") or {}
        if close.get("order_id") and close["order_id"] not in known:
            holds.append({"order_id": close["order_id"], "role": "market_close"})
            known.add(close["order_id"])
        column_ids = set(getattr(executor, "_column_close_ids", []) or [])
        for level in (checkpoint or {}).get("levels") or []:
            seated = (level.get("close_order") or {}).get("order_id")
            if seated:
                column_ids.add(seated)
                if seated not in known:
                    holds.append({"order_id": seated, "role": "close"})
                    known.add(seated)
        for banned in (checkpoint or {}).get("close_bans") or []:
            if banned:
                column_ids.add(str(banned))
        executor._column_close_ids = column_ids
        executor._close_holds = holds

    def _remember_prefix_ids(self, order_ids) -> None:
        seen = [str(item) for item in getattr(self, "_seen_prefix_ids", []) or [] if item]
        for order_id in order_ids or []:
            if order_id and str(order_id) not in seen:
                seen.append(str(order_id))
        self._seen_prefix_ids = seen

    def _prefix_delete_held(self, connector, text: str) -> bool:
        """Until the close brake is up, a prefix id must not be cancelled."""
        if self._close_delete_blocked(text):
            return True
        if not getattr(self, "_resume_holding_prefix", False):
            return False
        prefix = str(getattr(connector, "client_order_id_prefix", "") or "")
        return bool(prefix) and bool(text) and text.startswith(prefix)

    def _remember_journal_prefix_ids(self) -> None:
        """A priced owner line is not an entry until a fresh list says so."""
        try:
            from utils.executor_checkpoint import read_ledger_filtered
            rows = read_ledger_filtered()
        except Exception:
            return
        ids = []
        for row in rows or []:
            order_id = row.get("order_id")
            if not order_id or not row.get("executor_id") or row.get("price") in (None, ""):
                continue
            if row.get("filled") or row.get("role") in ("open", "open_order"):
                continue
            ids.append(str(order_id))
        self._remember_prefix_ids(ids)

    async def _remember_open_prefix_ids(self, rows: list) -> None:
        """Read the list before resume rows are published. Record ids before the next await."""
        seen_keys = set()
        for row in rows:
            if row.get("executor_type") != "grid_executor":
                continue
            key = (row.get("account_name"), row.get("connector_name"), row.get("trading_pair"))
            if key in seen_keys or not all(key):
                continue
            seen_keys.add(key)
            try:
                trading_interface = self._get_trading_interface(row["account_name"])
                await self._prepare_market(row["account_name"], row["connector_name"], row["trading_pair"])
                connector = trading_interface.get_connector(row["connector_name"])
                exchange_pass = await self._exchange_open_pass(
                    row["account_name"], connector, row["trading_pair"],
                )
            except Exception:
                logger.exception("open list was not read before resume rows were published")
                self._prefix_lists_armed = False
                continue
            if not exchange_pass.get("read"):
                self._prefix_lists_armed = False
                continue
            prefix = str(getattr(connector, "client_order_id_prefix", "") or "")
            ids = []
            for item in exchange_pass.get("orders") or []:
                order_id = str(item.get("order_id") or item.get("client_order_id") or "")
                if order_id and (not prefix or order_id.startswith(prefix)):
                    ids.append(order_id)
            unowned = self._raw_unowned_ids()
            self._remember_prefix_ids([order_id for order_id in ids if order_id not in unowned])

    def _note_seen_prefix(
        self, connector, exchange_pass: Dict[str, Any], account: str, connector_name: str, pair: str,
    ) -> bool:
        """A seen prefix id needs one owner and a durable price before placement."""
        if not exchange_pass.get("read"):
            return False
        prefix = str(getattr(connector, "client_order_id_prefix", "") or "")
        seen = []
        for row in exchange_pass.get("orders") or []:
            order_id = str(row.get("order_id") or row.get("client_order_id") or "")
            if not order_id or (prefix and not order_id.startswith(prefix)):
                continue
            seen.append(order_id)
        try:
            known = read_merged_ledger()
        except Exception:
            return False
        unowned = self._raw_unowned_ids()
        self._remember_prefix_ids([order_id for order_id in seen if order_id not in unowned])
        protected = self._close_price_skip_ids()
        for order_id in seen:
            if order_id in unowned or order_id in protected:
                continue
            item = known.get(order_id) or {}
            if item.get("executor_id") and item.get("price") not in (None, ""):
                continue
            owner = self._known_owner(order_id, account, connector_name, pair)
            if not owner:
                continue
            price = None
            for row in exchange_pass.get("orders") or []:
                if str(row.get("order_id") or row.get("client_order_id") or "") == order_id:
                    price = row.get("price")
                    break
            if price in (None, ""):
                return False
            if not append_durable_row({
                "order_id": order_id,
                "executor_id": owner,
                "price": price,
                "ts": time.time(),
            }):
                return False
            known[order_id] = {"order_id": order_id, "executor_id": owner, "price": price}
        return True

    def _resolve_close_bans(self, executor, checkpoint, live_orders, side_name) -> None:
        """A role-less line is an entry only when the fresh list says so.

        Until then the id stays in the ban seat_known_opens never reads.
        """
        resolved = resolve_close_bans(
            checkpoint, live_orders, side_name,
            column_ids=getattr(executor, "_column_close_ids", None),
        )
        executor._close_ban_ids = {
            str(item) for item in (resolved or set())
            if item and not order_is_unsent(str(item))
        }

    @staticmethod
    def _restore_close_memory(executor, checkpoint, original_bans) -> None:
        """A thrown seating must not let the next capture erase the column hold."""
        restore_close_memory(executor, checkpoint, original_bans)

    async def _commit_close_holds_before_prune(self, rows: list) -> None:
        """Await the column write before the ledger file may be rewritten."""
        try:
            merged = read_merged_ledger()
        except Exception:
            logger.exception("order ledger unreadable; prune stays blocked")
            self._close_prune_allowed = False
            self._close_placement_allowed = False
            return
        self._close_placement_allowed = True
        if not merged:
            self._close_prune_allowed = True
            return
        for row in rows:
            checkpoint = parse_checkpoint(row.get("checkpoint")) or {}
            holds = list(checkpoint.get(CLOSE_HOLD_KEY) or [])
            known = {item.get("order_id") for item in holds if isinstance(item, dict)}
            retained = list(checkpoint.get("retained_orders") or [])
            retained_ids = {item.get("order_id") for item in retained if isinstance(item, dict)}
            bans = [str(item) for item in checkpoint.get(CLOSE_BAN_KEY) or []]
            changed = False
            for order_id, item in merged.items():
                owner = item.get("executor_id")
                if not item.get("executor_id"):
                    continue
                if owner not in (None, row.get("executor_id")):
                    continue
                if is_close_ledger_row(item):
                    if order_id in known:
                        continue
                    holds.append({"order_id": order_id, "role": item.get("role")})
                    known.add(order_id)
                    changed = True
                    continue
                if owner != row.get("executor_id"):
                    continue
                if item.get("role") in ("open", "open_order"):
                    if order_id not in retained_ids:
                        retained.append({
                            "order_id": order_id, "filled": False, "order": None, "role": "open_order",
                        })
                        retained_ids.add(order_id)
                        changed = True
                    continue
                if order_id not in bans:
                    bans.append(order_id)
                    changed = True
            if not changed:
                continue
            checkpoint[CLOSE_HOLD_KEY] = holds
            checkpoint[CLOSE_BAN_KEY] = bans
            checkpoint["retained_orders"] = retained
            strip_close_holds_from_saved(checkpoint)
            body = json.dumps(checkpoint)
            try:
                async with self.db_manager.get_session_context() as session:
                    await ExecutorRepository(session).update_executor(
                        executor_id=row["executor_id"], checkpoint=body
                    )
            except Exception:
                logger.exception("close hold commit failed; ledger file stays")
                self._close_prune_allowed = False
                self._close_placement_allowed = False
                return
            row["checkpoint"] = body
        self._close_prune_allowed = True

    def _prunable_proven_pairs(self, proven: Optional[set], started_ids: set) -> set:
        """A held id without a visible end is not erasable, price proof or not."""
        from utils.executor_checkpoint import order_ledger
        try:
            rows = self._partial_rows()
        except Exception:
            rows = None
        erasable = set()
        for executor_id, order_id in proven or ():
            if executor_id not in started_ids:
                continue
            executor = self._active_executors.get(executor_id)
            if executor is None or order_id not in order_ledger(executor):
                continue
            if rows is None:
                continue
            parsed = self._partial_parsed(executor_id)
            if parsed is None:
                continue
            if str(order_id) in partial_holding_ids(rows, executor_id) and not partial_end_visible(parsed, str(order_id)):
                self._log_silent_exit(
                    "prune-held-kept", "", f"order_id=…{str(order_id)[-8:]}", executor_id,
                    interval=0.0, label="silent stop exit:", key=str(order_id),
                )
                continue
            erasable.add((executor_id, order_id))
        return erasable

    async def resume_running_executors(self) -> None:
        """Rebuild RUNNING executors and reattach their orders. Does not place new ones.

        Called after order reconcile and before cleanup_orphaned_executors. A row
        that cannot be reattached is left RUNNING so cleanup still marks it, and
        the unattended list still names it. Starting it from the create-config
        would open a second trade beside the first.
        """
        self._resume_holding_prefix = True
        self._remember_journal_prefix_ids()
        # Prune only after close ids are durable. A failure must not skip resume
        # and must not allow a new close. The early call drops only today's garbage.
        self._close_prune_allowed = False
        self._close_placement_allowed = False
        try:
            recover_folder_debt()
            protected = self._ledger_ids_without_column()
            if protected is None:
                logger.warning("order ledger was not read; not pruning")
            else:
                prune_order_ledger(protected=protected)
        except Exception:
            logger.exception("order ledger prune failed; resume continues")
        if not self.db_manager:
            self._resume_holding_prefix = False
            return
        # Уже закрытую раннюю остановку подъём не открывает
        # (план stop-finished-not-reopened).
        try:
            async with self.db_manager.get_session_context() as session:
                rows = await ExecutorRepository(session).get_running_for_resume()
        except Exception as e:
            logger.error(f"Error loading executors to resume: {e}", exc_info=True)
            return

        owned_by: Dict[str, str] = {}
        ledger_ids = {
            row["executor_id"]: recent_order_ids(row["executor_id"])
            for row in rows
        }
        self._remember_journal_prefix_ids()
        self._prefix_lists_armed = True
        await self._remember_open_prefix_ids(rows)
        self._resume_rows = rows
        self._resume_ledger_ids = ledger_ids

        await self._commit_close_holds_before_prune(rows)
        if self._close_prune_allowed:
            try:
                prune_order_ledger()
            except Exception:
                logger.exception("order ledger prune failed; file left unchanged")
                self._close_prune_allowed = False
        resumed = 0
        started_ids = set()
        for row in rows:
            if row["executor_id"] in self._active_executors:
                started_ids.add(row["executor_id"])
                continue
            # A row's claims publish only when its executor starts. A row that
            # returns before start() withdraws them before the next row runs,
            # so a non-started journal line never makes an id foreign to a
            # starter that holds it.
            parsed = checkpoint_view(row.get("checkpoint"))
            claims = set(saved_order_ids(parsed))
            claims.update(ledger_ids.get(row["executor_id"]) or [])
            claims.update(accounted_orders(row["executor_id"], parsed))
            added = []
            for order_id in sorted(claims, key=str):
                if order_id not in owned_by:
                    owned_by[order_id] = row["executor_id"]
                    added.append(order_id)
            reason = await self._resume_one(row, owned_by)
            if reason is None:
                resumed += 1
                started_ids.add(row["executor_id"])
            else:
                for order_id in added:
                    if owned_by.get(order_id) == row["executor_id"]:
                        owned_by.pop(order_id, None)
                logger.warning(
                    f"Not resuming {row['executor_id']} ({row['executor_type']} "
                    f"{row['connector_name']} {row['trading_pair']}): {reason}"
                )
        if resumed:
            logger.info(f"Resumed {resumed} executor(s) after restart")
        # The one erasure pass, after the loop: only ids whose executor reached
        # start() in this same resume and already leads the number from its
        # hold. The column read happens before the prune takes the ledger lock.
        try:
            proven = await self._ledger_price_proofs() if self.db_manager else None
        except Exception:
            proven = None
        if proven:
            erasable = self._prunable_proven_pairs(proven, started_ids)
            if erasable:
                try:
                    prune_order_ledger(protected=ledger_order_ids(), proven=erasable)
                except Exception:
                    logger.exception("order ledger prune after resume failed")
        if getattr(self, "_prefix_lists_armed", False):
            self._resume_holding_prefix = False

    @staticmethod
    def _config_side_name(config: Dict[str, Any]) -> Optional[str]:
        side = config.get("side")
        if side in (1, "1", "BUY"):
            return "BUY"
        if side in (2, "2", "SELL"):
            return "SELL"
        return side if isinstance(side, str) else None

    @staticmethod
    def _resume_market_price(connector: Any, trading_pair: str):
        if connector is None:
            return None
        try:
            from hummingbot.core.data_type.common import PriceType
            return connector.get_price_by_type(trading_pair, PriceType.MidPrice)
        except Exception:
            return None

    def _install_touch_cap(self, executor: ExecutorBase, plan: Dict[str, Any]) -> None:
        """Limit new grid orders. A failure here must not drop the running row."""
        try:
            install_grid_resume_cap(executor, plan)
        except Exception:
            logger.exception("grid resume cap failed; keeping known prices blocked")
            try:
                conservative_grid_cap(executor, plan)
            except Exception:
                logger.exception("conservative grid cap failed; blocking new orders")
                try:
                    blank_grid_cap(executor)
                except Exception:
                    logger.exception("blank grid cap failed; resume continues")

    @staticmethod
    def _open_venue(connector: Any) -> Optional[str]:
        return open_list_venue(type(connector).__name__ if connector is not None else "")

    @staticmethod
    def _open_limit_id(connector: Any, venue: str) -> str:
        """A path the throttler already knows. The request URL stays the open-order list."""
        name = type(connector).__name__.lower() if connector is not None else ""
        if venue == "okx" and "perpetual" in name:
            return "GET-/api/v5/trade/order"
        if venue == "okx":
            return "/api/v5/trade/order"
        return "spot/orders"

    def _open_list_state(self) -> None:
        if not hasattr(self, "_exchange_slots"):
            self._exchange_slots = {}
            self._exchange_open_passes = {}
            self._exchange_fail_at = {}
            self._exchange_inflight = {}
            self._open_list_targets = {}

    def _shared_open_slot(self, account: str, connector: Any, pair: str) -> Dict[str, Any]:
        """Same dict for every grid on this account, connector, and pair."""
        self._open_list_state()
        key = exchange_pass_key(account, connector, pair)
        self._open_list_targets[key] = (connector, pair)
        slot = self._exchange_slots.get(key)
        if slot is None:
            cached = self._exchange_open_passes.get(key)
            slot = cached if isinstance(cached, dict) and cached.get("read") else new_exchange_slot()
            self._exchange_slots[key] = slot
        return slot

    async def _exchange_open_pass(self, account: str, connector: Any, pair: str) -> Dict[str, Any]:
        """Return the shared slot. A failed read is not stored as an empty book."""
        slot = self._shared_open_slot(account, connector, pair)
        if slot.get("read"):
            return slot
        if open_list_venue(type(connector).__name__ if connector is not None else "") is None:
            return slot
        key = exchange_pass_key(account, connector, pair)
        now = time.monotonic()
        failed_at = self._exchange_fail_at.get(key)
        if failed_at is not None and now - failed_at < self.OPEN_LIST_RETRY_SECONDS:
            return slot
        running = self._exchange_inflight.get(key)
        if running is None:
            task = asyncio.create_task(self._fill_open_slot(key, connector, pair))
            self._exchange_inflight[key] = (task, now)
        else:
            task = running[0]
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            if asyncio.current_task().cancelling():
                raise
            self._exchange_fail_at[key] = time.monotonic()
        return slot

    async def _fill_open_slot(self, key: tuple, connector: Any, pair: str) -> None:
        """Write a successful list into the shared slot. A failure is only a timestamp."""
        self._open_list_state()
        slot = self._exchange_slots.get(key)
        if slot is None:
            slot = new_exchange_slot()
            self._exchange_slots[key] = slot
        if slot.get("read"):
            return
        now = time.monotonic()
        failed_at = self._exchange_fail_at.get(key)
        if failed_at is not None and now - failed_at < self.OPEN_LIST_RETRY_SECONDS:
            return
        current = self._exchange_inflight.get(key)
        if current is not None and current[0] is not asyncio.current_task():
            return
        try:
            result = await asyncio.wait_for(
                self._read_exchange_open_pass(connector, pair),
                timeout=self.OPEN_LIST_REQUEST_TIMEOUT,
            )
            if isinstance(result, dict) and result.get("read"):
                slot["orders"] = list(result.get("orders") or [])
                slot["read"] = True
                self._exchange_open_passes[key] = slot
                self._exchange_fail_at.pop(key, None)
            else:
                self._exchange_fail_at[key] = time.monotonic()
        except asyncio.TimeoutError:
            self._exchange_fail_at[key] = time.monotonic()
        except Exception:
            logger.exception("exchange open list failed for %s", pair)
            self._exchange_fail_at[key] = time.monotonic()
        finally:
            running = self._exchange_inflight.get(key)
            if running is not None and running[0] is asyncio.current_task():
                self._exchange_inflight.pop(key, None)

    def _expire_open_list_tasks(self, now: float) -> None:
        for key, (task, started) in list(self._exchange_inflight.items()):
            if task.done():
                self._exchange_inflight.pop(key, None)
                continue
            if now - started < self.OPEN_LIST_REQUEST_TIMEOUT:
                continue
            task.cancel()
            self._exchange_inflight.pop(key, None)
            self._exchange_fail_at[key] = now

    async def _retry_unread_open_lists(self, now: float) -> None:
        """One retry per key when a grid still wants an open. Does not call the proposer."""
        self._open_list_state()
        self._expire_open_list_tasks(now)
        wanted = set()
        for executor in list(self._active_executors.values()):
            if not getattr(executor, "_occupied_wants_open", False):
                continue
            key = getattr(executor, "_occupied_pass_key", None)
            if key is not None:
                wanted.add(key)
        for key in wanted:
            slot = self._exchange_slots.get(key)
            if slot is not None and slot.get("read"):
                continue
            if key in self._exchange_inflight:
                continue
            failed_at = self._exchange_fail_at.get(key)
            if failed_at is not None and now - failed_at < self.OPEN_LIST_RETRY_SECONDS:
                continue
            target = self._open_list_targets.get(key)
            if not target:
                continue
            connector, pair = target
            task = asyncio.create_task(self._fill_open_slot(key, connector, pair))
            self._exchange_inflight[key] = (task, now)

    async def _read_exchange_open_pass(self, connector: Any, pair: str) -> Dict[str, Any]:
        venue = self._open_venue(connector)
        if venue is None:
            return {"read": False, "orders": []}
        symbol = await connector.exchange_symbol_associated_to_pair(pair)
        if not symbol:
            return {"read": False, "orders": []}
        pages = []
        after = None
        page = 1
        for _ in range(1000):
            request = exchange_open_request(venue, symbol, page=page, after=after)
            if request is None:
                return {"read": False, "orders": []}
            body = await connector._api_get(
                path_url=request["path"],
                params=request["params"],
                is_auth_required=True,
                limit_id=self._open_limit_id(connector, venue),
            )
            parsed = parse_exchange_open_page(body, venue, symbol)
            pages.append(parsed)
            if not parsed.get("read") or parsed.get("done"):
                break
            if venue == "gate":
                page += 1
            else:
                after = parsed.get("cursor")
                if not after:
                    return {"read": False, "orders": []}
        else:
            return {"read": False, "orders": []}
        return merge_exchange_open_pages(pages)

    def _foreign_ids_now(self, self_id: str, owned_by: Dict[str, str]) -> set:
        foreign = foreign_order_ids(
            getattr(self, "_resume_rows", []) or [],
            getattr(self, "_resume_ledger_ids", {}) or {},
            self_id,
        )
        foreign.update(order_id for order_id, owner in owned_by.items() if owner != self_id)
        for executor_id, executor in self._active_executors.items():
            if executor_id == self_id:
                continue
            for level in getattr(executor, "grid_levels", []) or []:
                for tracked in (
                    getattr(level, "active_open_order", None),
                    getattr(level, "active_close_order", None),
                ):
                    order_id = getattr(tracked, "order_id", None)
                    if order_id:
                        foreign.add(order_id)
        return foreign

    @staticmethod
    def _prior_tracker_ids(connector: Any) -> set:
        found = set()
        inflight = getattr(connector, "in_flight_orders", None)
        if isinstance(inflight, dict):
            found.update(str(order_id) for order_id in inflight if order_id)
        tracker = getattr(connector, "_order_tracker", None)
        cached = getattr(tracker, "cached_orders", None) if tracker is not None else None
        if isinstance(cached, dict):
            found.update(str(order_id) for order_id in cached if order_id)
        return found

    def _resume_own_ids(self, executor_id: str, checkpoint: Optional[Dict[str, Any]]) -> set:
        found = {str(item) for item in saved_order_ids(checkpoint) if item}
        found.update(str(item) for item in (recent_order_ids(executor_id) or []) if item)
        for level in (checkpoint or {}).get("levels") or []:
            if not isinstance(level, dict):
                continue
            for key in ("open_order", "close_order"):
                order_id = (level.get(key) or {}).get("order_id")
                if order_id:
                    found.add(str(order_id))
        close = ((checkpoint or {}).get("close_order") or {}).get("order_id")
        if close:
            found.add(str(close))
        for hold in (checkpoint or {}).get("holds") or []:
            if isinstance(hold, dict) and hold.get("order_id"):
                found.add(str(hold.get("order_id")))
        found.discard(STOP_MARK_KEY)
        return found

    def _resume_own_side(self, checkpoint: Optional[Dict[str, Any]], order_id: str) -> str:
        text = str(order_id)
        for level in (checkpoint or {}).get("levels") or []:
            if not isinstance(level, dict):
                continue
            close = (level.get("close_order") or {}).get("order_id")
            if str(close or "") == text:
                return "sell"
            open_id = (level.get("open_order") or {}).get("order_id")
            if str(open_id or "") == text:
                return "buy"
        close = ((checkpoint or {}).get("close_order") or {}).get("order_id")
        if str(close or "") == text:
            return "sell"
        for hold in (checkpoint or {}).get("holds") or []:
            if not isinstance(hold, dict) or str(hold.get("order_id") or "") != text:
                continue
            if str(hold.get("role") or "") in ("close", "market_close", "close_order"):
                return "sell"
        return ""

    def _resume_fill_size(self, row: Dict[str, Any]) -> Optional[Decimal]:
        for key in ("fill_size", "amount", "accFillSz"):
            if row.get(key) in (None, ""):
                continue
            try:
                size = Decimal(str(row.get(key)))
            except Exception:
                return None
            if size > 0:
                return size
        return None

    def _resume_proof_ids(self, executor_id: str, checkpoint: Optional[Dict[str, Any]]) -> Optional[set]:
        from utils.executor_checkpoint import _read_ledger
        try:
            raw_rows = _read_ledger()
        except Exception:
            return None
        found = {
            str(row.get("order_id"))
            for row in raw_rows or []
            if row.get("executor_id") == executor_id and row.get("order_id")
        }
        found.update(self._resume_own_ids(executor_id, checkpoint))
        found.update(self._ids_of((checkpoint or {}).get("close_holds")))
        found.update(self._ban_ids(checkpoint))
        found.update(self._partial_end_order_ids(checkpoint))
        found.update(self._checkpoint_close_ids(checkpoint, past=True))
        found.discard(STOP_MARK_KEY)
        return found

    async def _marked_resume_gate(self, row: Dict[str, Any]) -> Optional[str]:
        """seat, keep, or None. None means the resume path is unchanged."""
        checkpoint = parse_checkpoint_cached(row.get("checkpoint"))
        if not stop_mark_visible(checkpoint):
            return None
        executor_id = str(row.get("executor_id") or "")
        pair = str(row.get("trading_pair") or "")
        ids = self._resume_proof_ids(executor_id, checkpoint)
        if not ids:
            return "keep"
        account = row.get("account_name")
        connector_name = row.get("connector_name")
        try:
            trading_interface = self._get_trading_interface(account)
            await self._prepare_market(account, connector_name, pair)
            connector = trading_interface.get_connector(connector_name)
        except Exception:
            return "keep"
        if connector is None or not hasattr(connector, "_api_get"):
            return "keep"

        def lists_state(fresh):
            if not self._lists_ready(fresh):
                return None
            inflight = getattr(getattr(connector, "in_flight_orders", None), "copy", lambda: {})()
            lost = getattr(
                getattr(getattr(connector, "_order_tracker", None), "lost_orders", None),
                "copy", lambda: {},
            )()
            missing = []
            for text in sorted(ids):
                if order_send_unknown(text) or text in inflight or text in lost:
                    return None
                if self._shutdown_text_present((fresh or {}).get("opens"), text):
                    return None
                finished = self._finished_row(text, fresh)
                if finished is not None:
                    if not self._closed_row_filled(finished):
                        return None
                    continue
                missing.append(text)
            return missing

        fresh = await self._fresh_raw_lists(connector, pair, None)
        missing = lists_state(fresh)
        if missing is None:
            return "keep"
        archive = {}
        if missing:
            archive = await self._archive_death_bases(connector, pair, missing)
            if not archive:
                return "keep"
        fresh = await self._fresh_raw_lists(connector, pair, None)
        missing_after = lists_state(fresh)
        if missing_after is None:
            return "keep"
        for text in missing_after:
            if text not in archive:
                return "keep"
        net = Decimal("0")
        for text in sorted(ids):
            side = self._resume_own_side(checkpoint, text)
            if side not in ("buy", "sell"):
                return "keep"
            finished = self._finished_row(text, fresh)
            size = self._resume_fill_size(finished) if finished is not None else archive.get(text)
            if size is None:
                return "keep"
            net += size if side == "buy" else -size
        if net != 0:
            return None
        return "seat"

    async def _write_resume_close(self, executor_id: str) -> bool:
        """Запись конца в базу. stop() и статус в памяти не ставим."""
        if not self.db_manager:
            return False
        from database.repositories.executor_repository import ExecutorRepository
        try:
            async with self.db_manager.get_session_context() as session:
                written = await ExecutorRepository(session).update_executor(
                    executor_id, status="TERMINATED", close_type="EARLY_STOP",
                )
                if written is None:
                    return False
        except Exception:
            logger.exception("resume close was not written for %s", executor_id)
            return False
        return True


    async def _mark_completed_cycles(self, executor_id: str, checkpoint: Dict[str, Any]) -> Optional[Set[Tuple[str, str, str]]]:
        """Долговечная метка конца завершённых кругов ДО посадки (план
        okx-resume-complete-level).

        Один подъёмный CAS на круг: метка той же формы, что у живого пути продаж,
        плюс запись тел исполнения (номер, цена, размер) и чистые слоты — одной
        записью, без второй. Живой _partial_release не зовётся и не меняется.
        None — отказ записи: посадку не звать, колонка остаётся под замком.
        При видимой метке стопа конца не пишем (запись конца — путь стопа),
        но возвращаем круги (level_id, вход, продажа), чьё оформление живёт
        только в памяти: замок записи снимается, когда в колонке виден конец
        именно номеров этого круга — не любая старая метка уровня.
        """
        cycles = completed_cycle_levels(checkpoint)
        if stop_mark_visible(checkpoint):
            return {
                (cycle["level_id"], cycle["open_id"], cycle["close_id"])
                for cycle in cycles
            }
        for cycle in cycles:
            seated = await self._partial_cas(
                executor_id,
                resume_complete_mutate(cycle["level_id"], cycle["open_id"], cycle["close_id"]),
                resume_complete_visible(cycle["level_id"], cycle["open_id"]),
                cycle["level_id"],
            )
            if not seated:
                return None
        return set()
    async def _resume_one(self, row: Dict[str, Any], owned_by: Optional[Dict[str, str]] = None) -> Optional[str]:
        """None on success. A string is why this row was left for cleanup."""
        # Замок записи колонки на весь подъём строки (план okx-resume-complete-level):
        # ставится до любого await и до регистрации исполнителя, снимается только
        # после успешного apply_checkpoint. На отказных путях остаётся — колонка
        # заморожена, как сегодня при «close seating failed».
        hold = self.__dict__.setdefault("_resume_column_hold", {})
        hold[str(row.get("executor_id") or "")] = set()
        checkpoint = parse_checkpoint_cached(row.get("checkpoint"))
        gate = await self._marked_resume_gate(row)
        if gate == "seat":
            if not await self._write_resume_close(str(row.get("executor_id") or "")):
                self.__dict__.setdefault("_kept_ids", set()).add(row["executor_id"])
                return "marked close was not written"
            return "seated marked close"
        if gate == "keep":
            self.__dict__.setdefault("_kept_ids", set()).add(row["executor_id"])
            return "marked number unproven"
        # Подъём до start: метка из колонки прогревает RAM (план stop-freeze п.1).
        if stop_mark_visible(checkpoint):
            self.__dict__.setdefault("_stop_mark_ram", {})[str(row["executor_id"])] = True
        accounted = {
            order_id: price
            for order_id, price in accounted_orders(row["executor_id"], checkpoint).items()
            if (owned_by or {}).get(order_id, row["executor_id"]) == row["executor_id"]
        }
        if must_keep_executor(row["executor_id"], ledger_executor_ids(), row.get("checkpoint")) or self._partial_must_keep(row):
            self.__dict__.setdefault("_kept_ids", set()).add(row["executor_id"])
        if not row.get("config"):
            return "no stored config"
        try:
            config = json.loads(row["config"])
        except (json.JSONDecodeError, TypeError):
            return "config is not JSON"
        if not isinstance(config, dict):
            return "config is not an object"
        config["id"] = row["executor_id"]
        config["type"] = row["executor_type"]

        account = row["account_name"]
        started = False
        try:
            trading_interface = self._get_trading_interface(account)
            await self._prepare_market(account, row["connector_name"], row["trading_pair"])
            connector = trading_interface.get_connector(row["connector_name"])
            pair = row["trading_pair"]
            others = {
                order_id for order_id, owner in (owned_by or {}).items()
                if owner != row["executor_id"]
            }
            live_orders = [
                order for order in (
                    getattr(connector, "in_flight_orders", {}).values() if connector else []
                )
                if getattr(order, "trading_pair", None) == pair
                and getattr(order, "client_order_id", None) not in others
            ]
            views = [live_order_view(order) for order in live_orders]
            if checkpoint and row["executor_type"] == "grid_executor":
                checkpoint = merge_recent_ids(checkpoint, recent_ids_kept_as_open(row["executor_id"]))
            reason = can_resume(checkpoint, views)
            if reason:
                self.__dict__.setdefault("_kept_ids", set()).add(row["executor_id"])
                if row["executor_id"] not in self._active_executors:
                    try:
                        executor_class, _, typed_config = self._validate_executor_config(
                            config, default_timestamp=trading_interface.current_timestamp
                        )
                        metadata = {
                            "account_name": account,
                            "connector_name": row["connector_name"],
                            "trading_pair": row["trading_pair"],
                            "executor_type": row["executor_type"],
                            "controller_id": row["controller_id"],
                            "created_at": row.get("created_at") or datetime.now(timezone.utc),
                            "config": config,
                        }
                        self._instantiate_and_register(
                            executor_class, typed_config, trading_interface, metadata, start=False
                        )
                    except Exception:
                        logger.exception("resume failed before start; row stays RUNNING")
                return reason
            plan = None
            if checkpoint and row["executor_type"] == "grid_executor":
                checkpoint = dict(checkpoint)
                checkpoint["trading_pair"] = row.get("trading_pair") or ""
                plan = grid_resume_plan(
                    checkpoint,
                    views,
                    self._resume_market_price(connector, pair),
                    self._config_side_name(config),
                )

            executor_class, _, typed_config = self._validate_executor_config(
                config, default_timestamp=trading_interface.current_timestamp
            )
            if typed_config.id != row["executor_id"]:
                return "rebuilt config id does not match the stored executor"
            metadata = {
                "account_name": account,
                "connector_name": row["connector_name"],
                "trading_pair": row["trading_pair"],
                "executor_type": row["executor_type"],
                "controller_id": row["controller_id"],
                "created_at": row.get("created_at") or datetime.now(timezone.utc),
                "config": config,
            }
            _, executor = self._instantiate_and_register(
                executor_class, typed_config, trading_interface, metadata, start=False
            )
            self._install_accounted_listener(connector, account, row["connector_name"])
            holds = list((checkpoint or {}).get(CLOSE_HOLD_KEY) or [])
            close_present = bool(holds) or bool((checkpoint or {}).get(CLOSE_BAN_KEY)) or bool(
                (checkpoint or {}).get("close_order")
            )
            if row["executor_type"] == "grid_executor" and not install_close_without_level(
                executor, row["executor_id"],
                frozen=lambda eid=str(row["executor_id"]): self._stop_mark_now(eid),
            ) and close_present:
                self.__dict__.setdefault("_kept_ids", set()).add(row["executor_id"])
                return "close brake was not installed"
            ledger_failed = False
            try:
                ledger_known = bool(read_merged_ledger())
            except Exception:
                ledger_known = True
                ledger_failed = True
            journal_rows = self._partial_rows()
            if holds or ledger_known or has_exposure(checkpoint) or partial_exposure(
                journal_rows, checkpoint, row["executor_id"]
            ):
                async def _skip_balance_check():
                    return None
                executor.validate_sufficient_balance = _skip_balance_check
            await self._partial_copy_five_before_start(row["executor_id"], checkpoint, config)
            try:
                column_text = await self._partial_column_text(row["executor_id"])
            except Exception:
                logger.exception("checkpoint column refresh failed for %s", row["executor_id"])
                column_text = None
            refreshed = parse_checkpoint_cached(column_text or row.get("checkpoint"))
            if refreshed:
                checkpoint = refreshed
            pending_release: Set[Tuple[str, str, str]] = set()
            if checkpoint and row["executor_type"] == "grid_executor":
                # Долговечная метка конца ДО посадки (план okx-resume-complete-level).
                pending = await self._mark_completed_cycles(row["executor_id"], checkpoint)
                if pending is None:
                    self.__dict__.setdefault("_kept_ids", set()).add(row["executor_id"])
                    return "complete level end mark was not committed"
                pending_release = pending
            original_bans = [
                str(item) for item in ((checkpoint or {}).get(CLOSE_BAN_KEY) or []) if item
            ]
            kept_bans: set = set(original_bans)
            try:
                if checkpoint:
                    self._resolve_close_bans(
                        executor, checkpoint, live_orders, self._config_side_name(config)
                    )
                    kept_bans = set(getattr(executor, "_close_ban_ids", set()) or set())
                apply_checkpoint(executor, checkpoint, live_orders)
                hold = self.__dict__.setdefault("_resume_column_hold", {})
                if pending_release:
                    # Метка стопа: оформление круга живёт только в памяти — запись
                    # держим, пока путь стопа не отпустит уровень своей меткой
                    # (код-ревью 20261110-062315: иначе склейка возвращает продажу).
                    hold[str(row["executor_id"])] = set(pending_release)
                else:
                    hold.pop(str(row["executor_id"]), None)
                executor._notebook_resumed = True
                seat_role_close_before_next(executor, row["executor_id"])
                if not seat_recorded_level_opens(executor, row["executor_id"]):
                    hold_new_opens(executor)
                    self.__dict__.setdefault("_kept_ids", set()).add(row["executor_id"])
                    return "level journal was not read"
                seed_accounted_orders(executor, accounted, connector)
                self._copy_column_closes_into_holds(executor, checkpoint)
                executor._close_seat_ready = False
                executor._close_list_ready = False
                executor._close_may_open = (
                    bool(getattr(self, "_close_placement_allowed", False)) and not ledger_failed
                )
            except Exception:
                logger.exception("close seating failed; brake stays closed")
                self._restore_close_memory(executor, checkpoint, sorted(kept_bans))
                executor._close_seat_ready = False
                executor._close_list_ready = False
                self.__dict__.setdefault("_kept_ids", set()).add(row["executor_id"])
                return "close seating failed"
            notice = None
            if plan:
                self._install_touch_cap(executor, plan)
                notice = plan.get("notice")
            if row["executor_type"] == "grid_executor":
                # A throw here must not reach the outer except: that pops the row and
                # cleanup marks SYSTEM_CLEANUP while the exchange order is still live.
                try:
                    exchange_pass = await self._exchange_open_pass(account, connector, pair)
                    own_ids = {
                        str(order_id)
                        for order_id in saved_order_ids(checkpoint)
                        if not order_is_unsent(str(order_id))
                    }
                    own_ids.update(recent_order_ids(row["executor_id"]))
                    own_ids.update(accounted)
                    own_prices = {
                        order_id: price
                        for order_id, price in accounted.items()
                        if price not in (None, "")
                    }
                    if not self._note_seen_prefix(
                        connector, exchange_pass, account, row["connector_name"], pair,
                    ):
                        hold_new_opens(executor)
                        ban = set(getattr(executor, "_close_ban_ids", set()) or set())
                        unowned = self._raw_unowned_ids()
                        protected = self._close_price_skip_ids()
                        ban.update(
                            item for item in (getattr(self, "_seen_prefix_ids", []) or [])
                            if item not in unowned and str(item) not in protected
                        )
                        executor._close_ban_ids = {item for item in ban if item and item not in unowned}
                        self.__dict__.setdefault("_kept_ids", set()).add(row["executor_id"])
                        return "prefix close was not durably owned"
                    try:
                        noted = read_merged_ledger()
                    except Exception:
                        noted = {}
                    for order_id, item in (noted or {}).items():
                        if not item.get("executor_id") or item.get("price") in (None, ""):
                            continue
                        if item.get("filled") or item.get("role") in ("open", "open_order"):
                            continue
                        if str(order_id) in {
                            str(row_id.get("order_id") or row_id.get("client_order_id") or "")
                            for row_id in exchange_pass.get("orders") or []
                            if str(row_id.get("side") or row_id.get("trade_type") or "").upper() in ("BUY", "OPEN")
                        }:
                            continue
                        own_prices[order_id] = item["price"]
                        self._remember_prefix_ids([order_id])
                    install_occupied_open_brake(
                        executor,
                        exchange_pass,
                        lambda: self._foreign_ids_now(row["executor_id"], owned_by or {}),
                        own_ids=own_ids,
                        prior_tracker_ids=self._prior_tracker_ids(connector),
                        own_prices=own_prices,
                    )
                    executor._occupied_pass_key = exchange_pass_key(account, connector, pair)
                except Exception:
                    logger.exception("occupied open brake was not installed; blocking new opens")
                    self._prefix_lists_armed = False
                    hold_new_opens(executor)
                    self.__dict__.setdefault("_kept_ids", set()).add(row["executor_id"])
                    return "open list was not read"
            committed = {
                item.get("order_id")
                for item in (checkpoint or {}).get(CLOSE_HOLD_KEY) or []
                if isinstance(item, dict)
            }
            protected = self._close_price_skip_ids()
            seen = [
                str(item) for item in (getattr(self, "_seen_prefix_ids", []) or [])
                if item and not order_is_unsent(str(item)) and str(item) not in protected
            ]
            if seen and not set(seen).issubset(committed):
                executor._close_may_open = False
            ban = set(getattr(executor, "_close_ban_ids", set()) or set())
            ban.update(item for item in (seen or []) if item and self._has_owner_id(str(item)) and str(item) not in protected)
            executor._close_ban_ids = {item for item in ban if item}
            self._remember_ban_prices(executor, executor._close_ban_ids, locals().get("own_prices") or {})
            if getattr(executor, "_close_may_open", False) and not executor._close_ban_ids:
                executor._close_seat_ready = True
                executor._close_list_ready = True
            self._start_registered_executor(row["executor_id"], executor)
            started = True
            # Routes publish only after start(): a row that returns early never
            # holds the listener route. Saved ids this row owns get the route
            # too — the journal may name an executor outside this resume.
            led = set(accounted)
            led.update(
                order_id for order_id in saved_order_ids(checkpoint)
                if (owned_by or {}).get(order_id) == row["executor_id"]
            )
            if led:
                routes = self.__dict__.setdefault("_accounted_routes", {})
                for order_id in led:
                    routes.setdefault(order_id, row["executor_id"])
            async with self.db_manager.get_session_context() as session:
                await ExecutorRepository(session).update_executor(
                    executor_id=row["executor_id"],
                    status="RUNNING",
                    error_log=notice,
                )
            if row["executor_type"] == "grid_executor":
                await self._checkpoint_executor(row["executor_id"])
            if notice:
                logger.warning(notice)
        except Exception as e:
            if started and row["executor_id"] in self._active_executors:
                logger.error(
                    "Resumed %s is running but the database note failed; leaving it registered: %s",
                    row["executor_id"],
                    e,
                    exc_info=True,
                )
                return None
            self.__dict__.setdefault("_kept_ids", set()).add(row["executor_id"])
            logger.error(
                "Resume of %s failed before start; leaving it registered: %s",
                row["executor_id"],
                e,
                exc_info=True,
            )
            return str(e)
        logger.info(
            f"Resumed {row['executor_type']} {row['executor_id']} "
            f"on {row['connector_name']}/{row['trading_pair']}"
        )
        return None

    async def create_executor(
        self,
        executor_config: Dict[str, Any],
        account_name: Optional[str] = None,
        controller_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create and start a new executor.

        Args:
            executor_config: Executor configuration dictionary (must include 'type')
            account_name: Account to use (defaults to master_account)

        Returns:
            Dictionary with executor_id and initial status
        """
        account = account_name or self.default_account
        trading_interface = self._get_trading_interface(account)

        # Validate executor type and build the typed config
        executor_class, _config_class, typed_config = self._validate_executor_config(
            executor_config, default_timestamp=trading_interface.current_timestamp
        )
        executor_type = executor_config["type"]

        # Ensure connector and market are ready
        connector_name = executor_config.get("connector_name")
        trading_pair = executor_config.get("trading_pair")
        await self._prepare_market(account, connector_name, trading_pair)

        # Instantiate the executor, register it in memory and start it
        controller_id = controller_id or getattr(typed_config, "controller_id", "main") or "main"
        stored_config = dict(executor_config)
        stored_config["id"] = typed_config.id
        stored_config["type"] = executor_type
        metadata = {
            "account_name": account,
            "connector_name": connector_name,
            "trading_pair": trading_pair,
            "executor_type": executor_type,
            "controller_id": controller_id,
            "created_at": datetime.now(timezone.utc),
            "config": stored_config,
        }
        executor_id, executor = self._instantiate_and_register(
            executor_class, typed_config, trading_interface, metadata, start=False
        )
        await self._persist_executor_created(executor_id, executor)
        self._start_registered_executor(executor_id, executor)

        # Capture created_at before potential cleanup
        created_at = metadata["created_at"].isoformat()

        # Check if executor terminated immediately (e.g., insufficient balance)
        # If so, handle completion now rather than waiting for control loop
        if executor.is_closed:
            await self._handle_executor_completion(executor_id)

        logger.info(f"Created {executor_type} executor {executor_id} for {connector_name}/{trading_pair}")

        return {
            "executor_id": executor_id,
            "executor_type": executor_type,
            "connector_name": connector_name,
            "trading_pair": trading_pair,
            "controller_id": controller_id,
            "status": executor.status.name,
            "created_at": created_at
        }

    async def get_executors(
        self,
        account_name: Optional[str] = None,
        connector_name: Optional[str] = None,
        trading_pair: Optional[str] = None,
        executor_type: Optional[str] = None,
        status: Optional[str] = None,
        controller_id: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get list of executors with optional filtering.

        Combines active executors from memory with completed executors from database.

        Args:
            account_name: Filter by account name
            connector_name: Filter by connector name
            trading_pair: Filter by trading pair
            executor_type: Filter by executor type
            status: Filter by status
            controller_id: Filter by controller ID

        Returns:
            List of executor information dictionaries
        """
        result = []

        # Process active executors from memory
        for executor_id, executor in self._active_executors.items():
            metadata = self._executor_metadata.get(executor_id, {})

            # Apply filters
            if account_name and metadata.get("account_name") != account_name:
                continue
            if connector_name and metadata.get("connector_name") != connector_name:
                continue
            if trading_pair and metadata.get("trading_pair") != trading_pair:
                continue
            if executor_type and metadata.get("executor_type") != executor_type:
                continue
            if status and executor.status.name != status:
                continue
            if controller_id and metadata.get("controller_id", "main") != controller_id:
                continue

            result.append(self._format_executor_info(executor_id, executor))

        # Get completed executors from database
        if self.db_manager:
            try:
                async with self.db_manager.get_session_context() as session:
                    repo = ExecutorRepository(session)

                    db_executors = await repo.get_executors(
                        account_name=account_name,
                        connector_name=connector_name,
                        trading_pair=trading_pair,
                        executor_type=executor_type,
                        status=status,
                        controller_id=controller_id,
                        limit=limit
                    )

                    for record in db_executors:
                        # Skip if already in active executors (safety check)
                        if record.executor_id not in self._active_executors:
                            result.append(self._format_db_record(record))
            except Exception as e:
                logger.error(f"Error fetching executors from database: {e}")

        return result

    async def get_executor(self, executor_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific executor.

        Checks active executors in memory first, then falls back to database.

        Args:
            executor_id: The executor ID

        Returns:
            Detailed executor information or None if not found
        """
        # Check active executors first (memory)
        executor = self._active_executors.get(executor_id)
        if executor:
            return self._format_executor_info(executor_id, executor)

        # Fallback to database for completed executors
        if self.db_manager:
            try:
                async with self.db_manager.get_session_context() as session:
                    repo = ExecutorRepository(session)

                    record = await repo.get_executor_by_id(executor_id)
                    if record:
                        return self._format_db_record(record)
            except Exception as e:
                logger.error(f"Error fetching executor from database: {e}")

        return None

    def get_executor_logs(
        self,
        executor_id: str,
        level: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[dict]:
        """
        Get captured log entries for an executor.

        Only available for active executors (logs are cleared on completion).

        Args:
            executor_id: The executor ID
            level: Optional filter by level (ERROR, WARNING, INFO, DEBUG)
            limit: Maximum number of entries to return

        Returns:
            List of log entry dicts
        """
        return self._log_capture.get_logs(executor_id, level=level, limit=limit)

    def _shutdown_level_flat(self, level: Dict[str, Any], fresh: Optional[Dict[str, Any]]) -> bool:
        """Уровень плоский, без позиции: все номера zero-dead либо покупка и
        продажи исполнены (bought == sold, форма flat из _partial_reject)."""
        slot = level.get("open_order") or {}
        close = level.get("close_order") or {}
        opened = str(slot.get("order_id") or "")
        ids = [opened, str(close.get("order_id") or "")]
        ids.extend(
            str(item.get("order_id") or "")
            for item in level.get("partial_closes") or []
            if isinstance(item, dict)
        )
        ids = [item for item in ids if item]
        if ids and all(self._proven_dead_zero(item, fresh) for item in ids):
            return True
        if not opened or not self._proven_dead_filled(opened, fresh):
            return False
        bought = partial_decimal(level.get("partial_bought"))
        if bought is None or bought <= 0:
            return False
        sold = Decimal("0")
        for item in level.get("partial_closes") or []:
            if not isinstance(item, dict):
                continue
            row = self._finished_row(str(item.get("order_id") or ""), fresh)
            if row is None or not self._closed_row_filled(row):
                return False
            sold += self._shutdown_sold_base(row)
        close_id = str(close.get("order_id") or "")
        if close_id:
            row = self._finished_row(close_id, fresh)
            if row is None or not self._closed_row_filled(row):
                return False
            sold += self._shutdown_sold_base(row)
        return sold >= bought

    def _shutdown_position_open(self, level: Dict[str, Any], numbers, fresh: Optional[Dict[str, Any]]) -> bool:
        """Позиция уровня ещё открыта: куплено больше, чем продано.

        Слот open уже может быть другой отменённый номер. Исполненная покупка
        в журнале того же уровня всё равно держит цену тейка.
        """
        close_ids = set()
        closed = str((level.get("close_order") or {}).get("order_id") or "")
        if closed:
            close_ids.add(closed)
        for item in level.get("partial_closes") or []:
            if isinstance(item, dict) and item.get("order_id"):
                close_ids.add(str(item.get("order_id")))
        sold = Decimal("0")
        for text in close_ids:
            row = self._finished_row(text, fresh)
            if row is not None and self._closed_row_filled(row):
                sold += self._shutdown_sold_base(row)
        bought = partial_decimal(level.get("partial_bought"))
        if bought is not None and bought > 0 and sold < bought:
            return True
        for text in numbers or []:
            if text in close_ids or not self._proven_dead_filled(text, fresh):
                continue
            if bought is None or bought <= 0 or sold < bought:
                return True
        return False

    def _level_position_open(self, level: Dict[str, Any], body: Optional[Dict[str, Any]], fresh: Optional[Dict[str, Any]]) -> bool:
        """Позиция открыта по журналу и holds того же уровня, не только по слоту."""
        level_id = str(level.get("id") or "")
        numbers = [
            str((level.get("open_order") or {}).get("order_id") or ""),
            str((level.get("close_order") or {}).get("order_id") or ""),
        ]
        for hold in (body or {}).get("holds") or []:
            if isinstance(hold, dict) and str(hold.get("level_id") or "") == level_id and hold.get("order_id"):
                numbers.append(str(hold.get("order_id")))
        try:
            journal = read_ledger_filtered()
        except Exception:
            return True
        if journal is None:
            return True
        for row in journal:
            if str(row.get("level_id") or "") == level_id and row.get("order_id"):
                numbers.append(str(row.get("order_id")))
        return self._shutdown_position_open(level, [item for item in numbers if item], fresh)

    def _shutdown_sold_base(self, row: Dict[str, Any]) -> Decimal:
        """База исполненной продажи: amount минус left, иначе fill_size."""
        base = partial_raw_base(row)
        if base is not None:
            return base
        return partial_decimal(row.get("fill_size")) or Decimal("0")

    def _shutdown_id_dead(self, order_id: str, fresh: Optional[Dict[str, Any]]) -> bool:
        """Номер доказанно мёртв только по записи finished этой заявки.

        Пустой список, даже покрывающий время строки, не доказательство: заявка
        могла дойти до биржи и ещё не сидеть в слоте. Конец отпустил бы гашение,
        а отмена её не видит.
        """
        text = str(order_id or "")
        if not text or not self._lists_ready(fresh):
            return False
        if partial_text_in(fresh.get("opens"), text):
            return False
        return self._finished_row(text, fresh) is not None

    def _shutdown_end_mark(self, ends: Dict[str, Any], order_id: str) -> None:
        """Конец отдельного номера: без price и без level_id, ключ — номер."""
        text = str(order_id)
        if partial_end_visible({"partial_ends": ends}, text):
            return
        ends[text] = {"kind": "flat", "order_ids": [text]}

    def _recorded_market_close(self, executor_id: str) -> str:
        """Номер продажи гашения: журнал или hold чекпоинта. Непрочитанный журнал — пусто, не «номера нет» для чеканки."""
        held = ""
        for hold in (self._partial_parsed(executor_id) or {}).get("holds") or []:
            if isinstance(hold, dict) and hold.get("role") == "market_close" and hold.get("order_id"):
                held = str(hold.get("order_id"))
                break
        rows = self._partial_rows()
        if rows is None:
            return held or None
        for row in rows:
            if str(row.get("executor_id") or "") != str(executor_id):
                continue
            if str(row.get("role") or "") == "market_close" and row.get("order_id"):
                return str(row.get("order_id"))
        return held

    def _shutdown_close_ids(self, executor_id: str) -> Optional[List[str]]:
        """Все записанные номера продажи гашения: журнал по порядку, потом holds.

        Непрочитанный журнал — None, не пустой список: пустой список значит «номеров нет»,
        и чеканка следующего была бы законна.
        """
        rows = self._partial_rows()
        if rows is None:
            return None
        found: List[str] = []
        for row in rows:
            if str(row.get("executor_id") or "") != str(executor_id):
                continue
            if str(row.get("role") or "") == "market_close" and row.get("order_id"):
                name = str(row.get("order_id"))
                if name not in found:
                    found.append(name)
        for hold in (self._partial_parsed(executor_id) or {}).get("holds") or []:
            if isinstance(hold, dict) and hold.get("role") == "market_close" and hold.get("order_id"):
                name = str(hold.get("order_id"))
                if name not in found:
                    found.append(name)
        return found

    def _shutdown_sell_missing(self, order_id: str, fresh: Optional[Dict[str, Any]]) -> bool:
        """Номера нет ни на книге, ни в finished. Это не смерть и не покрытие."""
        if not self._lists_ready(fresh):
            return False
        text = str(order_id or "")
        if not text or partial_text_in((fresh or {}).get("opens"), text):
            return False
        return self._finished_row(text, fresh) is None

    def _shutdown_missing_market_closes(self, executor_id: str, fresh: Optional[Dict[str, Any]]) -> List[str]:
        """Записанные продажи гашения, которых нет в тик-книге. Их исполнение ищет архив."""
        found: List[str] = []
        for text in self._shutdown_close_ids(executor_id) or []:
            if self._exchange_gone_end(executor_id, text):
                continue
            if self._shutdown_sell_missing(text, fresh) and text not in found:
                found.append(text)
        return found

    def _archive_sold_base(self, executor_id: str, order_id: str) -> Optional[Decimal]:
        """База продажи гашения из архива, в монетах. Нет доказательства — None, не ноль."""
        if (self.__dict__.get("_retained_archive_status") or {}).get(executor_id) != "read":
            return None
        cached = (self.__dict__.get("_retained_archive_rows") or {}).get(executor_id)
        if not isinstance(cached, list):
            return None
        text = str(order_id or "")
        for row in cached:
            if not isinstance(row, dict):
                continue
            name = str(row.get("clOrdId") or row.get("text") or row.get("order_id") or "")
            if name != text:
                continue
            if str(row.get("state") or "") != "filled":
                return None
            size = partial_decimal(row.get("coin_fill_size"))
            if size is None or size <= 0:
                return None
            return size
        return None

    def _shutdown_sell_settled(self, executor_id: str, executor: Any) -> bool:
        """True только когда каждый записанный market_close доказанно не ушёл

        (order_is_unsent) либо записанных нет. Исполнение, концы и
        непрочитанное отпуск не дают: штатную постановку не пускаем.
        """
        ids = self._shutdown_close_ids(executor_id)
        if ids is None:
            return False
        for text in ids:
            if not order_is_unsent(text):
                return False
        return True

    def _shutdown_remainder(self, executor_id: str, fresh: Optional[Dict[str, Any]]) -> Optional[Decimal]:
        """Остаток сетки по своим номерам. Не доказан — None, продажу не слать."""
        rows = self._partial_rows()
        if rows is None or not self._lists_ready(fresh):
            return None
        by_id: Dict[str, str] = {}
        for row in rows:
            if str(row.get("executor_id") or "") != str(executor_id):
                continue
            name = str(row.get("order_id") or "")
            if not name:
                continue
            role = str(row.get("role") or "")
            if name not in by_id or role:
                by_id[name] = role
        if not by_id:
            return Decimal("0")
        bought = Decimal("0")
        sold = Decimal("0")
        for name, role in by_id.items():
            sell = role in ("close", "market_close")
            buy = role in ("", "open", "open_order")
            if not sell and not buy:
                return None
            finished = self._finished_row(name, fresh)
            if self._exchange_gone_end(executor_id, name):
                if partial_text_in((fresh or {}).get("opens"), name) or finished is None:
                    return None
                if self._closed_row_filled(finished):
                    pass
                elif self._finished_zero_cancel(finished):
                    continue
                else:
                    return None
            if sell and role == "market_close" and self._shutdown_sell_missing(name, fresh):
                archived = self._archive_sold_base(executor_id, name)
                if archived is None:
                    return None
                sold += archived
                continue
            if sell and not self._proven_dead(name, fresh):
                return None
            if buy and (finished is None or not self._closed_row_filled(finished)):
                if finished is None or not self._finished_zero_cancel(finished):
                    return None
                continue
            if finished is None or not self._closed_row_filled(finished):
                continue
            if partial_raw_base(finished) is None and partial_decimal(finished.get("fill_size")) is None:
                return None
            base = self._shutdown_sold_base(finished)
            if buy:
                bought += base
            else:
                sold += base
        if bought <= 0:
            return None
        if sold >= bought:
            return Decimal("0")
        return bought - sold

    def _shutdown_mark_seconds(self, ts: Any) -> Optional[float]:
        """Метка журнала в секундах. Миллисекунды, как у архива, делим."""
        try:
            mark = float(ts)
        except (TypeError, ValueError):
            return None
        if mark > 1_000_000_000_000:
            mark = mark / 1000
        return mark

    def _shutdown_row_text(self, raw: Any) -> str:
        if not isinstance(raw, dict):
            return ""
        return str(raw.get("text") or raw.get("order_id") or "")

    def _shutdown_text_present(self, rows: Any, name: str) -> bool:
        return any(self._shutdown_row_text(raw) == name for raw in rows or [])

    def _shutdown_our_marks(self, executor_id: str) -> Optional[Dict[str, Optional[float]]]:
        """Номера этой сетки и самая старая метка. Чужой executor_id не входит."""
        rows = self._partial_rows()
        if rows is None:
            return None
        marks: Dict[str, Optional[float]] = {}
        for row in rows:
            if str(row.get("executor_id") or "") != str(executor_id):
                continue
            name = str(row.get("order_id") or "")
            if not name:
                continue
            mark = self._shutdown_mark_seconds(row.get("ts"))
            if name not in marks:
                marks[name] = mark
                continue
            current = marks[name]
            if mark is not None and (current is None or mark < current):
                marks[name] = mark
        return marks

    def _shutdown_archive_need(self, executor_id: str, fresh: Dict[str, Any]) -> Dict[str, Any]:
        """skip — живой список уже держит номера. block — архив не спрашиваем и не продаём.

        fetch — окно от самой старой недоказанной метки этой сетки, старше 7 дней и не старше 90.
        """
        marks = self._shutdown_our_marks(executor_id)
        if marks is None or not self._lists_ready(fresh):
            return {"need": "block"}
        now_sec = time.time()
        history_sec = OKX_HISTORY_MS / 1000
        archive_sec = OKX_ARCHIVE_MS / 1000
        oldest: Optional[float] = None
        for name, mark in marks.items():
            if self._shutdown_text_present(fresh.get("opens"), name):
                continue
            if self._finished_row(name, fresh) is not None:
                continue
            if mark is None or now_sec - mark > archive_sec:
                return {"need": "block"}
            if now_sec - mark > history_sec:
                if oldest is None or mark < oldest:
                    oldest = mark
        if oldest is None:
            return {"need": "skip"}
        return {
            "need": "fetch",
            "begin": str(int((oldest - 300) * 1000)),
            "end": str(int(now_sec * 1000)),
        }

    def _shutdown_coin_book(
        self, executor_id: str, connector: Any, pair: str, book: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Сырой accFillSz этой сетки в монетах. Смесь форм и отказ перевода — не продавать."""
        marks = self._shutdown_our_marks(executor_id)
        if marks is None:
            return None
        ours = set(marks)
        saw_coins = False
        saw_contracts = False
        for raw in book.get("finished") or []:
            if self._shutdown_row_text(raw) not in ours:
                continue
            if "amount" in raw and "left" in raw:
                saw_coins = True
            elif "fill_size" in raw:
                saw_contracts = True
        if saw_coins and saw_contracts:
            return None
        if not saw_contracts:
            return book
        converted: List[Dict[str, Any]] = []
        for raw in book.get("finished") or []:
            if not isinstance(raw, dict):
                return None
            text = self._shutdown_row_text(raw)
            if text not in ours or "amount" in raw or "fill_size" not in raw:
                converted.append(raw)
                continue
            size = partial_decimal(raw.get("fill_size"))
            if size is None or size < 0:
                return None
            if size == 0:
                converted.append(raw)
                continue
            base = self._contract_base(connector, pair, size)
            if base is None:
                return None
            copied = dict(raw)
            copied["fill_size"] = str(base)
            converted.append(copied)
        out = dict(book)
        out["finished"] = converted
        return out

    def _retained_archive_window(self, now: Optional[float] = None) -> Dict[str, str]:
        """Не шире 90 дней. Чужую более новую метку журнала в begin не берём."""
        moment = time.time() if now is None else float(now)
        end_ms = int(moment * 1000)
        return {
            "begin": str(end_ms - OKX_ARCHIVE_MS),
            "end": str(end_ms),
        }

    def _note_retained_archive(
        self, executor_id: str, status: str, rows: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Кэш строк не меняет форму: список или пусто. Исход чтения — отдельно."""
        bag = self.__dict__.setdefault("_retained_archive_rows", {})
        note = self.__dict__.setdefault("_retained_archive_status", {})
        if status == "read":
            bag[executor_id] = list(rows or [])
        else:
            bag[executor_id] = None
        note[executor_id] = status

    def _retained_archive_detail(self, executor_id: str, order_id: str) -> str:
        """unread, empty или сырой accFillSz. Подставленный ноль сюда не попадает."""
        text = str(order_id or "")
        status = (self.__dict__.get("_retained_archive_status") or {}).get(executor_id)
        if status != "read":
            reason = str(status or "unread")
            if not reason.startswith("unread"):
                reason = f"unread:{reason}"
            return f"order={text} result={reason}"
        cached = (self.__dict__.get("_retained_archive_rows") or {}).get(executor_id)
        if not isinstance(cached, list):
            return f"order={text} result=unread"
        parts: List[str] = []
        for row in cached:
            if not isinstance(row, dict):
                continue
            name = str(row.get("clOrdId") or row.get("text") or row.get("order_id") or "")
            if name != text:
                continue
            state = str(row.get("state") or "")
            size = "missing" if "accFillSz" not in row else str(row.get("accFillSz"))
            parts.append(f"state={state} accFillSz={size}")
        if not parts:
            return f"order={text} result=empty"
        return f"order={text} result=row " + " ".join(parts)

    def _log_retained_archive(self, executor_id: str, pair: str) -> None:
        """Одна строка на номер с концом 51400. Журнал завершение не отпускает."""
        for text in self._retained_ids(executor_id):
            if not self._exchange_gone_end(executor_id, text):
                continue
            detail = self._retained_archive_detail(executor_id, text)
            self._log_silent_exit(
                "retained-archive", pair, detail, executor_id,
                interval=300.0, label="silent stop exit:", key=f"{text}:{detail}",
            )

    async def _refresh_retained_archive(
        self, executor_id: str, executor: Any, connector: Any, pair: str,
    ) -> None:
        """Сырой accFillSz архива для номера из retained_orders. В слот пары не пишется."""
        bag = self.__dict__.setdefault("_retained_archive_rows", {})
        need = [
            text for text in self._retained_ids(executor_id)
            if self._exchange_gone_end(executor_id, text)
        ]
        for text in self._shutdown_missing_market_closes(executor_id, self._tick_fresh(pair or "")):
            if text not in need:
                need.append(text)
        if not need:
            bag[executor_id] = None
            return

        def finish(status: str, rows: Optional[List[Dict[str, Any]]] = None) -> None:
            self._note_retained_archive(executor_id, status, rows)
            self._log_retained_archive(executor_id, pair)

        if connector is None:
            finish("unread:no-connector")
            return
        venue = open_list_venue(type(connector).__name__ if connector is not None else "")
        if venue != "okx":
            finish("unread:not-okx")
            return
        symbol = await self._gate_symbol(connector, pair)
        if not symbol:
            finish("unread:no-symbol")
            return
        inst_type = "SWAP" if "perpetual" in type(connector).__name__.lower() else "SPOT"
        window = self._retained_archive_window()
        from utils.executor_checkpoint import OPEN_PAGE_LIMIT
        rows: List[Dict[str, Any]] = []
        after = None
        try:
            for _ in range(1000):
                request = okx_closed_request(
                    symbol, after=after, begin=window["begin"], end=window["end"],
                    inst_type=inst_type, archive=True,
                )
                if request is None:
                    finish("unread:no-request")
                    return
                body = await connector._api_get(
                    path_url=request["path"],
                    params=request["params"],
                    is_auth_required=True,
                    limit_id=self._open_limit_id(connector, "okx"),
                )
                if not isinstance(body, dict) or str(body.get("code")) != "0" or not isinstance(body.get("data"), list):
                    finish("unread:bad-body")
                    return
                data = body["data"]
                for raw in data:
                    if not isinstance(raw, dict):
                        finish("unread:bad-body")
                        return
                    try:
                        created = int(str(raw.get("cTime")))
                    except (TypeError, ValueError):
                        finish("unread:bad-body")
                        return
                    if created < int(window["begin"]) or created > int(window["end"]):
                        continue
                    size = partial_decimal(raw.get("accFillSz"))
                    base = self._contract_base(connector, pair, size) if size is not None and size > 0 else None
                    if base is not None:
                        raw["coin_fill_size"] = str(base)
                    rows.append(raw)
                if len(data) < OPEN_PAGE_LIMIT:
                    finish("read", rows)
                    return
                cursor = data[-1].get("ordId") if isinstance(data[-1], dict) else None
                if not cursor:
                    finish("unread:no-cursor")
                    return
                after = str(cursor)
        except Exception as exc:
            finish("unread:exception:" + type(exc).__name__)
            return
        finish("unread:page-cap")

    async def _shutdown_remainder_book(
        self, executor_id: str, connector: Any, pair: str, fresh: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """Копия книги для остатка. В слот пары не пишется. Непрочитанный архив — не ноль."""
        if connector is None or not isinstance(fresh, dict) or not self._lists_ready(fresh):
            return None
        copied: List[Dict[str, Any]] = []
        for raw in fresh.get("finished") or []:
            if not isinstance(raw, dict):
                return None
            copied.append(dict(raw))
        book = dict(fresh)
        book["finished"] = copied
        need = self._shutdown_archive_need(executor_id, fresh)
        if need.get("need") == "block":
            return None
        if need.get("need") == "fetch":
            venue = open_list_venue(type(connector).__name__ if connector is not None else "")
            if venue != "okx":
                return None
            symbol = await self._gate_symbol(connector, pair)
            if not symbol:
                return None
            inst_type = "SWAP" if "perpetual" in type(connector).__name__.lower() else "SPOT"
            archived = await self._okx_closed_pages(
                connector, symbol, pair, inst_type, True, need["begin"], need["end"],
            )
            if not isinstance(archived, dict) or not archived.get("read") or not archived.get("done"):
                return None
            known = {self._shutdown_row_text(row) for row in copied}
            for raw in archived.get("orders") or []:
                if not isinstance(raw, dict):
                    return None
                text = self._shutdown_row_text(raw)
                if not text or text in known:
                    continue
                copied.append(dict(raw))
                known.add(text)
        return self._shutdown_coin_book(executor_id, connector, pair, book)

    _NOTHING_TO_CLOSE = (
        "you don't have any positions in this direction for this contract to reduce or close"
    )

    def _nothing_to_close_text(self, text: Any) -> bool:
        """Только фраза «в эту сторону закрывать нечего». Часы и прочий отказ — нет."""
        blob = str(text or "")
        if "50102" in blob or "50113" in blob:
            return False
        return self._NOTHING_TO_CLOSE in blob

    def _nothing_to_close_end(self, executor_id: str, order_id: str) -> bool:
        """Конец продажи гашения: не 51400 и не исполнение. Без price и без level_id."""
        mark = ((self._partial_parsed(executor_id) or {}).get("partial_ends") or {}).get(str(order_id)) or {}
        return (
            isinstance(mark, dict)
            and mark.get("kind") == "nothing-to-close"
            and "price" not in mark
            and "level_id" not in mark
        )

    async def _shutdown_close_refusal(self, order_id: str) -> Optional[bool]:
        """Фраза отказа этого номера. False — таблица прочитана, фразы нет. None — не прочитана."""
        text = str(order_id or "")
        if not text or not self.db_manager:
            return None
        try:
            async with self.db_manager.get_session_context() as session:
                from database.repositories.order_repository import OrderRepository
                row = await OrderRepository(session).get_order_by_client_id(text)
        except Exception:
            return None
        if row is None:
            return False
        return self._nothing_to_close_text(getattr(row, "error_message", None))

    async def _remember_nothing_to_close(
        self, executor_id: str, order_id: str, fresh: Optional[Dict[str, Any]], connector: Any = None,
    ) -> bool:
        """Конец до того, как продажу перестанем слать. Живой номер на книге конец не получает."""
        text = str(order_id or "")
        if not executor_id or not text or not self._lists_ready(fresh):
            return False
        if self._shutdown_text_present((fresh or {}).get("opens"), text):
            return False
        if connector is not None and self._id_still_live(fresh, connector, text):
            return False
        if self._nothing_to_close_end(executor_id, text):
            return True

        def mutate(body: Dict[str, Any]) -> None:
            ends = dict(body.get("partial_ends") or {})
            current = ends.get(text) or {}
            if isinstance(current, dict) and current.get("kind") == "nothing-to-close":
                return
            ends[text] = {"kind": "nothing-to-close", "order_ids": [text]}
            body["partial_ends"] = ends

        def visible(parsed: Optional[Dict[str, Any]]) -> bool:
            mark = ((parsed or {}).get("partial_ends") or {}).get(text) or {}
            return (
                isinstance(mark, dict)
                and mark.get("kind") == "nothing-to-close"
                and "price" not in mark
                and "level_id" not in mark
            )

        return await self._partial_cas(executor_id, mutate, visible)

    def _shutdown_close_proven(self, executor_id: str, fresh: Optional[Dict[str, Any]] = None) -> bool:
        """Продажа остатка доказана finished или концом «закрывать нечего».

        Копия с архивом, если её передали. Без неё — книга пары: старая
        недоказанная покупка этим вызовом не становится нулём. Непрочитанное
        — не доказательство. Живой номер на открытой книге флаг не поднимает.
        """
        if fresh is None:
            pair = ""
            executor = (getattr(self, "_active_executors", None) or {}).get(executor_id)
            if executor is not None:
                pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
            fresh = self._pair_book(pair)
        if self._partial_rows() is None or not self._lists_ready(fresh):
            return False
        if self._shutdown_remainder(executor_id, fresh) == 0:
            return True
        close_ids = self._shutdown_close_ids(executor_id)
        if not close_ids:
            return False
        for text in close_ids:
            if self._shutdown_text_present((fresh or {}).get("opens"), text):
                return False
            if not self._nothing_to_close_end(executor_id, text):
                return False
        return True

    async def _seat_market_close(self, executor_id: str, order_id: str) -> bool:
        """Номер гашения в чекпоинт до продажи. Метку не снимает."""
        def mutate(body: Dict[str, Any]) -> None:
            holds = list(body.get("holds") or [])
            for item in holds:
                if isinstance(item, dict) and str(item.get("order_id") or "") == order_id:
                    return
            holds.append({"order_id": order_id, "role": "market_close"})
            body["holds"] = holds

        def visible(parsed: Optional[Dict[str, Any]]) -> bool:
            for item in (parsed or {}).get("holds") or []:
                if isinstance(item, dict) and str(item.get("order_id") or "") == order_id and item.get("role") == "market_close":
                    return True
            return False

        return await self._partial_cas(executor_id, mutate, visible)

    def _clear_pair_position(self, pair: str) -> None:
        (self.__dict__.get("_pair_position_body") or {}).pop(pair, None)

    def _position_body_flat(self, body: Any, symbol: str) -> Optional[bool]:
        """Плоско только по телу этой пары. Пустой кэш и чужая пара — не плоско."""
        if not isinstance(body, dict) or str(body.get("code")) != "0" or not isinstance(body.get("data"), list):
            return None
        wanted = str(symbol or "")
        if not wanted:
            return None
        saw_own = False
        saw_foreign = False
        for row in body["data"]:
            if not isinstance(row, dict):
                return None
            inst = str(row.get("instId") or "")
            if inst and inst != wanted:
                saw_foreign = True
                continue
            if "pos" not in row:
                return None
            try:
                size = Decimal(str(row.get("pos")))
            except Exception:
                return None
            saw_own = True
            if size != 0:
                return False
        if saw_foreign and not saw_own:
            return None
        return True

    def _executor_position_flat(self, executor: Any) -> Optional[bool]:
        """Тело этого тика на исполнителе. Общий слот пары чужой тик стирает."""
        slot = getattr(executor, "_flat_position_tick", None)
        if not isinstance(slot, dict) or slot.get("body") is None:
            return None
        return self._position_body_flat(slot.get("body"), str(slot.get("symbol") or ""))

    def _stored_position_flat(self, pair: str) -> Optional[bool]:
        slot = (self.__dict__.get("_pair_position_body") or {}).get(pair)
        if not isinstance(slot, dict) or slot.get("body") is None:
            return None
        return self._position_body_flat(slot.get("body"), str(slot.get("symbol") or ""))

    async def _reread_executor_position(self, executor: Any) -> Optional[bool]:
        """Свежее тело этой сетки. Пустой ответ — не плоско."""
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        if connector is None or not hasattr(connector, "_api_get"):
            return self._executor_position_flat(executor)
        symbol = ""
        body = None
        try:
            if pair:
                symbol = await self._gate_symbol(connector, pair) or ""
            if symbol:
                body = await connector._api_get(
                    path_url="/api/v5/account/positions",
                    params={"instId": symbol},
                    is_auth_required=True,
                    limit_id=self._open_limit_id(connector, "okx"),
                )
        except Exception:
            body = None
        executor._flat_position_tick = {
            "symbol": symbol,
            "body": body if isinstance(body, dict) else None,
        }
        return self._executor_position_flat(executor)

    async def _read_pair_position_body(self, connector: Any, pair: str) -> None:
        """Тело позиций одной пары. `_update_positions` не зовём и кэш коннектора не читаем."""
        symbol = ""
        body = None
        try:
            if connector is not None and pair:
                symbol = await self._gate_symbol(connector, pair) or ""
            if symbol and connector is not None:
                body = await connector._api_get(
                    path_url="/api/v5/account/positions",
                    params={"instId": symbol},
                    is_auth_required=True,
                    limit_id=self._open_limit_id(connector, "okx"),
                )
        except Exception:
            body = None
        self.__dict__.setdefault("_pair_position_body", {})[pair] = {
            "symbol": symbol,
            "body": body if isinstance(body, dict) else None,
        }

    async def _flat_close_live_blocks(self, executor_id: str, executor: Any) -> bool:
        """Живой номер или неизвестная отправка — строку раннего стопа не пишем.

        Открытые берём новым чтением, не снимком начала тика.
        """
        if order_miss_unreadable():
            return True
        if not self._executor_proofs_released(executor_id):
            return True
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        fresh = None
        if connector is not None and pair:
            try:
                fresh = await self._fresh_raw_lists(connector, pair, None)
            except Exception:
                fresh = None
        if not self._lists_ready(fresh):
            return True
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        inflight = getattr(connector, "in_flight_orders", None)
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None)
        if not isinstance(inflight, dict) or not isinstance(lost, dict):
            return True
        column_ids = self._column_ids_for_executor(executor_id)
        if column_ids is None:
            return True
        try:
            from utils.executor_checkpoint import read_ledger_filtered
            rows = read_ledger_filtered()
        except Exception:
            return True
        ids = {
            str(row.get("order_id"))
            for row in rows or []
            if row.get("executor_id") == executor_id and row.get("order_id")
        }
        ids.update(column_ids)
        ids.update(self._close_hold_ids_for(executor_id, executor))
        for text in ids:
            if order_send_unknown(text) or text in inflight or text in lost:
                return True
            if self._shutdown_text_present((fresh or {}).get("opens"), text):
                return True
        return False

    async def _commit_flat_early_stop(self, executor_id: str, executor: Any) -> bool:
        """Ранний стоп в базе и перечитка. Цикл гасим только после неё. Флаг не ставим."""
        if await self._reread_executor_position(executor) is not True:
            return False
        if not self._stop_mark_now(executor_id) or await self._flat_close_live_blocks(executor_id, executor):
            return False
        if await self._reread_executor_position(executor) is not True:
            return False
        if not self.db_manager:
            return False
        from database.repositories.executor_repository import ExecutorRepository
        try:
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)
                current = await repo.get_executor_by_id(executor_id)
                if current is not None and str(getattr(current, "close_type", None) or "") == "FAILED":
                    return False
                if current is not None and str(getattr(current, "status", None) or "") != "RUNNING":
                    if str(getattr(current, "close_type", None) or "") != "EARLY_STOP":
                        return False
                written = await repo.update_executor(
                    executor_id, status="TERMINATED", close_type="EARLY_STOP",
                )
                if written is None:
                    return False
                if await self._flat_close_live_blocks(executor_id, executor):
                    written.status = "RUNNING"
                    written.close_type = ""
                    reverted = True
                else:
                    reverted = False
            async with self.db_manager.get_session_context() as session:
                seen = await ExecutorRepository(session).get_executor_by_id(executor_id)
        except Exception:
            return False
        if seen is None:
            return False
        status = str(getattr(seen, "status", None) or "")
        close_type = str(getattr(seen, "close_type", None) or "")
        if reverted and status != "RUNNING":
            await self._restore_running_after_flat(executor_id)
            return False
        if reverted:
            return False
        if status == "RUNNING" or close_type != "EARLY_STOP" or close_type == "FAILED":
            return False
        if await self._reread_executor_position(executor) is not True:
            await self._restore_running_after_flat(executor_id)
            return False
        if await self._flat_checkpoint_number_live(executor_id, executor):
            await self._restore_running_after_flat(executor_id)
            return False
        self.__dict__.setdefault("_flat_stopped", set()).add(executor_id)
        self._leave_loop_after_flat_row(executor)
        return True

    async def _restore_running_after_flat(self, executor_id: str) -> bool:
        """Номер ожил, пока запись шла: вернуть строку в работу и перечитать."""
        from database.repositories.executor_repository import ExecutorRepository
        try:
            async with self.db_manager.get_session_context() as session:
                written = await ExecutorRepository(session).update_executor(
                    executor_id, status="RUNNING", close_type="",
                )
                if written is None:
                    return False
            async with self.db_manager.get_session_context() as session:
                seen = await ExecutorRepository(session).get_executor_by_id(executor_id)
        except Exception:
            return False
        if seen is None:
            return False
        status = str(getattr(seen, "status", None) or "")
        close_type = str(getattr(seen, "close_type", None) or "")
        return status == "RUNNING" and close_type != "FAILED"

    async def _early_stop_body_not_flat(self, row: Dict[str, Any]) -> bool:
        """Неплоское или непрочитанное тело — повод поднять хозяина."""
        connector = None
        try:
            interface = self._get_trading_interface(row.get("account_name") or "")
            if interface is not None:
                connector = interface.get_connector(row.get("connector_name") or "")
        except Exception:
            return True
        if connector is None or not hasattr(connector, "_api_get"):
            return True
        holder = SimpleNamespace(
            config=SimpleNamespace(
                trading_pair=row.get("trading_pair") or "",
                connector_name=row.get("connector_name") or "",
            ),
            connectors={row.get("connector_name") or "": connector},
        )
        flat = await self._reread_executor_position(holder)
        return flat is not True

    async def _restore_early_stop_if_needed(self, executor_id: str) -> bool:
        """Пока строка ранний стоп, а тело уже не плоско — снова писать работу."""
        if not self.db_manager:
            return False
        from database.repositories.executor_repository import ExecutorRepository
        try:
            async with self.db_manager.get_session_context() as session:
                current = await ExecutorRepository(session).get_executor_by_id(executor_id)
        except Exception:
            return False
        if current is None or str(getattr(current, "close_type", None) or "") != "EARLY_STOP":
            return False
        return await self._restore_running_after_flat(executor_id)

    async def _restart_flat_owner_if_needed(self, executor_id: str, executor: Any) -> None:
        """Спящий цикл уже вышел, номер жив: start() при метке стопа."""
        if self._executor_loop_running(executor) or not self._stop_mark_now(executor_id):
            return
        terminated = getattr(executor, "terminated", None)
        if terminated is None or not terminated.is_set():
            return
        if not await self._flat_stop_needs_owner(executor_id, executor):
            return
        await self._restore_running_after_flat(executor_id)
        from hummingbot.strategy_v2.executors.executor_base import RunnableStatus
        executor._status = RunnableStatus.NOT_STARTED
        if "is_closed" in getattr(executor, "__dict__", {}):
            executor.is_closed = False
        start = getattr(executor, "start", None)
        if callable(start):
            start()

    async def _early_stop_number_seen_live(self, row: Dict[str, Any], ids: set) -> bool:
        """Непрочитанные открытые и неготовый finished — как живой номер."""
        if order_miss_unreadable():
            return True
        connector = None
        try:
            interface = self._get_trading_interface(row.get("account_name") or "")
            if interface is not None:
                connector = interface.get_connector(row.get("connector_name") or "")
        except Exception:
            connector = None
        if connector is None:
            return True
        inflight = getattr(connector, "in_flight_orders", None)
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None)
        if not isinstance(inflight, dict) or not isinstance(lost, dict):
            return True
        for text in ids:
            if order_send_unknown(text) or text in inflight or text in lost:
                return True
        pair = row.get("trading_pair") or ""
        if not pair:
            return True
        try:
            fresh = await self._fresh_raw_lists(connector, pair, None)
        except Exception:
            return True
        if not isinstance(fresh, dict):
            return True
        opens_read = bool(fresh.get("opens_read") and fresh.get("opens_done"))
        if opens_read and any(
            self._shutdown_text_present(fresh.get("opens"), text) for text in ids
        ):
            return True
        if not self._lists_ready(fresh):
            return True
        return False

    async def _reclaim_early_stop_with_a_live_number(self) -> None:
        """Обрыв после раннего стопа: живой номер возвращает строку в работу."""
        if not self.db_manager:
            return
        from database.repositories.executor_repository import ExecutorRepository
        from utils.executor_checkpoint import saved_order_ids, parse_checkpoint
        try:
            async with self.db_manager.get_session_context() as session:
                rows = await ExecutorRepository(session).get_early_stop_rows()
        except Exception:
            return
        for row in rows:
            executor_id = str(row.get("executor_id") or "")
            if not executor_id or str(row.get("close_type") or "") == "FAILED":
                continue
            parsed = parse_checkpoint(row.get("checkpoint"))
            ids = set(saved_order_ids(parsed))
            for hold in (parsed or {}).get("holds") or []:
                if isinstance(hold, dict) and hold.get("order_id"):
                    ids.add(str(hold["order_id"]))
            ids.update(self._ids_of((parsed or {}).get("close_holds")))
            journal_failed = False
            try:
                from utils.executor_checkpoint import read_ledger_filtered
                ids.update(
                    str(item.get("order_id"))
                    for item in read_ledger_filtered() or []
                    if item.get("executor_id") == executor_id and item.get("order_id")
                )
            except Exception:
                journal_failed = True
            body_not_flat = await self._early_stop_body_not_flat(row)
            if journal_failed or body_not_flat or await self._early_stop_number_seen_live(row, ids):
                restored = False
                for _ in range(5):
                    try:
                        restored = await self._restore_running_after_flat(executor_id)
                    except Exception:
                        restored = False
                    if restored:
                        break
                if not restored:
                    self.__dict__.setdefault("_flat_restore_pending", set()).add(executor_id)
                if executor_id not in self._active_executors:
                    self.__dict__.setdefault("_stop_mark_ram", {})[executor_id] = True
                    await self._resume_one(row)

    def _bind_executor_loop_task(self, executor: Any) -> None:
        """Запомнить task цикла. Sleep потом прячет кадр, task остаётся."""
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        if current is not None and not current.done():
            executor._flat_loop_task = current
            return
        try:
            tasks = asyncio.all_tasks()
        except RuntimeError:
            return
        for task in tasks:
            if task.done():
                continue
            if self._task_is_executor_loop(task, executor):
                executor._flat_loop_task = task
                return

    def _close_hold_ids_for(self, executor_id: str, executor: Any = None) -> set:
        """Номера close_holds: журнал их уже мог вычистить, открытые — нет."""
        from utils.executor_checkpoint import parse_checkpoint
        found = set()
        for row in getattr(self, "_hold_rows", None) or []:
            key = str(row.get("executor_id") or row.get("row_executor_id") or "")
            if key != str(executor_id):
                continue
            parsed = parse_checkpoint(row.get("checkpoint"))
            found.update(self._ids_of((parsed or {}).get("close_holds")))
        target = executor if executor is not None else self._active_executors.get(executor_id)
        found.update(self._ids_of(getattr(target, "_close_holds", []) or []))
        return found

    def _checkpoint_ids_for_flat_stop(self, executor_id: str) -> set:
        """Номера из строки, даже если sweep спрятал executor_id у TERMINATED."""
        from utils.executor_checkpoint import parse_checkpoint, saved_order_ids
        found = set()
        rows = getattr(self, "_hold_rows", None) or []
        for row in rows:
            key = str(row.get("executor_id") or row.get("row_executor_id") or "")
            if key != str(executor_id):
                continue
            parsed = parse_checkpoint(row.get("checkpoint"))
            found.update(str(item) for item in saved_order_ids(parsed))
            for hold in (parsed or {}).get("holds") or []:
                if isinstance(hold, dict) and hold.get("order_id"):
                    found.add(str(hold["order_id"]))
            found.update(self._ids_of((parsed or {}).get("close_holds")))
        found.update(self._close_hold_ids_for(executor_id))
        column = self._column_ids_for_executor(executor_id)
        if column:
            found.update(str(item) for item in column)
        return found

    async def _flat_checkpoint_number_live(self, executor_id: str, executor: Any) -> bool:
        """Живой номер из чекпоинта, не из ряда, который sweep уже спрятал."""
        if order_miss_unreadable():
            return True
        ids = self._checkpoint_ids_for_flat_stop(executor_id)
        if not ids:
            return True
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        inflight = getattr(connector, "in_flight_orders", None)
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None)
        if not isinstance(inflight, dict) or not isinstance(lost, dict):
            return True
        for text in ids:
            if order_send_unknown(text) or text in inflight or text in lost:
                return True
        fresh = None
        if connector is not None and pair:
            try:
                fresh = await self._fresh_raw_lists(connector, pair, None)
            except Exception:
                return True
        if not isinstance(fresh, dict) or not (fresh.get("opens_read") and fresh.get("opens_done")):
            return True
        return any(self._shutdown_text_present(fresh.get("opens"), text) for text in ids)

    async def _flat_stop_needs_owner(self, executor_id: str, executor: Any) -> bool:
        """Живой номер или неплоское тело — исполнителя не снимать.

        Последний await — свежие открытые, не тело: пока тело читалось, номер мог ожить.
        """
        if executor_id not in (self.__dict__.get("_flat_stopped") or set()):
            return False
        await self._reread_executor_position(executor)
        if await self._flat_close_live_blocks(executor_id, executor):
            return True
        return self._executor_position_flat(executor) is not True

    async def _flat_stop_still_live(self, executor_id: str, executor: Any) -> bool:
        """После раннего стопа номер ожил — исполнителя не вынимать."""
        if executor_id not in (self.__dict__.get("_flat_stopped") or set()):
            return False
        return await self._flat_close_live_blocks(executor_id, executor)

    def _executor_loop_running(self, executor: Any) -> bool:
        """Цикл жив, пока его task не done. Пустой cr_frame у sleep — не смерть."""
        bound = getattr(executor, "_flat_loop_task", None)
        if bound is not None and not bound.done():
            return True
        try:
            tasks = asyncio.all_tasks()
        except RuntimeError:
            return False
        for task in tasks:
            if task.done():
                continue
            if self._task_is_executor_loop(task, executor):
                executor._flat_loop_task = task
                return True
        return False

    def _task_is_executor_loop(self, task: Any, executor: Any) -> bool:
        coro = task.get_coro()
        seen = set()
        while coro is not None and id(coro) not in seen:
            seen.add(id(coro))
            code = getattr(coro, "cr_code", None)
            if getattr(code, "co_name", "") == "control_loop":
                frame = getattr(coro, "cr_frame", None)
                if frame is not None and frame.f_locals.get("self") is executor:
                    return True
                if frame is None and getattr(executor, "_flat_loop_task", None) is task:
                    return True
            awaited = getattr(coro, "cr_await", None)
            coro = awaited if hasattr(awaited, "cr_code") else None
        return False

    def _reopen_closed_executor(self, executor: Any, executor_id: str = "") -> None:
        """is_closed снимаем статусом RUNNING. terminated не трогаем: спящий цикл выходит.

        Если цикл уже вышел, start() только при метке стопа: она держит постановку и снятие.
        """
        from hummingbot.strategy_v2.executors.executor_base import RunnableStatus
        executor._status = RunnableStatus.RUNNING
        if "is_closed" in getattr(executor, "__dict__", {}):
            executor.is_closed = False
        if self._executor_loop_running(executor):
            return
        terminated = getattr(executor, "terminated", None)
        if (
            executor_id
            and terminated is not None
            and terminated.is_set()
            and self._stop_mark_now(executor_id)
        ):
            executor._status = RunnableStatus.NOT_STARTED
            start = getattr(executor, "start", None)
            if callable(start):
                start()

    def _leave_loop_after_flat_row(self, executor: Any) -> None:
        """Выход из цикла после перечитки. early_stop и обёрнутый stop не зовём."""
        from hummingbot.strategy_v2.runnable_base import RunnableBase
        executor_id = str(getattr(getattr(executor, "config", None), "id", "") or "")
        if executor_id and self._own_unproven_sends(executor_id):
            return
        strategy = getattr(executor, "_strategy", None)
        executor.close_timestamp = getattr(strategy, "current_timestamp", 0)
        self._bind_executor_loop_task(executor)
        RunnableBase.stop(executor)
        unregister = getattr(executor, "unregister_events", None)
        if callable(unregister):
            unregister()

    async def _send_shutdown_close(
        self, executor_id: str, executor: Any, connector: Any, pair: str, fresh: Optional[Dict[str, Any]],
    ) -> None:
        """Одна продажа доказанного остатка. Запись не прошла — продажу не звать."""
        if self._stop_mark_now(executor_id):
            return
        if connector is None or self._partial_rows() is None or not self._lists_ready(fresh):
            return
        remainder = self._shutdown_remainder(executor_id, fresh)
        if remainder is None or remainder <= 0:
            return
        recorded_ids = self._shutdown_close_ids(executor_id)
        if recorded_ids is None:
            return
        known = list(recorded_ids)
        pending = getattr(executor, "_shutdown_close_id", None)
        if pending and str(pending) not in known:
            known.append(str(pending))

        def dead(text: str) -> bool:
            return bool(text) and self._finished_row(text, fresh) is not None

        order_id = ""
        unfinished = [text for text in known if not dead(text)]
        if unfinished:
            for text in unfinished:
                if self._id_still_live(fresh, connector, text):
                    return
            pending = [text for text in unfinished if not self._nothing_to_close_end(executor_id, text)]
            if not pending:
                return
            order_id = pending[0]
            refusal = await self._shutdown_close_refusal(order_id)
            if refusal is None:
                return
            if refusal is True:
                await self._remember_nothing_to_close(executor_id, order_id, fresh, connector)
                return
        elif any(self._nothing_to_close_end(executor_id, text) for text in known):
            return
        else:
            from hummingbot.connector.utils import get_new_client_order_id
            order_id = get_new_client_order_id(
                is_buy=False,
                trading_pair=pair,
                hbot_order_id_prefix=getattr(connector, "client_order_id_prefix", ""),
                max_id_len=getattr(connector, "client_order_id_max_length", 32),
            )
            if not order_id:
                return
            executor._shutdown_close_id = order_id
            executor._shutdown_close_seated = False
        if self._id_still_live(fresh, connector, order_id):
            return
        if not await self._seat_market_close(executor_id, order_id):
            return
        from utils.executor_checkpoint import place_market_close
        executor._shutdown_close_id = order_id
        executor._shutdown_close_amount = remainder
        executor._shutdown_close_seated = True
        sent = place_market_close(executor, executor_id)
        if sent and getattr(executor, "_close_order", None) is not None:
            return

    async def _shutdown_release_step(self, executor_id: str, executor: Any) -> None:
        """Сборка концов гашения: снимает только доказанно мёртвые следы.

        Плоский уровень (все номера zero-dead либо покупка и продажи исполнены)
        снимается целиком меткой формата _partial_release. Уровень с исполненной
        покупкой и открытой позицией не снимается: его мёртвым номерам пишутся
        концы без level_id, запись close_order мёртвого закрытия снимается.
        Номера вне слотов и hold рыночного закрытия снимаются по доказательству
        номера при любой позиции. Живой номер не трогается никогда. Запись в
        базу идёт до снятия в памяти: отказ записи оставляет память как была.
        """
        parsed = self._partial_parsed(executor_id)
        if parsed is None:
            return
        config = getattr(executor, "config", None)
        pair = getattr(config, "trading_pair", None) or ""
        connector_name = getattr(config, "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        fresh = self._tick_fresh(pair)
        if not self._lists_ready(fresh):
            return
        # Нечитаемый miss — не пустая база. Конец раньше стопа отпустил бы номер,
        # который мог дойти до биржи и ещё не сидит в слоте.
        if order_miss_unreadable():
            return
        inflight = getattr(connector, "in_flight_orders", None)
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None)
        # Пустой словарь живой книги нельзя подменять новым: заявка садится в него
        # пока запись идёт, и повторная проверка должна её увидеть.
        if not isinstance(inflight, dict):
            inflight = {}
        if not isinstance(lost, dict):
            lost = {}

        def proven_dead(order_id: str) -> bool:
            text = str(order_id or "")
            if order_miss_unreadable() or not text or text in inflight or text in lost or order_send_unknown(text):
                return False
            return self._shutdown_id_dead(text, fresh)

        try:
            journal = read_ledger_filtered()
        except Exception:
            journal = None

        def level_numbers(level: Dict[str, Any]) -> Optional[list]:
            """Все номера уровня: слот, журнал, holds и текущий RAM. Журнал не читается — уровень не плоский."""
            if journal is None:
                return None
            level_id = str(level.get("id") or "")
            found = []
            for key in ("open_order", "close_order"):
                text = str((level.get(key) or {}).get("order_id") or "")
                if text:
                    found.append(text)
            for item in level.get("partial_closes") or []:
                if isinstance(item, dict) and item.get("order_id"):
                    found.append(str(item.get("order_id")))
            for row in journal:
                if row.get("executor_id") != executor_id:
                    continue
                if str(row.get("level_id") or "") != level_id or not row.get("order_id"):
                    continue
                found.append(str(row.get("order_id")))
            for hold in parsed.get("holds") or []:
                if isinstance(hold, dict) and str(hold.get("level_id") or "") == level_id and hold.get("order_id"):
                    found.append(str(hold.get("order_id")))
            for ram in getattr(executor, "grid_levels", []) or []:
                if str(getattr(ram, "id", "") or "") != level_id:
                    continue
                for seated in (getattr(ram, "active_open_order", None), getattr(ram, "active_close_order", None)):
                    text = str(getattr(seated, "order_id", "") or "")
                    if text:
                        found.append(text)
            return list(dict.fromkeys(found))

        slot_ids = set()
        for level in parsed.get("levels") or []:
            if not isinstance(level, dict):
                continue
            for key in ("open_order", "close_order"):
                text = str((level.get(key) or {}).get("order_id") or "")
                if text:
                    slot_ids.add(text)
            for item in level.get("partial_closes") or []:
                if isinstance(item, dict) and item.get("order_id"):
                    slot_ids.add(str(item.get("order_id")))

        flat_levels = []
        flat_numbers = {}
        dead_closes = []
        lone_ends = []
        for level in parsed.get("levels") or []:
            if not isinstance(level, dict) or not level.get("id"):
                continue
            if partial_level_released(parsed, str(level.get("id"))):
                continue
            slot = level.get("open_order") or {}
            close = level.get("close_order") or {}
            opened = str(slot.get("order_id") or "")
            closed = str(close.get("order_id") or "")
            level_ids = [item for item in (opened, closed) if item]
            level_ids.extend(
                str(item.get("order_id") or "")
                for item in level.get("partial_closes") or []
                if isinstance(item, dict) and item.get("order_id")
            )
            # Плоский уровень — только если мёртв каждый его номер, включая тот,
            # что постановка уже посадила в память, но ещё не записала в слот.
            numbers = level_numbers(level)
            if (
                numbers is not None
                and self._shutdown_level_flat(level, fresh)
                and all(proven_dead(item) for item in numbers)
                and not self._shutdown_position_open(level, numbers, fresh)
            ):
                flat_levels.append(level)
                flat_numbers[str(level.get("id"))] = list(numbers)
                continue
            # Уровень не плоский. Мёртвый номер всё равно получает конец, иначе
            # он держит финализацию навсегда: после early_stop шаг больше не
            # вызывается. Живой и неизвестный номер не трогаем.
            for text in level_ids:
                if proven_dead(text) and not partial_end_visible(parsed, text):
                    lone_ends.append(text)
            # Мёртвая запись закрытия держит стоп как живой тейк, даже если
            # покупка этого уровня ещё жива или неизвестна. Снять запись, не уровень.
            if closed and proven_dead(closed):
                dead_closes.append((str(level.get("id")), closed))
        for hold in parsed.get("holds") or []:
            if not isinstance(hold, dict) or not hold.get("order_id"):
                continue
            text = str(hold.get("order_id"))
            if text in slot_ids or partial_end_visible(parsed, text):
                continue
            if proven_dead(text):
                lone_ends.append(text)
        if journal is not None:
            for row in journal:
                if row.get("executor_id") != executor_id or not row.get("order_id"):
                    continue
                text = str(row.get("order_id"))
                if text in slot_ids or partial_end_visible(parsed, text):
                    continue
                if text not in lone_ends and proven_dead(text):
                    lone_ends.append(text)
        market_id = str(getattr(getattr(executor, "_close_order", None), "order_id", "") or "")
        if not market_id:
            for hold in getattr(executor, "_close_holds", []) or []:
                if isinstance(hold, dict) and hold.get("role") == "market_close" and hold.get("order_id"):
                    market_id = str(hold.get("order_id"))
                    break
        market_dead = bool(market_id) and market_id not in slot_ids and proven_dead(market_id)
        if not flat_levels and not dead_closes and not lone_ends and not market_dead:
            return
        lone_ends = list(dict.fromkeys(lone_ends))

        wrote_flat: list = []
        wrote_ends: list = []
        wrote_ids: list = []

        def ids_of_body(level: Dict[str, Any], body: Dict[str, Any], live_journal) -> list:
            found = []
            for key in ("open_order", "close_order"):
                text = str((level.get(key) or {}).get("order_id") or "")
                if text:
                    found.append(text)
            for item in level.get("partial_closes") or []:
                if isinstance(item, dict) and item.get("order_id"):
                    found.append(str(item.get("order_id")))
            level_id = str(level.get("id") or "")
            for row in live_journal or []:
                if row.get("executor_id") == executor_id and str(row.get("level_id") or "") == level_id and row.get("order_id"):
                    found.append(str(row.get("order_id")))
            for hold in body.get("holds") or []:
                if isinstance(hold, dict) and str(hold.get("level_id") or "") == level_id and hold.get("order_id"):
                    found.append(str(hold.get("order_id")))
            for ram in getattr(executor, "grid_levels", []) or []:
                if str(getattr(ram, "id", "") or "") != level_id:
                    continue
                for seated in (getattr(ram, "active_open_order", None), getattr(ram, "active_close_order", None)):
                    text = str(getattr(seated, "order_id", "") or "")
                    if text:
                        found.append(text)
            return list(dict.fromkeys(found))

        def flat_on_body(level: Dict[str, Any], body: Dict[str, Any], live_journal) -> bool:
            if live_journal is None:
                return False
            ids = ids_of_body(level, body, live_journal)
            return bool(ids) and self._shutdown_level_flat(level, fresh) and all(
                proven_dead(item) for item in ids
            ) and not self._shutdown_position_open(level, ids, fresh)

        def mutate(body: Dict[str, Any]) -> None:
            try:
                live_journal = read_ledger_filtered()
            except Exception:
                live_journal = None
            ends = dict(body.get("partial_ends") or {})
            wrote_flat.clear()
            for level in body.get("levels") or []:
                if not isinstance(level, dict):
                    continue
                level_id = str(level.get("id") or "")
                # Решение по телу, которое пишем сейчас, не по снимку до чтения.
                if level_id in {str(item.get("id")) for item in flat_levels} and flat_on_body(level, body, live_journal):
                    ids = [
                        text for text in ids_of_body(level, body, live_journal)
                        if proven_dead(text)
                    ]
                    key = ids[0] if ids else level_id
                    ends[key] = {"kind": "flat", "level_id": level_id, "order_ids": ids}
                    level.pop("take_profit_price", None)
                    level["close_order"] = None
                    level["partial_closes"] = []
                    slot = level.get("open_order") or {}
                    if slot.get("filled"):
                        level["open_order"] = None
                    self._strip_recorded(body, ids)
                    wrote_flat.append(level_id)
                    wrote_ids.extend(ids)
                for dead_level, close_id in dead_closes:
                    if level_id != dead_level:
                        continue
                    current = str((level.get("close_order") or {}).get("order_id") or "")
                    if current == close_id and proven_dead(current):
                        level["close_order"] = None
                        wrote_ids.append(current)
            wrote_ends.clear()
            for text in lone_ends:
                if not proven_dead(text):
                    continue
                self._shutdown_end_mark(ends, text)
                wrote_ends.append(text)
            if market_dead and proven_dead(market_id):
                self._shutdown_end_mark(ends, market_id)
                wrote_ends.append(market_id)
            if wrote_ends or wrote_flat:
                body["partial_ends"] = ends
            if wrote_ends:
                self._strip_recorded(body, wrote_ends)

        def visible(stored: Optional[Dict[str, Any]]) -> bool:
            for level_id in wrote_flat:
                if not partial_level_released(stored, level_id):
                    return False
            for text in wrote_ends:
                if not partial_end_visible(stored, text):
                    return False
            return True

        if not await self._partial_cas(executor_id, mutate, visible, None):
            return
        released_ids = [*wrote_ends, *wrote_ids]
        self._forget_close_memory(executor, released_ids)
        if market_id in wrote_ends:
            executor._close_order = None
        flat_ids = set(wrote_flat)
        for level in getattr(executor, "grid_levels", []) or []:
            level_id = str(getattr(level, "id", "") or "")
            opened_now = str(getattr(getattr(level, "active_open_order", None), "order_id", "") or "")
            closed_now = str(getattr(getattr(level, "active_close_order", None), "order_id", "") or "")
            if level_id in flat_ids:
                if opened_now and opened_now in released_ids:
                    level.active_open_order = None
                if closed_now and closed_now in released_ids:
                    level.active_close_order = None
                continue
            if closed_now and closed_now in released_ids:
                level.active_close_order = None

    async def stop_executor(
        self,
        executor_id: str,
        keep_position: bool = False
    ) -> Dict[str, Any]:
        """
        Stop an active executor.

        Args:
            executor_id: The executor ID to stop
            keep_position: Whether to keep the position open

        Returns:
            Dictionary with stop confirmation
        """
        executor = self._active_executors.get(executor_id)
        if not executor:
            # Terminal executors are popped from memory within one control-loop tick,
            # so "not in memory" usually means "already terminated", not "unknown".
            # Fall back to the DB and answer with the final state as a no-op success.
            # This deliberately includes rows still marked RUNNING in the DB: an
            # executor known to the DB but absent from memory is dead regardless of
            # its stored status (completion race, failed persist, restart window),
            # and answering 404 there is the gateway#678 dead end. 404 is reserved
            # for executor ids the DB has never seen (or a DB outage, which
            # get_executor logs and swallows to None).
            db_record = await self.get_executor(executor_id)
            if db_record:
                custom_info = db_record.get("custom_info") or {}
                logger.info(
                    f"Stop requested for already-terminated executor {executor_id} "
                    f"(db status: {db_record.get('status')}, close_type: {db_record.get('close_type')}) - no-op"
                )
                return {
                    "executor_id": executor_id,
                    "status": "already_terminated",
                    "keep_position": keep_position,
                    "close_type": db_record.get("close_type"),
                    "position_address": custom_info.get("position_address"),
                    "orphaned_position": bool(custom_info.get("orphaned_position", False)),
                    "hold_reason": custom_info.get("hold_reason"),
                }
            raise HTTPException(status_code=404, detail=f"Executor {executor_id} not found")

        if executor.is_closed:
            raise HTTPException(status_code=400, detail=f"Executor {executor_id} is already closed")

        # Заморозка (план stop-freeze): метка «стоп запрошен» — отдельной CAS,
        # коммитом и перечиткой НОВОЙ сессией ДО early_stop. Шаг сборки концов
        # в этом срезе не вызывается: записи уровней/тел/закрытий не трогаются.
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        # Свежая перечитка идёт всегда и до early_stop: показанная метка
        # прогревает RAM, иначе обёртка смотрит в холодную память и не пускает
        # отмены (вердикт кода 20261012-212613-69817). Непрочитанное не
        # размораживает: метку из памяти перечитка не снимает.
        already = await self._read_stop_mark_fresh(executor_id) or self._stop_mark_now(executor_id)
        if not already:
            # Держателей по цене нет (правило номера, план stop-freeze п.3):
            # чужая заявка на цене тейка и нечитаемые списки биржи ни на что
            # не влияют. Метка пишется всегда и сразу — одно нажатие доводится
            # программой сама.
            confirmed = False
            try:
                confirmed = await self._freeze_stop_mark(executor_id)
            except Exception:
                logger.exception("stop mark failed for %s", executor_id)
            if not confirmed:
                return {
                    "executor_id": executor_id,
                    "status": "not_accepted",
                    "reason": "stop mark was not confirmed in a fresh read",
                    "keep_position": keep_position,
                }

        # Доставка отмен по нашим номерам — сразу и не за готовностью списков
        # (план stop-freeze п.3): пока номер не доказан снятым или мёртвым,
        # доводка крутится сама, без повторного нажатия.
        pending_ids = self._stop_unfinished_ids(executor_id, self._tick_fresh(pair))
        pending = (
            "undelivered-cancel ids=" + ",".join(str(item)[-8:] for item in pending_ids)
            if pending_ids else None
        )
        if pending:
            self._log_silent_exit(
                "stop-aside", pair, pending, executor_id,
                interval=0.0, label="silent stop exit:", key=executor_id,
            )
        self.__dict__.setdefault("_stop_keep_position", {})[str(executor_id)] = bool(keep_position)

        # Свой номер без ответа сажается в пустой слот до штатной отмены.
        # Чужой id не вытесняем. stop() сами не зовём.
        self._seat_unanswered_before_close(executor_id, executor)
        refresh = getattr(executor, "update_grid_levels", None)
        if callable(refresh):
            refresh()
        # Trigger early stop
        try:
            executor.early_stop(keep_position=keep_position)
        except Exception as e:
            logger.error(f"Error stopping executor {executor_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Error stopping executor: {str(e)}")

        # Первая доставка отмен — тоже сразу (план п.3).
        connector = (getattr(executor, "connectors", None) or {}).get(
            getattr(getattr(executor, "config", None), "connector_name", "") or ""
        )
        if connector is not None:
            await self._stop_deliver_cancels(executor_id, executor, connector, pair)

        logger.info(f"Initiated stop for executor {executor_id} (keep_position={keep_position})")

        return {
            "executor_id": executor_id,
            "status": "frozen" if already else "stopping",
            "pending": pending,
            "keep_position": keep_position,
        }

    async def get_orphaned_positions(self) -> List[Dict[str, Any]]:
        """
        List executors that terminated while potentially still owning an on-chain position.

        Covers both orphan classes:
        - close_type FAILED with a position_address in the persisted final state
          (e.g. an LP close that exhausted retries - gateway#678)
        - close_type SYSTEM_CLEANUP (RUNNING rows rewritten after an API restart);
          these have no final state, so the position address is unknown and the
          on-chain state must be reconciled externally

        This listing is DB-side only: cross-check candidates against on-chain reality
        (gateway CLMM/AMM positions-owned endpoints) before recovering. Recovered
        orphans are silenced with resolve_orphaned_position().

        Raises on DB errors rather than returning [] - "no orphans" from a broken DB
        would read as all-clear on a safety endpoint.
        """
        if not self.db_manager:
            raise RuntimeError("Orphan listing unavailable: no database configured")

        # lp_executor is the only executor type that owns an on-chain position
        # account; filtering in SQL keeps the limit meaningful (a Python-side filter
        # over the newest N mixed candidates can silently drop older real orphans).
        async with self.db_manager.get_session_context() as session:
            repo = ExecutorRepository(session)
            records = await repo.get_executors_by_close_types(
                ["FAILED", "SYSTEM_CLEANUP", "POSITION_HOLD"], executor_type="lp_executor"
            )

        orphans: List[Dict[str, Any]] = []
        for record in records:
            final_state: Dict[str, Any] = {}
            if record.final_state:
                try:
                    final_state = json.loads(record.final_state)
                except (json.JSONDecodeError, TypeError):
                    final_state = {}

            if final_state.get("orphan_resolved"):
                continue

            position_address = final_state.get("position_address")
            if record.close_type == "FAILED" and not position_address:
                # Failed without on-chain exposure - not an orphan
                continue
            if record.close_type == "POSITION_HOLD" and not (
                final_state.get("orphaned_position") or final_state.get("hold_reason")
            ):
                # Voluntary hold (keep_position=True stop) - position was closed on-chain
                continue

            # The DEX and pool live in the executor config, not in any column: for lp_executor the
            # connector_name column carries the network id. Both are needed to close the position,
            # and LP-executor positions are opened by the bot straight against Gateway so they are
            # never in the API's own CLMM position table to be looked up there.
            config: Dict[str, Any] = {}
            if record.config:
                try:
                    config = json.loads(record.config)
                except (json.JSONDecodeError, TypeError):
                    config = {}

            orphans.append({
                "executor_id": record.executor_id,
                "executor_type": record.executor_type,
                "account_name": record.account_name,
                "connector_name": record.connector_name,
                "trading_pair": record.trading_pair,
                "lp_provider": config.get("lp_provider"),
                "pool_address": config.get("pool_address"),
                "controller_id": record.controller_id or "main",
                "close_type": record.close_type,
                "closed_at": record.closed_at.isoformat() if record.closed_at else None,
                "position_address": position_address,
                "state": final_state.get("state"),
                "hold_reason": final_state.get("hold_reason"),
                "needs_onchain_reconciliation": position_address is None,
            })

        return orphans

    async def resolve_orphaned_position(self, executor_id: str) -> Dict[str, Any]:
        """
        Mark an orphaned position as recovered so it stops surfacing.

        Call this after the stranded on-chain position has been closed (or adopted)
        externally. Sets orphan_resolved in the persisted final state, which removes
        the record from get_orphaned_positions() and from agent-facing warnings.
        """
        if not self.db_manager:
            raise HTTPException(status_code=503, detail="No database configured")

        async with self.db_manager.get_session_context() as session:
            repo = ExecutorRepository(session)
            record = await repo.get_executor_by_id(executor_id)
            if not record:
                raise HTTPException(status_code=404, detail=f"Executor {executor_id} not found")
            if record.status == "RUNNING":
                raise HTTPException(status_code=400, detail=f"Executor {executor_id} is still running")

            final_state: Dict[str, Any] = {}
            if record.final_state:
                try:
                    final_state = json.loads(record.final_state)
                except (json.JSONDecodeError, TypeError):
                    final_state = {}

            is_involuntary_hold = record.close_type == "POSITION_HOLD" and (
                final_state.get("orphaned_position") or final_state.get("hold_reason")
            )
            if record.close_type not in ("FAILED", "SYSTEM_CLEANUP") and not is_involuntary_hold:
                raise HTTPException(
                    status_code=400,
                    detail=f"Executor {executor_id} (close_type: {record.close_type}) is not an orphan candidate",
                )
            final_state["orphaned_position"] = False
            final_state["orphan_resolved"] = True
            await repo.update_executor(executor_id=executor_id, final_state=json.dumps(final_state))

        logger.info(f"Orphaned position for executor {executor_id} marked resolved")
        return {"executor_id": executor_id, "orphan_resolved": True}

    def _phantom_close_ids(self, executor_id: str, executor: Any) -> set:
        """Номер продажи, которого нет в прочитанных открытых и нет в полёте."""
        pair = str(getattr(getattr(executor, "config", None), "trading_pair", None) or "")
        fresh = self._tick_fresh(pair) if pair else None
        if not isinstance(fresh, dict) or not fresh.get("opens_read") or not fresh.get("opens_done"):
            return set()
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        if connector is None:
            return set()
        open_ids = set()
        for row in fresh.get("opens") or []:
            if not isinstance(row, dict):
                continue
            text = str(row.get("text") or row.get("order_id") or "")
            if text:
                open_ids.add(text)
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        flying = {str(item) for item in inflight} | {str(item) for item in lost}
        close_ids = set()
        for level in getattr(executor, "grid_levels", None) or []:
            text = str(getattr(getattr(level, "active_close_order", None), "order_id", "") or "")
            if text:
                close_ids.add(text)
        parsed = self._partial_parsed(executor_id) or {}
        for level in parsed.get("levels") or []:
            if not isinstance(level, dict):
                continue
            text = str((level.get("close_order") or {}).get("order_id") or "")
            if text:
                close_ids.add(text)
        return {text for text in close_ids if text not in open_ids and text not in flying}

    def _drop_phantom_close_liquidity(self, executor_id: str, executor: Any) -> None:
        """Ложная стоящая продажа не держит штатное гашение. Слот не стираем."""
        phantoms = self._phantom_close_ids(executor_id, executor)
        if not phantoms:
            return
        kept = Decimal("0")
        for level in getattr(executor, "grid_levels", None) or []:
            tracked = getattr(level, "active_close_order", None)
            text = str(getattr(tracked, "order_id", "") or "")
            if not text or text in phantoms:
                continue
            try:
                executed = Decimal(str(getattr(tracked, "executed_amount_base", 0) or 0))
                quote = Decimal(str(getattr(level, "amount_quote", 0) or 0))
            except Exception:
                return
            if executed == Decimal("0"):
                kept += quote
        executor.close_liquidity_placed = kept

    def _unproven_delivery_ids(self, executor_id: str, executor: Any) -> list:
        """Номера уже существующей доставки отмен, которые ещё не доказаны мёртвыми."""
        pair = str(getattr(getattr(executor, "config", None), "trading_pair", None) or "")
        fresh = self._tick_fresh(pair) if pair else None
        phantoms = self._phantom_close_ids(executor_id, executor)
        return [
            text for text in self._stop_unfinished_ids(executor_id, fresh)
            if text not in phantoms
        ]

    async def _feed_shutdown_stock_truth(self, executor_id: str, executor: Any) -> None:
        """Правда о теле перед штатным гашением. Свой stop() не зовём, слот не пишем."""
        from utils.executor_checkpoint import (
            _exchange_close_filled,
            _known_close_id,
            marked_stop_ready,
        )
        ids = self._shutdown_close_ids(executor_id)
        if not ids:
            return
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        if connector is None or not pair:
            return
        deaths = await self._archive_death_bases(connector, pair, ids)
        if not deaths:
            return
        if await self._reread_executor_position(executor) is not True:
            return
        known = str(_known_close_id(executor, executor_id) or "")
        coin = deaths.get(known)
        if not known or coin is None or coin <= 0:
            return
        previous = getattr(executor, "_tick_blank_fills", None)
        if not _exchange_close_filled(executor, known):
            row = {"text": known, "fill_size": str(coin)}
            executor._tick_blank_fills = list(previous or []) + [row]
        if not marked_stop_ready(executor, executor_id):
            executor._tick_blank_fills = previous
            return
        return

    
    def _live_delivery_reads_zero_liquidity(self, executor_id: str, executor: Any) -> bool:
        """Живой номер доставки, который штатное гашение прочитало бы как нулевую ликвидность."""
        if not self._unproven_delivery_ids(executor_id, executor):
            return False
        try:
            open_liq = Decimal(str(getattr(executor, "open_liquidity_placed", 0) or 0))
            close_liq = Decimal(str(getattr(executor, "close_liquidity_placed", 0) or 0))
        except Exception:
            return False
        return open_liq == 0 and close_liq == 0

    def _stop_unfinished_ids(self, executor_id: str, fresh: Optional[Dict[str, Any]] = None) -> list:
        """Наши номера без доказательства «снят или мёртв» (план stop-freeze п.3).

        Доставка отмены ведётся по каждому такому номеру: журнал, слоты уровней,
        holds. Заявки опознаются ТОЛЬКО по номеру; чужая заявка на цене и
        нечитаемые списки биржи сюда не входят.
        """
        parsed = self._partial_parsed(executor_id)
        numbers = set(self._own_ids(executor_id))
        for level in (parsed or {}).get("levels") or []:
            for key in ("open_order", "close_order"):
                text = str((level.get(key) or {}).get("order_id") or "")
                if text:
                    numbers.add(text)
            for item in level.get("partial_closes") or []:
                if isinstance(item, dict) and item.get("order_id"):
                    numbers.add(str(item.get("order_id")))
        for hold in (parsed or {}).get("holds") or []:
            if isinstance(hold, dict) and hold.get("order_id") and hold.get("role") != "market_close":
                numbers.add(str(hold.get("order_id")))
        for saved in (parsed or {}).get("retained_orders") or []:
            if isinstance(saved, dict) and saved.get("order_id"):
                numbers.add(str(saved.get("order_id")))
        out = []
        for text in sorted(numbers):
            if partial_end_visible(parsed, text):
                continue
            if text in (self.__dict__.get("_cancel_51400_ids") or set()):
                continue
            if self._proven_dead(text, fresh):
                continue
            if self._shown_fill_row(fresh, text, "BUY") is not None or self._shown_fill_row(fresh, text, "SELL") is not None:
                continue
            out.append(text)
        return out

    async def _stop_deliver_cancel(self, executor: Any, connector: Any, text: str, pair: str, executor_id: str = "") -> bool:
        """Доставка отмены по нашему номеру до биржи (план stop-freeze п.3).

        Номер в трекере — штатный путь (`strategy.cancel` → `_api_delete`); наш
        номер вне трекера — отмена по client id через `_place_cancel`, как в
        `_cancel_copied_open` при пустом gate-запросе. Посадка и вызов, который
        обёртка превратила в `None`, доставкой не считаются.
        """
        token = _cancel_delivery_id.set(text)
        try:
            tracked = getattr(connector, "in_flight_orders", None) or {}
            lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
            if text in tracked or text in lost:
                strategy = getattr(executor, "_strategy", None)
                cancel = getattr(strategy, "cancel", None)
                if cancel is None:
                    return False
                connector_name = getattr(getattr(executor, "config", None), "connector_name", None) or ""
                trading_pair = pair or getattr(getattr(executor, "config", None), "trading_pair", None) or ""
                result = cancel(
                    connector_name=connector_name,
                    trading_pair=trading_pair,
                    order_id=text,
                )
                if asyncio.iscoroutine(result):
                    result = await result
                return result is not None
            place = getattr(connector, "_place_cancel", None)
            if place is None:
                return False
            row = type("TrackedCancel", (), {})()
            row.exchange_order_id = ""
            row.client_order_id = text
            row.trading_pair = pair
            result = place(text, row)
            if asyncio.iscoroutine(result):
                result = await result
            return result is True
        except Exception as exc:
            if self._exchange_says_gone(exc):
                # Ответ «уже исполнено» гасит повтор. Конец заявки до продажи не пишем.
                self.__dict__.setdefault("_cancel_51400_ids", set()).add(str(text))
                return True
            if not self._exchange_says_gone(exc):
                logger.exception("stop cancel delivery failed for %s", text)
            return False
        finally:
            _cancel_delivery_id.reset(token)

    def _exchange_says_gone(self, exc: BaseException) -> bool:
        """Биржа назвала этот номер снятым, исполненным или несуществующим.

        Только код 51400. Отказ часов и прочая ошибка сюда не входят.
        """
        blob = " ".join([str(exc), *[str(item) for item in (getattr(exc, "args", ()) or ())]])
        if "50102" in blob or "50113" in blob:
            return False
        return "'sCode': '51400'" in blob or '"sCode": "51400"' in blob

    def _finished_zero_cancel(self, raw: Optional[Dict[str, Any]]) -> bool:
        """Снятие с нулевой базой: форма Gate или строка OKX. Пустой размер — не ноль."""
        if not isinstance(raw, dict) or self._closed_row_filled(raw):
            return False
        if gate_base_is_zero(raw):
            return True
        state = str(raw.get("state") or "").lower()
        if state not in ("canceled", "cancelled", "mmp_canceled") or "fill_size" not in raw:
            return False
        try:
            return Decimal(str(raw.get("fill_size"))) == 0
        except (ArithmeticError, ValueError, TypeError):
            return False

    def _retained_ids(self, executor_id: str) -> set:
        found = set()
        for saved in (self._partial_parsed(executor_id) or {}).get("retained_orders") or []:
            if isinstance(saved, dict) and saved.get("order_id"):
                found.add(str(saved.get("order_id")))
        return found

    def _retained_level_id(self, executor_id: str, order_id: str) -> str:
        text = str(order_id or "")
        for saved in (self._partial_parsed(executor_id) or {}).get("retained_orders") or []:
            if isinstance(saved, dict) and str(saved.get("order_id") or "") == text:
                return str(saved.get("level_id") or "")
        return ""

    def _level_close_ids(self, executor_id: str, level_id: str) -> set:
        """Закрытие этого уровня: слот close_order и partial_closes. Чужой уровень не входит."""
        found = set()
        if not level_id:
            return found
        for level in (self._partial_parsed(executor_id) or {}).get("levels") or []:
            if not isinstance(level, dict) or str(level.get("id") or "") != level_id:
                continue
            close = level.get("close_order") or {}
            if isinstance(close, dict) and close.get("order_id"):
                found.add(str(close.get("order_id")))
            for item in level.get("partial_closes") or []:
                if isinstance(item, dict) and item.get("order_id"):
                    found.add(str(item.get("order_id")))
        return found

    def _retained_sale_covers(
        self, executor_id: str, order_id: str, fresh: Optional[Dict[str, Any]], buy_row: Dict[str, Any],
    ) -> bool:
        """Продажа этого номера: close того же level_id. Чужой close и один close на двоих — нет."""
        text = str(order_id or "")
        level_id = self._retained_level_id(executor_id, text)
        close_ids = self._level_close_ids(executor_id, level_id)
        if not level_id or not close_ids:
            return False
        for other in self._retained_ids(executor_id):
            if other == text:
                continue
            if self._level_close_ids(executor_id, self._retained_level_id(executor_id, other)) & close_ids:
                return False
        rows = self._partial_rows()
        if not rows:
            return False
        bought = self._shutdown_sold_base(buy_row)
        if bought is None or bought <= 0:
            return False
        sold = Decimal("0")
        for item in rows:
            name = str(item.get("order_id") or "")
            if name not in close_ids:
                continue
            if str(item.get("executor_id") or "") != str(executor_id):
                continue
            if str(item.get("level_id") or "") != level_id:
                continue
            if str(item.get("role") or "") not in ("close", "market_close"):
                continue
            finished = self._finished_row(name, fresh)
            if finished is None or not self._closed_row_filled(finished):
                continue
            sold += self._shutdown_sold_base(finished)
            close_ids.discard(name)
        return sold >= bought

    def _retained_book_blocks(self, executor: Any, order_id: str, fresh: Optional[Dict[str, Any]]) -> bool:
        """Непрочитанная книга — не «нет в открытых». Живой номер архив не отпускает."""
        text = str(order_id or "")
        if not text or not self._lists_ready(fresh):
            return True
        if self._shutdown_text_present((fresh or {}).get("opens"), text):
            return True
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        inflight = getattr(connector, "in_flight_orders", None)
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None)
        if not isinstance(inflight, dict) or not isinstance(lost, dict):
            return True
        if text in inflight or text in lost or order_send_unknown(text):
            return True
        return False

    def _archive_explicit_zero(self, raw_rows: Any, order_id: str) -> bool:
        """Нулевая база — только явный accFillSz. Подставленный fill_size «0» не считается."""
        text = str(order_id or "")
        if not text or not isinstance(raw_rows, list):
            return False
        found = False
        for row in raw_rows:
            if not isinstance(row, dict):
                return False
            name = str(row.get("clOrdId") or row.get("text") or row.get("order_id") or "")
            if name != text:
                continue
            found = True
            if str(row.get("state") or "") not in ("canceled", "mmp_canceled"):
                return False
            if "accFillSz" not in row or str(row.get("accFillSz")) != "0":
                return False
        return found

    def _retained_finish_proven(self, executor_id: str, order_id: str, fresh: Optional[Dict[str, Any]]) -> bool:
        """Явный ноль архива или продажа уровня. Подставленный fill_size «0» не снятие."""
        text = str(order_id or "")
        if fresh is None:
            pair = getattr(
                getattr((getattr(self, "_active_executors", None) or {}).get(executor_id), "config", None),
                "trading_pair", None,
            ) or ""
            fresh = self._pair_book(pair) if pair else None
        executor = (getattr(self, "_active_executors", None) or {}).get(executor_id)
        if self._retained_book_blocks(executor, text, fresh):
            return False
        row = self._finished_row(text, fresh)
        if row is not None and self._closed_row_filled(row):
            return self._retained_sale_covers(executor_id, text, fresh, row)
        cached = (self.__dict__.get("_retained_archive_rows") or {}).get(executor_id)
        return self._archive_explicit_zero(cached, text)

    def _retained_51400_unproven(self, executor_id: str, fresh: Optional[Dict[str, Any]] = None) -> set:
        """Конец 51400 в retained_orders не отпускает завершение, пока нет finished-доказательства.

        Пустой журнал и `_shutdown_close_proven` сюда не входят: они эту покупку не видят.
        """
        if fresh is None:
            pair = ""
            executor = (getattr(self, "_active_executors", None) or {}).get(executor_id)
            pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
            if not pair:
                for row in getattr(self, "_hold_rows", None) or []:
                    if row.get("executor_id") == executor_id:
                        pair = row.get("trading_pair") or ""
                        break
            fresh = self._pair_book(pair) if pair else None
        blocked = set()
        for text in self._retained_ids(executor_id):
            if not self._exchange_gone_end(executor_id, text):
                continue
            if self._retained_finish_proven(executor_id, text, fresh):
                continue
            blocked.add(text)
        return blocked

    def _cancel_end_blocked(self, executor: Any, order_id: str, pair: str) -> bool:
        """До конца 51400. `_remember_exchange_gone` этих проверок не делает."""
        text = str(order_id or "")
        fresh = self._pair_book(pair or "")
        if not text or not self._lists_ready(fresh):
            return True
        if self._shutdown_text_present((fresh or {}).get("opens"), text):
            return True
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        inflight = getattr(connector, "in_flight_orders", None)
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None)
        if not isinstance(inflight, dict) or not isinstance(lost, dict):
            return True
        if text in inflight or text in lost or order_send_unknown(text):
            return True
        return False

    def _exchange_gone_end(self, executor_id: str, order_id: str) -> bool:
        mark = ((self._partial_parsed(executor_id) or {}).get("partial_ends") or {}).get(str(order_id)) or {}
        return isinstance(mark, dict) and mark.get("kind") == "gone" and mark.get("exchange") == "51400"

    async def _remember_exchange_gone(self, executor_id: str, order_id: str, pair: str) -> bool:
        """Конец номера до того, как отмену перестанем повторять. Запись не прошла — не отпускать."""
        text = str(order_id or "")
        if not executor_id or not text:
            return False
        fresh = self._pair_book(pair or "")
        if not self._lists_ready(fresh) or partial_text_in((fresh or {}).get("opens"), text):
            return False
        if self._exchange_gone_end(executor_id, text):
            return True

        def mutate(body: Dict[str, Any]) -> None:
            ends = dict(body.get("partial_ends") or {})
            current = ends.get(text) or {}
            if isinstance(current, dict) and current.get("kind") == "gone":
                return
            ends[text] = {"kind": "gone", "order_ids": [text], "exchange": "51400"}
            body["partial_ends"] = ends

        def visible(parsed: Optional[Dict[str, Any]]) -> bool:
            mark = ((parsed or {}).get("partial_ends") or {}).get(text) or {}
            return isinstance(mark, dict) and mark.get("kind") == "gone" and mark.get("exchange") == "51400" and "price" not in mark

        return await self._partial_cas(executor_id, mutate, visible)

    async def _stop_deliver_cancels(self, executor_id: str, executor: Any, connector: Any, pair: str = "") -> None:
        """Доводка заморозки: доставка отмены по каждому нашему номеру.

        Одно нажатие (план stop-freeze п.3): программа сама повторяет доставку,
        с паузой, пока есть номера без доказательства «снят или мёртв» — и для
        `sent` тоже. Не за готовностью списков и не за судьбой отправки; после
        перезагрузки продолжается сама (метка живёт в записи, номера — в
        журнале).
        """
        key = str(executor_id)
        pending = self._stop_unfinished_ids(executor_id, self._tick_fresh(pair or ""))
        if not pending:
            return
        stamps = self.__dict__.setdefault("_stop_finish_last", {})
        now = time.time()
        if stamps.get(key) is not None and now - stamps[key] < STOP_FINISH_RETRY_SECONDS:
            return
        stamps[key] = now
        self._log_silent_exit(
            "stop-aside", pair or "",
            "undelivered-cancel ids=" + ",".join(str(item)[-8:] for item in pending),
            executor_id, interval=300.0, label="silent stop exit:", key=executor_id,
        )
        for text in pending:
            await self._stop_deliver_cancel(executor, connector, text, pair or "", executor_id)

    async def _handle_executor_completion(self, executor_id: str):
        """Handle cleanup when an executor completes."""
        executor = self._active_executors.get(executor_id)
        if executor is not None and await self._flat_stop_needs_owner(executor_id, executor):
            await self._restore_running_after_flat(executor_id)
            self._reopen_closed_executor(executor, executor_id)
            return
        # После штатного конца не спрашиваем, жив ли номер, и не открываем
        # исполнителя снова (план stop-finished-not-reopened).
        if executor is None:
            return

        metadata = self._executor_metadata.get(executor_id, {})

        # Check if this is a POSITION_HOLD close type (keep_position=True)
        if executor.close_type == CloseType.POSITION_HOLD:
            await self._aggregate_position_hold(executor_id, executor, metadata)

        # Persist final state to database. Запись не села — исполнитель остаётся
        # в памяти с is_closed, следующий тик повторяет запись (stock-close-not-gated):
        # вынутая до записи строка осталась бы RUNNING и перезапуск поднял бы её снова.
        if not await self._persist_executor_completed(executor_id, executor):
            return
        self._active_executors.pop(executor_id, None)

        # The rent refund is only known once the close confirms, which is here. A
        # successful close has already cleared position_address from custom_info, so this
        # relies on the address remembered while the executor was live.
        if metadata.get("executor_type") == "lp_executor":
            await self._record_lp_position_rent(executor_id, executor, final=True)
        # A Gateway swap an executor made, which no /gateway/swap route ever saw. See
        # _record_executor_swap; non-Gateway executors carry no transaction hash and fall
        # straight back out.
        if self.db_manager:
            await self._record_executor_swap(executor_id, executor)
        self._lp_position_addresses.pop(executor_id, None)
        self._lp_rent_recorded.discard(executor_id)
        self._lp_rent_retry_after.pop(executor_id, None)

        # Active executor already claimed via pop above; drop its metadata last
        # (metadata is read above and re-fetched inside the persist/aggregate
        # helpers, so it must stay until after those awaits complete).
        if executor_id in self._executor_metadata:
            del self._executor_metadata[executor_id]

        # Clean up captured logs
        self._log_capture.clear(executor_id)

        close_type = executor.close_type.name if executor.close_type else "UNKNOWN"
        logger.info(f"Executor {executor_id} completed with close_type: {close_type}")

        # Surface stranded on-chain exposure loudly: a live position address on an
        # involuntary hold (hold_reason set) or a legacy FAILED means the position
        # has no automated owner from this point on.
        if executor.close_type in (CloseType.FAILED, CloseType.POSITION_HOLD):
            try:
                completion_info = executor.get_custom_info()
                position_address = completion_info.get("position_address")
                hold_reason = completion_info.get("hold_reason")
            except Exception:
                position_address = None
                hold_reason = None
            if position_address and (hold_reason or executor.close_type == CloseType.FAILED):
                logger.error(
                    f"Executor {executor_id} ended {close_type} with position {position_address} "
                    f"still open on-chain (hold_reason: {hold_reason}) - orphaned position requires "
                    "recovery (flagged in DB record; see /executors/positions/orphaned)"
                )

    def _format_executor_info(
        self,
        executor_id: str,
        executor: ExecutorBase
    ) -> Dict[str, Any]:
        """Format executor information for API response."""
        metadata = self._executor_metadata.get(executor_id, {})
        executor_type = metadata.get("executor_type")

        # Get executor_info as a dict and strip heavy custom_info fields BEFORE
        # serialization so they never get coerced (fill_events, grid
        # levels_by_state, etc.); then coerce in-place to JSON-compatible
        # primitives instead of doing a json.dumps/json.loads string round-trip.
        executor_info = executor.executor_info
        dumped = executor_info.model_dump()
        dumped["custom_info"] = self._strip_heavy_fields(dumped.get("custom_info"), executor_type)
        result = _coerce_json_compatible(dumped)

        # Add metadata
        result["executor_id"] = executor_id
        result["executor_type"] = executor_type
        result["account_name"] = metadata.get("account_name")
        result["created_at"] = metadata.get("created_at").isoformat() if metadata.get("created_at") else None

        if metadata.get("connector_name"):
            result["connector_name"] = metadata.get("connector_name")
        if metadata.get("trading_pair"):
            result["trading_pair"] = metadata.get("trading_pair")
        result["controller_id"] = metadata.get("controller_id", "main")

        # Read status/close_type directly from executor
        result["status"] = executor.status.name
        result["close_type"] = executor.close_type.name if executor.close_type else None
        result["is_active"] = not executor.is_closed

        # Add side from executor_info (it's a property, not serialized by model_dump)
        side = executor_info.side
        if side is not None:
            # Convert TradeType enum or int to string
            result["side"] = side.name if hasattr(side, 'name') else str(side)

        # Add log capture info
        result["error_count"] = self._log_capture.get_error_count(executor_id)
        result["last_error"] = self._log_capture.get_last_error(executor_id)

        return result

    @staticmethod
    def _strip_heavy_fields(custom_info: Optional[Dict], executor_type: Optional[str] = None) -> Optional[Dict]:
        """Remove heavy fields from custom_info to reduce payload size."""
        if not custom_info:
            return custom_info
        heavy_fields = {"fill_events"}
        if executor_type == "grid_executor":
            heavy_fields |= {"levels_by_state", "filled_orders", "failed_orders", "canceled_orders"}
        return {k: v for k, v in custom_info.items() if k not in heavy_fields}

    def _format_db_record(self, record) -> Dict[str, Any]:
        """Format a database ExecutorRecord for API response."""
        # Parse error_log from DB for completed executors
        error_count = 0
        last_error = None
        if record.error_log:
            try:
                errors = json.loads(record.error_log)
                error_count = len(errors)
                if errors:
                    last_error = errors[-1].get("message")
            except (json.JSONDecodeError, TypeError):
                pass

        return {
            "executor_id": record.executor_id,
            "executor_type": record.executor_type,
            "account_name": record.account_name,
            "connector_name": record.connector_name,
            "trading_pair": record.trading_pair,
            "side": None,
            "status": record.status,
            "close_type": record.close_type,
            "is_active": record.status == "RUNNING",
            "is_trading": False,
            "timestamp": None,
            "created_at": record.created_at.isoformat() if record.created_at else None,
            "close_timestamp": record.closed_at.timestamp() if record.closed_at else None,
            "closed_at": record.closed_at.isoformat() if record.closed_at else None,
            "controller_id": record.controller_id or "main",
            "net_pnl_quote": float(record.net_pnl_quote) if record.net_pnl_quote else 0.0,
            "net_pnl_pct": float(record.net_pnl_pct) if record.net_pnl_pct else 0.0,
            "cum_fees_quote": float(record.cum_fees_quote) if record.cum_fees_quote else 0.0,
            "filled_amount_quote": float(record.filled_amount_quote) if record.filled_amount_quote else 0.0,
            "config": json.loads(record.config) if record.config else None,
            "custom_info": self._strip_heavy_fields(
                json.loads(record.final_state), record.executor_type
            ) if record.final_state else None,
            "error_count": error_count,
            "last_error": last_error,
        }

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics for active executors.

        Returns:
            Dictionary with aggregate statistics for active executors only.
        """
        executors = []

        # Get active executors from memory
        for executor_id, executor in self._active_executors.items():
            executors.append(self._format_executor_info(executor_id, executor))

        active_count = len(executors)
        total_pnl = sum(e.get("net_pnl_quote", 0) for e in executors)
        # filled_amount_quote is the volume traded on every executor type — an LP
        # executor derives it from the fees it earned rather than the capital it put up,
        # so this sums like with like and no separate field is needed.
        total_volume = sum(e.get("filled_amount_quote", 0) for e in executors)

        by_type: Dict[str, int] = {}
        by_connector: Dict[str, int] = {}
        by_status: Dict[str, int] = {}

        for e in executors:
            ex_type = e.get("executor_type", "unknown")
            connector = e.get("connector_name", "unknown")
            status = e.get("status", "unknown")

            by_type[ex_type] = by_type.get(ex_type, 0) + 1
            by_connector[connector] = by_connector.get(connector, 0) + 1
            by_status[status] = by_status.get(status, 0) + 1

        return {
            "total_active": active_count,
            "total_pnl_quote": total_pnl,
            "total_volume_quote": total_volume,
            "by_type": by_type,
            "by_connector": by_connector,
            "by_status": by_status
        }

    async def get_performance_report(
        self,
        controller_id: Optional[str] = None,
        market_data_service=None
    ) -> Dict[str, Any]:
        """
        Generate a performance report aggregating executor metrics.

        Combines database aggregations (completed executors) with in-memory
        active executor and position hold unrealized PnL.
        Excludes POSITION_HOLD close_type from realized PnL to avoid double-counting.

        Args:
            controller_id: Filter by controller ID (None = all)
            market_data_service: MarketDataService for position hold unrealized PnL

        Returns:
            Dictionary with performance metrics ready for PerformanceReportResponse.
        """
        report: Dict[str, Any] = {
            "controller_id": controller_id,
            "total_executors": 0,
            "by_status": {},
            "pnl_total_quote": 0.0,
            "unrealized_pnl_quote": 0.0,
            "global_pnl_quote": 0.0,
            "pnl_pct_avg": 0.0,
            "fees_total_quote": 0.0,
            "volume_total_quote": 0.0,
            "win_rate": 0.0,
            "sharpe_ratio": None,
            "by_type": [],
            "active_positions": 0,
        }

        if self.db_manager:
            # No try/except here on purpose: a database outage used to be swallowed and
            # reported as the zeroed report below, making it indistinguishable from
            # "no executors yet" to every consumer -- the route answered 200 with zeroes
            # and the /ws/executors performance channel pushed a confident row of them.
            # The failure now propagates: the route turns it into a 500 and the push loop
            # sends an error frame. The zeroed report is left to mean an empty dataset.
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)
                db_data = await repo.get_performance_report(controller_id=controller_id)

            report["total_executors"] = db_data["total_executors"]
            report["by_status"] = db_data["status_counts"]
            report["pnl_total_quote"] = db_data["pnl_total_quote"]
            report["pnl_pct_avg"] = db_data["pnl_pct_avg"]
            report["fees_total_quote"] = db_data["fees_total_quote"]
            report["volume_total_quote"] = db_data["volume_total_quote"]
            report["win_rate"] = db_data["win_rate"]
            report["by_type"] = db_data["by_type"]

            # Sharpe ratio: mean(pnl) / std(pnl). The repository returns the sample
            # standard deviation as an aggregate -- None below 2 completed executors --
            # so this never depends on the number of rows in the table.
            pnl_std = db_data["pnl_std"]
            completed_count = db_data["completed_count"]
            if pnl_std:  # None below 2 executors, 0.0 when every PnL is identical
                mean_pnl = db_data["pnl_total_quote"] / completed_count
                report["sharpe_ratio"] = round(mean_pnl / pnl_std, 4)

        # --- Unrealized PnL from active executors ---
        unrealized_pnl = 0.0
        for executor_id, executor in self._active_executors.items():
            metadata = self._executor_metadata.get(executor_id, {})
            if controller_id and metadata.get("controller_id", "main") != controller_id:
                continue
            try:
                unrealized_pnl += float(executor.executor_info.net_pnl_quote)
            except Exception:
                pass

        # --- Unrealized PnL from position holds ---
        positions = self.get_positions_held(controller_id=controller_id)
        report["active_positions"] = len(positions)

        # Accumulate fees from position holds (already paid, reduce PnL)
        position_hold_fees = sum(float(p.cum_fees_quote) for p in positions)

        if market_data_service:
            # First pass: try oracle for each position, collect misses grouped by connector
            missing_by_connector: Dict[str, List[tuple]] = {}  # connector_key -> [(position, trading_pair)]
            for p in positions:
                # A hyphenated base symbol produced three parts and was skipped here,
                # so the position simply contributed nothing to unrealized PnL.
                try:
                    base, quote = split_trading_pair(p.trading_pair)
                except InvalidTradingPair:
                    continue
                rate = market_data_service.get_rate(base, quote)
                if rate is not None:
                    unrealized_pnl += float(p.get_unrealized_pnl(rate))
                else:
                    # Group by connector+account for batch fallback
                    connector_key = f"{p.connector_name}|{p.account_name}"
                    missing_by_connector.setdefault(connector_key, []).append((p, p.trading_pair))

            # Second pass: batch-fetch missing prices from the actual connectors
            for connector_key, items in missing_by_connector.items():
                connector_name, account_name = connector_key.split("|", 1)
                trading_pairs = [tp for _, tp in items]
                try:
                    prices = await market_data_service.get_prices(
                        connector_name=connector_name,
                        trading_pairs=trading_pairs,
                        account_name=account_name,
                    )
                    if isinstance(prices, dict) and "error" not in prices:
                        for pos, tp in items:
                            price = prices.get(tp)
                            if price is not None and price > 0:
                                unrealized_pnl += float(pos.get_unrealized_pnl(Decimal(str(price))))
                except Exception as e:
                    logger.warning(f"Fallback price fetch failed for {connector_name}: {e}")

        # Subtract position hold fees from unrealized PnL
        unrealized_pnl -= position_hold_fees

        report["unrealized_pnl_quote"] = round(unrealized_pnl, 8)
        report["position_hold_fees_quote"] = round(position_hold_fees, 8)
        report["global_pnl_quote"] = round(report["pnl_total_quote"] + unrealized_pnl, 8)

        return report

    async def _persist_executor_created(self, executor_id: str, executor: ExecutorBase):
        """Persist executor creation to database."""
        if not self.db_manager:
            return

        try:
            metadata = self._executor_metadata.get(executor_id, {})

            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)

                await repo.create_executor(
                    executor_id=executor_id,
                    executor_type=metadata.get("executor_type"),
                    account_name=metadata.get("account_name"),
                    connector_name=metadata.get("connector_name"),
                    trading_pair=metadata.get("trading_pair"),
                    config=json.dumps(metadata.get("config", {}), default=_json_default),
                    status=executor.status.name,
                    controller_id=metadata.get("controller_id", "main"),
                    checkpoint=self._initial_checkpoint(executor_id, executor),
                )

            logger.debug(f"Persisted executor {executor_id} creation to database")

        except IntegrityError:
            # The executor closed before this INSERT landed and its completion already
            # wrote the row (see upsert_executor_completion). That row carries the final
            # state; re-inserting a RUNNING one is exactly what must not happen.
            logger.debug(
                f"Executor {executor_id} row already written by its completion; "
                f"skipping the creation insert"
            )
        except Exception as e:
            logger.error(f"Error persisting executor creation: {e}")

    async def _persist_executor_completed(self, executor_id: str, executor: ExecutorBase):
        """Persist executor completion to database."""
        if not self.db_manager:
            return False

        try:
            # Read status/close_type directly from executor (most reliable)
            status_name = executor.status.name
            close_type = executor.close_type.name if executor.close_type else None

            # Get PnL values from executor_info
            try:
                executor_info = executor.executor_info
                net_pnl_quote = executor_info.net_pnl_quote
                net_pnl_pct = executor_info.net_pnl_pct
                cum_fees_quote = executor_info.cum_fees_quote
                filled_amount_quote = executor_info.filled_amount_quote
                metrics_are_measured = True
            except Exception as e:
                logger.debug(f"Error accessing executor_info for persistence: {e}")
                net_pnl_quote = Decimal("0")
                net_pnl_pct = Decimal("0")
                cum_fees_quote = Decimal("0")
                filled_amount_quote = Decimal("0")
                # The record still takes these zeros, as it always has. The snapshot
                # series does not: a terminal row is what a reader takes for the
                # executor's final value, and a fabricated zero there is indistinguishable
                # from an executor that genuinely made nothing.
                metrics_are_measured = False

            # Get custom_info directly from executor to avoid Pydantic serialization issues
            # with TrackedOrder and other complex types
            custom_info = executor.get_custom_info()

            # A stranded live on-chain position: an involuntary hold (close retries
            # exhausted -> POSITION_HOLD with hold_reason set, gateway#678) or a legacy
            # FAILED-with-position (force-stop straggler, older wheel). Flag it in the
            # persisted final_state so /executors/positions/orphaned, dashboards, and
            # agents can find and recover it. Voluntary holds never match: a successful
            # close clears position_address before the executor terminates.
            if custom_info.get("position_address") and (
                custom_info.get("hold_reason") or close_type == "FAILED"
            ):
                custom_info["orphaned_position"] = True
            # Serialize custom_info, fallback to None if serialization fails
            final_state_json = None
            metadata = self._executor_metadata.get(executor_id, {})
            executor_type = metadata.get("executor_type")
            if executor_type == "grid_executor":
                heavy_fields = {
                    "levels_by_state",
                    "filled_orders",
                    "failed_orders",
                    "canceled_orders",
                }
                custom_info = {k: v for k, v in custom_info.items() if k not in heavy_fields}

            try:
                final_state_json = json.dumps(custom_info, default=_json_default)
            except Exception as e:
                logger.warning(f"Failed to serialize custom_info for {executor_id}: {e}")
                # Try a simpler serialization without complex objects
                try:
                    simple_info = {k: v for k, v in custom_info.items()
                                   if isinstance(v, (str, int, float, bool, list, dict, type(None)))}
                    final_state_json = json.dumps(simple_info)
                except Exception:
                    final_state_json = None

            # Capture error logs before persisting
            error_log_json = None
            error_count = self._log_capture.get_error_count(executor_id)
            if error_count > 0:
                try:
                    error_entries = self._log_capture.get_logs(executor_id, level="ERROR")
                    error_log_json = json.dumps([
                        {
                            "timestamp": entry.get("timestamp"),
                            "message": entry.get("message"),
                            "exc_info": entry.get("exc_info"),
                        }
                        for entry in error_entries
                    ])
                except Exception as e:
                    logger.debug(f"Failed to serialize error logs for {executor_id}: {e}")

            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)

                # Upsert, not update: an executor that closes in milliseconds can reach
                # here before _persist_executor_created's INSERT has landed (or after it
                # failed outright), and a plain select-then-update would silently drop
                # this final state, leaving a phantom RUNNING executor forever.
                record, repaired = await repo.upsert_executor_completion(
                    executor_id=executor_id,
                    executor_type=executor_type,
                    account_name=metadata.get("account_name"),
                    connector_name=metadata.get("connector_name"),
                    trading_pair=metadata.get("trading_pair"),
                    controller_id=metadata.get("controller_id", "main"),
                    config=json.dumps(metadata.get("config", {}), default=_json_default),
                    created_at=metadata.get("created_at"),
                    status=status_name,
                    close_type=close_type,
                    net_pnl_quote=net_pnl_quote,
                    net_pnl_pct=net_pnl_pct,
                    cum_fees_quote=cum_fees_quote,
                    filled_amount_quote=filled_amount_quote,
                    final_state=final_state_json,
                    error_log=error_log_json
                )

                # The terminal row, in the same transaction as the record update so the
                # two never disagree. It is what makes a closed executor's series
                # answerable from executor_performance_snapshots alone -- no join, and no
                # "and then append the final value" rule for every future reader.
                #
                # Behind a SAVEPOINT, because the direction of the coupling matters: the
                # record update is the accounting and the snapshot is a point on a chart,
                # so a failing snapshot must roll back only itself. Without it a bad
                # INSERT here would abort the transaction and lose the completion, which
                # is the failure 60041a8 went to some trouble to make impossible.
                if record is not None and metrics_are_measured:
                    terminal_row = self._build_snapshot_row(
                        executor_id,
                        executor,
                        is_terminal=True,
                        metrics={
                            "net_pnl_quote": net_pnl_quote,
                            "net_pnl_pct": net_pnl_pct,
                            "cum_fees_quote": cum_fees_quote,
                            "filled_amount_quote": filled_amount_quote,
                        },
                        status=status_name,
                        close_type=close_type,
                    )
                    if terminal_row is not None:
                        try:
                            async with session.begin_nested():
                                await ExecutorPerformanceRepository(session).save_snapshots(
                                    [terminal_row]
                                )
                        except Exception as e:
                            logger.error(
                                f"Could not write the terminal performance snapshot for "
                                f"{executor_id}; its series ends at the last periodic row: {e}"
                            )

            if record is None:
                logger.error(
                    f"Could not persist completion for executor {executor_id}: no row to "
                    f"update and the repair insert did not take"
                )
                return False
            elif repaired:
                logger.warning(
                    f"Executor {executor_id} completed before its creation row existed; "
                    f"inserted the record from its final state"
                )
            else:
                logger.debug(f"Persisted executor {executor_id} completion to database")
            return True

        except Exception as e:
            logger.error(f"Error persisting executor completion: {e}")
            return False

    # ========================================
    # Position Hold Tracking Methods
    # ========================================

    def _get_position_key(
        self,
        account_name: str,
        connector_name: str,
        trading_pair: str,
        controller_id: str = "main"
    ) -> str:
        """Generate a unique key for position tracking."""
        return f"{account_name}|{connector_name}|{trading_pair}|{controller_id}"

    async def _aggregate_position_hold(
        self,
        executor_id: str,
        executor: ExecutorBase,
        metadata: Dict[str, Any]
    ):
        """
        Aggregate position data from an executor stopped with keep_position=True.

        This extracts the filled amounts from the executor and adds them to
        the aggregated position tracking.
        """
        account_name = metadata.get("account_name", self.default_account)
        connector_name = metadata.get("connector_name", "")
        trading_pair = metadata.get("trading_pair", "")
        controller_id = metadata.get("controller_id", "main")

        if not connector_name or not trading_pair:
            logger.warning(f"Cannot aggregate position for executor {executor_id}: missing connector/pair info")
            return

        position_key = self._get_position_key(account_name, connector_name, trading_pair, controller_id)

        # Get or create position hold
        if position_key not in self._positions_held:
            self._positions_held[position_key] = PositionHold(
                trading_pair=trading_pair,
                connector_name=connector_name,
                account_name=account_name,
                controller_id=controller_id
            )

        position = self._positions_held[position_key]

        # Extract filled amounts from executor
        try:
            # Try to get executor info
            try:
                executor_info = executor.executor_info
                custom_info = executor_info.custom_info or {}
            except Exception:
                custom_info = executor.get_custom_info() if hasattr(executor, 'get_custom_info') else {}

            # Get side from config or custom_info
            config = metadata.get("config", {})
            side = config.get("side", custom_info.get("side", "BUY"))

            # Extract filled amounts - try different sources
            filled_amount_base = Decimal("0")
            filled_amount_quote = Decimal("0")

            # Try from executor attributes directly
            if hasattr(executor, 'filled_amount_base'):
                filled_amount_base = Decimal(str(executor.filled_amount_base or 0))
            if hasattr(executor, 'filled_amount_quote'):
                filled_amount_quote = Decimal(str(executor.filled_amount_quote or 0))

            # Fallback to custom_info
            if filled_amount_base == 0 and custom_info:
                filled_amount_base = Decimal(str(custom_info.get("filled_amount_base", 0)))
            if filled_amount_quote == 0 and custom_info:
                filled_amount_quote = Decimal(str(custom_info.get("filled_amount_quote", 0)))

            # Check for held_position_orders (used by grid_executor, position_executor, etc.)
            held_orders = custom_info.get("held_position_orders", []) if custom_info else []

            # Extract cumulative fees from the executor
            executor_fees = Decimal("0")
            try:
                executor_fees = Decimal(str(executor.cum_fees_quote or 0))
            except Exception:
                pass

            if held_orders:
                buy_filled_base = Decimal("0")
                buy_filled_quote = Decimal("0")
                sell_filled_base = Decimal("0")
                sell_filled_quote = Decimal("0")
                orders_fees = Decimal("0")

                for order in held_orders:
                    if isinstance(order, dict):
                        trade_type = order.get("trade_type", "BUY")
                        exec_base = Decimal(str(order.get("executed_amount_base", 0)))
                        exec_quote = Decimal(str(order.get("executed_amount_quote", 0)))
                        orders_fees += Decimal(str(order.get("cumulative_fee_paid_quote", 0)))

                        if trade_type == "BUY":
                            buy_filled_base += exec_base
                            buy_filled_quote += exec_quote
                        else:
                            sell_filled_base += exec_base
                            sell_filled_quote += exec_quote

                # Use order-level fees if available, otherwise fall back to executor-level
                fees = orders_fees if orders_fees > 0 else executor_fees

                # Add buy and sell fills separately
                if buy_filled_base > 0:
                    # Split fees proportionally between buy and sell by quote volume
                    total_quote = buy_filled_quote + sell_filled_quote
                    buy_fee_share = fees * (buy_filled_quote / total_quote) if total_quote > 0 else fees
                    position.add_fill("BUY", buy_filled_base, buy_filled_quote, executor_id, fees_quote=buy_fee_share)
                if sell_filled_base > 0:
                    total_quote = buy_filled_quote + sell_filled_quote
                    sell_fee_share = fees * (sell_filled_quote / total_quote) if total_quote > 0 else fees
                    position.add_fill("SELL", sell_filled_base, sell_filled_quote, executor_id, fees_quote=sell_fee_share)

                logger.info(
                    f"Aggregated executor {executor_id} to position {position_key}: "
                    f"buy={buy_filled_base} base, sell={sell_filled_base} base, fees={fees} quote"
                )

            elif filled_amount_base > 0:
                # For non-grid executors with a single side
                position.add_fill(side, filled_amount_base, filled_amount_quote, executor_id, fees_quote=executor_fees)
                logger.info(
                    f"Aggregated executor {executor_id} to position {position_key}: "
                    f"{side} {filled_amount_base} base @ {filled_amount_quote} quote"
                )
            else:
                logger.debug(f"Executor {executor_id} has no filled amounts to aggregate")

            # Persist position hold to the dedicated table
            await self._persist_position_hold(position)

        except Exception as e:
            logger.error(f"Error aggregating position for executor {executor_id}: {e}", exc_info=True)

    async def _persist_position_hold(self, position: PositionHold):
        """Persist a position hold to the dedicated position_holds table."""
        if not self.db_manager:
            return
        try:
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)
                await repo.upsert_position_hold(
                    account_name=position.account_name,
                    connector_name=position.connector_name,
                    trading_pair=position.trading_pair,
                    controller_id=position.controller_id,
                    buy_amount_base=position.buy_amount_base,
                    buy_amount_quote=position.buy_amount_quote,
                    sell_amount_base=position.sell_amount_base,
                    sell_amount_quote=position.sell_amount_quote,
                    realized_pnl_quote=position.realized_pnl_quote,
                    cum_fees_quote=position.cum_fees_quote,
                    executor_ids=position.executor_ids,
                )
        except Exception as e:
            logger.error(f"Error persisting position hold: {e}", exc_info=True)

    def get_positions_held(
        self,
        account_name: Optional[str] = None,
        connector_name: Optional[str] = None,
        trading_pair: Optional[str] = None,
        controller_id: Optional[str] = None
    ) -> List[PositionHold]:
        """
        Get held positions with optional filtering.

        Args:
            account_name: Filter by account name
            connector_name: Filter by connector name
            trading_pair: Filter by trading pair
            controller_id: Filter by controller ID

        Returns:
            List of PositionHold objects matching the filters
        """
        positions = []

        for position in self._positions_held.values():
            # Apply filters
            if account_name and position.account_name != account_name:
                continue
            if connector_name and position.connector_name != connector_name:
                continue
            if trading_pair and position.trading_pair != trading_pair:
                continue
            if controller_id and position.controller_id != controller_id:
                continue

            # Only include positions with actual volume
            if position.buy_amount_base > 0 or position.sell_amount_base > 0:
                positions.append(position)

        return positions

    def get_position_held(
        self,
        account_name: str,
        connector_name: str,
        trading_pair: str,
        controller_id: str = "main"
    ) -> Optional[PositionHold]:
        """
        Get a specific held position.

        Args:
            account_name: Account name
            connector_name: Connector name
            trading_pair: Trading pair
            controller_id: Controller ID

        Returns:
            PositionHold or None if not found
        """
        position_key = self._get_position_key(account_name, connector_name, trading_pair, controller_id)
        return self._positions_held.get(position_key)

    async def clear_position_held(
        self,
        account_name: str,
        connector_name: str,
        trading_pair: str,
        controller_id: str = "main"
    ) -> bool:
        """
        Clear a specific held position (after manual close or full exit).

        Args:
            account_name: Account name
            connector_name: Connector name
            trading_pair: Trading pair
            controller_id: Controller ID

        Returns:
            True if cleared, False if not found
        """
        position_key = self._get_position_key(account_name, connector_name, trading_pair, controller_id)
        if position_key in self._positions_held:
            del self._positions_held[position_key]
            # Mark position hold as CLEARED in the dedicated table
            if self.db_manager:
                try:
                    async with self.db_manager.get_session_context() as session:
                        repo = ExecutorRepository(session)
                        cleared = await repo.clear_position_hold(
                            account_name=account_name,
                            connector_name=connector_name,
                            trading_pair=trading_pair,
                            controller_id=controller_id
                        )
                        logger.info(f"Cleared position hold record from database for {position_key}: {cleared}")
                except Exception as e:
                    logger.error(f"Failed to clear position hold from database: {e}", exc_info=True)
            logger.info(f"Cleared position hold for {position_key}")
            return True
        return False

    def get_positions_summary(self) -> Dict[str, Any]:
        """
        Get summary of all held positions.

        Returns:
            Dictionary with total positions, PnL, and position list
        """
        positions = self.get_positions_held()
        total_realized_pnl = sum(float(p.realized_pnl_quote) for p in positions)

        return {
            "total_positions": len(positions),
            "total_realized_pnl": total_realized_pnl,
            "positions": [
                {
                    "trading_pair": p.trading_pair,
                    "connector_name": p.connector_name,
                    "account_name": p.account_name,
                    "buy_amount_base": float(p.buy_amount_base),
                    "buy_amount_quote": float(p.buy_amount_quote),
                    "sell_amount_base": float(p.sell_amount_base),
                    "sell_amount_quote": float(p.sell_amount_quote),
                    "net_amount_base": float(p.net_amount_base),
                    "buy_breakeven_price": float(p.buy_breakeven_price) if p.buy_breakeven_price else None,
                    "sell_breakeven_price": float(p.sell_breakeven_price) if p.sell_breakeven_price else None,
                    "matched_amount_base": float(p.matched_amount_base),
                    "unmatched_amount_base": float(p.unmatched_amount_base),
                    "position_side": p.position_side,
                    "realized_pnl_quote": float(p.realized_pnl_quote),
                    "cum_fees_quote": float(p.cum_fees_quote),
                    "executor_count": len(p.executor_ids),
                    "executor_ids": p.executor_ids,
                    "last_updated": p.last_updated.isoformat() if p.last_updated else None
                }
                for p in positions
            ]
        }

    def _partial_drop_released_pending(self, rows: List[Dict[str, Any]], released: set) -> None:
        pair = ""
        for row in rows or []:
            if row.get("trading_pair"):
                pair = row.get("trading_pair")
                break
        if not pair:
            pair = next(iter(self.__dict__.get("_tick_reads") or {}), "")
        self._drop_dead_pending(pair)

    def _partial_close_amount(self, executor: Any, level: Any) -> Optional[Decimal]:
        """Close size is the filled base, not a missing GridLevel.amount."""
        getter = getattr(executor, "_get_close_order_candidate", None)
        candidate = None
        if getter is not None:
            try:
                candidate = getter(level)
            except Exception:
                candidate = None
        raw = getattr(candidate, "amount", None)
        if raw in (None, ""):
            opened = getattr(level, "active_open_order", None)
            raw = getattr(opened, "executed_amount_base", None)
        if raw in (None, ""):
            return None
        try:
            amount = Decimal(str(raw))
        except (ArithmeticError, ValueError, TypeError):
            return None
        if amount <= 0:
            return None
        return amount

    def _partial_released_ids_for_pair(self, pair: str) -> set:
        found = set()
        for row in getattr(self, "_hold_rows", None) or []:
            if pair and row.get("trading_pair") not in (None, pair):
                continue
            found.update(partial_released_ids(checkpoint_master(row.get("checkpoint"))))
        return found

    def _partial_pair_seat_prices(self, pair: str, released: set) -> List[Any]:
        """Take-profit and close prices from every column of this pair, just read."""
        prices: List[Any] = []
        for row in getattr(self, "_hold_rows", None) or []:
            if pair and row.get("trading_pair") not in (None, "", pair):
                continue
            parsed = checkpoint_master(row.get("checkpoint"))
            if parsed is None and row.get("checkpoint"):
                prices.append(None)
                continue
            for level in (parsed or {}).get("levels") or []:
                level_id = str(level.get("id") or "")
                opened = str((level.get("open_order") or {}).get("order_id") or "")
                closed = str((level.get("close_order") or {}).get("order_id") or "")
                fresh = self._tick_fresh(pair)
                # Уровень пропускается только когда все его номера мёртвы без
                # исполнения (zero-dead). Исполненная покупка держит позицию —
                # её пара (opened, take_profit_price) остаётся в фиде.
                position_open = self._level_position_open(level, parsed, fresh)
                if opened and self._proven_dead_zero(opened, fresh) and (not closed or self._proven_dead_zero(closed, fresh)) and not position_open:
                    continue
                if level.get("take_profit_price") not in (None, "") and (
                    not self._proven_dead_zero(opened, fresh) or position_open
                ):
                    prices.append((opened, level.get("take_profit_price")))
                close = level.get("close_order") or {}
                if not self._proven_dead(closed, fresh) and close.get("price") not in (None, ""):
                    prices.append((closed, close.get("price")))
        return prices

    def _partial_lone_five(self, rows: List[Dict[str, Any]], order_id: str) -> bool:
        return partial_lone_five(rows, order_id)

    def _partial_price_belongs_to_released(self, rows: List[Dict[str, Any]], released: set, price: Any) -> bool:
        if price in (None, ""):
            return False
        for row in rows:
            order_id = str(row.get("order_id") or "")
            if order_id not in released:
                continue
            if str(row.get("price") or "") == str(price) or str(row.get("take_profit_price") or "") == str(price):
                return True
        return False

    def _partial_rows(self) -> Optional[List[Dict[str, Any]]]:
        """The snapshot's rows: read-only, a reader that edits takes its own copy."""
        try:
            return read_ledger_filtered()
        except Exception:
            return None

    def _partial_parsed(self, executor_id: str) -> Optional[Dict[str, Any]]:
        """Тело строки этого исполнителя: общий взгляд, только чтение.

        Правящий читатель берёт свою копию (parse_checkpoint_cached или
        copy.deepcopy): правка взгляда не должна остаться в кэше."""
        rows = getattr(self, "_hold_rows", None) or []
        for row in rows:
            if row.get("executor_id") == executor_id:
                return checkpoint_view(row.get("checkpoint"))
        return None

    async def _partial_column_text(self, executor_id: str) -> Optional[str]:
        """Столбец checkpoint новой сессией. Ошибка базы БРОСАЕТСЯ, а не маскируется
        в «записи нет»: непрочитанное — не «метки нет» (план stop-freeze п.1).
        """
        if not self.db_manager:
            return None
        async with self.db_manager.get_session_context() as session:
            row = await ExecutorRepository(session).get_executor_by_id(executor_id)
        return row.checkpoint if row is not None else None

    def _partial_must_keep(self, row: Dict[str, Any]) -> bool:
        parsed = checkpoint_master(row.get("checkpoint"))
        if partial_exposure(self._partial_rows(), parsed, row.get("executor_id") or ""):
            return True
        for level in (parsed or {}).get("levels") or []:
            close = (level or {}).get("close_order") or {}
            if close.get("order_id"):
                return True
        return False

    def _partial_column_pending(self, executor_id: str) -> bool:
        parsed = self._partial_parsed(executor_id)
        for level in (parsed or {}).get("levels") or []:
            slot = (level or {}).get("open_order") or {}
            close = (level or {}).get("close_order") or {}
            if slot.get("order_id") or close.get("order_id") or level.get("take_profit_price"):
                return True
        return False

    def _partial_hold_witness(self, executor_id: str) -> str:
        """Name the lines that hold the stop: send-mark, remember, annotation-only, column."""
        try:
            rows = self._partial_rows()
        except Exception:
            rows = None
        if rows is None:
            return " hold=ledger-unread"
        parsed = self._partial_parsed(executor_id)
        parts = []
        for order_id, kind in partial_hold_kinds(rows, parsed, executor_id).items():
            parts.append(f"hold={kind}:{str(order_id)[-8:]}")
        if parsed is not None:
            for level_id in partial_column_hold_levels(parsed):
                parts.append(f"hold=column:{level_id}")
        if not parts:
            return ""
        return " " + " ".join(parts)

    def _unproven_holding_ids(self, executor_id: str, connector: Any, pair: str) -> set:
        """Номера holding без видимого конца, которые ещё могут оказаться на бирже.

        Номер не подтверждён (нет ни в трекерах, ни в живом списке opens) и свежие
        списки не покрывают время его строки — история ещё не может судить.
        Подтверждённая живая заявка и доказанно-мёртвый номер сюда не входят:
        первую отменит early_stop, второму конец пишет шаг сборки.
        """
        rows = self._partial_rows()
        parsed = self._partial_parsed(executor_id)
        found = set()
        if rows is None:
            return found
        fresh = self._tick_fresh(pair or "")
        for order_id in partial_holding_ids(rows, executor_id):
            text = str(order_id)
            if partial_end_visible(parsed, text):
                continue
            if self._id_still_live(fresh, connector, text):
                continue
            if self._proven_dead(text, fresh):
                continue
            # Нет записи finished — стоп держит, даже если списки покрывают строку.
            # Пустая база не разрешение пройти: отмена видит только слоты.
            found.add(text)
        return found

    def _partial_forbids(self, executor_id: str) -> bool:
        if (self.__dict__.get("_stock_tick_forbid") or {}).get(executor_id):
            return True
        ready = bool((self.__dict__.get("_partial_proof_ready") or {}).get(executor_id))
        executor = self._active_executors.get(executor_id)
        config = getattr(executor, "config", None)
        connector_name = getattr(config, "connector_name", None)
        pair = getattr(config, "trading_pair", None) or ""
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        return partial_stop_forbidden(
            self._partial_rows(),
            self._partial_parsed(executor_id),
            executor_id,
            proof_ready=ready,
            unproven_ids=self._unproven_holding_ids(executor_id, connector, pair),
        )

    def _partial_keep_running(self, executor: Any) -> None:
        if self._stop_mark_now(self._executor_id_of(executor)):
            return
        status = getattr(executor, "_status", None)
        enum = type(status) if status is not None else None
        running = getattr(enum, "RUNNING", None)
        if running is None:
            try:
                from hummingbot.strategy_v2.executors.executor_base import RunnableStatus
                running = RunnableStatus.RUNNING
            except Exception:
                running = None
        if running is not None:
            executor._status = running

    def _partial_five(self, executor: Any, close: bool = False) -> Optional[Dict[str, Any]]:
        config = getattr(executor, "config", None)
        barrier = getattr(config, "triple_barrier_config", None)
        side = getattr(getattr(config, "side", None), "name", None)
        if close:
            side = "SELL" if side == "BUY" else "BUY" if side == "SELL" else None
        kind = "take_profit_order_type" if close else "open_order_type"
        order_type = getattr(barrier, kind, None)
        order_type = getattr(order_type, "name", order_type)
        fields = {
            "trading_pair": getattr(config, "trading_pair", None),
            "order_type": order_type,
            "trade_type": side,
            "leverage": getattr(config, "leverage", None),
            "position": "CLOSE" if close else "OPEN",
        }
        if any(value in (None, "") for value in fields.values()):
            return None
        return fields

    def _partial_level_profit(self, executor: Any, level_id: str) -> Any:
        for level in list(getattr(executor, "grid_levels", None) or []) + list(getattr(getattr(executor, "config", None), "levels", None) or []):
            if str(getattr(level, "id", None) or (level.get("id") if isinstance(level, dict) else "")) == str(level_id):
                if isinstance(level, dict):
                    return level.get("take_profit")
                return getattr(level, "take_profit", None)
        return None

    def _partial_send_mark(self, row: Optional[Dict[str, Any]]) -> str:
        if not isinstance(row, dict):
            return "absent"
        if partial_full_fill(row):
            return "filled"
        if row.get("finish_as") == "cancelled" and "left" in row and "amount" in row:
            return f"cancel:{row.get('amount')}:{row.get('left')}"
        return "absent"

    def _partial_same_send_already(self, sent: Dict[str, str], order_id: str, row: Optional[Dict[str, Any]]) -> bool:
        mark = self._partial_send_mark(row)
        if mark == "filled":
            return True
        return sent.get(order_id) == mark

    def _partial_column_take(self, executor_id: Any, level_id: Any) -> tuple:
        profit = None
        side = "BUY"
        for row in getattr(self, "_hold_rows", None) or []:
            if not row.get("executor_id") or row.get("executor_id") != executor_id:
                continue
            parsed = checkpoint_master(row.get("checkpoint"))
            for level in (parsed or {}).get("levels") or []:
                if str(level.get("id")) != str(level_id):
                    continue
                if level.get("take_profit") not in (None, ""):
                    profit = level.get("take_profit")
                if level.get("side"):
                    side = str(level.get("side"))
        return profit, side

    def _partial_missing_take_prices(self, rows: List[Dict[str, Any]], executor: Any, pair: str) -> List[Any]:
        """A missing journal field is not permission to buy the take-profit tick."""
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        if connector is None or not pair:
            return []
        released = self._partial_released_ids_for_pair(pair)
        found = []
        for row in rows or []:
            order_id = str(row.get("order_id") or "")
            fresh = self._tick_fresh(pair)
            if not order_id or self._proven_dead(order_id, fresh) or not row.get("level_id") or row.get("price") in (None, ""):
                continue
            if row.get("take_profit_price") not in (None, ""):
                continue
            if row.get("trading_pair") not in (None, "", pair):
                continue
            profit, side = self._partial_column_take(row.get("executor_id"), row.get("level_id"))
            quant = partial_take_profit_quant(connector, pair, row.get("price"), profit, side)
            if quant:
                found.append((order_id, quant))
        return found

    def _partial_flight_prices(self, executor: Any) -> List[Any]:
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        if connector is None:
            return []
        books = [getattr(connector, "in_flight_orders", None) or {}]
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        books.append(lost)
        found = []
        for book in books:
            for order in book.values():
                price = getattr(order, "price", None)
                if price not in (None, ""):
                    found.append(price)
        return found

    def _partial_sell_texts(self, executor_id: str, parsed: Optional[Dict[str, Any]]) -> set:
        """Sell ids of this fill. A remember_order_now line has no level and no price."""
        buys = set()
        sells = set()
        for row in self._partial_rows() or []:
            if row.get("executor_id") != executor_id or not row.get("order_id"):
                continue
            order_id = str(row.get("order_id"))
            if row.get("level_id"):
                buys.add(order_id)
            else:
                sells.add(order_id)
        for level in (parsed or {}).get("levels") or []:
            opened = str((level.get("open_order") or {}).get("order_id") or "")
            closed = str((level.get("close_order") or {}).get("order_id") or "")
            if opened:
                buys.add(opened)
            if closed:
                sells.add(closed)
        return {item for item in sells if item and item not in buys}

    def _partial_seen_opens(self, pair: str) -> List[Dict[str, Any]]:
        fresh = (self.__dict__.get("_partial_fresh") or {}).get(pair) or {}
        return list(fresh.get("opens") or [])

    def _partial_sell_is_live(self, executor_id: str, connector: Any, parsed: Optional[Dict[str, Any]], pair: str) -> bool:
        """Наша продажа стоит — только по НАШЕМУ номеру (правило номера: цена
        не опознаёт заявку, чужая на нашей цене не клеится к слоту)."""
        texts = self._partial_sell_texts(executor_id, parsed)
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        opens = self._partial_seen_opens(pair)
        for text in texts:
            if text in inflight or text in lost or partial_text_in(opens, text):
                return True
            for level in (parsed or {}).get("levels") or []:
                close = level.get("close_order") or {}
                if str(close.get("order_id") or "") == text and close.get("level_id"):
                    return True
        return False

    def _partial_live_sell_prices(self, executor_id: str, connector: Any, parsed: Optional[Dict[str, Any]], pair: str) -> List[Any]:
        texts = self._partial_sell_texts(executor_id, parsed)
        if not texts:
            return []
        found = []
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        for text in texts:
            for book in (inflight, lost):
                order = book.get(text)
                price = getattr(order, "price", None)
                if price not in (None, ""):
                    found.append(price)
            for raw in self._partial_seen_opens(pair):
                if str(raw.get("text") or "") == text and raw.get("price") not in (None, ""):
                    found.append(raw.get("price"))
            for level in (parsed or {}).get("levels") or []:
                close = level.get("close_order") or {}
                if str(close.get("order_id") or "") == text and close.get("price") not in (None, ""):
                    found.append(close.get("price"))
        return found

    def _partial_live_sell(self, connector: Any, parsed: Optional[Dict[str, Any]], level_id: str) -> bool:
        return False

    def _partial_owner_quant(self, row: Dict[str, Any]) -> Optional[str]:
        """Take-profit tick from the row owner's level, not from the caller."""
        owner = self._active_executors.get(row.get("executor_id"))
        if owner is None or not row.get("level_id"):
            return None
        config = getattr(owner, "config", None)
        pair = row.get("trading_pair") or getattr(config, "trading_pair", None)
        connector_name = getattr(config, "connector_name", None)
        connector = (getattr(owner, "connectors", None) or {}).get(connector_name)
        if not pair or connector is None:
            return None
        side = getattr(getattr(config, "side", None), "name", "BUY")
        return partial_take_profit_quant(
            connector, pair, row.get("price"), self._partial_level_profit(owner, str(row.get("level_id"))), side,
        )

    def _partial_rows_with_owner_take(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """A take-profit field that does not match the owner is not the sell price."""
        cleaned = []
        for row in rows:
            copy = dict(row)
            trusted = self._partial_owner_quant(row)
            if trusted:
                copy["take_profit_price"] = trusted
            elif row.get("executor_id") in self._active_executors:
                copy.pop("take_profit_price", None)
            cleaned.append(copy)
        return cleaned

    def _partial_prepare_sync(self, executor_id: str, executor: Any, connector: Any, pair: str) -> None:
        """Hold the owner's take-profit tick before any await. Only the owner appends it."""
        try:
            rows = read_ledger_filtered()
        except Exception:
            return
        released = self._partial_released_ids_for_pair(pair)
        pending = self.__dict__.setdefault("_partial_pending_ticks", set())
        for row in rows:
            order_id = row.get("order_id")
            if not order_id or not row.get("level_id"):
                continue
            if not partial_old_remember(row) and not partial_send_marked(rows, str(order_id)):
                continue
            if partial_close_send_line(row):
                continue
            if row.get("trading_pair") not in (None, "", pair) and str(row.get("executor_id") or "") != str(executor_id):
                continue
            owned = str(row.get("executor_id") or "") == str(executor_id)
            quant = self._partial_owner_quant(row)
            if owned and quant is None:
                side = getattr(getattr(getattr(executor, "config", None), "side", None), "name", "BUY")
                quant = partial_take_profit_quant(
                    connector, pair, row.get("price"), self._partial_level_profit(executor, str(row.get("level_id"))), side,
                )
            if quant and self._tick_fresh(pair) is not None:
                pending.add((str(order_id), str(quant)))
            if not owned or not quant:
                continue
            if str(row.get("take_profit_price") or "") == quant:
                continue
            tail = str(order_id)[-8:]
            line = {
                "executor_id": row.get("executor_id") or executor_id,
                "order_id": order_id,
                "level_id": row.get("level_id"),
                "price": row.get("price"),
                "trading_pair": pair,
                "take_profit_price": quant,
                "ts": time.time(),
            }
            with LEDGER_LOCK:
                try:
                    fresh_rows = read_ledger_filtered()
                except Exception:
                    fresh_rows = None
                if fresh_rows is None:
                    continue
                if any(
                    str(item.get("take_profit_price") or "") == str(quant)
                    for item in partial_lines(fresh_rows, str(order_id))
                ):
                    self._log_silent_exit(
                        "ledger-repeat-refused", pair,
                        f"order_id=…{tail} level_id={row.get('level_id')} writer=prepare-sync", executor_id,
                        interval=300.0, key=f"prepare-sync:{tail}",
                    )
                    continue
                if not _append_ledger_line(dict(line)):
                    continue
                try:
                    after_rows = read_ledger_filtered()
                except Exception:
                    after_rows = None
                if after_rows is not None and sum(
                    1 for item in partial_lines(after_rows, str(order_id))
                    if str(item.get("take_profit_price") or "") == str(quant)
                ) > 1:
                    self._log_silent_exit(
                        "ledger-repeat-written", pair,
                        f"order_id=…{tail} level_id={row.get('level_id')} writer=prepare-sync", executor_id,
                        interval=300.0, key=f"prepare-sync:{tail}",
                    )

    def _partial_refuses_open(self, executor_id: str, executor: Any, level: Any, price_text: str, tick: Any) -> bool:
        own_id = _own_fresh_send.get()
        try:
            rows = read_ledger_filtered()
        except Exception:
            return True
        parsed = self._partial_parsed(executor_id)
        pair = getattr(getattr(executor, "config", None), "trading_pair", None)
        level_id = str(getattr(level, "id", "") or "")
        held = partial_held_level_ids(rows, parsed, executor_id)
        if "*" in held:
            return True
        if level_id and level_id in held and not self._held_level_is_this_send(
            rows, parsed, executor_id, level_id, own_id,
        ):
            return True
        released = self._partial_released_ids_for_pair(pair or "")
        fresh = self._tick_fresh(pair or "")
        if self._lists_ready(fresh):
            self._partial_drop_released_pending(rows, released)
            parsed = self._parsed_without_dead(parsed, fresh)
            rows = [row for row in rows if not self._proven_dead(str(row.get("order_id") or ""), fresh)]
        prices = list(self._feed_prices(rows, parsed, fresh))
        prices.extend(self._pending_ticks_live())
        prices.extend(self._partial_missing_take_prices(rows, executor, pair or ""))
        prices.extend(self._partial_flight_prices(executor))
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        if connector is not None:
            prices.extend(self._partial_live_sell_prices(executor_id, connector, parsed, pair or ""))
        prices.extend(self._partial_pair_seat_prices(pair or "", set()))
        if own_id:
            prices = self._without_own_placement(prices, own_id, rows)
        prices = self._prices_for_blocks(prices, fresh)
        if own_id and self._open_text_holds(fresh, price_text, tick, own_id):
            return True
        if own_id and tick is None and self._placement_hold_prices(
            fresh, connector, price_text, own_id, rows,
        ):
            return True
        if not (own_id and tick is None) and recorded_price_blocks(prices, price_text, tick):
            return True
        for order_id in partial_holding_ids(rows, executor_id):
            lines = [row for row in rows if str(row.get("order_id") or "") == order_id]
            if lines and not any(row.get("price") not in (None, "") for row in lines):
                return True
        return False

    def _partial_mark_send(
        self,
        executor_id: str,
        executor: Any,
        connector: Any,
        pair: str,
        order_id: str,
        level_id: Any,
        price_text: str,
        account: str,
        connector_name: str,
    ) -> bool:
        """Two appended lines. False does not send and does not rewrite the marker file."""
        five = self._partial_five(executor, close=False)
        if not five or not order_id or not level_id:
            return False
        side = five.get("trade_type") or "BUY"
        profit = self._partial_level_profit(executor, str(level_id))
        quant = partial_take_profit_quant(connector, pair, price_text, profit, side)
        line = {
            "executor_id": executor_id,
            "order_id": order_id,
            "level_id": level_id,
            "price": price_text,
            "account_name": account,
            "connector": connector_name,
            "trading_pair": pair,
            "ts": time.time(),
        }
        line.update(five)
        if quant:
            line["take_profit_price"] = quant
        if not _append_ledger_line(dict(line)):
            return False
        if not _append_ledger_line(dict(line)):
            return False
        owed = {
            "order_id": order_id,
            "price": price_text,
            "executor_id": executor_id,
            "account_name": account,
            "connector": connector_name,
            "trading_pair": pair,
            "ts": line["ts"],
        }
        note_owed(owed)
        self.__dict__.setdefault("_partial_presend", {})[order_id] = dict(line)
        if quant:
            self.__dict__.setdefault("_partial_pending_ticks", set()).add((str(order_id), str(quant)))
        return True

    def _partial_text_protected(self, text: str) -> bool:
        if not text:
            return False
        if _delivery_owns(text):
            return False
        rows = self._partial_rows()
        if rows is None:
            return True
        released = set()
        for entry in self._hold_index():
            released.update(entry["released_ids"])
        if text in released:
            return False
        for row in rows:
            if str(row.get("order_id") or "") == text and row.get("level_id"):
                if partial_old_remember(row) or partial_send_marked(rows, text) or partial_has_five(row):
                    return True
        for entry in self._hold_index():
            if text in entry["protected_ids"]:
                return True
        return False

    def _released_texts(self, pair: str) -> set:
        found = set()
        for entry in self._hold_index():
            if entry["pair"] not in (None, "", pair):
                continue
            found.update(entry["released_texts"])
        return found

    def _price_kept_by_other(self, pair: str, fresh: Optional[Dict[str, Any]], price: Any, released: set) -> bool:
        """A live or foreign source still owns this price. The released own text does not."""
        wanted = str(price)
        for raw in (fresh or {}).get("opens") or []:
            if str(raw.get("price") or "") == wanted:
                return True
        for raw in (fresh or {}).get("finished") or []:
            text = str(raw.get("text") or raw.get("order_id") or "")
            if text in released:
                continue
            if text and str(raw.get("price") or "") == wanted:
                return True
        for other in (getattr(self, "_active_executors", None) or {}).values():
            other_pair = getattr(getattr(other, "config", None), "trading_pair", None)
            if pair and other_pair and other_pair != pair:
                continue
            connector_name = getattr(getattr(other, "config", None), "connector_name", None)
            connector = (getattr(other, "connectors", None) or {}).get(connector_name)
            inflight = getattr(connector, "in_flight_orders", None) or {}
            lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
            for book in (inflight, lost):
                for order_id, order in book.items():
                    if str(order_id) in released:
                        continue
                    live_price = getattr(order, "price", None)
                    if live_price not in (None, "") and str(live_price) == wanted:
                        return True
        for row in self._partial_rows() or []:
            order_id = str(row.get("order_id") or "")
            if not order_id or order_id in released:
                continue
            for key in ("price", "take_profit_price"):
                if str(row.get(key) or "") == wanted and not self._proven_dead(order_id, fresh):
                    return True
        return False

    def _id_still_live(self, fresh: Optional[Dict[str, Any]], connector: Any, order_id: str) -> bool:
        if not order_id:
            return False
        if partial_text_in((fresh or {}).get("opens"), order_id):
            return True
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        return order_id in inflight or order_id in lost

    def _stock_filled_order(
        self,
        *,
        client_id: str,
        trading_pair: str,
        trade_name: str,
        position_name: str,
        amount: Decimal,
        price: Decimal,
        leverage: int,
    ) -> Optional[Any]:
        """Recorded fill. is_filled comes from FILLED plus the base, not an assigned flag."""
        if not client_id or amount <= 0 or price <= 0 or not trading_pair or not trade_name:
            return None
        try:
            from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
            from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
            order = InFlightOrder(
                client_order_id=str(client_id),
                trading_pair=trading_pair,
                order_type=OrderType.LIMIT,
                trade_type=getattr(TradeType, trade_name),
                amount=amount,
                creation_timestamp=0,
                price=price,
                initial_state=OrderState.FILLED,
                leverage=int(leverage or 1),
                position=getattr(PositionAction, position_name),
            )
            order.executed_amount_base = amount
            order.executed_amount_quote = amount * price
            order.check_filled_condition()
            if not order.is_filled or not order.completely_filled_event.is_set():
                return None
            return order
        except Exception:
            return None

    def _filled_pair_ready(self, executor_id: str, executor: Any, level_id: str) -> Optional[Dict[str, Any]]:
        """Filled close matched by recorded id. A live id or a foreign text is not ready."""
        parsed = self._partial_parsed(executor_id)
        level_body = next(
            (item for item in (parsed or {}).get("levels") or [] if str(item.get("id") or "") == str(level_id)),
            None,
        )
        if not isinstance(level_body, dict):
            return None
        close = level_body.get("close_order") or {}
        close_id = str(close.get("order_id") or "")
        slot = level_body.get("open_order") or {}
        buy_id = str(slot.get("order_id") or "")
        if not close_id or close.get("price") in (None, "") or not close.get("level_id") or not buy_id:
            return None
        if slot.get("filled") is not True or self._filled_order_base(level_body) is None:
            return None
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        fresh = self._tick_fresh(pair)
        if not self._lists_ready(fresh) or not self._okx_window_open(fresh, self._buy_journal_ts(buy_id)):
            return None
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        if self._id_still_live(fresh, connector, buy_id) or self._id_still_live(fresh, connector, close_id):
            return None
        row = partial_find_text((fresh or {}).get("finished"), close_id)
        if not isinstance(row, dict) or str(row.get("text") or "") != close_id:
            return None
        fill_size = partial_decimal(row.get("fill_size"))
        if str(row.get("state") or "") != "filled" or fill_size is None or fill_size <= 0:
            return None
        return {"level": level_body, "row": row, "buy_id": buy_id, "close_id": close_id}

    def _buy_journal_ts(self, order_id: str) -> Any:
        rows = self._partial_rows()
        if rows is None:
            return None
        return next(
            (row.get("ts") for row in rows if str(row.get("order_id") or "") == str(order_id) and row.get("ts") is not None),
            None,
        )

    def _ids_from_saved_lists(self, parsed: Any) -> Optional[set]:
        """Номера из записи этой сетки. None — запись не разобрана."""
        if not isinstance(parsed, dict):
            return None
        ids = set()
        ids.update(self._ids_of(parsed.get("holds")))
        ids.update(self._ids_of(parsed.get("close_holds")))
        ids.update(self._ids_of(parsed.get("retained_orders")))
        for ban in parsed.get("close_bans") or []:
            if isinstance(ban, dict) and ban.get("order_id"):
                ids.add(str(ban["order_id"]))
            elif isinstance(ban, str) and ban:
                ids.add(ban)
        for level in parsed.get("levels") or []:
            if not isinstance(level, dict):
                continue
            for key in ("open_order", "close_order"):
                slot = level.get(key) or {}
                if isinstance(slot, dict) and slot.get("order_id"):
                    ids.add(str(slot["order_id"]))
        top = parsed.get("close_order") or {}
        if isinstance(top, dict) and top.get("order_id"):
            ids.add(str(top["order_id"]))
        return ids

    def _empty_grid_ids(
        self, executor_id: str, executor: Any, connector: Any, fresh: Any, parsed: Any,
    ) -> Optional[set]:
        """Свои номера. None — запись не прочитана, это не пустая сетка."""
        if order_miss_unreadable():
            return None
        rows = self._partial_rows()
        saved = self._ids_from_saved_lists(parsed)
        if rows is None or saved is None:
            return None
        if not self._lists_ready(fresh):
            return None
        inflight = getattr(connector, "in_flight_orders", None)
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None)
        if not isinstance(inflight, dict) or not isinstance(lost, dict):
            return None
        if getattr(executor, "grid_levels", None) is None:
            return None
        ids = {
            str(row.get("order_id"))
            for row in rows
            if str(row.get("executor_id") or "") == str(executor_id) and row.get("order_id")
        }
        ids.update(saved)
        ids.update(self._ids_of(getattr(executor, "_close_holds", []) or []))
        close = getattr(executor, "_close_order", None)
        close_id = str(getattr(close, "order_id", "") or "")
        if close_id:
            ids.add(close_id)
        for ban in getattr(executor, "_close_ban_ids", None) or []:
            if ban:
                ids.add(str(ban))
        for level in executor.grid_levels or []:
            for slot in (
                getattr(level, "active_open_order", None),
                getattr(level, "active_close_order", None),
            ):
                order_id = str(getattr(slot, "order_id", "") or "")
                if order_id:
                    ids.add(order_id)
        if any(order_send_unknown(text) for text in ids):
            return None
        return ids

    def _empty_ids_live(self, ids: set, fresh: Any, connector: Any) -> bool:
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        for text in ids:
            if text in inflight or text in lost or self._shutdown_text_present((fresh or {}).get("opens"), text):
                return True
        return False

    def _standard_stop_finished(self, executor: Any) -> bool:
        """Штатный stop() уже поставил закончено. Наша запись статуса этим не является."""
        closed = getattr(executor, "is_closed", False)
        return bool(closed() if callable(closed) else closed)

    def _dust_rows_below_step(self, executor: Any, connector: Any, pair: str) -> bool:
        """Каждая своя строка строго меньше шага. Непрочитанное — не крошка."""
        slot = getattr(executor, "_flat_position_tick", None)
        if not isinstance(slot, dict):
            return False
        body = slot.get("body")
        symbol = str(slot.get("symbol") or "")
        if (
            not symbol
            or not isinstance(body, dict)
            or str(body.get("code")) != "0"
            or not isinstance(body.get("data"), list)
        ):
            return False
        rules = getattr(connector, "trading_rules", None) or {}
        rule = rules.get(pair) if isinstance(rules, dict) else None
        try:
            step = Decimal(str(getattr(rule, "min_base_amount_increment", None)))
        except Exception:
            return False
        if step <= 0:
            return False
        saw_own = False
        for row in body["data"]:
            if not isinstance(row, dict):
                return False
            inst = str(row.get("instId") or "")
            if inst and inst != symbol:
                continue
            if "pos" not in row:
                return False
            try:
                size = abs(Decimal(str(row.get("pos"))))
            except Exception:
                return False
            saw_own = True
            if size == 0:
                continue
            base = self._contract_base(connector, pair, size)
            if base is None or base >= step:
                return False
        return saw_own

    def _dust_metrics_below_min(self, executor: Any) -> bool:
        """Размер следующего тика, не сырое поле. Вызов кнопки до ожидания не считается."""
        refresh = getattr(executor, "update_grid_levels", None)
        metrics = getattr(executor, "update_position_metrics", None)
        if not callable(refresh) or not callable(metrics):
            return False
        try:
            refresh()
            metrics()
            size = Decimal(str(getattr(executor, "position_size_base", None)))
            minimum = Decimal(str(getattr(getattr(executor, "trading_rules", None), "min_order_size", None)))
        except Exception:
            return False
        if minimum <= 0:
            return False
        return size < minimum

    def _dust_number_blocks(
        self, executor_id: str, executor: Any, connector: Any, pair: str, parsed: Any,
    ) -> bool:
        """Те же свои номера, что у пустой сетки. Нет в трёх списках — ещё не смерть."""
        fresh = self._pair_book(pair)
        ids = self._empty_grid_ids(executor_id, executor, connector, fresh, parsed)
        if ids is None:
            return True
        ids = set(ids)
        ids.update(self._close_hold_ids_for(executor_id, executor))
        if any(order_send_unknown(text) for text in ids):
            return True
        if not self._lists_ready(fresh):
            return True
        inflight = getattr(connector, "in_flight_orders", None)
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None)
        if not isinstance(inflight, dict) or not isinstance(lost, dict):
            return True
        for text in ids:
            if (
                text in inflight
                or text in lost
                or self._shutdown_text_present((fresh or {}).get("opens"), text)
                or not self._proven_dead(text, fresh)
            ):
                return True
        return False

    def _dust_gate(
        self, executor_id: str, executor: Any, connector: Any, pair: str, parsed: Any,
    ) -> bool:
        if self._standard_stop_finished(executor):
            return False
        if not self._dust_rows_below_step(executor, connector, pair):
            return False
        if not self._dust_metrics_below_min(executor):
            return False
        return not self._dust_number_blocks(executor_id, executor, connector, pair, parsed)

    async def _dust_force_running(self, executor_id: str, executor: Any) -> bool:
        """Снять закрыто, пока новая сессия не прочитала работу. Законченный stop() не открываем."""
        if self._standard_stop_finished(executor):
            return False
        from database.repositories.executor_repository import ExecutorRepository
        while not self._standard_stop_finished(executor):
            try:
                async with self.db_manager.get_session_context() as session:
                    if self._standard_stop_finished(executor):
                        raise RuntimeError("stop finished before running write")
                    written = await ExecutorRepository(session).update_executor(
                        executor_id, status="RUNNING", close_type="",
                    )
                    if self._standard_stop_finished(executor):
                        if written is not None:
                            written.status = "TERMINATED"
                            written.close_type = "EARLY_STOP"
                        raise RuntimeError("stop finished before running commit")
                async with self.db_manager.get_session_context() as session:
                    seen = await ExecutorRepository(session).get_executor_by_id(executor_id)
            except Exception:
                if self._standard_stop_finished(executor):
                    return False
                continue
            if seen is not None and str(getattr(seen, "status", None) or "") == "RUNNING":
                if self._standard_stop_finished(executor):
                    return False
                return True
        return False

    async def _commit_dust_below_step(
        self, executor_id: str, executor: Any, connector: Any, pair: str, parsed: Any,
    ) -> bool:
        """Крошка пишет закрыто только после проверки и до коммита. Номер не чеканит."""
        if self._standard_stop_finished(executor) or not self.db_manager:
            return False
        if not self._dust_gate(executor_id, executor, connector, pair, parsed):
            return False
        from database.repositories.executor_repository import ExecutorRepository
        try:
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)
                current = await repo.get_executor_by_id(executor_id)
                if current is None or str(getattr(current, "close_type", None) or "") == "FAILED":
                    return False
                from utils.executor_checkpoint import parse_checkpoint
                saved = parse_checkpoint(getattr(current, "checkpoint", None))
                if not self._dust_gate(executor_id, executor, connector, pair, saved):
                    return False
                written = await repo.update_executor(
                    executor_id, status="TERMINATED", close_type="EARLY_STOP",
                )
                again = await repo.get_executor_by_id(executor_id)
                saved = parse_checkpoint(getattr(again, "checkpoint", None))
                if written is None or not self._dust_gate(executor_id, executor, connector, pair, saved):
                    if written is not None and not self._standard_stop_finished(executor):
                        written.status = "RUNNING"
                        written.close_type = ""
                    return False
        except Exception:
            if not self._standard_stop_finished(executor):
                await self._dust_force_running(executor_id, executor)
            return False
        try:
            async with self.db_manager.get_session_context() as session:
                seen = await ExecutorRepository(session).get_executor_by_id(executor_id)
        except Exception:
            seen = None
        from utils.executor_checkpoint import parse_checkpoint
        saved = parse_checkpoint(getattr(seen, "checkpoint", None)) if seen is not None else None
        if (
            seen is None
            or str(getattr(seen, "status", None) or "") != "TERMINATED"
            or str(getattr(seen, "close_type", None) or "") == "FAILED"
            or not self._dust_gate(executor_id, executor, connector, pair, saved)
        ):
            if not self._standard_stop_finished(executor):
                await self._dust_force_running(executor_id, executor)
            return False
        return True

    async def _close_empty_grid_if_proven(self, executor_id: str, executor: Any) -> bool:
        """Пустая сетка пишет закрыто. Непрочитанное и живой номер — нет. stop() не зовём."""
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        if not self._stop_mark_now(executor_id):
            self._log_silent_exit(
                "empty-grid", pair, "stop mark not seen",
                executor_id, interval=0.0, label="silent stop exit:", key=executor_id,
            )
            return False
        if not self.db_manager:
            self._log_silent_exit(
                "empty-grid", pair, "no database",
                executor_id, interval=0.0, label="silent stop exit:", key=executor_id,
            )
            return False
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        from database.repositories.executor_repository import ExecutorRepository
        try:
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)
                current = await repo.get_executor_by_id(executor_id)
                if current is None or str(getattr(current, "close_type", None) or "") == "FAILED":
                    self._log_silent_exit(
                        "empty-grid", pair, "row missing or failed",
                        executor_id, interval=0.0, label="silent stop exit:", key=executor_id,
                    )
                    return False
                parsed = parse_checkpoint(getattr(current, "checkpoint", None))
                if await self._reread_executor_position(executor) is not True:
                    if await self._commit_dust_below_step(executor_id, executor, connector, pair, parsed):
                        return True
                    self._log_silent_exit(
                        "empty-grid", pair, "position not flat",
                        executor_id, interval=0.0, label="silent stop exit:", key=executor_id,
                    )
                    return False
                book = self._pair_book(pair)
                if not self._lists_ready(book):
                    self._log_silent_exit(
                        "empty-grid", pair, "book not read",
                        executor_id, interval=0.0, label="silent stop exit:", key=executor_id,
                    )
                    return False
                current = await repo.get_executor_by_id(executor_id)
                parsed = parse_checkpoint(getattr(current, "checkpoint", None))
                ids = self._empty_grid_ids(executor_id, executor, connector, book, parsed)
                if ids is None or ids or self._empty_ids_live(ids or set(), book, connector):
                    self._log_silent_exit(
                        "empty-grid", pair, "own number or unread lists",
                        executor_id, interval=0.0, label="silent stop exit:", key=executor_id,
                    )
                    return False
                written = await repo.update_executor(
                    executor_id, status="TERMINATED", close_type="EARLY_STOP",
                )
                if written is None:
                    self._log_silent_exit(
                        "empty-grid", pair, "write did not land",
                        executor_id, interval=0.0, label="silent stop exit:", key=executor_id,
                    )
                    return False
                again_row = await repo.get_executor_by_id(executor_id)
                again = self._empty_grid_ids(
                    executor_id, executor, connector, book,
                    parse_checkpoint(getattr(again_row, "checkpoint", None)),
                )
                if again is None or again or self._empty_ids_live(again, book, connector):
                    if not self._standard_stop_finished(executor):
                        written.status = "RUNNING"
                        written.close_type = ""
                    if self._standard_stop_finished(executor):
                        written.status = "TERMINATED"
                        written.close_type = "EARLY_STOP"
                    self._log_silent_exit(
                        "empty-grid", pair, "reread not empty",
                        executor_id, interval=0.0, label="silent stop exit:", key=executor_id,
                    )
                    return False
        except Exception:
            self._log_silent_exit(
                "empty-grid", pair, "read failed",
                executor_id, interval=0.0, label="silent stop exit:", key=executor_id,
            )
            if not self._standard_stop_finished(executor):
                await self._dust_force_running(executor_id, executor)
            return False
        try:
            async with self.db_manager.get_session_context() as session:
                seen = await ExecutorRepository(session).get_executor_by_id(executor_id)
        except Exception:
            self._log_silent_exit(
                "empty-grid", pair, "confirm read failed",
                executor_id, interval=0.0, label="silent stop exit:", key=executor_id,
            )
            if not self._standard_stop_finished(executor):
                await self._dust_force_running(executor_id, executor)
            return False
        if seen is None or str(getattr(seen, "status", None) or "") != "TERMINATED":
            self._log_silent_exit(
                "empty-grid", pair, "confirm not closed",
                executor_id, interval=0.0, label="silent stop exit:", key=executor_id,
            )
            if not self._standard_stop_finished(executor):
                await self._dust_force_running(executor_id, executor)
            return False
        seen_ids = self._empty_grid_ids(
            executor_id, executor, connector, self._pair_book(pair),
            parse_checkpoint(getattr(seen, "checkpoint", None)),
        )
        if seen_ids is None or seen_ids or self._empty_ids_live(seen_ids or set(), self._pair_book(pair), connector):
            if not self._standard_stop_finished(executor):
                await self._dust_force_running(executor_id, executor)
            return False
        if str(getattr(seen, "close_type", None) or "") == "FAILED":
            self._log_silent_exit(
                "empty-grid", pair, "confirm failed",
                executor_id, interval=0.0, label="silent stop exit:", key=executor_id,
            )
            return False
        return True

    def _own_unproven_sends(self, executor_id: str) -> list:
        """Свои отметки отправки без доказательства смерти. Чужой текст не здесь."""
        rows = self._partial_rows()
        if not rows:
            return []
        found = []
        for row in rows:
            if str(row.get("executor_id") or "") != str(executor_id):
                continue
            order_id = str(row.get("order_id") or "")
            if not order_id or order_id in found or not partial_send_marked(rows, order_id):
                continue
            if self._proven_dead(order_id, self._tick_fresh(
                str(row.get("trading_pair") or "")
            )):
                continue
            found.append(order_id)
        return found

    def _seat_unanswered_before_close(self, executor_id: str, executor: Any) -> None:
        """Пустой слот получает свой id. Занятый другим id не заменяем."""
        rows = self._partial_rows() or []
        for order_id in self._own_unproven_sends(executor_id):
            level_id = self._own_level_id(executor_id, order_id)
            if not level_id or self._slot_has_other(executor, level_id, False, order_id):
                continue
            if order_id in self._seated_ids(executor):
                continue
            self._seat_not_filled(executor, level_id, order_id, False)

    def _seat_not_filled(self, executor: Any, level_id: str, order_id: str, close: bool) -> None:
        from hummingbot.strategy_v2.models.executors import TrackedOrder
        if not close:
            pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
            side = getattr(getattr(getattr(executor, "config", None), "side", None), "name", "")
            if side in ("BUY", "SELL") and self._shown_fill_row(self._tick_fresh(pair), order_id, side) is not None:
                return
        for level in list(getattr(executor, "grid_levels", None) or []):
            if str(getattr(level, "id", "") or "") != str(level_id):
                continue
            tracked = TrackedOrder(order_id=str(order_id))
            if close:
                level.active_close_order = tracked
            else:
                level.active_open_order = tracked

    def _restore_live_reserve(self, connector: Any, pair: str, prices: List[Any]) -> None:
        fresh = self._tick_fresh(pair)
        released = self._released_texts(pair)
        reserves = self.__dict__.setdefault("_quant_reserves", {})
        connector_key = str(id(connector))
        for price in dict.fromkeys(str(item) for item in prices if item not in (None, "")):
            if self._price_kept_by_other(pair, fresh, price, released):
                claim_quant(reserves, connector_key, pair, price)

    async def _hand_stock_picture(self, executor_id: str, executor: Any, connector: Any, pair: str) -> str:
        """Seat recorded ids. Release only a filled pair. Do not clear a live id."""
        parsed = self._partial_parsed(executor_id)
        fresh = self._tick_fresh(pair)
        witness = ""
        if not self._lists_ready(fresh):
            return ""
        leverage = getattr(getattr(executor, "config", None), "leverage", None) or 1
        side = getattr(getattr(getattr(executor, "config", None), "side", None), "name", "BUY")
        close_side = "SELL" if side == "BUY" else "BUY" if side == "SELL" else ""
        for level_body in (parsed or {}).get("levels") or []:
            if not isinstance(level_body, dict):
                continue
            level_id = str(level_body.get("id") or "")
            ready = self._filled_pair_ready(executor_id, executor, level_id)
            slot = level_body.get("open_order") or {}
            close = level_body.get("close_order") or {}
            buy_id = str(slot.get("order_id") or "")
            close_id = str(close.get("order_id") or "")
            if ready is None:
                if buy_id and self._id_still_live(fresh, connector, buy_id):
                    self._seat_not_filled(executor, level_id, buy_id, False)
                if close_id and self._id_still_live(fresh, connector, close_id):
                    self._seat_not_filled(executor, level_id, close_id, True)
                continue
            if not close_side:
                continue
            order = slot.get("order") if isinstance(slot.get("order"), dict) else {}
            buy_price = partial_decimal(order.get("price"))
            close_price = partial_decimal(close.get("price"))
            buy_base = self._filled_order_base(level_body)
            fill_size = partial_decimal((ready.get("row") or {}).get("fill_size"))
            if buy_price is None or close_price is None or buy_base is None or fill_size is None:
                continue
            opened = self._stock_filled_order(
                client_id=buy_id,
                trading_pair=pair,
                trade_name=side,
                position_name="OPEN",
                amount=buy_base,
                price=buy_price,
                leverage=int(leverage),
            )
            closed = self._stock_filled_order(
                client_id=close_id,
                trading_pair=pair,
                trade_name=close_side,
                position_name="CLOSE",
                amount=fill_size,
                price=close_price,
                leverage=int(leverage),
            )
            if opened is None or closed is None:
                continue
            target = next(
                (level for level in list(getattr(executor, "grid_levels", None) or []) if str(getattr(level, "id", "") or "") == level_id),
                None,
            )
            if target is None:
                continue
            from hummingbot.strategy_v2.models.executors import TrackedOrder
            target.active_open_order = TrackedOrder(order_id=buy_id)
            target.active_open_order.order = opened
            target.active_close_order = TrackedOrder(order_id=close_id)
            target.active_close_order.order = closed
            if connector is None:
                continue
            prices = [buy_price, close_price, level_body.get("take_profit_price")]
            if await self._partial_release(executor_id, buy_id, level_id, "stock_flat", connector, pair):
                self._restore_live_reserve(connector, pair, prices)
                witness = close_id
        if not witness:
            for level_body in (self._partial_parsed(executor_id) or {}).get("levels") or []:
                slot = (level_body or {}).get("open_order") or {}
                if slot.get("order_id"):
                    return str(slot.get("order_id"))
        return witness

    def _install_partial_guards(self, executor_id: str, executor: Any) -> None:
        if getattr(executor, "_partial_guarded", False):
            return
        service = self

        def _wrap_stop(name: str) -> None:
            original = getattr(executor, name, None)
            if original is None or getattr(original, "_partial_wrapped", False):
                return

            def wrapped(*args, **kwargs):
                # Счётчик повторов при доказанном гашении ставит FAILED. Это не конец.
                if (
                    name == "evaluate_max_retries"
                    and service._stop_mark_now(executor_id)
                    and service._shutdown_close_proven(executor_id)
                ):
                    return None
                if (
                    name == "evaluate_max_retries"
                    and service._stop_mark_now(executor_id)
                    and service._executor_position_flat(executor) is True
                ):
                    return None
                if (
                    name == "evaluate_max_retries"
                    and service._stop_mark_now(executor_id)
                    and recorded_close_holds_retry(executor, executor_id)
                ):
                    return None
                if service._stop_mark_now(executor_id):
                    return original(*args, **kwargs)
                parsed = service._partial_parsed(executor_id)
                connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
                pair = getattr(getattr(executor, "config", None), "trading_pair", None)
                connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
                live = connector is not None and service._partial_sell_is_live(executor_id, connector, parsed, pair or "")
                if live or service._partial_forbids(executor_id):
                    service._partial_keep_running(executor)
                    return None
                return original(*args, **kwargs)

            wrapped._partial_wrapped = True
            setattr(executor, name, wrapped)

        brake_names = set()
        if getattr(executor, "_close_brake", False):
            brake_names.update({"stop", "place_close_order_and_cancel_open_orders"})
        for name in ("stop", "early_stop", "evaluate_max_retries", "place_close_order_and_cancel_open_orders"):
            if name in brake_names:
                continue
            _wrap_stop(name)
        original_on_start = getattr(executor, "on_start", None)
        if original_on_start is not None:

            async def wrapped_on_start(*args, **kwargs):
                await original_on_start(*args, **kwargs)
                if service._partial_forbids(executor_id):
                    service._partial_keep_running(executor)

            executor.on_start = wrapped_on_start
        original_balance = getattr(executor, "validate_sufficient_balance", None)
        if original_balance is not None:

            async def wrapped_balance(*args, **kwargs):
                if partial_exposure(service._partial_rows(), service._partial_parsed(executor_id), executor_id):
                    return None
                return await original_balance(*args, **kwargs)

            executor.validate_sufficient_balance = wrapped_balance
        original_task = getattr(executor, "control_task", None)
        if original_task is not None:

            async def wrapped_task(*args, **kwargs):
                connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
                connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
                pair = getattr(getattr(executor, "config", None), "trading_pair", None)
                if connector is not None and pair:
                    await service._store_tick_fresh(connector, pair)
                frozen = await service._refresh_stop_mark(executor_id)
                # Доставка — только от ОПУБЛИКОВАННОЙ метки (п.1): «непрочитано»
                # держит отправку, но не запускает отмены без заморозки.
                fresh_now = service._tick_fresh(pair or "")
                await service._note_shown_buys(executor_id, fresh_now)
                service._seat_shown_buys(executor_id, executor, fresh_now)
                if service._stop_mark_now(executor_id) and connector is not None:
                    # Доставка отмен не за готовностью списков и не за судьбой
                    # номеров (план stop-freeze п.3).
                    await service._stop_deliver_cancels(executor_id, executor, connector, pair or "")
                    await service._refresh_retained_archive(
                        executor_id, executor, connector, pair or "",
                    )
                    # Конец, возврат в работу и продажа остатка — не решения
                    # обёртки шага (план stop-wrapper-three-calls).
                fresh = service._tick_fresh(pair or "")
                executor._tick_opens = list((fresh or {}).get("opens") or [])
                executor._tick_finished = list((fresh or {}).get("finished") or [])
                executor._tick_blank_fills = list((fresh or {}).get("finished_blank_px") or [])
                if service._stop_mark_now(executor_id) and (
                    not service._lists_ready(fresh) or service._partial_rows() is None
                ):
                    return await original_task(*args, **kwargs)
                if service._restart_seat_due(executor_id, executor):
                    if not service._finish_restart_seat(executor_id, executor, fresh):
                        return None
                elif not service._lists_ready(fresh):
                    if service._partial_forbids(executor_id) and not service._stop_mark_now(executor_id):
                        service._partial_keep_running(executor)
                        service._log_silent_exit(
                            "task-partial-forbids", pair or "",
                            f"control task not called{service._partial_hold_witness(executor_id)}", executor_id,
                            interval=300.0, label="silent stop exit:", key=executor_id,
                        )
                    return None
                was_forbidden = service._partial_forbids(executor_id)
                if was_forbidden and not service._stop_mark_now(executor_id):
                    service._partial_keep_running(executor)
                    service.__dict__.setdefault("_stock_tick_forbid", {})[executor_id] = True
                    service.__dict__.setdefault("_stock_tick_ids", {})[executor_id] = set(service._partial_close_ids(executor_id))
                if frozen:
                    witness = ""
                else:
                    await service._partial_tick(executor_id, executor)
                    witness = await service._hand_stock_picture(executor_id, executor, connector, pair or "")
                fresh = service._tick_fresh(pair or "")
                service._seat_visible_own(executor_id, executor, connector, fresh)
                if service._partial_rows() is None:
                    return None
                filled_down = service._seat_filled_while_down(executor_id, executor, connector, fresh)
                service.__dict__.setdefault("_filled_down_blocked", {})[executor_id] = filled_down
                stuck = service._unseated_level_ids(executor_id, executor, connector, fresh)
                service.__dict__.setdefault("_unseated_levels", {})[executor_id] = stuck
                await service._rescue_seat_filled_close(executor_id, executor, connector, pair or "")
                aside = service.__dict__.setdefault("_stepped_aside_ids", set())
                aside.add(str(executor_id))
                service._log_silent_exit(
                    "stock-step-called", pair or "",
                    step_close_witness(bool(frozen), str(witness or "")),
                    interval=300.0, label="silent stop exit:", key=executor_id,
                )
                try:
                    return await original_task(*args, **kwargs)
                finally:
                    aside.discard(str(executor_id))
                    (service.__dict__.get("_unseated_levels") or {}).pop(executor_id, None)
                    (service.__dict__.get("_filled_down_blocked") or {}).pop(executor_id, None)
                    (service.__dict__.get("_stock_tick_forbid") or {}).pop(executor_id, None)
                    (service.__dict__.get("_stock_tick_ids") or {}).pop(executor_id, None)

            executor.control_task = wrapped_task
        original_close_get = getattr(executor, "get_close_orders_to_create", None)
        if original_close_get is not None and not getattr(executor, "_rescue_close_wrapped", False):
            def rescue_close_get(*args, **kwargs):
                found = list(original_close_get(*args, **kwargs) or [])
                if not found:
                    return found
                pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
                fresh = service._tick_fresh(pair)
                if not service._lists_ready(fresh):
                    return found
                blocked = set()
                for level in found:
                    level_id = str(getattr(level, "id", "") or "")
                    target = next((
                        item for item in list(getattr(executor, "grid_levels", None) or [])
                        if str(getattr(item, "id", "") or "") == level_id
                    ), None)
                    if target is None:
                        continue
                    open_slot = getattr(target, "active_open_order", None)
                    if open_slot is None or not getattr(open_slot, "is_filled", False):
                        continue
                    if service._rescue_close_blocked(executor_id, level_id, fresh, executor):
                        blocked.add(level_id)
                if not blocked:
                    return found
                return [level for level in found if str(getattr(level, "id", "") or "") not in blocked]
            executor.get_close_orders_to_create = rescue_close_get
            executor._rescue_close_wrapped = True
        original_shutdown = getattr(executor, "control_shutdown_process", None)
        if original_shutdown is not None and not getattr(executor, "_close_brake", False):

            async def wrapped_shutdown(*args, **kwargs):
                service._drop_phantom_close_liquidity(executor_id, executor)
                marked = service._stop_mark_now(executor_id)
                if marked:
                    await service._feed_shutdown_stock_truth(executor_id, executor)
                    return await original_shutdown(*args, **kwargs)
                if service._live_delivery_reads_zero_liquidity(executor_id, executor):
                    return None
                if service._partial_forbids(executor_id):
                    service._log_silent_exit(
                        "stop-partial-forbids", "", "shutdown not called", executor_id,
                        interval=300.0, label="silent stop exit:", key=executor_id,
                    )
                    return None
                return await original_shutdown(*args, **kwargs)

            executor.control_shutdown_process = wrapped_shutdown
        original_cancel_orders = getattr(executor, "cancel_open_orders", None)
        if original_cancel_orders is not None:

            def wrapped_cancel_orders(*args, **kwargs):
                if service._stop_mark_now(executor_id):
                    return original_cancel_orders(*args, **kwargs)
                stuck = (service.__dict__.get("_unseated_levels") or {}).get(executor_id) or set()
                if service._stepped_aside(executor_id) and not stuck:
                    return original_cancel_orders(*args, **kwargs)
                if service._partial_forbids(executor_id):
                    return None
                return original_cancel_orders(*args, **kwargs)

            executor.cancel_open_orders = wrapped_cancel_orders
        original_ids = getattr(executor, "get_close_order_ids_to_cancel", None)
        if original_ids is not None:

            def wrapped_ids(*args, **kwargs):
                found = list(original_ids(*args, **kwargs) or [])
                if service._stop_mark_now(executor_id):
                    return found
                if service._stepped_aside(executor_id):
                    protected = set()
                    pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
                    fresh = service._tick_fresh(pair)
                    connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
                    connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
                    rows = service._partial_rows() or []
                    for level in list(getattr(executor, "grid_levels", None) or []):
                        for order_id in service._unanswered_send_ids(
                            executor_id, level, rows, fresh, connector,
                        ):
                            protected.add(order_id)
                    stuck = (service.__dict__.get("_unseated_levels") or {}).get(executor_id) or set()
                    for order_id in service._own_ids(executor_id):
                        if service._own_level_id(executor_id, order_id) in stuck:
                            protected.add(order_id)
                    return [order_id for order_id in found if str(order_id) not in protected]
                if not service._partial_forbids(executor_id):
                    return found
                banned = service._partial_close_ids(executor_id)
                banned.update((service.__dict__.get("_stock_tick_ids") or {}).get(executor_id) or set())
                return [order_id for order_id in found if str(order_id) not in banned]

            executor.get_close_order_ids_to_cancel = wrapped_ids
        original_opens = getattr(executor, "get_open_orders_to_create", None)
        if original_opens is not None:

            def wrapped_opens(*args, **kwargs):
                if service._stop_mark_now(executor_id):
                    return []
                rows = service._partial_rows()
                parsed = service._partial_parsed(executor_id)
                held = partial_held_level_ids(rows, parsed, executor_id)
                if "*" in held:
                    return []
                pair = getattr(getattr(executor, "config", None), "trading_pair", None)
                fresh = service._tick_fresh(pair or "")
                if not service._lists_ready(fresh):
                    service._log_silent_exit(
                        "open-orders-wrapper", pair or "", service._silent_exit_flags(fresh), executor_id
                    )
                    return []
                released = service._partial_released_ids_for_pair(pair or "")
                service._partial_drop_released_pending(rows or [], released)
                parsed = service._parsed_without_dead(parsed, fresh)
                feed_rows = [
                    row for row in (service._partial_rows_with_owner_take(rows or []) or [])
                    if not service._proven_dead(str(row.get("order_id") or ""), fresh)
                ]
                prices = list(service._feed_prices(feed_rows, parsed, fresh))
                prices.extend(service._pending_ticks_live())
                prices.extend(service._ban_pairs(pair, fresh))
                connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
                connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
                pair = getattr(getattr(executor, "config", None), "trading_pair", None)
                rules = getattr(connector, "trading_rules", None) or {}
                tick = getattr(rules.get(pair) if isinstance(rules, dict) else None, "min_price_increment", None)
                prices = service._prices_for_blocks(prices, fresh)
                offered = list(original_opens(*args, **kwargs) or [])
                service._log_silent_exit(
                    "wrapper-tick", pair or "", f"levels={len(offered)}", executor_id, interval=300.0
                )
                proposed = []
                for level in offered:
                    level_id = str(getattr(level, "id", "") or "")
                    filled_down = (service.__dict__.get("_filled_down_blocked") or {}).get(executor_id) or set()
                    if level_id in filled_down:
                        continue
                    stuck = (service.__dict__.get("_unseated_levels") or {}).get(executor_id) or set()
                    if level_id in stuck:
                        service._log_silent_exit(
                            "wrapper-level-held", pair or "", f"level_id={level_id}", executor_id
                        )
                        continue
                    pending = None
                    if service._stepped_aside(executor_id):
                        pending_ids = service._unanswered_send_ids(
                            executor_id, level, rows, fresh, connector,
                        )
                        pending = pending_ids[0] if len(pending_ids) == 1 else None
                    visible_here = service._level_visible_own(executor_id, level, connector, fresh)
                    if visible_here:
                        continue
                    waiting = service._own_unproven_sends(executor_id)
                    seated = service._seated_ids(executor)
                    if any(
                        service._own_level_id(executor_id, item) == level_id and item not in seated
                        for item in waiting
                    ):
                        service._log_silent_exit(
                            "wrapper-level-held", pair or "", f"level_id={level_id}", executor_id
                        )
                        continue
                    if level_id in held and not pending:
                        service._log_silent_exit(
                            "wrapper-level-held", pair or "", f"level_id={level_id}", executor_id
                        )
                        continue
                    if connector is not None and pair and hasattr(connector, "quantize_order_price"):
                        placement = str(connector.quantize_order_price(pair, getattr(level, "price", None)))
                        if pending and tick is None:
                            if service._placement_hold_prices(fresh, connector, placement, pending, rows):
                                continue
                        # Одну цену входа нескольким уровням ставит штатная сетка.
                        # На живом шаге эти уровни не удерживаем.
                    proposed.append(level)
                return proposed

            executor.get_open_orders_to_create = wrapped_opens
        original_close = getattr(executor, "adjust_and_place_close_order", None)
        if original_close is not None and not getattr(executor, "_close_brake", False):

            def wrapped_close(level, *args, **kwargs):
                level_id = str(getattr(level, "id", "") or "")
                opened = getattr(getattr(level, "active_open_order", None), "order", None)
                if service._filled_down_open(executor_id, executor, level) or getattr(opened, "is_filled", False):
                    return original_close(level, *args, **kwargs)
                if service._filled_pair_ready(executor_id, executor, level_id) is not None:
                    return None
                if level_id in partial_held_level_ids(
                    service._partial_rows(), service._partial_parsed(executor_id), executor_id
                ):
                    return None
                task = asyncio.get_event_loop().create_task(
                    service._partial_neighbor_close(executor_id, executor, level)
                )
                service.__dict__.setdefault("_accept_tasks", set()).add(task)
                task.add_done_callback(service.__dict__["_accept_tasks"].discard)
                return None

            executor.adjust_and_place_close_order = wrapped_close
        original_canceled = getattr(executor, "process_order_canceled_event", None)
        if original_canceled is not None:

            def wrapped_canceled(*args, **kwargs):
                if service._partial_forbids(executor_id):
                    return None
                return original_canceled(*args, **kwargs)

            executor.process_order_canceled_event = wrapped_canceled
        original_loop = getattr(executor, "control_loop", None)
        if original_loop is not None and not getattr(executor, "_close_brake", False):

            async def wrapped_loop(*args, **kwargs):
                try:
                    return await original_loop(*args, **kwargs)
                except Exception:
                    if service._partial_forbids(executor_id):
                        service._partial_keep_running(executor)
                        return None
                    raise

            executor.control_loop = wrapped_loop
        strategy = getattr(executor, "_strategy", None)
        original_strategy_cancel = getattr(strategy, "cancel", None)
        if original_strategy_cancel is not None and not getattr(original_strategy_cancel, "_union_cancel", False):
            stock_cancel = getattr(strategy, "_stock_cancel", None) or original_strategy_cancel

            def wrapped_strategy_cancel(*args, **kwargs):
                order_id = kwargs.get("order_id")
                if order_id is None and args:
                    order_id = args[-1]
                if service._cancel_blocked(str(order_id or "")):
                    return None
                return stock_cancel(*args, **kwargs)

            wrapped_strategy_cancel._union_cancel = True
            wrapped_strategy_cancel._partial_wrapped = True
            strategy._stock_cancel = stock_cancel
            strategy.cancel = wrapped_strategy_cancel
        executor._partial_owns_level = lambda level: service._partial_level_needs_take_profit(
            executor_id, executor, level,
        )
        # Правило номера, п.6: своя продажа уровня уже вычеканена (журнал или
        # чекпоинт) — второй номер не чеканить, слать тот же или стоять.
        executor._refuse_second_close = lambda level: bool(
            service._level_close_recorded(executor_id, level)
        )
        executor._refuse_close_price = lambda price, own=None: service._close_quant_held(
            executor_id, executor, price, own,
        )
        executor._refuse_market_close = lambda: bool(
            service._occupied_close_prices(executor, include_opens=False)
        ) or service._unpriced_close_blocks(executor)
        executor._refuse_unpriced_close = lambda: service._unpriced_close_blocks(executor)

        async def _refuse_frozen_close(order_id=None) -> bool:
            """Метка держит новую заявку. Уже зафиксированный номер гашения — одна продажа."""
            allowed = getattr(executor, "_shutdown_close_id", None)
            if (
                order_id
                and allowed
                and str(order_id) == str(allowed)
                and getattr(executor, "_shutdown_close_seated", False)
            ):
                return False
            return await service._refresh_stop_mark(executor_id)

        executor._refuse_frozen_close = _refuse_frozen_close
        executor._partial_guarded = True

    def _pair_tick(self, executor: Any):
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        pair = getattr(getattr(executor, "config", None), "trading_pair", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        rules = getattr(connector, "trading_rules", None) or {}
        rule = rules.get(pair) if isinstance(rules, dict) else None
        return getattr(rule, "min_price_increment", None)

    def _price_same(self, left: Any, right: Any, tick: Any) -> bool:
        if left in (None, "") or right in (None, ""):
            return False
        if tick in (None, ""):
            return str(left) == str(right)
        same = _same_tick(left, right, tick)
        return same is True

    def _live_open_prices(self, executor: Any) -> list:
        """Живые цены книги и трекера, которые держат (правило номера): наш
        живой номер (включая номер только из трекера) и голая цена без номера.
        Чужой текст с номером — не держит."""
        found = []
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        fresh = self._tick_fresh(pair)
        tracker_texts = set(getattr(connector, "in_flight_orders", None) or {}) | set(
            getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        )
        for text, order in (getattr(connector, "in_flight_orders", None) or {}).items():
            price = getattr(order, "price", None)
            if price not in (None, "") and self._text_holds(text, fresh, tracker_texts):
                found.append(price)
        for raw in getattr(connector, "opens", None) or []:
            if isinstance(raw, dict) and raw.get("price") not in (None, ""):
                if self._text_holds(raw.get("text") or raw.get("order_id"), fresh, tracker_texts):
                    found.append(raw.get("price"))
        return found

    def _remember_ban_prices(self, executor: Any, order_ids, price_map) -> None:
        stored = dict(getattr(executor, "_close_ban_prices", None) or {})
        for order_id in order_ids or []:
            if order_is_unsent(str(order_id)):
                continue
            price = (price_map or {}).get(str(order_id))
            if price in (None, ""):
                price = (price_map or {}).get(order_id)
            if price not in (None, ""):
                stored[str(order_id)] = price
        if stored:
            executor._close_ban_prices = stored

    def _occupied_close_prices(self, executor: Any, ignore_id: str = "", include_opens: bool = True) -> list:
        """Prices already taken by a close or a ban. A released id is not this list."""
        pair = getattr(getattr(executor, "config", None), "trading_pair", None)
        ignore = str(ignore_id or "")
        found = []
        rows = self._partial_rows() or []
        released = set()
        level_close_ids = set()
        for entry in self._hold_index():
            released.update(entry["price_released_ids"])
            level_close_ids.update(entry["level_close_ids"])
        fresh = self._tick_fresh(pair or "")
        for other in self._active_executors.values():
            other_pair = getattr(getattr(other, "config", None), "trading_pair", None)
            if pair and other_pair and other_pair != pair:
                continue
            ban_prices = getattr(other, "_close_ban_prices", None) or {}
            for ban_id in getattr(other, "_close_ban_ids", set()) or set():
                if str(ban_id) == ignore or str(ban_id) in released:
                    continue
                price = ban_prices.get(str(ban_id))
                if price not in (None, ""):
                    found.append(price)
            for item in getattr(other, "_close_holds", []) or []:
                if not isinstance(item, dict) or str(item.get("order_id") or "") in (ignore, ""):
                    continue
                if item.get("price") not in (None, ""):
                    found.append(item.get("price"))
            for level in getattr(other, "grid_levels", []) or []:
                close = getattr(level, "active_close_order", None)
                close_id = str(getattr(close, "order_id", "") or "")
                if not close_id or close_id == ignore or close_id in released:
                    continue
                price = getattr(close, "price", None)
                if price not in (None, ""):
                    found.append(price)
        for row in rows:
            order_id = str(row.get("order_id") or "")
            if not order_id or order_id == ignore or order_id in released:
                continue
            if row.get("role") not in ("close", "market_close", None) and row.get("price") in (None, ""):
                continue
            if not self._proven_dead(order_id, fresh):
                owned = False
                for other in self._active_executors.values():
                    bans = {str(item) for item in getattr(other, "_close_ban_ids", set()) or set()}
                    holds = {
                        str(item.get("order_id"))
                        for item in getattr(other, "_close_holds", []) or []
                        if isinstance(item, dict)
                    }
                    if order_id in bans or order_id in holds:
                        owned = True
                if owned and row.get("price") not in (None, ""):
                    found.append(row.get("price"))
            if row.get("take_profit_price") not in (None, "") and order_id not in released:
                if order_id in level_close_ids and self._take_holds(order_id, fresh):
                    found.append(row.get("take_profit_price"))
        for entry in self._hold_index():
            for close_id, price in entry["level_close_prices"]:
                if close_id != ignore and close_id not in released and not self._proven_dead(close_id, fresh):
                    found.append(price)
        if include_opens:
            found.extend(self._live_open_prices(executor))
        return found

    def _ledger_close_price_taken(
        self, price: Any, ignore_id: str = "", fresh: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Наш номер продажи на этой цене держит её, пока finished не доказал
        именно эту заявку исполненной или снятой (правило номера)."""
        try:
            from utils.executor_checkpoint import ledger_close_price_ids
            ids = ledger_close_price_ids(price)
        except Exception:
            return True
        for item in ids - {str(ignore_id or "")}:
            if not self._proven_dead(item, fresh):
                return True
        return False

    def _unpriced_close_blocks(self, executor: Any) -> bool:
        """A close id without a stored price blocks every send on this pair."""
        pair = getattr(getattr(executor, "config", None), "trading_pair", None)
        for other in self._active_executors.values():
            other_pair = getattr(getattr(other, "config", None), "trading_pair", None)
            if pair and other_pair and other_pair != pair:
                continue
            ids = {str(item) for item in getattr(other, "_close_ban_ids", set()) or set() if item}
            for item in getattr(other, "_close_holds", []) or []:
                if isinstance(item, dict) and item.get("order_id"):
                    ids.add(str(item.get("order_id")))
            close = getattr(other, "_close_order", None)
            if getattr(close, "order_id", None):
                ids.add(str(close.order_id))
            for order_id in ids:
                if self._close_price_known(order_id):
                    continue
                if self._close_left_the_book(other, order_id):
                    continue
                return True
        return False

    def _close_price_known(self, order_id: str) -> bool:
        """Journal or a checkpoint the send reads. RAM ban prices are not this."""
        try:
            known = ledger_price_known(order_id)
        except Exception:
            return False
        if known:
            return True
        wanted = str(order_id)
        for entry in self._hold_index():
            if entry["close_price_map"].get(wanted) not in (None, ""):
                return True
            if entry["close_hold_price_map"].get(wanted) not in (None, ""):
                return True
        return False

    def _close_left_the_book(self, executor: Any, order_id: str) -> bool:
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        fresh = (self.__dict__.get("_partial_fresh") or {}).get(
            getattr(getattr(executor, "config", None), "trading_pair", None)
        ) or {}
        if not fresh.get("opens_read") or not fresh.get("opens_done"):
            return False
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        if order_id in inflight or order_id in lost or partial_text_in(fresh.get("opens"), order_id):
            return False
        row = partial_find_text(fresh.get("finished"), order_id)
        if not isinstance(row, dict):
            return False
        return row.get("finish_as") in ("filled", "cancelled")

    def _close_tick_taken(self, executor: Any, price: Any, ignore_id: Optional[str] = None) -> bool:
        """Quant занят нашим живым номером (правило номера): наши структуры,
        живая книга и наш журнальный close (в т.ч. после рестарта). Чужой
        текст с номером и доказанная история не держат."""
        if price in (None, ""):
            return False
        tick = self._pair_tick(executor)
        ignore = str(ignore_id or "")
        if any(self._price_same(item, price, tick) for item in self._occupied_close_prices(executor, ignore)):
            return True
        pair = getattr(getattr(executor, "config", None), "trading_pair", None) or ""
        return self._ledger_close_price_taken(price, ignore, self._tick_fresh(pair))

    def _partial_level_needs_take_profit(self, executor_id: str, executor: Any, level: Any) -> bool:
        """Only a level this partial buy is closing. A filled open alone is not that."""
        held = partial_held_level_ids(self._partial_rows(), self._partial_parsed(executor_id), executor_id)
        level_id = str(getattr(level, "id", "") or "")
        return bool(level_id) and level_id in held

    def _partial_gone_blocked(self, executor_id: str, order_id: Optional[str]) -> bool:
        if not order_id:
            return False
        rows = self._partial_rows()
        if rows is None:
            return True
        text = str(order_id)
        owners = [executor_id] if executor_id else []
        owners.extend(self._active_executors.keys())
        for owner in owners:
            if text not in partial_holding_ids(rows, owner):
                continue
            if not partial_end_visible(self._partial_parsed(owner), text):
                return True
        return False

    def _cancel_blocked(self, order_id: str) -> bool:
        if not order_id:
            return False
        if _delivery_owns(order_id):
            return False
        if self._partial_text_protected(order_id):
            return True
        for executor in getattr(self, "_active_executors", {}).values():
            holds = {
                str(item.get("order_id"))
                for item in getattr(executor, "_close_holds", []) or []
                if isinstance(item, dict) and item.get("order_id")
            }
            bans = {str(item) for item in getattr(executor, "_close_ban_ids", set()) or set()}
            if order_id in holds or order_id in bans:
                return True
        return False

    def _partial_close_ids(self, executor_id: str) -> set:
        found = set()
        parsed = self._partial_parsed(executor_id)
        for level in (parsed or {}).get("levels") or []:
            close = (level or {}).get("close_order") or {}
            if close.get("order_id"):
                found.add(str(close.get("order_id")))
            slot = (level or {}).get("open_order") or {}
            if slot.get("order_id"):
                found.add(str(slot.get("order_id")))
            for item in level.get("partial_closes") or []:
                if isinstance(item, dict) and item.get("order_id"):
                    found.add(str(item.get("order_id")))
        rows = self._partial_rows() or []
        found.update(partial_holding_ids(rows, executor_id))
        return found

    def _executor_id_of(self, executor: Any) -> str:
        for key, value in (getattr(self, "_active_executors", None) or {}).items():
            if value is executor:
                return str(key)
        return str(getattr(getattr(executor, "config", None), "id", "") or "")

    def _stop_mark_now(self, executor_id: str) -> bool:
        """Синхронный гейт заморозки: RAM ИЛИ последняя перечитанная запись.

        Холодная RAM не значит «метки нет», если колонка её уже отдала
        (обрыв между коммитом метки и early_stop, вердикт кода
        20261012-212613-69817).
        """
        if (self.__dict__.get("_stop_mark_ram") or {}).get(str(executor_id)):
            return True
        return stop_mark_visible(self._partial_parsed(executor_id))

    async def _read_stop_mark_fresh(self, executor_id: str) -> Optional[bool]:
        """Перечитка метки НОВОЙ сессией — доказательство коммита записи.

        Возвращает None, если прочитать не удалось: непрочитанное — это НЕ
        «метки нет» (план stop-freeze п.1). Показанная метка прогревает RAM:
        любая свежая перечитка, показавшая метку, пишет её в память.
        """
        try:
            text = await self._partial_column_text(executor_id)
        except Exception:
            logger.exception("stop mark re-read failed for %s", executor_id)
            return None
        seen = stop_mark_visible(parse_checkpoint(text))
        if seen:
            self.__dict__.setdefault("_stop_mark_ram", {})[str(executor_id)] = True
        return seen

    async def _refresh_stop_mark(self, executor_id: str) -> bool:
        """Гейт: не чеканить и не слать, пока метка видна ИЛИ прочитать не удалось.

        Ошибка или нечитаемая перечитка НЕ открывает отправку (план п.2) и НЕ
        глушится в «метки нет». Гейт читает RAM ИЛИ последнюю перечитанную
        запись: холодная RAM не значит «метки нет», если колонка метку уже
        отдала. Прочитано чисто и метки ни там, ни там — сетка не заморожена.
        """
        try:
            seen = await self._read_stop_mark_fresh(executor_id)
        except Exception:
            logger.exception("stop mark re-read failed for %s", executor_id)
            seen = None
        if seen is None:
            return True
        return self._stop_mark_now(executor_id)

    async def _freeze_stop_mark(self, executor_id: str) -> bool:
        """Метка «стоп запрошен»: CAS → коммит (выход из сессии) → новая сессия."""
        key = str(executor_id)

        def mutate(body: Dict[str, Any]) -> None:
            ends = dict(body.get("partial_ends") or {})
            ends[STOP_MARK_KEY] = {"kind": "stop_requested", "order_ids": []}
            body["partial_ends"] = ends

        try:
            await self._partial_cas(executor_id, mutate, stop_mark_visible, None)
        except Exception:
            logger.exception("stop mark write failed for %s", executor_id)
        ram = self.__dict__.setdefault("_stop_mark_ram", {})
        confirmed = await self._read_stop_mark_fresh(executor_id)
        # Непрочитанное не размораживает: ложь поверх истины метку не стирает, а
        # закоммиченная CAS метка уже видна в перечитанной записи (план п.1).
        if confirmed:
            ram[key] = True
        return confirmed or self._stop_mark_now(executor_id)

    async def _partial_cas(self, executor_id: str, mutate: Any, visible: Any, target_id: Optional[str] = None) -> bool:
        if not self.db_manager:
            return False
        committed = {"done": False}
        try:
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)
                row = await repo.get_executor_by_id(executor_id)
                seen = row.checkpoint if row is not None else None
                body = partial_commit_body(seen, mutate, target_id)
                if body is None:
                    return False
                changed = await repo.replace_checkpoint_if_unchanged(executor_id, seen, body)
                if not changed:
                    return False
                fresh = await repo.get_executor_by_id(executor_id)
                stored = fresh.checkpoint if fresh is not None else None
                parsed = parse_checkpoint(stored)
                if not visible(parsed):
                    return False
                # Публикация — сразу после возврата commit и ДО session.close()
                # (план stop-freeze п.1): ошибка close не выглядит как
                # «не закоммичено» и не открывает `_create_order`.
                original_commit = getattr(session, "commit", None)

                async def commit_then_publish() -> None:
                    await original_commit()
                    self._publish_committed_body(executor_id, body)
                    committed["done"] = True

                if original_commit is not None:
                    session.commit = commit_then_publish
            if not committed["done"]:
                # Контекст без commit внутри (пробы): публикуем после выхода.
                self._publish_committed_body(executor_id, body)
                committed["done"] = True
            return True
        except Exception:
            if committed["done"]:
                return True
            logger.exception("partial compare-and-swap was not committed")
            return False

    def _publish_committed_body(self, executor_id: str, body: str) -> None:
        """Закоммиченное тело — в запись и RAM без ожиданий (план stop-freeze п.1)."""
        rows = [dict(row) for row in getattr(self, "_hold_rows", None) or []]
        for row in rows:
            if row.get("executor_id") == executor_id:
                row["checkpoint"] = body
                break
        else:
            rows.append({"executor_id": executor_id, "checkpoint": body})
        self._set_hold_rows(rows)
        if stop_mark_visible(parse_checkpoint(body)):
            self.__dict__.setdefault("_stop_mark_ram", {})[str(executor_id)] = True

    async def _partial_copy_five_before_start(self, executor_id: str, checkpoint: Optional[Dict[str, Any]], config: Dict[str, Any]) -> None:
        rows = self._partial_rows()
        if not rows:
            return
        marked = [
            row for row in rows
            if row.get("executor_id") == executor_id and row.get("order_id") and row.get("level_id")
            and (partial_send_marked(rows, str(row.get("order_id"))) or partial_old_remember(row))
        ]
        if not marked:
            return

        def mutate(body: Dict[str, Any]) -> None:
            holds = list(body.get("holds") or [])
            seen = {item.get("order_id") for item in holds if isinstance(item, dict)}
            for row in marked:
                if row.get("order_id") in seen:
                    continue
                addition = {"order_id": row.get("order_id"), "level_id": row.get("level_id"), "filled": False}
                if row.get("price") not in (None, ""):
                    addition["price"] = row.get("price")
                if partial_has_five(row):
                    for key in ("trading_pair", "order_type", "trade_type", "leverage", "position"):
                        addition[key] = row.get(key)
                holds.append(addition)
                seen.add(row.get("order_id"))
            body["holds"] = holds

        await self._partial_cas(executor_id, mutate, lambda parsed: True, None)

    def _partial_recorded_close(self, parsed: Optional[Dict[str, Any]], level_id: Any) -> bool:
        for level in (parsed or {}).get("levels") or []:
            if str(level.get("id")) != str(level_id):
                continue
            close = level.get("close_order") or {}
            if close.get("order_id"):
                return True
        return False

    def _filled_open_level(self, parsed: Optional[Dict[str, Any]], order_id: str) -> Optional[Dict[str, Any]]:
        for level in (parsed or {}).get("levels") or []:
            slot = level.get("open_order") or {}
            if str(slot.get("order_id") or "") == str(order_id) and slot.get("filled") is True:
                return level if isinstance(level, dict) else None
        return None

    def _filled_order_base(self, level: Dict[str, Any]) -> Optional[Decimal]:
        """Filled base lives inside open_order.order, not on the slot."""
        order = (level.get("open_order") or {}).get("order")
        if not isinstance(order, dict):
            return None
        base = partial_decimal(order.get("executed_amount_base"))
        if base is None or base <= 0:
            return None
        return base

    def _okx_window_open(self, fresh: Dict[str, Any], journal_ts: Any) -> bool:
        """OKX begin/end cover the journal mark. A missing mark is not cover."""
        if journal_ts is None or not fresh.get("finished_read") or not fresh.get("finished_done"):
            return False
        page = {"read": bool(fresh.get("finished_read")), "done": bool(fresh.get("finished_done"))}
        return history_covers_ts(fresh.get("finished_request"), journal_ts, page) is True

    def _okx_close_blocks(self, fresh: Dict[str, Any], order_id: str, journal_ts: Any, *, explicit: bool) -> bool:
        """A finished OKX close is not a new order. Gate finish_as is not this check."""
        if not explicit:
            return False
        row = partial_find_text(fresh.get("finished"), order_id)
        if isinstance(row, dict) and row.get("finish_as") and "state" not in row:
            return False
        if isinstance(row, dict) and str(row.get("state") or "") in _OKX_TERMINAL_STATES and "finish_as" not in row:
            return True
        if not fresh.get("finished_read") or not fresh.get("finished_done"):
            return True
        if not self._okx_window_open(fresh, journal_ts):
            return True
        return row is not None

    def _filled_buy_witness(self, pair: str, executor_id: str, order_id: str, reason: str) -> None:
        self._log_silent_exit(
            "filled-buy-close", pair or "", f"{reason} order_id=\u2026{str(order_id)[-8:]}", executor_id,
            interval=300.0, label="silent buy exit:", key=executor_id,
        )

    def _rescue_proved_close(self, executor_id: str, level_body: Dict[str, Any], fresh: Dict[str, Any],
                             live_open_id: Optional[str] = None, live_open_base: Optional[Decimal] = None):
        """Ранний номер close этого уровня, доказанный страницей исполненным.

        Метка круга — ts строки журнала живого слота открытия, не метка тела
        (исполненное тело сажается с нулевой меткой) и не чекпойнт. Записанный
        в чекпойнт close доказательство не гасит (подъём снимает слот, круг
        сажает заново), но чужой записанный номер не затирается. Поздняя
        отказная строка раннее исполнение не заслоняет и сама не сажается.
        """
        rows = self._partial_rows()
        if rows is None:
            return None
        open_id = str(live_open_id or "")
        if not open_id:
            return None
        if open_id != str((level_body.get("open_order") or {}).get("order_id") or ""):
            return None
        open_ts = next((
            row.get("ts") for row in rows
            if str(row.get("order_id") or "") == open_id and row.get("ts") is not None
        ), None)
        if not open_id or open_ts is None:
            return None
        best = None
        for row in rows:
            if str(row.get("executor_id") or "") != str(executor_id):
                continue
            if str(row.get("level_id") or "") != str(level_body.get("id") or ""):
                continue
            if str(row.get("role") or "") not in ("close", "market_close"):
                continue
            ts = row.get("ts")
            if ts is None or ts < open_ts:
                continue
            text = str(row.get("order_id") or "")
            if not text:
                continue
            found = partial_find_text(fresh.get("finished"), text)
            if not isinstance(found, dict):
                found = partial_find_text(fresh.get("finished_blank_px"), text)
            if not isinstance(found, dict):
                continue
            if str(found.get("state") or "") != "filled":
                continue
            size = partial_decimal(found.get("fill_size"))
            if size is None or size <= 0:
                continue
            if live_open_base is not None and size < live_open_base:
                continue
            if not self._okx_window_open(fresh, ts):
                continue
            if best is None or ts < best[2]:
                best = (text, size, ts, found)
        if best is not None:
            seated = str((level_body.get("close_order") or {}).get("order_id") or "")
            if seated and seated != best[0]:
                return None
        return best

    def _rescue_close_blocked(self, executor_id: str, level_id: str, fresh: Dict[str, Any],
                              executor: Any = None) -> bool:
        """Уровень не получает новую продажу, только пока его исполненный close доказан страницей."""
        parsed = self._partial_parsed(executor_id)
        level_body = next((
            item for item in (parsed or {}).get("levels") or []
            if str(item.get("id") or "") == str(level_id)
        ), None)
        if level_body is None:
            return False
        target = next((
            level for level in list(getattr(executor, "grid_levels", None) or [])
            if str(getattr(level, "id", "") or "") == str(level_id)
        ), None)
        open_slot = getattr(target, "active_open_order", None) if target is not None else None
        live_open_id = str(getattr(open_slot, "order_id", "") or "") if open_slot is not None else None
        if not live_open_id or not getattr(open_slot, "is_filled", False):
            return False
        live_open_base = partial_decimal(
            str(getattr(getattr(open_slot, "order", None), "executed_amount_base", "") or "")
        )
        if live_open_base is None or live_open_base <= 0:
            return False
        return self._rescue_proved_close(executor_id, level_body, fresh, live_open_id, live_open_base) is not None


    def _rescue_close_price(self, level_body: Dict[str, Any], found: Dict[str, Any]) -> Optional[Decimal]:
        """Цена тела продажи: средняя страницы, иначе тейк уровня. Пустой px не берём."""
        avg = partial_decimal(found.get("avgPx"))
        if avg is not None and avg > 0:
            return avg
        tp = partial_decimal(level_body.get("take_profit_price"))
        if tp is not None and tp > 0:
            return tp
        base = partial_decimal((level_body.get("open_order") or {}).get("price"))
        profit = partial_decimal(level_body.get("take_profit"))
        if base is not None and base > 0 and profit is not None:
            return base * (Decimal("1") + profit)
        return None

    async def _rescue_seat_filled_close(self, executor_id: str, executor: Any, connector: Any, pair: str) -> bool:
        """Посадить исполненный close уровня из страницы OKX. Сами не шлём ничего."""
        fresh = self._tick_fresh(pair or "")
        if not self._lists_ready(fresh):
            return False
        parsed = self._partial_parsed(executor_id)
        seated = False
        for level_body in (parsed or {}).get("levels") or []:
            if not isinstance(level_body, dict):
                continue
            target = next((
                level for level in list(getattr(executor, "grid_levels", None) or [])
                if str(getattr(level, "id", "") or "") == str(level_body.get("id") or "")
            ), None)
            if target is None:
                continue
            open_slot = getattr(target, "active_open_order", None)
            if open_slot is None or not getattr(open_slot, "is_filled", False):
                continue
            live_open_id = str(getattr(open_slot, "order_id", "") or "")
            live_open_base = partial_decimal(
                str(getattr(getattr(open_slot, "order", None), "executed_amount_base", "") or "")
            )
            if live_open_base is None or live_open_base <= 0:
                continue
            proof = self._rescue_proved_close(executor_id, level_body, fresh, live_open_id, live_open_base)
            if proof is None:
                continue
            text, size, _ts, found = proof
            price = self._rescue_close_price(level_body, found)
            if price is None or price <= 0:
                continue
            closed = self._stock_filled_order(
                client_id=text, trading_pair=pair, trade_name="SELL",
                position_name="CLOSE", amount=size, price=price, leverage=1,
            )
            if closed is None:
                continue
            body_json = None
            try:
                body_json = closed.to_json()
            except Exception:
                body_json = None
            if isinstance(body_json, dict):
                self.__dict__.setdefault("_rescue_close_bodies", {})[text] = body_json

            def mutate(column: Dict[str, Any], text=text, size=size, price=price, level_id=str(level_body.get("id") or "")) -> None:
                for item in column.get("levels") or []:
                    if str(item.get("id") or "") != level_id:
                        continue
                    if (item.get("close_order") or {}).get("order_id"):
                        return
                    item["close_order"] = {
                        "order_id": text,
                        "filled": True,
                        "price": str(price),
                        "order": (self.__dict__.get("_rescue_close_bodies") or {}).get(text)
                                 or {"client_order_id": text, "executed_amount_base": str(size),
                                     "price": str(price), "amount": str(size)},
                    }

            def visible(parsed_body: Optional[Dict[str, Any]], text=text, level_id=str(level_body.get("id") or "")) -> bool:
                for item in (parsed_body or {}).get("levels") or []:
                    if str(item.get("id") or "") == level_id:
                        return str((item.get("close_order") or {}).get("order_id") or "") == text
                return False



            if not await self._partial_cas(executor_id, mutate, visible, str(level_body.get("id") or "")):
                (self.__dict__.get("_rescue_close_bodies") or {}).pop(text, None)
                continue
            seated = True
            (self.__dict__.get("_rescue_close_bodies") or {}).pop(text, None)
            # Слот — только после севшей записи: штатный шаг не видит завершения,
            # которого нет на диске. Чужой стоящий номер не затирается.
            seated_slot = getattr(target, "active_close_order", None)
            seated_id = str(getattr(seated_slot, "order_id", "") or "")
            seated_event = getattr(getattr(seated_slot, "order", None), "completely_filled_event", None)
            if seated_slot is None or (seated_id == text and (seated_event is None or not seated_event.is_set())):
                if seated_slot is None:
                    target.active_close_order = TrackedOrder(order_id=text)
                target.active_close_order.order = closed
        return seated

    async def _partial_note_filled_price(
        self, executor_id: str, level_id: str, quant: str, bought: Decimal,
    ) -> bool:
        def mutate(body: Dict[str, Any]) -> None:
            for level in body.get("levels") or []:
                if str(level.get("id")) != str(level_id):
                    continue
                if level.get("take_profit_price") not in (None, ""):
                    return
                level["take_profit_price"] = str(quant)
                level["partial_bought"] = str(bought)

        def visible(parsed: Optional[Dict[str, Any]]) -> bool:
            for level in (parsed or {}).get("levels") or []:
                if str(level.get("id")) != str(level_id):
                    continue
                return level.get("take_profit_price") not in (None, "") and level.get("partial_bought") not in (None, "")
            return False

        return await self._partial_cas(executor_id, mutate, visible, level_id)

    async def _partial_filled_okx_buy(
        self,
        executor_id: str,
        executor: Any,
        connector: Any,
        pair: str,
        order_id: str,
        level_id: str,
        row_ts: Any,
        fresh: Dict[str, Any],
    ) -> bool:
        """One take-profit sell after a full OKX buy. Do not release the buy first."""
        parsed = self._partial_parsed(executor_id)
        if partial_end_visible(parsed, order_id):
            return False
        level = self._filled_open_level(parsed, order_id)
        if level is None:
            return False
        if not fresh.get("opens_read") or not fresh.get("opens_done"):
            self._filled_buy_witness(pair, executor_id, order_id, "opens-unread")
            return True
        found = partial_find_text(fresh.get("finished"), order_id)
        if isinstance(found, dict) and found.get("finish_as") and "state" not in found:
            return False
        bought = self._filled_order_base(level)
        if bought is None:
            return True
        if not self._okx_window_open(fresh, row_ts):
            self._filled_buy_witness(pair, executor_id, order_id, "window-uncovered")
            return True
        found = partial_find_text(fresh.get("finished"), order_id)
        if found is None:
            self._filled_buy_witness(pair, executor_id, order_id, "row-absent")
            return True
        fill_size = partial_decimal(found.get("fill_size"))
        if str(found.get("state") or "") != "filled" or fill_size is None or fill_size <= 0:
            self._filled_buy_witness(pair, executor_id, order_id, "not-filled")
            return True
        side = str(level.get("side") or "BUY")
        quant = partial_take_profit_quant(
            connector, pair, level.get("price"), level.get("take_profit"), side,
        )
        if not quant:
            return True
        if level.get("take_profit_price") in (None, ""):
            if not await self._partial_note_filled_price(executor_id, str(level.get("id") or level_id), quant, bought):
                return True
            self._filled_buy_witness(pair, executor_id, order_id, "seat-written")
            parsed = self._partial_parsed(executor_id)
            level = self._filled_open_level(parsed, order_id) or level
        close = (level.get("close_order") or {}) if isinstance(level, dict) else {}
        text = str(close.get("order_id") or "")
        if text and (
            not fresh.get("finished_read")
            or not fresh.get("finished_done")
            or (
                partial_find_text(fresh.get("finished"), text) is not None
                and not self._cancelled_uncovered_sell(
                    executor_id, str(level.get("id") or level_id),
                    self._finished_row(text, fresh),
                )
            )
        ):
            return True
        if not text:
            return True
        body = (level.get("open_order") or {}).get("order") if isinstance(level, dict) else None
        send_amount = self._amount_from_contracts(connector, pair, body, bought)
        if send_amount is None:
            return True
        await self._partial_send_committed(
            executor_id, executor, connector, pair, text, str(level.get("id") or level_id), str(quant), send_amount,
            journal_ts=row_ts,
        )
        self._filled_buy_witness(pair, executor_id, order_id, "close-sent" if not close.get("order_id") else "close-already")
        return True

    async def _partial_tick(self, executor_id: str, executor: Any) -> None:
        """Seat a partial buy and send one close. Do not place a second open or a market sell."""
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        pair = getattr(getattr(executor, "config", None), "trading_pair", None)
        if connector is None or not pair:
            return
        rows = self._partial_rows()
        if rows is None:
            return
        self._partial_prepare_sync(executor_id, executor, connector, pair)
        rows = self._partial_rows() or []
        self._set_hold_rows(await self._checkpoint_rows())
        parsed = self._partial_parsed(executor_id)
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        for order_id in list(partial_holding_ids(rows, executor_id)):
            if partial_end_visible(parsed, order_id):
                continue
            level_id = next((str(row.get("level_id")) for row in rows if str(row.get("order_id") or "") == order_id and row.get("level_id")), None)
            if not level_id:
                continue
            row_ts = next((row.get("ts") for row in rows if str(row.get("order_id") or "") == order_id and row.get("ts") is not None), None)
            fresh = await self._fresh_raw_lists(connector, pair, row_ts)
            self.__dict__.setdefault("_partial_fresh", {})[pair] = fresh
            covered = partial_lists_cover(fresh, row_ts)
            if not fresh.get("opens_read") or not fresh.get("opens_done"):
                await self._partial_filled_okx_buy(
                    executor_id, executor, connector, pair, order_id, level_id, row_ts, fresh,
                )
                continue
            if partial_text_in(fresh.get("opens"), order_id) or order_id in inflight or order_id in lost:
                continue
            if await self._partial_filled_okx_buy(
                executor_id, executor, connector, pair, order_id, level_id, row_ts, fresh,
            ):
                continue
            found = partial_find_text(fresh.get("finished"), order_id)
            if not covered:
                base = partial_fill_base(found)
                marked = partial_send_marked(rows, order_id) or any(
                    partial_old_remember(row) for row in rows if str(row.get("order_id") or "") == order_id
                )
                if base is not None and marked:
                    if await self._partial_seat(executor_id, executor, connector, pair, order_id, level_id, found, base):
                        await self._partial_close_cases(executor_id, executor, connector, pair, order_id, level_id, base, fresh)
                continue
            if self._partial_sale_still_open(parsed, order_id, level_id, fresh, inflight, lost):
                if not partial_seating_visible(parsed, order_id):
                    base = partial_fill_base(found)
                    marked = partial_send_marked(rows, order_id) or any(
                        partial_old_remember(row) for row in rows if str(row.get("order_id") or "") == order_id
                    )
                    if base is not None and marked:
                        await self._partial_seat(executor_id, executor, connector, pair, order_id, level_id, found, base)
                        parsed = self._partial_parsed(executor_id)
                bought = self._partial_bought_base(parsed, order_id, level_id, found)
                if bought is not None and partial_seating_visible(parsed, order_id):
                    await self._partial_close_cases(
                        executor_id, executor, connector, pair, order_id, level_id, bought, fresh,
                    )
                continue
            if partial_base_zero(found) and not partial_text_in(fresh.get("opens"), order_id):
                continue
            if found is None and order_id not in inflight and order_id not in lost:
                continue
            if partial_full_fill(found):
                if self._partial_sell_is_live(executor_id, connector, parsed, pair):
                    continue
                await self._partial_release(executor_id, order_id, level_id, "full_fill", connector, pair)
                continue
            base = partial_fill_base(found)
            marked = partial_send_marked(rows, order_id) or any(
                partial_old_remember(row) for row in rows if str(row.get("order_id") or "") == order_id
            )
            if base is None or not marked:
                continue
            if not await self._partial_seat(executor_id, executor, connector, pair, order_id, level_id, found, base):
                continue
            await self._partial_close_cases(executor_id, executor, connector, pair, order_id, level_id, base, fresh)
        await self._partial_refresh_proof(executor_id, connector, pair)

    def _partial_bought_base(
        self,
        parsed: Optional[Dict[str, Any]],
        order_id: str,
        level_id: str,
        found: Optional[Dict[str, Any]],
    ) -> Optional[Decimal]:
        base = partial_fill_base(found)
        if base is not None:
            return base
        for level in (parsed or {}).get("levels") or []:
            if str(level.get("id")) != str(level_id):
                continue
            if level.get("partial_bought") not in (None, ""):
                return Decimal(str(level.get("partial_bought")))
            slot = level.get("open_order") or {}
            amount = (slot.get("order") or {}).get("amount") if isinstance(slot.get("order"), dict) else None
            if str(slot.get("order_id") or "") == str(order_id) and amount not in (None, ""):
                return Decimal(str(amount))
        return None

    def _partial_sale_still_open(
        self,
        parsed: Optional[Dict[str, Any]],
        order_id: str,
        level_id: str,
        fresh: Dict[str, Any],
        inflight: Dict[str, Any],
        lost: Dict[str, Any],
    ) -> bool:
        """A visible seat or a live close is not proof the buy text is gone."""
        if partial_seating_visible(parsed, order_id):
            return True
        for level in (parsed or {}).get("levels") or []:
            if str(level.get("id")) != str(level_id):
                continue
            slot = level.get("open_order") or {}
            if str(slot.get("order_id") or "") == str(order_id) and slot.get("filled") is True:
                return True
            close = level.get("close_order") or {}
            text = str(close.get("order_id") or "")
            if not text:
                continue
            if text in inflight or text in lost or partial_text_in(fresh.get("opens"), text):
                return True
            if close.get("level_id") and close.get("price") not in (None, ""):
                return True
        return False

    def _partial_proof_ready(self, executor_id: str, fresh: Dict[str, Any], connector: Any) -> bool:
        parsed = self._partial_parsed(executor_id)
        rows = self._partial_rows()
        if rows is None or not partial_lists_cover(fresh, None):
            return False
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        for order_id in partial_holding_ids(rows, executor_id):
            if not partial_end_visible(parsed, order_id):
                return False
            if partial_text_in(fresh.get("opens"), order_id) or order_id in inflight or order_id in lost:
                return False
        for level in (parsed or {}).get("levels") or []:
            close = (level or {}).get("close_order") or {}
            if close.get("order_id") and not partial_level_released(parsed, str(level.get("id"))):
                return False
        return True

    async def _partial_refresh_proof(self, executor_id: str, connector: Any, pair: str) -> None:
        """Stop is allowed only after a fresh read covers this row ts and the end mark is visible."""
        ready_map = self.__dict__.setdefault("_partial_proof_ready", {})
        rows = self._partial_rows()
        parsed = self._partial_parsed(executor_id)
        if rows is None:
            ready_map[executor_id] = False
            return
        holding = partial_holding_ids(rows, executor_id)
        if self._partial_sell_is_live(executor_id, connector, parsed, pair):
            ready_map[executor_id] = False
            return
        if not holding:
            ready_map[executor_id] = True
            return
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        for order_id in holding:
            if not partial_end_visible(parsed, order_id):
                ready_map[executor_id] = False
                return
            row_ts = next(
                (row.get("ts") for row in rows if str(row.get("order_id") or "") == order_id and row.get("ts") is not None),
                None,
            )
            fresh = await self._fresh_raw_lists(connector, pair, row_ts)
            if not partial_lists_cover(fresh, row_ts):
                ready_map[executor_id] = False
                return
            if partial_text_in(fresh.get("opens"), order_id) or order_id in inflight or order_id in lost:
                ready_map[executor_id] = False
                return
        for level in (parsed or {}).get("levels") or []:
            close = (level or {}).get("close_order") or {}
            if close.get("order_id") and not partial_level_released(parsed, str(level.get("id"))):
                ready_map[executor_id] = False
                return
        ready_map[executor_id] = True

    def _partial_foreign_booked(
        self, executor_id: str, connector: Any, pair: str, quant: str, own_id: str,
        fresh: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Наша запись в колонке или журнале держит тик, пока finished не доказал
        именно эту заявку (правило номера): цена — нашим живым номером, тейк —
        пока позиция не закрыта. Чужой текст не держит."""
        rules = getattr(connector, "trading_rules", None) or {}
        tick = getattr(rules.get(pair) if isinstance(rules, dict) else None, "min_price_increment", None)
        rows = self._partial_rows() or []
        own = {str(own_id)}
        for row in rows:
            if str(row.get("order_id") or "") == str(own_id):
                own.add(str(row.get("order_id")))
        parsed = self._partial_parsed(executor_id)
        for level in (parsed or {}).get("levels") or []:
            close = (level or {}).get("close_order") or {}
            if str(close.get("order_id") or "") == str(own_id):
                slot = level.get("open_order") or {}
                if slot.get("order_id"):
                    own.add(str(slot.get("order_id")))
        prices = []
        released = set()
        for row in getattr(self, "_hold_rows", None) or []:
            raw = row.get("checkpoint")
            body = raw if isinstance(raw, dict) else checkpoint_master(raw)
            released.update(
                str(item) for item in partial_released_ids(body)
                if item and not str(item).startswith("level:")
            )
        for row in rows:
            order_id = str(row.get("order_id") or "")
            if not order_id or order_id in own or order_id in released:
                continue
            if row.get("price") not in (None, "") and self._text_holds(order_id, fresh):
                prices.append(row.get("price"))
            if row.get("take_profit_price") not in (None, "") and self._take_holds(order_id, fresh):
                prices.append(row.get("take_profit_price"))
        for row in getattr(self, "_hold_rows", None) or []:
            body = checkpoint_master(row.get("checkpoint"))
            for level in (body or {}).get("levels") or []:
                close = (level or {}).get("close_order") or {}
                close_id = str(close.get("order_id") or "")
                if close_id in own:
                    continue
                if close.get("price") not in (None, "") and self._text_holds(close_id, fresh):
                    prices.append(close.get("price"))
                slot_id = str((level.get("open_order") or {}).get("order_id") or "")
                if (
                    level.get("take_profit_price") not in (None, "")
                    and slot_id not in own
                    and self._take_holds(slot_id, fresh)
                ):
                    prices.append(level.get("take_profit_price"))
            for hold in (body or {}).get("holds") or []:
                if str(hold.get("order_id") or "") in own:
                    continue
                if order_is_unsent(str(hold.get("order_id") or "")):
                    continue
                if hold.get("price") not in (None, "") and self._text_holds(hold.get("order_id"), fresh):
                    prices.append(str(hold.get("price")))
        for price in prices:
            same = _same_tick(price, quant, tick)
            if same is None or same is True:
                return True
        return False

    async def _partial_release(self, executor_id: str, order_id: str, level_id: str, kind: str, connector: Any, pair: str) -> bool:
        if await self._refresh_stop_mark(executor_id):
            return False
        ids = [order_id]

        def mutate(body: Dict[str, Any]) -> None:
            # Гонка: mutate бежит ПОСЛЕ чтения строки CAS, и перечитки до await
            # недостаточно. Метка в этом теле — тело не меняется, запись конца
            # с level_id не проходит (план stop-freeze п.2).
            if stop_mark_visible(body):
                raise RuntimeError("stop mark")
            ends = dict(body.get("partial_ends") or {})
            ends[order_id] = {"kind": kind, "level_id": level_id, "order_ids": list(ids)}
            body["partial_ends"] = ends
            for level in body.get("levels") or []:
                if str(level.get("id")) != str(level_id):
                    continue
                close = level.get("close_order") or {}
                if close.get("order_id"):
                    ids.append(str(close.get("order_id")))
                for item in level.get("partial_closes") or []:
                    if isinstance(item, dict) and item.get("order_id"):
                        ids.append(str(item.get("order_id")))
                level.pop("take_profit_price", None)
                level["close_order"] = None
                level["partial_closes"] = []
                slot = level.get("open_order") or {}
                if slot.get("filled"):
                    level["open_order"] = None
            ends[order_id]["order_ids"] = list(dict.fromkeys(ids))
            self._strip_recorded(body, ids)

        visible = lambda parsed: partial_end_visible(parsed, order_id) and "price" not in ((parsed or {}).get("partial_ends") or {}).get(order_id, {})
        before = self._partial_parsed(executor_id)
        released_prices = []
        for row in self._partial_rows() or []:
            if str(row.get("order_id") or "") != str(order_id):
                continue
            if row.get("price") not in (None, ""):
                released_prices.append(str(row.get("price")))
            if row.get("take_profit_price") not in (None, ""):
                released_prices.append(str(row.get("take_profit_price")))
        for level in (before or {}).get("levels") or []:
            if str(level.get("id")) != str(level_id):
                continue
            if level.get("take_profit_price") not in (None, ""):
                released_prices.append(str(level.get("take_profit_price")))
            close = level.get("close_order") or {}
            if close.get("price") not in (None, ""):
                released_prices.append(str(close.get("price")))
        done = await self._partial_cas(executor_id, mutate, visible, level_id)
        if done:
            reserves = self.__dict__.get("_quant_reserves") or {}
            connector_key = str(id(connector))
            for price in dict.fromkeys(released_prices):
                release_quant(reserves, connector_key, pair, price)
            self.__dict__.setdefault("_released_order_ids", set()).update(str(item) for item in ids)
            self._forget_close_memory(self._active_executors.get(executor_id), ids)
        return done

    async def _partial_seat(
        self,
        executor_id: str,
        executor: Any,
        connector: Any,
        pair: str,
        order_id: str,
        level_id: str,
        raw: Dict[str, Any],
        base: Decimal,
    ) -> bool:
        if partial_seating_visible(self._partial_parsed(executor_id), order_id):
            return True
        five = self._partial_five(executor, close=False)
        rows = self._partial_rows() or []
        marked = next((row for row in rows if str(row.get("order_id") or "") == order_id and partial_has_five(row)), None)
        if five is None and marked is not None:
            five = {key: marked.get(key) for key in ("trading_pair", "order_type", "trade_type", "leverage", "position")}
        if not five or any(five.get(key) in (None, "") for key in ("trading_pair", "order_type", "trade_type", "leverage", "position")):
            return False
        if raw.get("price") in (None, "") or raw.get("id") in (None, "") or raw.get("filled_total") in (None, ""):
            return False
        side = five.get("trade_type") or "BUY"
        profit = self._partial_level_profit(executor, level_id)
        quant = partial_take_profit_quant(connector, pair, raw.get("price"), profit, side)
        if not quant:
            return False
        body = partial_order_body(
            client_order_id=order_id,
            trading_pair=five["trading_pair"],
            order_type=five["order_type"],
            trade_type=five["trade_type"],
            amount=str(base),
            price=raw.get("price"),
            exchange_order_id=raw.get("id"),
            leverage=five["leverage"],
            position=five["position"],
            executed_base=str(base),
            executed_quote=raw.get("filled_total"),
        )
        if body is None:
            return False

        def mutate(column: Dict[str, Any]) -> None:
            level = None
            for item in column.get("levels") or []:
                if str(item.get("id")) == str(level_id):
                    level = item
                    break
            if level is None:
                raise RuntimeError("level missing")
            existing = level.get("open_order") or {}
            if existing.get("order_id") and str(existing.get("order_id")) != order_id:
                raise RuntimeError("foreign open")
            level["open_order"] = {
                "order_id": order_id,
                "filled": True,
                "level_id": level_id,
                "order": body,
                "original_amount": raw.get("amount"),
            }
            level["take_profit_price"] = quant
            level["partial_bought"] = str(base)
            level["partial_closes"] = list(level.get("partial_closes") or [])
            column["holds"] = [
                item for item in column.get("holds") or []
                if str(item.get("order_id") or "") != order_id
            ]

        return await self._partial_cas(
            executor_id,
            mutate,
            lambda parsed: partial_seating_visible(parsed, order_id),
            level_id,
        )

    async def _partial_close_cases(
        self,
        executor_id: str,
        executor: Any,
        connector: Any,
        pair: str,
        order_id: str,
        level_id: str,
        bought: Decimal,
        fresh: Dict[str, Any],
    ) -> None:
        parsed = self._partial_parsed(executor_id)
        level = None
        for item in (parsed or {}).get("levels") or []:
            if str(item.get("id")) == str(level_id):
                level = item
        if not isinstance(level, dict):
            return
        close = level.get("close_order") or {}
        text = str(close.get("order_id") or "")
        quant = level.get("take_profit_price")
        if not text:
            await self._partial_commit_close(executor_id, executor, connector, pair, level_id, quant, bought)
            parsed = self._partial_parsed(executor_id)
            for item in (parsed or {}).get("levels") or []:
                if str(item.get("id")) == str(level_id):
                    close = item.get("close_order") or {}
                    text = str(close.get("order_id") or "")
                    quant = item.get("take_profit_price")
        if not text or not quant:
            return
        if not fresh.get("opens_read") or not fresh.get("opens_done"):
            return
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        if partial_text_in(fresh.get("opens"), text) or text in inflight or text in lost:
            return
        filled = Decimal("0")
        for item in level.get("partial_closes") or []:
            if isinstance(item, dict) and item.get("filled_base") not in (None, ""):
                filled += Decimal(str(item.get("filled_base")))
        row = partial_find_text(fresh.get("finished"), text)
        row_base = partial_raw_base(row) if row else None
        if row_base is not None and row_base > 0:
            filled += row_base
        if filled == bought and row is not None:
            await self._partial_case_three(executor_id, executor, connector, pair, order_id, level_id, bought, fresh)
            return
        amount = bought - filled if filled else bought
        if amount <= 0:
            return
        await self._partial_send_committed(executor_id, executor, connector, pair, text, level_id, str(quant), amount)

    def _buy_row_for_level(
        self, executor_id: str, level_id: str, rows: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Строка покупки, к которой относится чеканка закрытия: слот уровня, иначе
        поздняя покупка этого исполнителя в журнале. None журнала сюда не передавать."""
        slot_id = ""
        for level in (self._partial_parsed(executor_id) or {}).get("levels") or []:
            if str(level.get("id") or "") == str(level_id):
                slot_id = str((level.get("open_order") or {}).get("order_id") or "")
                break
        if rows is None:
            rows = []
        if slot_id:
            for row in rows:
                if str(row.get("order_id") or "") == slot_id:
                    return row
            return None
        latest = None
        latest_ts = None
        for row in rows:
            if str(row.get("executor_id") or "") != str(executor_id):
                continue
            if str(row.get("level_id") or "") != str(level_id):
                continue
            if str(row.get("role") or "") != "open":
                continue
            ts = partial_decimal(row.get("ts"))
            if ts is None:
                continue
            if latest_ts is None or ts >= latest_ts:
                latest = row
                latest_ts = ts
        return latest

    def _sell_of_this_buy(
        self, executor_id: str, level_id: str, rows: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        """Продажа этой покупки в журнале: тот же исполнитель, не старше покупки.
        Продажа прошлого круга и чужого исполнителя номер этой покупки не занимает.
        Журнал передаёт вызывающий: None здесь не значит «прочитано и пусто»."""
        if rows is None:
            return ""
        buy = self._buy_row_for_level(executor_id, level_id, rows)
        if not isinstance(buy, dict):
            return ""
        owner = str(buy.get("executor_id") or "")
        buy_ts = partial_decimal(buy.get("ts"))
        if not owner or buy_ts is None:
            return ""
        found = ""
        found_ts = None
        for row in rows:
            if str(row.get("level_id") or "") != str(level_id):
                continue
            if str(row.get("executor_id") or "") != owner:
                continue
            if str(row.get("role") or "") not in ("close", "market_close"):
                continue
            sell_ts = partial_decimal(row.get("ts"))
            if sell_ts is None or sell_ts < buy_ts:
                continue
            name = str(row.get("order_id") or "")
            if not name:
                continue
            if found_ts is None or sell_ts < found_ts:
                found = name
                found_ts = sell_ts
        return found

    def _cancelled_uncovered_sell(
        self, executor_id: str, level_id: str, row: Optional[Dict[str, Any]],
    ) -> bool:
        """Снятая продажа этой покупки, база которой покупку не покрыла.
        Исполненная и покрывшая — не она."""
        if not isinstance(row, dict):
            return False
        finish = str(row.get("finish_as") or "").lower()
        state = str(row.get("state") or "").lower()
        if finish != "cancelled" and state not in ("canceled", "cancelled", "mmp_canceled"):
            return False
        if finish == "filled" or state == "filled":
            return False
        bought = None
        for level in (self._partial_parsed(executor_id) or {}).get("levels") or []:
            if str(level.get("id") or "") == str(level_id):
                bought = partial_decimal(level.get("partial_bought"))
                break
        if bought is None or bought <= 0:
            return False
        return self._shutdown_sold_base(row) < bought

    async def _partial_commit_close(
        self,
        executor_id: str,
        executor: Any,
        connector: Any,
        pair: str,
        level_id: str,
        quant: Any,
        amount: Decimal,
    ) -> Optional[str]:
        if not quant:
            return None
        if await self._refresh_stop_mark(executor_id):
            return None
        # Один прочитанный журнал. None — не «продажи нет»: стоять, новый не чеканить.
        rows = self._partial_rows()
        if rows is None:
            return None
        # Номер этой покупки уже в журнале — новый не чеканить. Продажа прошлого
        # круга слот не занимает. Живая книга, непрочитанные списки и исполненная
        # продажа, покрывшая покупку, — стоять. Снятая, покупку не покрывшая, —
        # тот же номер в пустой слот.
        own = self._sell_of_this_buy(executor_id, level_id, rows)
        fresh = self._tick_fresh(pair)
        if own:
            if not self._lists_ready(fresh) or self._id_still_live(fresh, connector, own):
                return None
            finished = self._finished_row(own, fresh)
            if finished is not None and not self._cancelled_uncovered_sell(executor_id, level_id, finished):
                return None
            order_id = own
        else:
            from hummingbot.connector.utils import get_new_client_order_id
            order_id = get_new_client_order_id(
                is_buy=False,
                trading_pair=pair,
                hbot_order_id_prefix=getattr(connector, "client_order_id_prefix", ""),
                max_id_len=getattr(connector, "client_order_id_max_length", 32),
            )

        def mutate(body: Dict[str, Any]) -> None:
            # Гонка CAS: метка в прочитанном теле — level_id не пишем, запись
            # не проходит (план stop-freeze п.2).
            if stop_mark_visible(body):
                raise RuntimeError("stop mark")
            for level in body.get("levels") or []:
                if str(level.get("id")) != str(level_id):
                    continue
                existing = level.get("close_order") or {}
                if existing.get("order_id"):
                    raise RuntimeError("close exists")
                level["close_order"] = {"order_id": order_id, "level_id": level_id, "price": str(quant)}

        def visible(parsed: Optional[Dict[str, Any]]) -> bool:
            for level in (parsed or {}).get("levels") or []:
                close = level.get("close_order") or {}
                if str(level.get("id")) == str(level_id) and close.get("order_id") == order_id and close.get("level_id") and close.get("price") == str(quant):
                    return True
            return False

        if not await self._partial_cas(executor_id, mutate, visible, level_id):
            return None
        return order_id

    def _partial_tick_taken(
        self,
        connector: Any,
        pair: str,
        quant: str,
        own_id: str,
        fresh: Dict[str, Any],
        inflight: Dict[str, Any],
        lost: Dict[str, Any],
    ) -> bool:
        """Any other text already on this quant blocks the send. Own quant does not."""
        rules = getattr(connector, "trading_rules", None) or {}
        tick = getattr(rules.get(pair) if isinstance(rules, dict) else None, "min_price_increment", None)

        def occupied(price: Any, text: str) -> bool:
            # «Повтор своего номера» — только непустой номер, равный своему:
            # пустой текст (голая цена снимка тика) — не повтор и держит quant
            # даже при пустом own_id на чеканке (редакция плана 5).
            if text and text == str(own_id):
                return False
            if price in (None, ""):
                return True
            # Правило номера: чужой текст с номером не держит; голая цена и наш
            # живой номер (журнал, чекпоинт, трекер) — держат.
            if not self._text_holds(text, fresh, tracker_texts):
                return False
            same = _same_tick(price, quant, tick)
            return same is None or same is True

        tracker_texts = set(inflight) | set(lost)

        for raw in fresh.get("opens") or []:
            if occupied(raw.get("price"), str(raw.get("text") or "")):
                return True
        for text, order in list(inflight.items()) + list(lost.items()):
            if occupied(getattr(order, "price", None), str(text)):
                return True
        return False

    async def _partial_note_absent_if_proved(self, connector: Any, pair: str, order_id: str) -> None:
        rows = self._partial_rows() or []
        row_ts = next((row.get("ts") for row in rows if str(row.get("order_id") or "") == str(order_id) and row.get("ts") is not None), None)
        fresh = await self._fresh_raw_lists(connector, pair, row_ts)
        if not partial_lists_cover(fresh, row_ts):
            return
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        if partial_text_in(fresh.get("opens"), order_id) or partial_find_text(fresh.get("finished"), order_id) or order_id in inflight or order_id in lost:
            return
        note_partial_absent(order_id)

    def _contract_base(self, connector, pair: str, amount: Decimal):
        """Base coin from a raw OKX contract size. No size means do not guess."""
        sizes = getattr(connector, "_contract_sizes", None)
        if not isinstance(sizes, dict) or pair not in sizes or sizes[pair] in (None, "", 0):
            return None
        formatter = getattr(connector, "_format_size_to_amount", None)
        if not callable(formatter):
            return None
        try:
            base = Decimal(str(formatter(pair, Decimal(str(amount)))))
        except Exception:
            return None
        if base <= 0:
            return None
        return base

    def _amount_from_contracts(self, connector, pair: str, order, amount):
        """Raw accFillSz converts. A connector fill that already has an exchange id does not."""
        if self._local_exchange_id(order):
            return Decimal(str(amount))
        return self._contract_base(connector, pair, amount)

    def _local_exchange_id(self, row) -> str:
        if isinstance(row, dict):
            return str(row.get("exchange_order_id") or row.get("ordId") or "")
        return str(getattr(row, "exchange_order_id", None) or "")

    def _drop_unsent_local(self, connector, order_id: str) -> None:
        """A local row with no exchange id is not an accepted order."""
        stop = getattr(connector, "stop_tracking_order", None)
        if callable(stop):
            stop(order_id)
        tracker = getattr(connector, "_order_tracker", None)
        lost = getattr(tracker, "_lost_orders", None)
        if isinstance(lost, dict):
            row = lost.get(order_id)
            if row is not None and not self._local_exchange_id(row):
                lost.pop(order_id, None)
        book = getattr(connector, "in_flight_orders", None)
        if isinstance(book, dict):
            row = book.get(order_id)
            if row is not None and not self._local_exchange_id(row):
                book.pop(order_id, None)

    async def _partial_send_committed(
        self,
        executor_id: str,
        executor: Any,
        connector: Any,
        pair: str,
        order_id: str,
        level_id: str,
        quant: str,
        amount: Decimal,
        journal_ts: Any = _OKX_JOURNAL_UNSET,
    ) -> None:
        if await self._refresh_stop_mark(executor_id):
            return
        given_ts = journal_ts is not _OKX_JOURNAL_UNSET
        read_ts = journal_ts if given_ts else None
        fresh = await self._fresh_raw_lists(connector, pair, read_ts)
        if not fresh.get("opens_read") or not fresh.get("opens_done"):
            return
        sent = self.__dict__.setdefault("_partial_sent", {})
        reached = self.__dict__.setdefault("_reached_place", set())
        if order_id in reached or order_id in sent:
            return
        finished_row = partial_find_text(fresh.get("finished"), order_id)
        if self._partial_same_send_already(sent, order_id, finished_row):
            return
        parsed = self._partial_parsed(executor_id)
        slot_ok = False
        for level in (parsed or {}).get("levels") or []:
            close = level.get("close_order") or {}
            if close.get("order_id") == order_id and close.get("level_id") and str(close.get("price")) == str(quant):
                slot_ok = True
        if not slot_ok:
            return
        inflight = getattr(connector, "in_flight_orders", None) or {}
        lost = getattr(getattr(connector, "_order_tracker", None), "lost_orders", None) or {}
        if partial_text_in(fresh.get("opens"), order_id):
            return
        local = inflight.get(order_id)
        if local is None:
            local = lost.get(order_id)
        if local is not None:
            if self._local_exchange_id(local):
                return
            if not fresh.get("finished_read") or not fresh.get("finished_done"):
                return
            if partial_find_text(fresh.get("finished"), order_id) is not None:
                return
            self._drop_unsent_local(connector, order_id)
        if self._quant_held(executor_id, executor, connector, pair, quant, order_id, fresh, inflight, lost):
            return
        if self._unpriced_close_blocks(executor):
            return
        reserves = self.__dict__.setdefault("_quant_reserves", {})
        connector_key = str(id(connector))
        key = (connector_key, pair, str(quant))
        current = reserves.get(key)
        if current not in (None, order_id):
            return
        reserves[key] = order_id
        self._chain_place_once(connector, reached)
        passed_ts = journal_ts if given_ts else None
        resend = self._cancelled_uncovered_sell(
            executor_id, level_id, self._finished_row(order_id, fresh),
        )
        if given_ts and not resend and self._okx_close_blocks(fresh, order_id, passed_ts, explicit=True):
            if reserves.get(key) == order_id:
                reserves.pop(key, None)
            return
        reached.add(order_id)
        token = _placing_order_id.set(order_id)
        side_name = getattr(getattr(getattr(executor, "config", None), "side", None), "name", "BUY")
        try:
            from hummingbot.core.data_type.common import TradeType
            trade_type = TradeType.SELL if side_name != "SELL" else TradeType.BUY
        except Exception:
            trade_type = "SELL" if side_name != "SELL" else "BUY"
        self._seat_close_hold(executor, order_id)
        try:
            from hummingbot.core.data_type.common import PositionAction
            if await self._refresh_stop_mark(executor_id):
                if reserves.get(key) == order_id:
                    reserves.pop(key, None)
                return
            await connector._create_order(
                trade_type=trade_type,
                order_id=order_id,
                trading_pair=pair,
                amount=amount,
                order_type=getattr(getattr(executor.config, "triple_barrier_config", None), "take_profit_order_type", None),
                price=Decimal(str(quant)),
                position_action=PositionAction.CLOSE,
            )
        except Exception as exc:
            logger.exception("partial close create failed")
            if order_id not in reached:
                release_quant(reserves, connector_key, pair, str(quant))
            await self._partial_reject(executor_id, executor, connector, pair, order_id, level_id, quant, amount, exc)
            return
        finally:
            _placing_order_id.reset(token)
        if order_id not in reached:
            release_quant(reserves, connector_key, pair, str(quant))
            return
        sent[order_id] = self._partial_send_mark(finished_row)

    async def _partial_reject(
        self,
        executor_id: str,
        executor: Any,
        connector: Any,
        pair: str,
        order_id: str,
        level_id: str,
        quant: str,
        amount: Decimal,
        exc: Exception,
    ) -> None:
        if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
            return
        if await self._refresh_stop_mark(executor_id):
            return
        fresh = await self._fresh_raw_lists(connector, pair, None)
        if not fresh.get("opens_read") or not fresh.get("opens_done"):
            return
        row = partial_find_text(fresh.get("finished"), order_id)
        if row is None or row.get("finish_as") != "cancelled":
            return
        if partial_text_in(fresh.get("opens"), order_id):
            return
        # Досылка снятого номера: непрочитанный журнал и старая строка того же
        # номера не повод чеканить замену. None не схлопывать в пустой список.
        rows = self._partial_rows()
        if rows is None:
            return
        if self._sell_of_this_buy(executor_id, level_id, rows) == str(order_id):
            return
        filled = partial_raw_base(row) or Decimal("0")
        before = self._partial_parsed(executor_id)
        open_id = ""
        bought = Decimal("0")
        already = Decimal("0")
        past_ids = []
        for level in (before or {}).get("levels") or []:
            if str(level.get("id")) != str(level_id):
                continue
            slot = level.get("open_order") or {}
            open_id = str(slot.get("order_id") or "")
            if level.get("partial_bought") not in (None, ""):
                bought = Decimal(str(level.get("partial_bought")))
            for item in level.get("partial_closes") or []:
                if not isinstance(item, dict):
                    continue
                if item.get("order_id"):
                    past_ids.append(str(item.get("order_id")))
                if item.get("filled_base") not in (None, ""):
                    already += Decimal(str(item.get("filled_base")))
        flat = bool(open_id) and bought > 0 and already + filled >= bought
        if await self._refresh_stop_mark(executor_id):
            return
        new_id = None
        if not flat:
            from hummingbot.connector.utils import get_new_client_order_id
            new_id = get_new_client_order_id(
                is_buy=False,
                trading_pair=pair,
                hbot_order_id_prefix=getattr(connector, "client_order_id_prefix", ""),
                max_id_len=getattr(connector, "client_order_id_max_length", 32),
            )

        def mutate(body: Dict[str, Any]) -> None:
            # Гонка: метка могла появиться между перечиткой до await и чтением
            # строки CAS. Метка в этом теле — тело не меняется: ни конца с
            # level_id (flat), ни нового close_order с level_id (план п.2).
            if stop_mark_visible(body):
                raise RuntimeError("stop mark")
            for level in body.get("levels") or []:
                if str(level.get("id")) != str(level_id):
                    continue
                close = level.get("close_order") or {}
                if close.get("order_id") != order_id:
                    raise RuntimeError("foreign close")
                if flat:
                    ended = [open_id, order_id, *past_ids]
                    ends = dict(body.get("partial_ends") or {})
                    ends[open_id] = {
                        "kind": "flat",
                        "level_id": level_id,
                        "order_ids": list(dict.fromkeys(ended)),
                    }
                    body["partial_ends"] = ends
                    level.pop("take_profit_price", None)
                    level["close_order"] = None
                    level["partial_closes"] = []
                else:
                    closes = list(level.get("partial_closes") or [])
                    closes.append({"order_id": order_id, "filled_base": str(filled)})
                    level["partial_closes"] = closes
                    level["close_order"] = {"order_id": new_id, "level_id": level_id, "price": str(quant)}
            self._strip_recorded(body, [order_id, *past_ids] if flat else [order_id])

        def visible(parsed: Optional[Dict[str, Any]]) -> bool:
            if flat:
                mark = ((parsed or {}).get("partial_ends") or {}).get(open_id, {})
                return partial_end_visible(parsed, open_id) and "price" not in mark
            for level in (parsed or {}).get("levels") or []:
                close = level.get("close_order") or {}
                if str(level.get("id")) == str(level_id) and close.get("order_id") == new_id:
                    return True
            return False

        if not await self._partial_cas(executor_id, mutate, visible, level_id):
            return
        self._forget_close_memory(executor, [order_id])
        self._release_close_reserve(connector, pair, quant, order_id)
        if flat:
            return
        parsed = self._partial_parsed(executor_id)
        bought = Decimal("0")
        for level in (parsed or {}).get("levels") or []:
            if str(level.get("id")) == str(level_id) and level.get("partial_bought") not in (None, ""):
                bought = Decimal(str(level.get("partial_bought")))
        done = Decimal("0")
        for level in (parsed or {}).get("levels") or []:
            if str(level.get("id")) != str(level_id):
                continue
            for item in level.get("partial_closes") or []:
                if isinstance(item, dict) and item.get("filled_base") not in (None, ""):
                    done += Decimal(str(item.get("filled_base")))
        rest = bought - done
        if rest <= 0:
            return
        await self._partial_send_committed(executor_id, executor, connector, pair, new_id, level_id, quant, rest)

    async def _partial_neighbor_close(self, executor_id: str, executor: Any, level: Any) -> None:
        connector_name = getattr(getattr(executor, "config", None), "connector_name", None)
        connector = (getattr(executor, "connectors", None) or {}).get(connector_name)
        pair = getattr(getattr(executor, "config", None), "trading_pair", None)
        if connector is None or not pair:
            return
        side = getattr(getattr(getattr(executor, "config", None), "side", None), "name", "BUY")
        quant = partial_take_profit_quant(
            connector,
            pair,
            getattr(level, "price", None),
            getattr(level, "take_profit", None),
            side,
        )
        amount = self._partial_close_amount(executor, level)
        if not quant or amount is None or amount <= 0:
            return
        order_id = await self._partial_commit_close(
            executor_id, executor, connector, pair, str(getattr(level, "id", "")), quant, amount,
        )
        if not order_id:
            return
        await self._partial_send_committed(
            executor_id, executor, connector, pair, order_id, str(getattr(level, "id", "")), quant, Decimal(str(amount)),
        )

    async def _partial_case_three(
        self,
        executor_id: str,
        executor: Any,
        connector: Any,
        pair: str,
        order_id: str,
        level_id: str,
        bought: Decimal,
        fresh: Dict[str, Any],
    ) -> None:
        if await self._refresh_stop_mark(executor_id):
            return
        parsed = self._partial_parsed(executor_id)
        level_body = None
        for level in (parsed or {}).get("levels") or []:
            if str(level.get("id")) == str(level_id):
                level_body = level
        if not isinstance(level_body, dict):
            return
        close = level_body.get("close_order") or {}
        text = close.get("order_id")
        row = partial_find_text(fresh.get("finished"), text) if text else None
        open_row = partial_find_text(fresh.get("finished"), order_id)
        five_open = self._partial_five(executor, close=False)
        five_close = self._partial_five(executor, close=True)
        if not five_open or not five_close or row is None or open_row is None:
            return
        if row.get("price") in (None, "") or row.get("id") in (None, "") or row.get("filled_total") in (None, ""):
            return
        if open_row.get("price") in (None, "") or open_row.get("id") in (None, "") or open_row.get("filled_total") in (None, ""):
            return
        open_body = partial_order_body(
            client_order_id=order_id,
            trading_pair=five_open["trading_pair"],
            order_type=five_open["order_type"],
            trade_type=five_open["trade_type"],
            amount=str(bought),
            price=open_row.get("price"),
            exchange_order_id=open_row.get("id"),
            leverage=five_open["leverage"],
            position=five_open["position"],
            executed_base=str(bought),
            executed_quote=open_row.get("filled_total"),
        )
        close_body = partial_order_body(
            client_order_id=str(text),
            trading_pair=five_close["trading_pair"],
            order_type=five_close["order_type"],
            trade_type=five_close["trade_type"],
            amount=str(bought),
            price=row.get("price"),
            exchange_order_id=row.get("id"),
            leverage=five_close["leverage"],
            position=five_close["position"],
            executed_base=str(bought),
            executed_quote=row.get("filled_total"),
        )
        if open_body is None or close_body is None:
            return
        try:
            from hummingbot.core.data_type.in_flight_order import InFlightOrder
            from hummingbot.strategy_v2.models.executors import TrackedOrder
            opened = InFlightOrder.from_json(open_body)
            closed = InFlightOrder.from_json(close_body)
        except Exception:
            return
        for level in getattr(executor, "grid_levels", None) or []:
            if str(getattr(level, "id", "")) != str(level_id):
                continue
            level.active_open_order = TrackedOrder(order_id=order_id)
            level.active_open_order.order = opened
            level.active_close_order = TrackedOrder(order_id=str(text))
            level.active_close_order.order = closed
            if hasattr(executor, "update_grid_levels"):
                executor.update_grid_levels()
        await self._partial_release(executor_id, order_id, level_id, "case3", connector, pair)
