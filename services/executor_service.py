"""
ExecutorService manages executor lifecycle and orchestration.
This service enables running Hummingbot executors directly via API
without Docker containers or full strategy setup.
"""
import asyncio
import contextvars

_placing_order_id = contextvars.ContextVar("placing_order_id", default=None)
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Type

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
)
from utils.executor_log_capture import ExecutorLogCapture, current_executor_id
from utils.trading_pair import InvalidTradingPair, split_trading_pair

logger = logging.getLogger(__name__)


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
            return
        fresh = await self._fresh_pair_reads()
        if fresh is None:
            logger.warning("open and finished lists were not read; not marking SYSTEM_CLEANUP")
            return

        try:
            # Get list of currently active executor IDs in memory
            active_executor_ids = list(self._active_executors.keys())
            active_executor_ids.extend(self._executors_with_unproven_orders())
            active_executor_ids.extend(self.__dict__.get("_kept_ids", set()))
            rows = await self._running_rows()
            if rows is None:
                return
            for row in rows:
                key = (row.get("account_name"), row.get("connector_name"), row.get("trading_pair"))
                slot = fresh.get(key) or {}
                if not slot.get("opens_read") or not slot.get("finished_read"):
                    active_executor_ids.append(row["executor_id"])
                    continue
                if not self._row_proofs_released(row, slot):
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

        except Exception as e:
            logger.error(f"Error cleaning up orphaned executors: {e}", exc_info=True)

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
                        if not self._executor_proofs_released(executor_id):
                            continue
                        completed_ids.append(executor_id)
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
        if start:
            self._start_registered_executor(executor_id, executor)
        return executor_id, executor

    def _start_registered_executor(self, executor_id: str, executor: ExecutorBase) -> None:
        token = current_executor_id.set(executor_id)
        try:
            executor.start()
        finally:
            current_executor_id.reset(token)

    def _install_checkpoint_hook(self, executor_id: str, executor: ExecutorBase) -> None:
        """Write the id when the order is placed, and the fill when the fill event arrives."""
        if getattr(executor, "_checkpoint_hooked", False):
            return
        service = self
        original_place = executor.place_order

        def place_wrapped(*args, **kwargs):
            result = original_place(*args, **kwargs)
            order_id = result if isinstance(result, str) else None
            note_placed(executor, order_id)
            remember_order_now(executor_id, order_id)
            service._schedule_checkpoint(executor_id)
            return result

        executor.place_order = place_wrapped
        self._install_open_shell(executor_id, executor)
        for name, marker in (
            ("process_order_filled_event", "filled"),
            ("process_order_completed_event", "filled"),
            ("process_order_canceled_event", "gone"),
            ("process_order_failed_event", "gone"),
        ):
            original = getattr(executor, name, None)
            if original is None or marker == "gone":
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

    async def _fresh_raw_lists(self, connector, pair: str, ts: Any = None) -> Dict[str, Any]:
        """This tick's lists. A cached slot with read=True is not a proof."""
        empty = {
            "opens_read": False,
            "opens_done": False,
            "finished_read": False,
            "finished_done": False,
            "covers_ts": False,
            "opens": [],
            "finished": [],
        }
        venue = open_list_venue(type(connector).__name__ if connector is not None else "")
        if venue != "gate" or connector is None or not hasattr(connector, "_api_get"):
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
            parsed = parse_checkpoint(text)
            held = {
                str(item.get("order_id"))
                for item in (parsed or {}).get("holds") or []
                if item.get("order_id")
            }
            held.update(str(item) for item in saved_order_ids(parsed))
            if str(prefix) in text and not any(item.startswith(str(prefix)) for item in held):
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
        self._hold_rows = await self._checkpoint_rows()
        if self._hold_rows is None:
            self.__dict__["_sweep_failed"] = True
        for target in self._sweep_targets():
            connector = self._connector_for(target["account"], target["connector"])
            if connector is None:
                self.__dict__["_sweep_failed"] = True
                continue
            ts = self._oldest_ts(target)
            fresh = await self._fresh_raw_lists(connector, target["pair"], ts)
            self.__dict__.setdefault("_fresh_reads", {})[
                (target["account"], target["connector"], target["pair"])
            ] = fresh
            if not fresh.get("opens_read") or not fresh.get("finished_read") or not fresh.get("opens_done"):
                self.__dict__["_sweep_failed"] = True
                continue
            await self._delete_owned_texts(connector, target, fresh)
        self.__dict__["_sweep_seen"] = not self.__dict__.get("_sweep_failed")

    def _oldest_ts(self, target: Dict[str, Any]):
        found = []
        for item in owed_records() or []:
            if item.get("trading_pair") == target["pair"] and item.get("ts") is not None:
                found.append(item.get("ts"))
        try:
            from utils.executor_checkpoint import _read_ledger
            for row in _read_ledger():
                if row.get("ts") is not None:
                    found.append(row.get("ts"))
        except Exception:
            return None
        return min(found) if found else None

    async def _delete_owned_texts(self, connector, target: Dict[str, Any], fresh: Dict[str, Any]) -> None:
        if not fresh.get("opens_read"):
            return
        prefix = getattr(connector, "client_order_id_prefix", None)
        journal = ledger_order_ids()
        column = await self._column_order_ids(target["pair"])
        if column is None:
            column = set()
        owed_ids = {
            str(item.get("order_id"))
            for item in (owed_records() or [])
            if item.get("order_id") and item.get("trading_pair") == target["pair"]
            and item.get("account_name") == target["account"]
            and item.get("connector") == target["connector"]
        }
        for raw in list(fresh.get("opens") or []) + list(fresh.get("finished") or []):
            text = str(raw.get("text") or "")
            if not text:
                continue
            if raw.get("currency_pair") != fresh.get("symbol"):
                continue
            owned_by_prefix = bool(prefix) and text.startswith(str(prefix))
            in_journal = journal is not None and text in journal
            in_column = text in column
            in_owed = text in owed_ids
            if not owned_by_prefix and not in_journal and not in_column and not in_owed:
                continue
            owner = self._owner_for_client(text, target["account"], target["connector"], target["pair"])
            if not owner:
                continue
            self.__dict__.setdefault("_seen_client_ids", set()).add(text)
            price = raw.get("price")
            if not in_column and not await self._commit_price_hold(owner, text, price):
                continue
            if price in (None, ""):
                if raw in (fresh.get("opens") or []):
                    await self._delete_exchange_row(connector, raw)
                continue
            if self._proof_for_raw(text, fresh, connector, self._oldest_ts(target)):
                self.__dict__.setdefault("_released_order_ids", set()).add(text)
                continue
            if raw in (fresh.get("opens") or []):
                await self._delete_exchange_row(connector, raw)

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
        found = set()
        for row in rows:
            if row.get("trading_pair") != pair:
                continue
            text = row.get("checkpoint") or ""
            parsed = parse_checkpoint(text)
            if text and parsed is None:
                return None
            found.update(saved_order_ids(parsed))
            for hold in (parsed or {}).get("holds") or []:
                if hold.get("order_id"):
                    found.add(hold["order_id"])
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
        parsed = parse_checkpoint(row.get("checkpoint"))
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
            parsed = parse_checkpoint(text)
            if text and parsed is None:
                return None
            found.update(saved_order_ids(parsed))
            for hold in (parsed or {}).get("holds") or []:
                if hold.get("order_id"):
                    found.add(str(hold["order_id"]))
        return found if seen or rows is not None else None

    def _executor_proofs_released(self, executor_id: str) -> bool:
        if buy_blocked() or ledger_executor_ids() is None or not self.__dict__.get("_sweep_seen"):
            return False
        if self.__dict__.get("_sweep_failed"):
            return False
        column_ids = self._column_ids_for_executor(executor_id)
        if column_ids is None:
            return False
        try:
            from utils.executor_checkpoint import _read_ledger
            rows = _read_ledger()
        except Exception:
            return False
        ids = {
            str(row.get("order_id"))
            for row in rows
            if row.get("executor_id") == executor_id and row.get("order_id")
        }
        ids.update(column_ids)
        if not ids:
            return False
        released = self.__dict__.get("_released_order_ids", set())
        return all(order_id in released for order_id in ids)

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

    async def _delete_exchange_row(self, connector, raw: dict) -> bool:
        request = gate_delete_request(raw)
        if request is None or connector is None or not hasattr(connector, "_api_delete"):
            return False
        await connector._api_delete(
            path_url=request["path"],
            params=request["params"],
            is_auth_required=True,
        )
        return True

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
            parsed = parse_checkpoint(row.get("checkpoint"))
            if row.get("checkpoint") and parsed is None:
                return None
            for hold in (parsed or {}).get("holds") or []:
                prices.append(hold.get("price"))
            for saved in (parsed or {}).get("retained_orders") or []:
                prices.append(saved.get("price"))
            for level in (parsed or {}).get("levels") or []:
                for key in ("open_order", "close_order"):
                    saved_price = (level.get(key) or {}).get("price")
                    if saved_price:
                        prices.append(saved_price)
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
        for source in list(getattr(self, "_resume_rows", []) or []) + list(getattr(self, "_hold_rows", []) or []):
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
            parsed = parse_checkpoint(row.get("checkpoint"))
            if row.get("checkpoint") and parsed is None:
                return True
            for hold in (parsed or {}).get("holds") or []:
                if hold.get("order_id") and hold.get("price") in (None, ""):
                    return True
        return False

    def _remember_seen_texts(self, connector, fresh: Dict[str, Any], column_ids) -> None:
        seen = self.__dict__.setdefault("_seen_client_ids", set())
        for raw in list(fresh.get("opens") or []) + list(fresh.get("finished") or []):
            text = str(raw.get("text") or "")
            if self._text_is_owned(text, connector, column_ids):
                seen.add(text)

    def _seen_prefix_unrecorded(self, connector, fresh: Dict[str, Any], column_ids) -> bool:
        """A prefix we already saw, with no hold and no fresh row, is not an empty book."""
        journal = ledger_order_ids() or set()
        column = set() if column_ids is None else {str(item) for item in column_ids}
        visible = {
            str(raw.get("text") or "")
            for raw in list(fresh.get("opens") or []) + list(fresh.get("finished") or [])
        }
        for text in self.__dict__.get("_seen_client_ids", set()):
            if text in journal or text in column or text in visible:
                continue
            return True
        return False

    async def _claim_visible_texts(self, executor_id, connector, pair, fresh, column_ids) -> bool:
        """Commit a hold and cancel opens before any new buy. False means do not send."""
        account = (self._executor_metadata.get(executor_id) or {}).get("account_name")
        connector_name = (self._executor_metadata.get(executor_id) or {}).get("connector_name")
        claimed = self.__dict__.setdefault("_claimed_prices", [])
        seen = set(column_ids or [])
        for raw in list(fresh.get("opens") or []) + list(fresh.get("finished") or []):
            text = str(raw.get("text") or "")
            if not self._text_is_owned(text, connector, column_ids):
                continue
            if raw.get("currency_pair") != fresh.get("symbol"):
                continue
            self.__dict__.setdefault("_seen_client_ids", set()).add(text)
            price = raw.get("price")
            if text not in seen:
                owner = self._owner_for_client(text, account, connector_name, pair)
                if not owner:
                    return False
                hold = {"order_id": text}
                if price not in (None, ""):
                    hold["price"] = str(price)
                moment = None
                for key in ("create_time", "update_time", "ts"):
                    if raw.get(key) not in (None, ""):
                        moment = raw.get(key)
                        break
                if moment is not None:
                    hold["ts"] = moment
                if not await self._commit_price_hold(owner, text, price, extra=hold):
                    return False
                seen.add(text)
            if price not in (None, ""):
                claimed.append(str(price))
            if raw in (fresh.get("opens") or []):
                try:
                    deleted = await self._delete_exchange_row(connector, raw)
                except Exception:
                    return False
                if not deleted:
                    return False
            if price in (None, ""):
                return False
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
            from utils.executor_checkpoint import _read_ledger
            for row in _read_ledger():
                add(row.get("ts"))
        except Exception:
            return []
        for row in getattr(self, "_hold_rows", None) or []:
            if row.get("trading_pair") not in (None, pair):
                continue
            parsed = parse_checkpoint(row.get("checkpoint"))
            for hold in (parsed or {}).get("holds") or []:
                add(hold.get("ts"))
        for raw in list(fresh.get("opens") or []) + list(fresh.get("finished") or []):
            for key in ("create_time", "update_time", "ts"):
                add(raw.get(key))
        return found

    def _install_open_shell(self, executor_id: str, executor: ExecutorBase) -> None:
        """Grid opens do not call the stock place. The task is kept, not discarded."""
        if not hasattr(executor, "adjust_and_place_open_order"):
            return
        service = self
        reserves = self.__dict__.setdefault("_quant_reserves", {})
        tasks = self.__dict__.setdefault("_accept_tasks", set())

        def wrapped(level):
            task = asyncio.get_event_loop().create_task(
                service._durable_open(executor_id, executor, level, reserves)
            )
            tasks.add(task)
            task.add_done_callback(tasks.discard)

        executor.adjust_and_place_open_order = wrapped
        original_stop = getattr(executor, "control_shutdown_process", None)

        async def stop_held(*args, **kwargs):
            if buy_blocked() or not service._executor_proofs_released(executor_id):
                return None
            if original_stop is None:
                return None
            return await original_stop(*args, **kwargs)

        if original_stop is not None:
            executor.control_shutdown_process = stop_held

    async def _durable_open(self, executor_id: str, executor, level, reserves: dict) -> None:
        """Journal the client id, then send. Do not call stock adjust, buy, or sell."""
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
            from utils.executor_checkpoint import _read_ledger
            miss = self.__dict__.get("_local_miss_ids", set())
            for row in _read_ledger():
                if row.get("order_id") in miss:
                    continue
                stored.append(row.get("price"))
                if row.get("ts") is not None:
                    journal_ts.append(row.get("ts"))
        except Exception:
            return
        for item in owed_records() or []:
            if item.get("trading_pair") == pair and item.get("price"):
                stored.append(item.get("price"))
                if item.get("ts") is not None:
                    journal_ts.append(item.get("ts"))
        rules = getattr(connector, "trading_rules", None) or {}
        tick = getattr(rules.get(pair) if isinstance(rules, dict) else None, "min_price_increment", None)
        column_prices = await self._column_prices(pair)
        if column_prices is None:
            return
        fresh = await self._fresh_raw_lists(connector, pair, journal_ts[0] if journal_ts else None)
        if not fresh.get("opens_read") or not fresh.get("opens_done"):
            return
        self._hold_rows = await self._checkpoint_rows()
        column_ids = await self._column_order_ids(pair)
        if column_ids is None:
            return
        if self._raw_prefix_without_hold(pair, getattr(connector, "client_order_id_prefix", None)):
            return
        if not await self._claim_visible_texts(executor_id, connector, pair, fresh, column_ids):
            return
        if self._owned_row_lacks_price(connector, fresh, column_ids):
            return
        self._hold_rows = await self._checkpoint_rows()
        column_ids = await self._column_order_ids(pair)
        if column_ids is None or self._column_hold_lacks_price(pair):
            return
        moments = self._known_acceptance_times(pair, fresh)
        earliest = min(moments) if moments else None
        request = fresh.get("finished_request") or {}
        if earliest is not None and not history_covers_ts(request, earliest, {
            "read": fresh.get("finished_read"),
            "done": fresh.get("finished_done"),
        }):
            fresh = await self._fresh_raw_lists(connector, pair, earliest)
            request = fresh.get("finished_request") or {}
            if not fresh.get("opens_read") or not fresh.get("opens_done"):
                return
            if not await self._claim_visible_texts(executor_id, connector, pair, fresh, column_ids):
                return
            if self._owned_row_lacks_price(connector, fresh, column_ids):
                return
            self._hold_rows = await self._checkpoint_rows()
            column_ids = await self._column_order_ids(pair)
            if column_ids is None or self._column_hold_lacks_price(pair):
                return
        self._remember_seen_texts(connector, fresh, column_ids)
        if self._seen_prefix_unrecorded(connector, fresh, column_ids):
            return
        owned_visible = (
            self._has_owned_text(connector, fresh, column_ids)
            or bool(journal_ts)
            or bool(column_ids)
            or bool(self.__dict__.get("_seen_client_ids"))
        )
        covered = history_covers_ts(request, earliest, {
            "read": fresh.get("finished_read"),
            "done": fresh.get("finished_done"),
        })
        window_ok = (not owned_visible) or (
            earliest is not None and covered and "from" in (request.get("params") or {})
        )
        if not window_ok:
            return
        released = self.__dict__.get("_released_order_ids", set())
        prices = list(stored)
        prices.extend(column_prices)
        prices.extend(self.__dict__.get("_claimed_prices", []))
        for raw in list(fresh.get("opens") or []) + list(fresh.get("finished") or []):
            if str(raw.get("text") or "") in released:
                continue
            if raw.get("price") not in (None, ""):
                prices.append(raw.get("price"))
        price = connector.quantize_order_price(pair, candidate.price)
        price_text = str(price)
        if send_blocked(
            ledger_ok=ledger_order_ids() is not None,
            column_ok=True,
            opens_ok=bool(fresh.get("opens_read")) and bool(fresh.get("opens_done")),
            finished_ok=bool(fresh.get("finished_read")) and bool(fresh.get("finished_done")),
            history_covers=window_ok and bool(fresh.get("finished_read")) and bool(fresh.get("finished_done")),
            prices=prices,
            placement=price_text,
            tick=tick,
        ):
            return
        if recorded_price_blocks(stored, price_text, tick):
            return
        if self._owner_for_pair(account, connector_name, pair) is None and stored:
            return
        connector_key = str(id(connector))
        if not claim_quant(reserves, connector_key, pair, price_text):
            return
        from hummingbot.connector.utils import get_new_client_order_id
        order_id = get_new_client_order_id(
            is_buy=getattr(getattr(executor.config, "side", None), "name", "BUY") != "SELL",
            trading_pair=pair,
            hbot_order_id_prefix=connector.client_order_id_prefix,
            max_id_len=connector.client_order_id_max_length,
        )
        level_id = getattr(level, "id", None)
        if not remember_open_before_send(
            executor_id,
            order_id,
            level_id,
            price_text,
            account_name=account,
            connector=connector_name,
            trading_pair=pair,
        ):
            release_quant(reserves, connector_key, pair, price_text)
            return
        if self._raw_prefix_without_hold(pair, getattr(connector, "client_order_id_prefix", None)):
            release_quant(reserves, connector_key, pair, price_text)
            return
        from hummingbot.strategy_v2.models.executors import TrackedOrder
        level.active_open_order = TrackedOrder(order_id=order_id)
        reached = self.__dict__.setdefault("_reached_place", set())
        token = _placing_order_id.set(order_id)
        self._chain_place_once(connector, reached)
        try:
            await connector._create_order(
                trade_type=executor.config.side,
                order_id=order_id,
                trading_pair=pair,
                amount=candidate.amount,
                order_type=executor.config.triple_barrier_config.open_order_type,
                price=price,
            )
        except Exception:
            logger.exception("durable open create failed")
        finally:
            _placing_order_id.reset(token)
        if order_id not in reached:
            release_quant(reserves, connector_key, pair, price_text)
            level.active_open_order = None
            self.__dict__.setdefault("_local_miss_ids", set()).add(order_id)
            return
        await self._commit_open_hold(executor_id, order_id, level_id, price_text, connector)

    async def _commit_open_hold(self, executor_id, order_id, level_id, price_text, connector) -> None:
        """Write the hold after the exchange id exists. Do not seat a fill."""
        live = (getattr(connector, "in_flight_orders", None) or {}).get(order_id)
        exchange_id = getattr(live, "exchange_order_id", None) if live is not None else None
        state = getattr(live, "current_state", None) if live is not None else None
        name = getattr(state, "name", str(state or ""))
        if not exchange_id or name not in ("OPEN", "FILLED", "PARTIALLY_FILLED"):
            return
        if not self.db_manager:
            return
        from utils.executor_checkpoint import checkpoint_plus
        try:
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)
                row = await repo.get_executor_by_id(executor_id)
                seen = row.checkpoint if row is not None else None
                body = checkpoint_plus(seen, {
                    "order_id": order_id,
                    "level_id": level_id,
                    "price": price_text,
                    "filled": name != "OPEN",
                })
                if body is None:
                    return
                changed = await repo.replace_checkpoint_if_unchanged(executor_id, seen, body)
                if changed:
                    return
                fresh = await repo.get_executor_by_id(executor_id)
                seen = fresh.checkpoint if fresh is not None else None
                if seen and order_id in seen:
                    return
                body = checkpoint_plus(seen, {
                    "order_id": order_id,
                    "level_id": level_id,
                    "price": price_text,
                    "filled": name != "OPEN",
                })
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
            result = original(*args, **kwargs)
            order_id = service._event_order_id(args)
            if marker == "filled":
                note_filled(executor, order_id, service._event_order_json(executor, order_id))
            else:
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
        try:
            captured = capture_executor(executor, executor_type)
        except Exception as e:
            logger.error(f"Error capturing checkpoint for {executor_id}: {e}", exc_info=True)
            return
        try:
            async with self.db_manager.get_session_context() as session:
                row = await ExecutorRepository(session).get_executor_by_id(executor_id)
                seen = row.checkpoint if row is not None else None
                body = checkpoint_keeping_holds(seen, captured)
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

    async def resume_running_executors(self) -> None:
        """Rebuild RUNNING executors and reattach their orders. Does not place new ones.

        Called after order reconcile and before cleanup_orphaned_executors. A row
        that cannot be reattached is left RUNNING so cleanup still marks it, and
        the unattended list still names it. Starting it from the create-config
        would open a second trade beside the first.
        """
        # Before any resumed grid can place. A prune failure must not skip resume.
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
            return
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
        self._resume_rows = rows
        self._resume_ledger_ids = ledger_ids
        for row in rows:
            for order_id in saved_order_ids(parse_checkpoint(row.get("checkpoint"))):
                owned_by.setdefault(order_id, row["executor_id"])
            for order_id in ledger_ids[row["executor_id"]]:
                owned_by.setdefault(order_id, row["executor_id"])

        resumed = 0
        for row in rows:
            if row["executor_id"] in self._active_executors:
                continue
            reason = await self._resume_one(row, owned_by)
            if reason is None:
                resumed += 1
            else:
                logger.warning(
                    f"Not resuming {row['executor_id']} ({row['executor_type']} "
                    f"{row['connector_name']} {row['trading_pair']}): {reason}"
                )
        if resumed:
            logger.info(f"Resumed {resumed} executor(s) after restart")

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

    async def _resume_one(self, row: Dict[str, Any], owned_by: Optional[Dict[str, str]] = None) -> Optional[str]:
        """None on success. A string is why this row was left for cleanup."""
        checkpoint = parse_checkpoint(row.get("checkpoint"))
        if must_keep_executor(row["executor_id"], ledger_executor_ids(), row.get("checkpoint")):
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
                checkpoint = merge_recent_ids(checkpoint, recent_order_ids(row["executor_id"]))
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
            if has_exposure(checkpoint):
                async def _skip_balance_check():
                    return None
                executor.validate_sufficient_balance = _skip_balance_check
            apply_checkpoint(executor, checkpoint, live_orders)
            notice = None
            if plan:
                self._install_touch_cap(executor, plan)
                notice = plan.get("notice")
            if row["executor_type"] == "grid_executor":
                # A throw here must not reach the outer except: that pops the row and
                # cleanup marks SYSTEM_CLEANUP while the exchange order is still live.
                try:
                    exchange_pass = await self._exchange_open_pass(account, connector, pair)
                    own_ids = set(saved_order_ids(checkpoint))
                    own_ids.update(recent_order_ids(row["executor_id"]))
                    install_occupied_open_brake(
                        executor,
                        exchange_pass,
                        lambda: self._foreign_ids_now(row["executor_id"], owned_by or {}),
                        own_ids=own_ids,
                        prior_tracker_ids=self._prior_tracker_ids(connector),
                    )
                    executor._occupied_pass_key = exchange_pass_key(account, connector, pair)
                except Exception:
                    logger.exception("occupied open brake was not installed; blocking new opens")
                    try:
                        hold_new_opens(executor)
                    except Exception:
                        logger.exception("could not block new opens; resume still continues")
            self._start_registered_executor(row["executor_id"], executor)
            started = True
            async with self.db_manager.get_session_context() as session:
                await ExecutorRepository(session).update_executor(
                    executor_id=row["executor_id"],
                    status="RUNNING",
                    error_log=notice,
                )
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

        # Trigger early stop
        try:
            executor.early_stop(keep_position=keep_position)
        except Exception as e:
            logger.error(f"Error stopping executor {executor_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Error stopping executor: {str(e)}")

        logger.info(f"Initiated stop for executor {executor_id} (keep_position={keep_position})")

        return {
            "executor_id": executor_id,
            "status": "stopping",
            "keep_position": keep_position
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

    async def _handle_executor_completion(self, executor_id: str):
        """Handle cleanup when an executor completes."""
        # Atomically claim the executor so a concurrent completion (e.g. the
        # control loop racing with the synchronous call in create_executor)
        # returns early instead of double-persisting / double-aggregating.
        if not self._executor_proofs_released(executor_id):
            return
        executor = self._active_executors.pop(executor_id, None)
        if executor is None:
            return

        metadata = self._executor_metadata.get(executor_id, {})

        # Check if this is a POSITION_HOLD close type (keep_position=True)
        if executor.close_type == CloseType.POSITION_HOLD:
            await self._aggregate_position_hold(executor_id, executor, metadata)

        # Persist final state to database
        await self._persist_executor_completed(executor_id, executor)

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
            return

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
            elif repaired:
                logger.warning(
                    f"Executor {executor_id} completed before its creation row existed; "
                    f"inserted the record from its final state"
                )
            else:
                logger.debug(f"Persisted executor {executor_id} completion to database")

        except Exception as e:
            logger.error(f"Error persisting executor completion: {e}")

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
