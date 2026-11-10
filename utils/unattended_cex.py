"""CEX executors the API marked dead after a restart, without touching the exchange.

``cleanup_orphaned_executors`` rewrites every leftover ``RUNNING`` row to
``TERMINATED`` / ``SYSTEM_CLEANUP``. It does not cancel orders and it does not
flatten a position. This module only names those CEX rows so an operator can
see them. It never places or cancels an order.

``lp_executor`` is excluded on purpose. On-chain leftovers already have
``GET /executors/positions/orphaned``, and that listing tells an agent how to
close a pool position. A CEX row must not land there.
"""
import json
from typing import Any, Dict, FrozenSet, Optional

# Types the API can start that trade on a CEX (or a CEX-style connector), not a
# CLMM pool. Same set as ExecutorService.EXECUTOR_REGISTRY minus lp_executor.
CEX_UNATTENDED_TYPES: FrozenSet[str] = frozenset({
    "position_executor",
    "grid_executor",
    "dca_executor",
    "twap_executor",
    "arbitrage_executor",
    "xemm_executor",
    "order_executor",
})

UNATTENDED_WARNING = (
    "Exchange may still hold a position or an open order with no owner. "
    "This listing does not close anything and does not resume the executor."
)


def parse_final_state(raw: Optional[str]) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def is_unattended_cex(
    executor_type: Optional[str],
    close_type: Optional[str],
    final_state: Optional[Dict[str, Any]] = None,
) -> bool:
    """True when this row is a CEX restart leftover that has not been silenced."""
    if close_type != "SYSTEM_CLEANUP":
        return False
    if executor_type not in CEX_UNATTENDED_TYPES:
        return False
    if final_state and final_state.get("orphan_resolved"):
        return False
    return True


def unattended_view(record: Any) -> Optional[Dict[str, Any]]:
    """A warning row, or None when this record is not an unattended CEX executor."""
    final_state = parse_final_state(getattr(record, "final_state", None))
    if not is_unattended_cex(record.executor_type, record.close_type, final_state):
        return None
    closed_at = record.closed_at
    return {
        "executor_id": record.executor_id,
        "executor_type": record.executor_type,
        "account_name": record.account_name,
        "connector_name": record.connector_name,
        "trading_pair": record.trading_pair,
        "controller_id": record.controller_id or "main",
        "close_type": record.close_type,
        "closed_at": closed_at.isoformat() if closed_at else None,
        "warning": UNATTENDED_WARNING,
    }
