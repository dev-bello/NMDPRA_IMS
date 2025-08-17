from app import db
from app.models.inventory import Inventory
from app.models.inventory_transaction import InventoryTransaction
from app.models.request import Request
from decimal import Decimal

def calculate_periodic_wac_valuation(inventory_id, start_date, end_date):
    """
    Calculates inventory valuation metrics for a specific item over a given period
    using the Periodic Weighted-Average Cost (WAC) method. This function is designed
    to be accurate by processing the full transaction history to determine opening values.
    """
    # Fetch all transactions for the item up to the end of the report period.
    # This is more efficient than multiple smaller queries.
    transactions = InventoryTransaction.query.join(
        Inventory, InventoryTransaction.inventory_id == Inventory.id
    ).outerjoin(
        Request, InventoryTransaction.related_request_id == Request.id
    ).filter(
        InventoryTransaction.inventory_id == inventory_id,
        InventoryTransaction.timestamp <= end_date
    ).order_by(
        InventoryTransaction.timestamp.asc()
    ).with_entities(
        InventoryTransaction.quantity,
        InventoryTransaction.unit_price,
        InventoryTransaction.transaction_type,
        InventoryTransaction.timestamp,
        Request.location
    ).all()

    # --- Calculate Opening Balance ---
    opening_stock_qty = 0
    opening_stock_value = Decimal('0.0')
    last_known_price = Decimal('0.0')

    txns_before = [t for t in transactions if t.timestamp < start_date]

    if txns_before:
        # Find the last known price from transactions before the start date
        for t in reversed(txns_before):
            if t.transaction_type in ['initial', 'purchase', 'price_update'] and t.unit_price is not None:
                last_known_price = Decimal(t.unit_price)
                break
        
        # Calculate opening quantity by considering all transactions before the start date
        opening_stock_qty = sum(t.quantity for t in txns_before)
        opening_stock_value = Decimal(opening_stock_qty) * last_known_price

    # --- Process Transactions within the Period ---
    txns_during = [t for t in transactions if start_date <= t.timestamp]

    # --- Calculate Period WAC ---
    cost_of_goods_available = opening_stock_value
    qty_available = opening_stock_qty

    # Add the value of initial stock added during the period to the cost of goods available
    initial_stock_during = [t for t in txns_during if t.transaction_type == 'initial']
    for t in initial_stock_during:
        cost_of_goods_available += Decimal(t.quantity) * Decimal(t.unit_price or '0.0')
    
    for t in txns_during:
        if t.transaction_type in ['initial', 'purchase']:
            purchase_value = Decimal(t.quantity) * Decimal(t.unit_price or '0.0')
            cost_of_goods_available += purchase_value
            qty_available += t.quantity
        elif t.transaction_type == 'price_update':
            # A price update affects the value of the entire existing stock
            cost_of_goods_available = (qty_available * Decimal(t.unit_price or '0.0'))

    period_wac = (cost_of_goods_available / qty_available) if qty_available > 0 else last_known_price
    
    # --- Calculate Additions for the Period (for reporting purposes) ---
    purchases_qty = sum(t.quantity for t in txns_during if t.transaction_type == 'purchase')
    initial_qty_during = sum(t.quantity for t in txns_during if t.transaction_type == 'initial')
    total_additions_qty = purchases_qty + initial_qty_during

    # --- Process Issues and Adjustments for the Period ---
    hq_issues = abs(sum(t.quantity for t in txns_during if t.transaction_type == 'issue' and t.location == 'Headquarters'))
    jabi_issues = abs(sum(t.quantity for t in txns_during if t.transaction_type == 'issue' and t.location == 'Jabi'))
    # Capture issues that may not have a location specified in the request
    other_issues = abs(sum(t.quantity for t in txns_during if t.transaction_type == 'issue' and not t.location))
    total_issued_qty = hq_issues + jabi_issues + other_issues

    adjustments = sum(t.quantity for t in txns_during if t.transaction_type == 'adjustment')

    # --- Calculate Closing Balance and COGS ---
    closing_stock_qty = qty_available - total_issued_qty + adjustments
    closing_stock_value = Decimal(closing_stock_qty) * period_wac
    cogs = Decimal(total_issued_qty) * period_wac

    return {
        'opening_stock': opening_stock_qty,
        'purchases': total_additions_qty,
        'adjustments': adjustments,
        'hq_issues': hq_issues,
        'jabi_issues': jabi_issues,
        'closing_stock': closing_stock_qty,
        'unit_price': period_wac,
        'total_value': closing_stock_value,
        'cogs': cogs
    }