import sqlite3
from datetime import datetime, timedelta


def _parse_date(date_str: str) -> datetime:
    """Helper to parse dates in MM/DD/YYYY format to datetime object."""
    return datetime.strptime(date_str, "%m/%d/%Y")


def _date_to_str(date_obj: datetime) -> str:
    """Convert datetime back to MM/DD/YYYY string."""
    return date_obj.strftime("%m/%d/%Y")


def _is_valuation_date(date_obj: datetime, extra_dates: set, first_month_dates: set) -> bool:
    """Return True if the date is a valuation date (first instance of month or user-specified)."""
    if _date_to_str(date_obj) in extra_dates:
        return True
    return _date_to_str(date_obj) in first_month_dates


def _get_first_month_dates(date_strings: list) -> set:
    """Get the first occurrence of each month from a list of date strings."""
    first_dates = set()
    months_seen = set()
    
    # Sort dates chronologically
    sorted_dates = sorted(date_strings, key=_parse_date)
    
    for date_str in sorted_dates:
        date_obj = _parse_date(date_str)
        month_year = (date_obj.year, date_obj.month)
        
        if month_year not in months_seen:
            first_dates.add(date_str)
            months_seen.add(month_year)
    
    return first_dates


def build_overall_table(db_path: str = "daily_accounting.db") -> None:
    """Recalculate the `overall` table inside the supplied SQLite database.

    The table schema is created if it does not yet exist.  This routine will:
    1. Pull daily records from the `broker` table (Date, P&L, Total Broker).
    2. Aggregate `Amount` from `other_transactions` for each date (can be zero).
    3. Compute End Fund Value (Accounts Total) = Total Broker + SUM(Amount).
    4. Determine the Period Starting NAV according to valuation-date rules.
       * A valuation date is the first occurrence of each month in the database 
         OR a date listed in the `valuation_dates` table (managed by the user).
       * The Period Starting NAV for a valuation period is the Start Fund Value (Accounts Total)
         recorded on the valuation date.
    5. Compute Start Fund Value (NAV + Cum. P&L) = Period Starting NAV + cumulative P&L since last valuation date.
    6. Compute End Fund Value (NAV + Cum. P&L) = Start Fund Value (NAV + Cum. P&L) + current day's Total P&L.

    The resulting data are stored in (and replace existing rows of) the
    `overall` table with schema:
        Date TEXT PRIMARY KEY,
        "Broker P&L" REAL,
        "Total Broker" REAL,
        "Other P&L" REAL,
        "Total Other" REAL,
        "Total P&L" REAL,
        "Period Starting NAV" REAL,
        "Start Fund Value (Accounts Total)" REAL,
        "End Fund Value (Accounts Total)" REAL,
        "Start Fund Value (NAV + Cum. P&L)" REAL,
        "End Fund Value (NAV + Cum. P&L)" REAL
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Ensure supporting tables exist ---------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS valuation_dates (
            "Date" TEXT PRIMARY KEY,
            "Fund Value" REAL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS overall (
            "Date" TEXT PRIMARY KEY,
            "Broker P&L" REAL,
            "Total Broker" REAL,
            "Other P&L" REAL,
            "Total Other" REAL,
            "Total P&L" REAL,
            "Period Starting NAV" REAL,
            "Start Fund Value (Accounts Total)" REAL,
            "End Fund Value (Accounts Total)" REAL,
            "Start Fund Value (NAV + Cum. P&L)" REAL,
            "End Fund Value (NAV + Cum. P&L)" REAL
        )
        """
    )

    # Gather user-specified valuation dates ---------------------------------------------------
    cur.execute("SELECT \"Date\", \"Fund Value\" FROM valuation_dates")
    valuation_data = cur.fetchall()
    extra_vals = {row[0] for row in valuation_data}
    valuation_fund_values = {row[0]: row[1] for row in valuation_data if row[1] is not None}

    # Pull broker rows (return early if broker table not present yet)
    try:
        cur.execute(
            "SELECT \"Date\", \"P&L\", \"Total Broker\" FROM broker"
        )
        broker_rows = cur.fetchall()
    except sqlite3.OperationalError:
        conn.close()
        return  # Broker table missing, nothing to aggregate yet

    if not broker_rows:
        conn.close()
        return  # Nothing to do yet

    # Order chronologically using parsed dates
    broker_rows.sort(key=lambda r: _parse_date(r[0]))

    # Get first occurrence dates for each month
    all_broker_dates = [row[0] for row in broker_rows]
    first_month_dates = _get_first_month_dates(all_broker_dates)

    # Helper dicts for other transaction data
    other_amounts = {}
    other_pl_amounts = {}
    overnight_amounts = {}  # Add dict for overnight transactions
    try:
        # Get total amounts per date for all other transactions
        cur.execute(
            "SELECT \"Date\", SUM(\"Amount\") FROM other_transactions GROUP BY \"Date\""
        )
        other_amounts = {row[0]: (row[1] or 0.0) for row in cur.fetchall()}
        
        # Get P&L amounts per date for transactions counted in P&L
        cur.execute(
            "SELECT \"Date\", SUM(\"Amount\") FROM other_transactions WHERE \"Counted in P&L\" = 1 GROUP BY \"Date\""
        )
        other_pl_amounts = {row[0]: (row[1] or 0.0) for row in cur.fetchall()}
        
        # Get overnight amounts per date for transactions where Overnight is true
        cur.execute(
            "SELECT \"Date\", SUM(\"Amount\") FROM other_transactions WHERE \"Overnight\" = 1 GROUP BY \"Date\""
        )
        overnight_amounts = {row[0]: (row[1] or 0.0) for row in cur.fetchall()}
    except sqlite3.OperationalError:
        # Table may not exist yet if no other transactions have been loaded
        other_amounts = {}
        other_pl_amounts = {}
        overnight_amounts = {}

    # Calculate Total Other (running total starting from on 01/19/2023)
    # First, get all unique dates from both broker and other transactions and sort them
    all_dates = set(row[0] for row in broker_rows)
    all_dates.update(other_amounts.keys())
    all_dates = sorted(all_dates, key=_parse_date)
    
    total_other_by_date = {}
    running_total_other = 0.0
    start_date = datetime.strptime('01/19/2023', '%m/%d/%Y')
    
    for date_str in all_dates:
        current_date = _parse_date(date_str)
        
        # Initialize running total
        if current_date == start_date:
            running_total_other = 0
        
        # Get sum of all other transactions for this date
        daily_other_total = other_amounts.get(date_str, 0.0)
        
        # Add daily total to running total
        running_total_other += daily_other_total
        
        total_other_by_date[date_str] = running_total_other

    # Iterate and compute --------------------------------------------------------------------
    results = []  # list of tuples for all the columns
    period_start_nav = None  # will be updated when we hit valuation dates
    prev_end_fund_value_accounts = None  # needed for calculating start fund value (accounts total)
    prev_date_str = None  # track previous date for overnight calculations
    
    # Track cumulative P&L since last valuation date for NAV + Cum. P&L calculations
    cumulative_pl_since_valuation = 0.0
    last_valuation_date_idx = None

    for idx, (date_str, broker_pl, total_broker) in enumerate(broker_rows):
        # Get other transaction data
        other_pl = other_pl_amounts.get(date_str, 0.0)
        total_other = total_other_by_date.get(date_str, 0.0)
        overnight_today = overnight_amounts.get(date_str, 0.0)  # Overnight transactions for today
        
        # Calculate totals
        total_pl = (broker_pl or 0.0) + other_pl
        
        # Calculate End Fund Value (Accounts Total): Total Broker + Total Other - overnight transactions today
        end_fund_value_accounts = (total_broker or 0.0) + total_other - overnight_today

        date_obj = _parse_date(date_str)

        # Calculate Start Fund Value (Accounts Total)
        # Use valuation fund value if specified, otherwise calculate based on previous day
        if date_str in valuation_fund_values:
            start_fund_value_accounts = valuation_fund_values[date_str]
        else:
            if prev_end_fund_value_accounts is not None and prev_date_str is not None:
                # Previous day's End Fund Value (Accounts Total) + previous day's overnight transactions
                prev_overnight = overnight_amounts.get(prev_date_str, 0.0)
                start_fund_value_accounts = prev_end_fund_value_accounts + prev_overnight
            else:
                # First day or no previous data
                start_fund_value_accounts = end_fund_value_accounts

        # If today is a valuation date => update period_start_nav and reset cumulative P&L
        if _is_valuation_date(date_obj, extra_vals, first_month_dates):
            period_start_nav = start_fund_value_accounts
            cumulative_pl_since_valuation = 0.0
            last_valuation_date_idx = idx

        # Calculate Start Fund Value (NAV + Cum. P&L)
        # This is Period Starting NAV + cumulative P&L from last valuation date to previous day
        start_fund_value_nav_cum_pl = period_start_nav + cumulative_pl_since_valuation if period_start_nav is not None else None
        
        # Calculate End Fund Value (NAV + Cum. P&L)
        # This is Start Fund Value (NAV + Cum. P&L) + current day's Total P&L
        end_fund_value_nav_cum_pl = start_fund_value_nav_cum_pl + total_pl if start_fund_value_nav_cum_pl is not None else None
        
        # Update cumulative P&L for next iteration (add current day's P&L)
        cumulative_pl_since_valuation += total_pl

        results.append(
            (
                date_str,
                broker_pl if broker_pl is not None else None,
                total_broker if total_broker is not None else None,
                other_pl if other_pl is not None else None,
                total_other if total_other is not None else None,
                total_pl if total_pl is not None else None,
                period_start_nav if period_start_nav is not None else None,
                start_fund_value_accounts if start_fund_value_accounts is not None else None,
                end_fund_value_accounts if end_fund_value_accounts is not None else None,
                start_fund_value_nav_cum_pl if start_fund_value_nav_cum_pl is not None else None,
                end_fund_value_nav_cum_pl if end_fund_value_nav_cum_pl is not None else None,
            )
        )

        # Update previous values for next iteration
        prev_end_fund_value_accounts = end_fund_value_accounts
        prev_date_str = date_str

    # Clear and insert -----------------------------------------------------------------------
    cur.execute("DELETE FROM overall")
    cur.executemany(
        """
        INSERT INTO overall ("Date", "Broker P&L", "Total Broker", "Other P&L", "Total Other", "Total P&L", "Period Starting NAV", "Start Fund Value (Accounts Total)", "End Fund Value (Accounts Total)", "Start Fund Value (NAV + Cum. P&L)", "End Fund Value (NAV + Cum. P&L)")
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        results,
    )

    conn.commit()
    conn.close() 