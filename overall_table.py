"""
Overall Table Manager

This module provides functionality to build and manage the overall table
that aggregates data from broker transactions, other transactions, and
valuation dates for the Daily Accounting Tool.

Features:
- Aggregates data from multiple sources
- Calculates period starting NAV based on valuation dates
- Computes cumulative P&L and fund values
- Handles overnight transactions properly
- Provides comprehensive error handling and logging
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class OverallTableManager:
    """
    A class to manage the overall table that aggregates all transaction data.
    """
    
    # Database schema for the overall table
    OVERALL_TABLE_SCHEMA = {
        'Date': 'TEXT PRIMARY KEY',
        'Broker P&L': 'REAL',
        'Total Broker': 'REAL',
        'Other P&L': 'REAL',
        'Total Other': 'REAL',
        'Total P&L': 'REAL',
        'Period Starting NAV': 'REAL',
        'Start Fund Value (Accounts Total)': 'REAL',
        'End Fund Value (Accounts Total)': 'REAL',
        'Start Fund Value (NAV + Cum. P&L)': 'REAL',
        'End Fund Value (NAV + Cum. P&L)': 'REAL'
    }
    
    def __init__(self, db_path: str = "daily_accounting.db"):
        """
        Initialize the OverallTableManager with database path.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.start_date = datetime.strptime('01/19/2023', '%m/%d/%Y')
    
    def _parse_date(self, date_str: str) -> datetime:
        """Helper to parse dates in MM/DD/YYYY format to datetime object."""
        return datetime.strptime(date_str, "%m/%d/%Y")
    
    def _date_to_str(self, date_obj: datetime) -> str:
        """Convert datetime back to MM/DD/YYYY string."""
        return date_obj.strftime("%m/%d/%Y")
    
    def _is_valuation_date(self, date_obj: datetime, extra_dates: Set[str], first_month_dates: Set[str]) -> bool:
        """Return True if the date is a valuation date (first instance of month or user-specified)."""
        date_str = self._date_to_str(date_obj)
        return date_str in extra_dates or date_str in first_month_dates
    
    def _get_first_month_dates(self, date_strings: List[str]) -> Set[str]:
        """Get the first occurrence of each month from a list of date strings."""
        first_dates = set()
        months_seen = set()
        
        # Sort dates chronologically
        sorted_dates = sorted(date_strings, key=self._parse_date)
        
        for date_str in sorted_dates:
            date_obj = self._parse_date(date_str)
            month_year = (date_obj.year, date_obj.month)
            
            if month_year not in months_seen:
                first_dates.add(date_str)
                months_seen.add(month_year)
        
        return first_dates
    
    def _create_supporting_tables(self, cursor: sqlite3.Cursor) -> None:
        """Create supporting tables if they don't exist."""
        # Create valuation_dates table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS valuation_dates (
                "Date" TEXT PRIMARY KEY,
                "Fund Value" REAL
            )
            """
        )
        
        # Create overall table
        field_definitions = [f'"{field}" {field_type}' for field, field_type in self.OVERALL_TABLE_SCHEMA.items()]
        create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS overall (
                {', '.join(field_definitions)}
            )
        '''
        cursor.execute(create_table_sql)
    
    def _get_valuation_data(self, cursor: sqlite3.Cursor) -> Tuple[Set[str], Dict[str, float]]:
        """Get user-specified valuation dates and their fund values."""
        cursor.execute("SELECT \"Date\", \"Fund Value\" FROM valuation_dates")
        valuation_data = cursor.fetchall()
        
        extra_vals = {row[0] for row in valuation_data}
        valuation_fund_values = {row[0]: row[1] for row in valuation_data if row[1] is not None}
        
        return extra_vals, valuation_fund_values
    
    def _get_broker_data(self, cursor: sqlite3.Cursor) -> Optional[List[Tuple]]:
        """Get broker transaction data, return None if table doesn't exist."""
        try:
            cursor.execute(
                "SELECT \"Date\", \"P&L\", \"Total Broker\" FROM broker"
            )
            broker_rows = cursor.fetchall()
            
            if not broker_rows:
                logger.warning("No broker data found")
                return None
            
            # Sort chronologically
            broker_rows.sort(key=lambda r: self._parse_date(r[0]))
            return broker_rows
            
        except sqlite3.OperationalError as e:
            logger.warning(f"Broker table not found: {e}")
            return None
    
    def _get_other_transaction_data(self, cursor: sqlite3.Cursor) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]:
        """Get other transaction data aggregated by date."""
        other_amounts = {}
        other_pl_amounts = {}
        overnight_amounts = {}
        
        try:
            # Get total amounts per date for all other transactions
            cursor.execute(
                "SELECT \"Date\", SUM(\"Amount\") FROM other_transactions GROUP BY \"Date\""
            )
            other_amounts = {row[0]: (row[1] or 0.0) for row in cursor.fetchall()}
            
            # Get P&L amounts per date for transactions counted in P&L
            cursor.execute(
                "SELECT \"Date\", SUM(\"Amount\") FROM other_transactions WHERE \"Counted in P&L\" = 1 GROUP BY \"Date\""
            )
            other_pl_amounts = {row[0]: (row[1] or 0.0) for row in cursor.fetchall()}
            
            # Get overnight amounts per date for transactions where Overnight is true
            cursor.execute(
                "SELECT \"Date\", SUM(\"Amount\") FROM other_transactions WHERE \"Overnight\" = 1 GROUP BY \"Date\""
            )
            overnight_amounts = {row[0]: (row[1] or 0.0) for row in cursor.fetchall()}
            
        except sqlite3.OperationalError as e:
            logger.warning(f"Other transactions table not found: {e}")
        
        return other_amounts, other_pl_amounts, overnight_amounts
    
    def _calculate_total_other_by_date(self, broker_rows: List[Tuple], other_amounts: Dict[str, float]) -> Dict[str, float]:
        """Calculate running total of other transactions starting from 01/19/2023."""
        # Get all unique dates and sort them
        all_dates = set(row[0] for row in broker_rows)
        all_dates.update(other_amounts.keys())
        all_dates = sorted(all_dates, key=self._parse_date)
        
        total_other_by_date = {}
        running_total_other = 0.0
        
        for date_str in all_dates:
            current_date = self._parse_date(date_str)
            
            # Initialize running total at start date
            if current_date == self.start_date:
                running_total_other = 0
            
            # Add daily total to running total
            daily_other_total = other_amounts.get(date_str, 0.0)
            running_total_other += daily_other_total
            
            total_other_by_date[date_str] = running_total_other
        
        return total_other_by_date
    
    def _calculate_fund_values(self, broker_rows: List[Tuple], 
                              total_other_by_date: Dict[str, float],
                              other_pl_amounts: Dict[str, float],
                              overnight_amounts: Dict[str, float],
                              extra_vals: Set[str],
                              first_month_dates: Set[str],
                              valuation_fund_values: Dict[str, float]) -> List[Tuple]:
        """Calculate all fund values and P&L metrics."""
        results = []
        period_start_nav = None
        prev_end_fund_value_accounts = None
        prev_date_str = None
        cumulative_pl_since_valuation = 0.0
        
        for idx, (date_str, broker_pl, total_broker) in enumerate(broker_rows):
            # Get transaction data for this date
            other_pl = other_pl_amounts.get(date_str, 0.0)
            total_other = total_other_by_date.get(date_str, 0.0)
            overnight_today = overnight_amounts.get(date_str, 0.0)
            
            # Calculate totals
            total_pl = (broker_pl or 0.0) + other_pl
            
            # Calculate End Fund Value (Accounts Total)
            end_fund_value_accounts = (total_broker or 0.0) + total_other - overnight_today
            
            date_obj = self._parse_date(date_str)
            
            # Calculate Start Fund Value (Accounts Total)
            if date_str in valuation_fund_values:
                start_fund_value_accounts = valuation_fund_values[date_str]
            else:
                if prev_end_fund_value_accounts is not None and prev_date_str is not None:
                    # Previous day's End Fund Value + previous day's overnight transactions
                    prev_overnight = overnight_amounts.get(prev_date_str, 0.0)
                    start_fund_value_accounts = prev_end_fund_value_accounts + prev_overnight
                else:
                    start_fund_value_accounts = end_fund_value_accounts
            
            # Check if this is a valuation date
            if self._is_valuation_date(date_obj, extra_vals, first_month_dates):
                period_start_nav = start_fund_value_accounts
                cumulative_pl_since_valuation = 0.0
            
            # Calculate NAV + Cum. P&L values
            start_fund_value_nav_cum_pl = (period_start_nav + cumulative_pl_since_valuation 
                                         if period_start_nav is not None else None)
            end_fund_value_nav_cum_pl = (start_fund_value_nav_cum_pl + total_pl 
                                       if start_fund_value_nav_cum_pl is not None else None)
            
            # Update cumulative P&L for next iteration
            cumulative_pl_since_valuation += total_pl
            
            # Build result tuple
            result_tuple = (
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
            
            results.append(result_tuple)
            
            # Update for next iteration
            prev_end_fund_value_accounts = end_fund_value_accounts
            prev_date_str = date_str
        
        return results
    
    def _insert_results(self, cursor: sqlite3.Cursor, results: List[Tuple]) -> None:
        """Insert results into the overall table."""
        # Clear existing data
        cursor.execute("DELETE FROM overall")
        
        # Insert new data
        insert_sql = '''
            INSERT INTO overall (
                "Date", "Broker P&L", "Total Broker", "Other P&L", "Total Other", "Total P&L", 
                "Period Starting NAV", "Start Fund Value (Accounts Total)", "End Fund Value (Accounts Total)", 
                "Start Fund Value (NAV + Cum. P&L)", "End Fund Value (NAV + Cum. P&L)"
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cursor.executemany(insert_sql, results)
    
    def build_overall_table(self) -> bool:
        """
        Recalculate the overall table inside the SQLite database.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Building overall table in database: {self.db_path}")
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create supporting tables if needed
                self._create_supporting_tables(cursor)
                
                # Get valuation data
                extra_vals, valuation_fund_values = self._get_valuation_data(cursor)
                
                # Get broker data
                broker_rows = self._get_broker_data(cursor)
                if not broker_rows:
                    logger.warning("No broker data available, skipping overall table build")
                    return False
                
                # Get other transaction data
                other_amounts, other_pl_amounts, overnight_amounts = self._get_other_transaction_data(cursor)
                
                # Calculate running totals for other transactions
                total_other_by_date = self._calculate_total_other_by_date(broker_rows, other_amounts)
                
                # Get first month dates
                all_broker_dates = [row[0] for row in broker_rows]
                first_month_dates = self._get_first_month_dates(all_broker_dates)
                
                # Calculate all fund values and metrics
                results = self._calculate_fund_values(
                    broker_rows, total_other_by_date, other_pl_amounts, overnight_amounts,
                    extra_vals, first_month_dates, valuation_fund_values
                )
                
                # Insert results into database
                self._insert_results(cursor, results)
                
                conn.commit()
                logger.info(f"Successfully built overall table with {len(results)} records")
                return True
                
        except Exception as e:
            logger.error(f"Error building overall table: {e}")
            return False
    
    def get_table_stats(self) -> Optional[Dict]:
        """
        Get statistics about the overall table.
        
        Returns:
            Dictionary with table statistics or None if error
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if table exists
                cursor.execute('''
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='overall'
                ''')
                
                if not cursor.fetchone():
                    return None
                
                # Get row count
                cursor.execute('SELECT COUNT(*) FROM overall')
                row_count = cursor.fetchone()[0]
                
                # Get date range
                cursor.execute('SELECT MIN("Date"), MAX("Date") FROM overall')
                date_range = cursor.fetchone()
                
                # Get some aggregate statistics
                cursor.execute('''
                    SELECT 
                        SUM("Total P&L") as total_pl,
                        AVG("Total P&L") as avg_pl,
                        MIN("Total P&L") as min_pl,
                        MAX("Total P&L") as max_pl
                    FROM overall
                ''')
                pl_stats = cursor.fetchone()
                
                return {
                    'row_count': row_count,
                    'date_range': date_range,
                    'total_pl': pl_stats[0],
                    'avg_pl': pl_stats[1],
                    'min_pl': pl_stats[2],
                    'max_pl': pl_stats[3]
                }
                
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
            return None


# Legacy function wrapper for backward compatibility
def build_overall_table(db_path: str = "daily_accounting.db") -> None:
    """Legacy wrapper for build_overall_table."""
    manager = OverallTableManager(db_path)
    manager.build_overall_table() 