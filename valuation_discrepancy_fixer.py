#!/usr/bin/env python3
"""
Fund Value Updater Module

This module provides functionality to check for discrepancies between expected
and calculated Start of Day Fund Values on valuation dates and creates correction
transactions as needed.
"""

import sqlite3
from datetime import datetime, timedelta
import os
import sys


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


def check_fund_value_discrepancies(db_path: str = "daily_accounting.db") -> list:
    """
    Check for discrepancies between expected and calculated Start of Day Fund Values
    on valuation dates.
    
    Returns:
        list: List of dictionaries containing discrepancy information
    """
    # Check if database exists
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file '{db_path}' not found.")
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    try:
        # Get all valuation dates (both custom and first-of-month)
        cur.execute("SELECT \"Date\", \"Fund Value\" FROM valuation_dates")
        valuation_data = cur.fetchall()
        extra_vals = {row[0] for row in valuation_data}
        valuation_fund_values = {row[0]: row[1] for row in valuation_data if row[1] is not None}
        
        # Get all dates from overall table
        cur.execute("SELECT \"Date\" FROM overall ORDER BY \"Date\"")
        overall_dates = [row[0] for row in cur.fetchall()]
        
        if not overall_dates:
            conn.close()
            return []
        
        first_month_dates = _get_first_month_dates(overall_dates)
        
        # Get overall table data for fund value calculations
        cur.execute("""
            SELECT "Date", "Start of Day Fund Value", "Total Fund Value" 
            FROM overall 
            ORDER BY "Date"
        """)
        overall_data = cur.fetchall()
        
        if not overall_data:
            conn.close()
            return []
        
        # Create dictionaries for quick lookup
        start_of_day_values = {row[0]: row[1] for row in overall_data}
        total_fund_values = {row[0]: row[2] for row in overall_data}
        
        # Get overnight transaction amounts by date
        cur.execute("""
            SELECT "Date", SUM("Amount") 
            FROM other_transactions 
            WHERE "Overnight" = 1 
            GROUP BY "Date"
        """)
        overnight_amounts = {row[0]: (row[1] or 0.0) for row in cur.fetchall()}
        
        discrepancies = []
        
        # Create sorted list of dates for finding previous business day
        sorted_dates = sorted(overall_dates, key=_parse_date)
        
        # Check each date to see if it's a valuation date
        for i, date_str in enumerate(sorted_dates):
            date_obj = _parse_date(date_str)
            
            if _is_valuation_date(date_obj, extra_vals, first_month_dates):
                # This is a valuation date - check for discrepancies
                expected_start_of_day = start_of_day_values.get(date_str)
                
                if expected_start_of_day is None:
                    continue
                
                # Find previous business day from database dates
                prev_day = None
                if i > 0:  # If not the first date
                    prev_day = sorted_dates[i - 1]
                
                if prev_day is None:
                    continue
                
                prev_total_fund_value = total_fund_values.get(prev_day)
                prev_overnight = overnight_amounts.get(prev_day, 0.0)
                
                if prev_total_fund_value is not None:
                    calculated_start_of_day = prev_total_fund_value + prev_overnight
                    
                    # Check for discrepancy (use small tolerance for floating point comparison)
                    tolerance = 0.1  # 10 cents tolerance
                    if abs(expected_start_of_day - calculated_start_of_day) > tolerance:
                        discrepancy_amount = calculated_start_of_day - expected_start_of_day
                        
                        discrepancies.append({
                            'valuation_date': date_str,
                            'previous_day': prev_day,
                            'expected_start_of_day': expected_start_of_day,
                            'calculated_start_of_day': calculated_start_of_day,
                            'discrepancy_amount': discrepancy_amount
                        })
        
        conn.close()
        return discrepancies
        
    except Exception as e:
        conn.close()
        raise e


def add_correction_transaction(date_str: str, amount: float, db_path: str = "daily_accounting.db") -> bool:
    """
    Add a correction transaction to the other_transactions table.
    
    Args:
        date_str (str): Date in MM/DD/YYYY format
        amount (float): Correction amount
        db_path (str): Path to database file
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS other_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                "Date" TEXT NOT NULL,
                "Amount" REAL,
                "Account Description" TEXT,
                "Transaction Description" TEXT,
                "Counted in P&L" BOOLEAN,
                "Overnight" BOOLEAN,
                "Additional Info" TEXT,
                UNIQUE("Date", "Account Description", "Transaction Description", "Amount")
            )
        ''')
        
        # Insert the correction transaction
        cursor.execute('''
            INSERT INTO other_transactions 
            ("Date", "Amount", "Account Description", "Transaction Description", 
             "Counted in P&L", "Overnight", "Additional Info")
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            date_str,
            amount,
            "Correction",
            "Valuation Correction",
            False,  # Counted in P&L = false
            True,   # Overnight = true
            f"Automatic correction for valuation discrepancy"
        ))
        
        conn.commit()
        conn.close()
        return True
        
    except sqlite3.IntegrityError:
        # Transaction already exists
        conn.close()
        return False
    except Exception as e:
        conn.close()
        raise e


def update_fund_values(db_path: str = "daily_accounting.db", auto_confirm: bool = False) -> bool:
    """
    Main function to check for fund value discrepancies and create correction transactions.
    
    Args:
        db_path (str): Path to the SQLite database file
        auto_confirm (bool): If True, automatically confirm all corrections without prompting
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print("Checking for fund value discrepancies...")
        
        # Import necessary modules for updating overall table
        import overall_table
        
        corrections_added = 0
        max_iterations = 100  # Safety limit to prevent infinite loops
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            
            # Check for discrepancies with current database state
            discrepancies = check_fund_value_discrepancies(db_path)
            
            if not discrepancies:
                if corrections_added == 0:
                    print("✓ No fund value discrepancies found.")
                else:
                    print(f"✓ All discrepancies corrected. Total corrections added: {corrections_added}")
                return True
            
            # Process only the first discrepancy found
            disc = discrepancies[0]
            
            print(f"\nFound discrepancy requiring correction:")
            print(f"   Valuation Date: {disc['valuation_date']}")
            print(f"   Previous Day: {disc['previous_day']}")
            print(f"   Expected Start of Day Fund Value: ${disc['expected_start_of_day']:,.2f}")
            print(f"   Calculated Start of Day Fund Value: ${disc['calculated_start_of_day']:,.2f}")
            print(f"   Discrepancy: ${disc['discrepancy_amount']:,.2f}")
            print(f"   Proposed Correction Transaction:")
            print(f"     Date: {disc['previous_day']}")
            print(f"     Amount: ${-disc['discrepancy_amount']:,.2f}")
            print(f"     Account Description: Correction")
            print(f"     Transaction Description: Valuation Correction")
            print(f"     Counted in P&L: false")
            print(f"     Overnight: true")
            
            # Ask for confirmation unless auto_confirm is True
            add_correction = auto_confirm
            if not auto_confirm:
                response = input(f"\n   Add this correction transaction? (y/N): ")
                add_correction = response.lower() in ['y', 'yes']
            
            if add_correction:
                # Add the correction transaction
                success = add_correction_transaction(
                    disc['previous_day'], 
                    -disc['discrepancy_amount'], 
                    db_path
                )
                
                if success:
                    corrections_added += 1
                    print(f"✓ Added correction transaction for {disc['previous_day']}: ${-disc['discrepancy_amount']:,.2f}")
                    
                    # Rebuild overall table to reflect the new transaction
                    print("   Updating overall table...")
                    overall_table.build_overall_table(db_path)
                    print("   ✓ Overall table updated.")
                    
                    # Continue to check for remaining discrepancies
                    continue
                else:
                    print(f"✗ Failed to add correction transaction for {disc['previous_day']} (may already exist)")
                    # If we couldn't add the transaction, break to avoid infinite loop
                    break
            else:
                print("   Correction declined.")
                # If user declined this correction, check if there are more
                if len(discrepancies) > 1:
                    print(f"   Skipping this correction. {len(discrepancies) - 1} more discrepancies found.")
                    # For now, we'll break here. In a more sophisticated version,
                    # we could ask about each discrepancy individually
                break
        
        if iteration >= max_iterations:
            print(f"✗ Maximum iterations ({max_iterations}) reached. There may be remaining discrepancies.")
            return False
        
        if corrections_added > 0:
            print(f"\n✓ Process completed. Total corrections added: {corrections_added}")
        else:
            print("\n✓ Process completed. No corrections were added.")
        
        return True
        
    except Exception as e:
        print(f"✗ Error updating fund values: {str(e)}")
        return False


if __name__ == '__main__':
    # For testing purposes
    update_fund_values() 