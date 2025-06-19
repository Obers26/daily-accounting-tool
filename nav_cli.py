#!/usr/bin/env python3
"""
NAV Data Management CLI Tool

This tool provides command-line interface for processing CSV files containing 
Net Asset Value (NAV) data and generating Excel reports.
"""

import argparse
import os
import sys
from datetime import datetime
import sqlite3
import csv

# Import functions from existing modules
import brokerCSV_to_SQLite
import otherCSV_to_SQLite
import valuationCSV_to_SQLite
from Excel_Report_Generator import generate_excel_report
import overall_table
import valuation_discrepancy_fixer

def validate_date(date_string):
    """Validate date format MM/DD/YYYY"""
    try:
        datetime.strptime(date_string, '%m/%d/%Y')
        return True
    except ValueError:
        return False

def load_broker_csv(args):
    """Load a single broker CSV file into the database"""
    if not os.path.exists(args.csv_file):
        print(f"Error: CSV file '{args.csv_file}' not found.")
        return False
    
    if not args.csv_file.lower().endswith('.csv'):
        print(f"Error: File '{args.csv_file}' is not a CSV file.")
        return False
    
    print(f"Loading broker CSV file: {args.csv_file}")
    print(f"Database: {args.database}")
    
    success, message = brokerCSV_to_SQLite.update_database(args.csv_file, args.database)
    if success:
        print(f"✓ {message}")
        return True
    else:
        print(f"✗ {message}")
        return False

def load_broker_folder(args):
    """Load all broker CSV files from a folder into the database"""
    if not os.path.exists(args.csv_folder):
        print(f"Error: Folder '{args.csv_folder}' not found.")
        return False
    
    if not os.path.isdir(args.csv_folder):
        print(f"Error: '{args.csv_folder}' is not a directory.")
        return False
    
    print(f"Loading broker CSV files from folder: {args.csv_folder}")
    print(f"Database: {args.database}")
    
    success, message = brokerCSV_to_SQLite.process_all_files(args.csv_folder, args.database)
    if success:
        print(f"✓ {message}")
        return True
    else:
        print(f"✗ {message}")
        return False

def load_other_csv(args):
    """Load a single other transactions CSV file into the database"""
    if not os.path.exists(args.csv_file):
        print(f"Error: CSV file '{args.csv_file}' not found.")
        return False
    
    if not args.csv_file.lower().endswith('.csv'):
        print(f"Error: File '{args.csv_file}' is not a CSV file.")
        return False
    
    print(f"Loading other transactions CSV file: {args.csv_file}")
    print(f"Database: {args.database}")
    
    success, message = otherCSV_to_SQLite.update_database(args.csv_file, args.database)
    if success:
        print(f"✓ {message}")
        return True
    else:
        print(f"✗ {message}")
        return False

def load_other_folder(args):
    """Load all other transactions CSV files from a folder into the database"""
    if not os.path.exists(args.csv_folder):
        print(f"Error: Folder '{args.csv_folder}' not found.")
        return False
    
    if not os.path.isdir(args.csv_folder):
        print(f"Error: '{args.csv_folder}' is not a directory.")
        return False
    
    print(f"Loading other transactions CSV files from folder: {args.csv_folder}")
    print(f"Database: {args.database}")
    
    success, message = otherCSV_to_SQLite.process_all_files(args.csv_folder, args.database)
    if success:
        print(f"✓ {message}")
        return True
    else:
        print(f"✗ {message}")
        return False

def load_valuation_csv(args):
    """Load valuation dates from a CSV file into the database"""
    if not os.path.exists(args.csv_file):
        print(f"Error: CSV file '{args.csv_file}' not found.")
        return False
    
    if not args.csv_file.lower().endswith('.csv'):
        print(f"Error: File '{args.csv_file}' is not a CSV file.")
        return False
    
    print(f"Loading valuation dates CSV file: {args.csv_file}")
    print(f"Database: {args.database}")
    
    success, message = valuationCSV_to_SQLite.update_database(args.csv_file, args.database)
    if success:
        print(f"✓ {message}")
        print("Rebuilding overall table to apply new valuation dates...")
        overall_table.build_overall_table(args.database)
        print("✓ Overall table updated successfully.")
        return True
    else:
        print(f"✗ {message}")
        return False

def generate_report(args):
    """Generate an Excel report for the specified date range"""
    # Validate dates
    if not validate_date(args.start_date):
        print(f"Error: Invalid start date format '{args.start_date}'. Use MM/DD/YYYY format.")
        return False
    
    if not validate_date(args.end_date):
        print(f"Error: Invalid end date format '{args.end_date}'. Use MM/DD/YYYY format.")
        return False
    
    # Check if start date is before end date
    start_dt = datetime.strptime(args.start_date, '%m/%d/%Y')
    end_dt = datetime.strptime(args.end_date, '%m/%d/%Y')
    
    if start_dt > end_dt:
        print("Error: Start date must be before or equal to end date.")
        return False
    
    # Check if database exists
    if not os.path.exists(args.database):
        print(f"Error: Database file '{args.database}' not found.")
        print("Run broker or other load commands first to create the database.")
        return False
    
    print(f"Generating Excel report...")
    print(f"Date range: {args.start_date} to {args.end_date}")
    print(f"Database: {args.database}")
    print(f"Output file: {args.output}")
    
    # Generate the report using the existing function
    success, result = generate_excel_report(args.start_date, args.end_date, args.output, args.database)
    if success:
        print(f"✓ Excel report generated successfully: {result}")
        return True
    else:
        print(f"✗ {result}")
        return False

def add_valuation_date(args):
    """Add a custom valuation date to the database"""
    # Validate date format
    if not validate_date(args.date):
        print(f"Error: Invalid date format '{args.date}'. Use MM/DD/YYYY format.")
        return False
    
    # Check if database exists
    if not os.path.exists(args.database):
        print(f"Error: Database file '{args.database}' not found.")
        print("Run broker or other load commands first to create the database.")
        return False
    
    try:
        # Connect to database
        conn = sqlite3.connect(args.database)
        cursor = conn.cursor()
        
        # Create valuation_dates table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS valuation_dates (
                "Date" TEXT PRIMARY KEY,
                "Fund Value" REAL
            )
        ''')
        
        # Check if date already exists
        cursor.execute('SELECT "Date", "Fund Value" FROM valuation_dates WHERE "Date" = ?', (args.date,))
        existing_row = cursor.fetchone()
        
        if existing_row:
            # Date exists, check if we need to update fund value
            if args.amount is not None:
                cursor.execute('UPDATE valuation_dates SET "Fund Value" = ? WHERE "Date" = ?', (args.amount, args.date))
                conn.commit()
                print(f"✓ Updated valuation date '{args.date}' with fund value: ${args.amount:,.2f}")
            else:
                print(f"Date '{args.date}' is already in the valuation dates list.")
                if existing_row[1] is not None:
                    print(f"  Current fund value: ${existing_row[1]:,.2f}")
                conn.close()
                return True
        else:
            # Insert the new valuation date
            cursor.execute('INSERT INTO valuation_dates ("Date", "Fund Value") VALUES (?, ?)', (args.date, args.amount))
            conn.commit()
            
            if args.amount is not None:
                print(f"✓ Added '{args.date}' to valuation dates list with fund value: ${args.amount:,.2f}")
            else:
                print(f"✓ Added '{args.date}' to valuation dates list.")
        
        conn.close()
        
        # Rebuild overall table to reflect the new valuation date
        print("Rebuilding overall table to apply new valuation date...")
        overall_table.build_overall_table(args.database)
        print("✓ Overall table updated successfully.")
        
        return True
        
    except Exception as e:
        print(f"✗ Error adding valuation date: {str(e)}")
        return False

def list_valuation_dates(args):
    """List all custom valuation dates in the database"""
    # Check if database exists
    if not os.path.exists(args.database):
        print(f"Error: Database file '{args.database}' not found.")
        print("Run broker or other load commands first to create the database.")
        return False
    
    try:
        # Connect to database
        conn = sqlite3.connect(args.database)
        cursor = conn.cursor()
        
        # Check if table exists and get dates
        cursor.execute('''
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='valuation_dates'
        ''')
        
        if not cursor.fetchone():
            print("No custom valuation dates have been added yet.")
            print("Note: The 1st of every month is automatically a valuation date.")
            conn.close()
            return True
        
        cursor.execute('SELECT "Date", "Fund Value" FROM valuation_dates ORDER BY "Date"')
        dates = cursor.fetchall()
        conn.close()
        
        if not dates:
            print("No custom valuation dates have been added yet.")
            print("Note: The 1st of every month is automatically a valuation date.")
        else:
            print("Custom valuation dates:")
            for date_row in dates:
                if date_row[1] is not None:
                    print(f"  • {date_row[0]} (Fund Value: ${date_row[1]:,.2f})")
                else:
                    print(f"  • {date_row[0]}")
            print("\nNote: The 1st of every month is also automatically a valuation date.")
        
        return True
        
    except Exception as e:
        print(f"✗ Error listing valuation dates: {str(e)}")
        return False

def delete_table(args):
    """Delete a specified table from the database"""
    # Validate table name
    valid_tables = ['broker', 'other_transactions', 'overall', 'valuation_dates']
    if args.table_name not in valid_tables:
        print(f"Error: Invalid table name '{args.table_name}'.")
        print(f"Valid table names are: {', '.join(valid_tables)}")
        return False
    
    # Check if database exists
    if not os.path.exists(args.database):
        print(f"Error: Database file '{args.database}' not found.")
        print("Run broker or other load commands first to create the database.")
        return False
    
    try:
        # Connect to database
        conn = sqlite3.connect(args.database)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute('''
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
        ''', (args.table_name,))
        
        if not cursor.fetchone():
            print(f"Table '{args.table_name}' does not exist in the database.")
            conn.close()
            return True
        
        # Confirm deletion unless --force flag is used
        if not args.force:
            response = input(f"Are you sure you want to delete the '{args.table_name}' table? This action cannot be undone. (y/N): ")
            if response.lower() not in ['y', 'yes']:
                print("Table deletion cancelled.")
                conn.close()
                return True
        
        # Drop the table
        cursor.execute(f'DROP TABLE "{args.table_name}"')
        conn.commit()
        conn.close()
        
        print(f"✓ Table '{args.table_name}' has been deleted successfully.")
        
        # If overall table was deleted, suggest rebuilding it
        if args.table_name == 'overall':
            print("Note: You can rebuild the overall table by running any broker or other load command.")
        
        return True
        
    except Exception as e:
        print(f"✗ Error deleting table: {str(e)}")
        return False

def delete_valuation_date(args):
    """Delete a specific valuation date entry from the database"""
    # Validate date format
    if not validate_date(args.date):
        print(f"Error: Invalid date format '{args.date}'. Use MM/DD/YYYY format.")
        return False
    
    # Check if database exists
    if not os.path.exists(args.database):
        print(f"Error: Database file '{args.database}' not found.")
        print("Run broker or other load commands first to create the database.")
        return False
    
    try:
        # Connect to database
        conn = sqlite3.connect(args.database)
        cursor = conn.cursor()
        
        # Check if valuation_dates table exists
        cursor.execute('''
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='valuation_dates'
        ''')
        
        if not cursor.fetchone():
            print("No custom valuation dates table exists.")
            conn.close()
            return True
        
        # Check if the date exists
        cursor.execute('SELECT "Date", "Fund Value" FROM valuation_dates WHERE "Date" = ?', (args.date,))
        existing_row = cursor.fetchone()
        
        if not existing_row:
            print(f"Valuation date '{args.date}' not found in the database.")
            conn.close()
            return True
        
        # Confirm deletion unless --force flag is used
        if not args.force:
            fund_value_text = f" (Fund Value: ${existing_row[1]:,.2f})" if existing_row[1] is not None else ""
            response = input(f"Are you sure you want to delete the valuation date '{args.date}'{fund_value_text}? This action cannot be undone. (y/N): ")
            if response.lower() not in ['y', 'yes']:
                print("Valuation date deletion cancelled.")
                conn.close()
                return True
        
        # Delete the valuation date
        cursor.execute('DELETE FROM valuation_dates WHERE "Date" = ?', (args.date,))
        conn.commit()
        conn.close()
        
        print(f"✓ Valuation date '{args.date}' has been deleted successfully.")
        
        # Rebuild overall table to reflect the change
        print("Updating overall table...")
        overall_table.build_overall_table(args.database)
        print("✓ Overall table updated successfully.")
        
        return True
        
    except Exception as e:
        print(f"✗ Error deleting valuation date: {str(e)}")
        return False

def add_other_transaction(args):
    """Add a single transaction directly to the other_transactions table"""
    # Validate date format
    if not validate_date(args.date):
        print(f"Error: Invalid date format '{args.date}'. Use MM/DD/YYYY format.")
        return False
    
    # Check if database exists, if not cr.\aceate it
    if not os.path.exists(args.database):
        print(f"Creating new database: {args.database}")
    
    try:
        # Connect to database
        conn = sqlite3.connect(args.database)
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
        
        # Parse boolean for "Counted in P&L"
        counted_in_pl = args.counted_in_pl.lower() in ['true', '1', 'yes', 'y']
        
        # Parse boolean for "Overnight"
        overnight = args.overnight.lower() in ['true', '1', 'yes', 'y']
        
        # Handle optional Additional Info
        additional_info = args.additional_info if args.additional_info else None
        
        # Insert the transaction
        try:
            cursor.execute('''
                INSERT INTO other_transactions 
                ("Date", "Amount", "Account Description", "Transaction Description", 
                 "Counted in P&L", "Overnight", "Additional Info")
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                args.date,
                args.amount,
                args.account_description,
                args.transaction_description,
                counted_in_pl,
                overnight,
                additional_info
            ))
            conn.commit()
            print(f"✓ Successfully added transaction:")
            print(f"   Date: {args.date}")
            print(f"   Amount: ${args.amount:,.2f}")
            print(f"   Account: {args.account_description}")
            print(f"   Description: {args.transaction_description}")
            print(f"   Counted in P&L: {counted_in_pl}")
            print(f"   Overnight: {overnight}")
            if additional_info:
                print(f"   Additional Info: {additional_info}")
            
        except sqlite3.IntegrityError:
            print(f"✗ Error: A transaction with the same date, account, description, and amount already exists.")
            conn.close()
            return False
        
        conn.close()
        
        # Rebuild overall table to reflect the new transaction
        print("Updating overall table...")
        overall_table.build_overall_table(args.database)
        print("✓ Overall table updated successfully.")
        
        return True
        
    except Exception as e:
        print(f"✗ Error adding transaction: {str(e)}")
        return False

def update_fund_values_cmd(args):
    """Check and correct fund value discrepancies on valuation dates"""
    # Check if database exists
    if not os.path.exists(args.database):
        print(f"Error: Database file '{args.database}' not found.")
        print("Run broker or other load commands first to create the database.")
        return False
    
    print(f"Checking fund value discrepancies in database: {args.database}")
    
    # Call the fund value updater function
    success = valuation_discrepancy_fixer.update_fund_values(args.database, args.auto_confirm)
    return success

def main():
    parser = argparse.ArgumentParser(
        description='NAV Data Management CLI Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load a single broker CSV file
  .\\acc broker-load-csv data.csv
  .\\acc blc data.csv

  # Load all broker CSV files from a folder
  .\\acc broker-load-folder /path/to/csv/folder
  .\\acc blf /path/to/csv/folder

  # Load a single other transactions CSV file
  .\\acc other-load-csv other_data.csv
  .\\acc olc other_data.csv

  # Load all other transactions CSV files from a folder
  .\\acc other-load-folder /path/to/other/csv/folder
  .\\acc olf /path/to/other/csv/folder

  # Load valuation dates from a CSV file
  .\\acc load-valuation-csv valuation_dates.csv
  .\\acc lvc valuation_dates.csv

  # Generate Excel report for date range
  .\\acc generate-report 01/01/2023 03/31/2023 -o my_report.xlsx
  .\\acc gr 01/01/2023 03/31/2023 

  # Add a custom valuation date to the database
  .\\acc add-valuation-date 04/01/2023
  .\\acc avd 04/01/2023 -a 1000000.50

  # List all custom valuation dates in the database
  .\\acc list-valuation-dates
  .\\acc lvd

  # Delete a specific valuation date entry from the database
  .\\acc delete-valuation-date 04/01/2023
  .\\acc dvd 04/01/2023

  # Delete a specified table from the database
  .\\acc delete-table broker
  .\\acc dt other_transactions

  # Add a single transaction directly to the other_transactions table
  .\\acc add-other-transaction 01/15/2023 -500.00 "Bank Account" "Wire Transfer Fee" true false
  .\\acc aot 01/15/2023 1000.00 "Cash Account" "Deposit" false true -i "Monthly funding"

  # Check and correct fund value discrepancies on valuation dates
  .\\acc update-fund-values
  .\\acc ufv -a
        """
    )
    
    # Add default database path
    default_db = os.path.join(os.getcwd(), 'daily_accounting.db')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Load single broker CSV command with aliases
    broker_csv_parser = subparsers.add_parser(
        'broker-load-csv', 
        aliases=['blc'],
        help='Load a single broker CSV file into the database'
    )
    broker_csv_parser.add_argument(
        'csv_file', 
        help='Path to the broker CSV file to load'
    )
    broker_csv_parser.add_argument(
        '-d', '--database', 
        default=default_db,
        help=f'Path to the SQLite database file (default: {default_db})'
    )
    
    # Load broker CSV folder command with aliases
    broker_folder_parser = subparsers.add_parser(
        'broker-load-folder', 
        aliases=['blf'],
        help='Load all broker CSV files from a folder into the database'
    )
    broker_folder_parser.add_argument(
        'csv_folder', 
        help='Path to the folder containing broker CSV files'
    )
    broker_folder_parser.add_argument(
        '-d', '--database', 
        default=default_db,
        help=f'Path to the SQLite database file (default: {default_db})'
    )
    
    # Load single other transactions CSV command with aliases
    other_csv_parser = subparsers.add_parser(
        'other-load-csv', 
        aliases=['olc'],
        help='Load a single other transactions CSV file into the database'
    )
    other_csv_parser.add_argument(
        'csv_file', 
        help='Path to the other transactions CSV file to load'
    )
    other_csv_parser.add_argument(
        '-d', '--database', 
        default=default_db,
        help=f'Path to the SQLite database file (default: {default_db})'
    )
    
    # Load other transactions CSV folder command with aliases
    other_folder_parser = subparsers.add_parser(
        'other-load-folder', 
        aliases=['olf'],
        help='Load all other transactions CSV files from a folder into the database'
    )
    other_folder_parser.add_argument(
        'csv_folder', 
        help='Path to the folder containing other transactions CSV files'
    )
    other_folder_parser.add_argument(
        '-d', '--database', 
        default=default_db,
        help=f'Path to the SQLite database file (default: {default_db})'
    )
    
    # Load valuation dates CSV command with aliases
    valuation_csv_parser = subparsers.add_parser(
        'load-valuation-csv', 
        aliases=['lvc'],
        help='Load valuation dates from a CSV file into the database'
    )
    valuation_csv_parser.add_argument(
        'csv_file', 
        help='Path to the valuation dates CSV file to load (must contain Date and Fund Value columns)'
    )
    valuation_csv_parser.add_argument(
        '-d', '--database', 
        default=default_db,
        help=f'Path to the SQLite database file (default: {default_db})'
    )
    
    # Generate report command with aliases
    report_parser = subparsers.add_parser(
        'generate-report', 
        aliases=['gr'],
        help='Generate an Excel report for a date range'
    )
    report_parser.add_argument(
        'start_date', 
        help='Start date in MM/DD/YYYY format'
    )
    report_parser.add_argument(
        'end_date', 
        help='End date in MM/DD/YYYY format'
    )
    report_parser.add_argument(
        '-o', '--output', 
        default=os.path.join(os.getcwd(), 'daily_accounting_report.xlsx'),
        help='Output Excel file path (default: daily_accounting_report.xlsx)'
    )
    report_parser.add_argument(
        '-d', '--database', 
        default=default_db,
        help=f'Path to the SQLite database file (default: {default_db})'
    )
    
    # Add valuation date command with aliases
    add_val_parser = subparsers.add_parser(
        'add-valuation-date', 
        aliases=['avd'],
        help='Add a custom valuation date to the database'
    )
    add_val_parser.add_argument(
        'date', 
        help='Date to add as valuation date in MM/DD/YYYY format'
    )
    add_val_parser.add_argument(
        '-a', '--amount', 
        type=float,
        help='Optional fund value to use as Start of Day Fund Value for this date'
    )
    add_val_parser.add_argument(
        '-d', '--database', 
        default=default_db,
        help=f'Path to the SQLite database file (default: {default_db})'
    )
    
    # List valuation dates command with aliases
    list_val_parser = subparsers.add_parser(
        'list-valuation-dates', 
        aliases=['lvd'],
        help='List all custom valuation dates in the database'
    )
    list_val_parser.add_argument(
        '-d', '--database', 
        default=default_db,
        help=f'Path to the SQLite database file (default: {default_db})'
    )
    
    # Delete table command with aliases
    delete_parser = subparsers.add_parser(
        'delete-table', 
        aliases=['dt'],
        help='Delete a specified table from the database'
    )
    delete_parser.add_argument(
        'table_name', 
        help='Name of the table to delete'
    )
    delete_parser.add_argument(
        '-d', '--database', 
        default=default_db,
        help=f'Path to the SQLite database file (default: {default_db})'
    )
    delete_parser.add_argument(
        '-f', '--force', 
        action='store_true',
        help='Force delete the table without confirmation'
    )
    
    # Delete valuation date command with aliases
    delete_val_parser = subparsers.add_parser(
        'delete-valuation-date', 
        aliases=['dvd'],
        help='Delete a specific valuation date entry from the database'
    )
    delete_val_parser.add_argument(
        'date', 
        help='Date to delete from valuation dates in MM/DD/YYYY format'
    )
    delete_val_parser.add_argument(
        '-d', '--database', 
        default=default_db,
        help=f'Path to the SQLite database file (default: {default_db})'
    )
    delete_val_parser.add_argument(
        '-f', '--force', 
        action='store_true',
        help='Force delete the valuation date without confirmation'
    )
    
    # Add other transaction command with aliases
    add_other_parser = subparsers.add_parser(
        'add-other-transaction', 
        aliases=['aot'],
        help='Add a single transaction directly to the other_transactions table'
    )
    add_other_parser.add_argument(
        'date', 
        help='Transaction date in MM/DD/YYYY format'
    )
    add_other_parser.add_argument(
        'amount', 
        type=float,
        help='Transaction amount (positive or negative)'
    )
    add_other_parser.add_argument(
        'account_description', 
        help='Account description'
    )
    add_other_parser.add_argument(
        'transaction_description', 
        help='Transaction description'
    )
    add_other_parser.add_argument(
        'counted_in_pl', 
        help='Whether counted in P&L (true/false, yes/no, 1/0)'
    )
    add_other_parser.add_argument(
        'overnight', 
        help='Whether overnight transaction (true/false, yes/no, 1/0)'
    )
    add_other_parser.add_argument(
        '-i', '--additional-info', 
        help='Additional information (optional)'
    )
    add_other_parser.add_argument(
        '-d', '--database', 
        default=default_db,
        help=f'Path to the SQLite database file (default: {default_db})'
    )
    
    # Update fund values command with aliases
    update_fund_parser = subparsers.add_parser(
        'update-fund-values', 
        aliases=['ufv'],
        help='Check and correct fund value discrepancies on valuation dates'
    )
    update_fund_parser.add_argument(
        '-d', '--database', 
        default=default_db,
        help=f'Path to the SQLite database file (default: {default_db})'
    )
    update_fund_parser.add_argument(
        '-a', '--auto-confirm', 
        action='store_true',
        help='Automatically confirm fund value updates without prompting'
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Execute the appropriate command
    success = False
    if args.command in ['broker-load-csv', 'blc']:
        success = load_broker_csv(args)
    elif args.command in ['broker-load-folder', 'blf']:
        success = load_broker_folder(args)
    elif args.command in ['other-load-csv', 'olc']:
        success = load_other_csv(args)
    elif args.command in ['other-load-folder', 'olf']:
        success = load_other_folder(args)
    elif args.command in ['load-valuation-csv', 'lvc']:
        success = load_valuation_csv(args)
    elif args.command in ['generate-report', 'gr']:
        success = generate_report(args)
    elif args.command in ['add-valuation-date', 'avd']:
        success = add_valuation_date(args)
    elif args.command in ['list-valuation-dates', 'lvd']:
        success = list_valuation_dates(args)
    elif args.command in ['delete-table', 'dt']:
        success = delete_table(args)
    elif args.command in ['delete-valuation-date', 'dvd']:
        success = delete_valuation_date(args)
    elif args.command in ['add-other-transaction', 'aot']:
        success = add_other_transaction(args)
    elif args.command in ['update-fund-values', 'ufv']:
        success = update_fund_values_cmd(args)
    
    return 0 if success else 1

if __name__ == '__main__':
    sys.exit(main())