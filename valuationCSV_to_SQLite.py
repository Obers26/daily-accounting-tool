#!/usr/bin/env python3
"""
Valuation CSV to SQLite Module

This module provides functionality to load valuation dates from CSV files
into a SQLite database for the Daily Accounting Tool.
"""

import sqlite3
import csv
from datetime import datetime
import overall_table

def validate_date(date_string):
    """Validate date format MM/DD/YYYY"""
    try:
        datetime.strptime(date_string, '%m/%d/%Y')
        return True
    except ValueError:
        return False

def update_database(csv_file_path, database_path):
    """
    Load valuation dates from a CSV file into the database
    
    Args:
        csv_file_path (str): Path to the CSV file containing valuation dates
        database_path (str): Path to the SQLite database file
    
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        # Connect to database
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        
        # Create valuation_dates table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS valuation_dates (
                "Date" TEXT PRIMARY KEY,
                "Fund Value" REAL
            )
        ''')
        
        # Read and process CSV file
        records_processed = 0
        records_updated = 0
        records_added = 0
        
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            # Try to detect delimiter
            sample = csvfile.read(1024)
            csvfile.seek(0)
            delimiter = ',' if ',' in sample else ';' if ';' in sample else ','
            
            reader = csv.DictReader(csvfile, delimiter=delimiter)
            
            # Validate required columns exist
            if 'Date' not in reader.fieldnames or 'Fund Value' not in reader.fieldnames:
                conn.close()
                return False, f"CSV file must contain 'Date' and 'Fund Value' columns. Found columns: {list(reader.fieldnames)}"
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 because header is row 1
                date_str = row['Date'].strip()
                fund_value_str = row['Fund Value'].strip()
                
                # Skip empty rows
                if not date_str and not fund_value_str:
                    continue
                
                # Validate date format
                if not validate_date(date_str):
                    print(f"Warning: Skipping row {row_num} - Invalid date format '{date_str}'. Expected MM/DD/YYYY.")
                    continue
                
                # Parse fund value
                fund_value = None
                if fund_value_str:
                    try:
                        # Remove any currency symbols and commas
                        clean_value = fund_value_str.replace('$', '').replace(',', '')
                        fund_value = float(clean_value)
                    except ValueError:
                        print(f"Warning: Skipping row {row_num} - Invalid fund value '{fund_value_str}'.")
                        continue
                
                # Check if date already exists
                cursor.execute('SELECT "Date", "Fund Value" FROM valuation_dates WHERE "Date" = ?', (date_str,))
                existing_row = cursor.fetchone()
                
                if existing_row:
                    # Update existing record
                    cursor.execute('UPDATE valuation_dates SET "Fund Value" = ? WHERE "Date" = ?', (fund_value, date_str))
                    records_updated += 1
                else:
                    # Insert new record
                    cursor.execute('INSERT INTO valuation_dates ("Date", "Fund Value") VALUES (?, ?)', (date_str, fund_value))
                    records_added += 1
                
                records_processed += 1
        
        conn.commit()
        conn.close()
        
        success_message = f"Successfully processed {records_processed} records: {records_added} new valuation dates added, {records_updated} existing valuation dates updated"
        
        # Rebuild overall table to reflect the new valuation dates
        if records_processed > 0:
            overall_table.build_overall_table(database_path)
        
        return True, success_message
        
    except Exception as e:
        return False, f"Error loading valuation dates from CSV: {str(e)}" 