#!/usr/bin/env python3
"""
Valuation CSV to SQLite Database Processor

This module provides functionality to load valuation dates from CSV files
into a SQLite database for the Daily Accounting Tool.

Features:
- Processes valuation date data from CSV files
- Validates date formats and fund values
- Handles duplicate entries appropriately
- Provides comprehensive error handling and logging
- Automatically rebuilds overall table after updates
"""

import sqlite3
import csv
from datetime import datetime
from typing import Optional, Tuple, Dict, List
import logging
from overall_table import OverallTableManager
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ValuationCSVProcessor:
    """
    A class to process valuation dates CSV files and manage database operations.
    """
    
    # Database schema field mapping
    DATABASE_FIELDS = {
        'Date': 'TEXT PRIMARY KEY',
        'Fund Value': 'REAL'
    }
    
    # Required columns that must exist in CSV files
    REQUIRED_COLUMNS = ['Date', 'Fund Value']
    
    def __init__(self, db_path: str = 'daily_accounting.db'):
        """
        Initialize the processor with database path.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
    
    def _validate_date(self, date_string: str) -> bool:
        """
        Validate date format MM/DD/YYYY.
        
        Args:
            date_string: Date string to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            datetime.strptime(date_string, '%m/%d/%Y')
            return True
        except ValueError:
            return False
    
    def _parse_fund_value(self, value_str: str) -> Optional[float]:
        """
        Parse fund value from string, removing currency symbols and commas.
        
        Args:
            value_str: Raw fund value string to parse
            
        Returns:
            Parsed float value or None if parsing fails
        """
        if not value_str or not value_str.strip():
            return None
        
        try:
            # Remove currency symbols and commas
            clean_value = value_str.replace('$', '').replace(',', '').strip()
            return float(clean_value) if clean_value else None
        except ValueError:
            logger.warning(f"Could not parse fund value '{value_str}'")
            return None
    
    def _detect_csv_delimiter(self, file_path: str) -> str:
        """
        Detect the delimiter used in the CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Detected delimiter character
        """
        try:
            with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                sample = csvfile.read(1024)
                if ',' in sample:
                    return ','
                elif ';' in sample:
                    return ';'
                else:
                    return ','
        except Exception:
            return ','
    
    def process_file(self, file_path: str) -> Optional[List[Dict]]:
        """
        Process a single CSV file with valuation dates data.
        
        Args:
            file_path: Path to the CSV file
        
        Returns:
            List of valuation records, or None if error
        """
        try:
            logger.info(f"Processing valuation file: {file_path}")
            
            # Detect delimiter
            delimiter = self._detect_csv_delimiter(file_path)
            
            valuation_records = []
            
            with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=delimiter)
                
                # Validate required columns exist
                if not all(col in reader.fieldnames for col in self.REQUIRED_COLUMNS):
                    logger.error(f"CSV file must contain {self.REQUIRED_COLUMNS} columns. Found: {list(reader.fieldnames)}")
                    return None
                
                for row_num, row in enumerate(reader, start=2):  # Start at 2 because header is row 1
                    try:
                        date_str = row['Date'].strip()
                        fund_value_str = row['Fund Value'].strip()
                        
                        # Skip empty rows
                        if not date_str and not fund_value_str:
                            continue
                        
                        # Validate date format
                        if not self._validate_date(date_str):
                            logger.warning(f"Skipping row {row_num}: Invalid date format '{date_str}'. Expected MM/DD/YYYY.")
                            continue
                        
                        # Parse fund value
                        fund_value = self._parse_fund_value(fund_value_str)
                        
                        # Create valuation record
                        valuation_record = {
                            'Date': date_str,
                            'Fund Value': fund_value
                        }
                        
                        valuation_records.append(valuation_record)
                        
                    except Exception as e:
                        logger.warning(f"Error processing row {row_num}: {str(e)}")
                        continue
            
            logger.info(f"Successfully processed {len(valuation_records)} valuation records")
            return valuation_records
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return None
    
    def _create_database_table(self, cursor: sqlite3.Cursor) -> None:
        """
        Create the valuation_dates table if it doesn't exist.
        
        Args:
            cursor: SQLite cursor object
        """
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS valuation_dates (
                "Date" TEXT PRIMARY KEY,
                "Fund Value" REAL
            )
        ''')
    
    def _insert_valuation_records(self, cursor: sqlite3.Cursor, records: List[Dict]) -> Tuple[int, int]:
        """
        Insert valuation records into the database.
        
        Args:
            cursor: SQLite cursor object
            records: List of valuation record dictionaries
            
        Returns:
            Tuple of (records_added, records_updated)
        """
        records_added = 0
        records_updated = 0
        
        for record in records:
            try:
                # Check if date already exists
                cursor.execute('SELECT "Date", "Fund Value" FROM valuation_dates WHERE "Date" = ?', (record['Date'],))
                existing_row = cursor.fetchone()
                
                if existing_row:
                    # Update existing record
                    cursor.execute('UPDATE valuation_dates SET "Fund Value" = ? WHERE "Date" = ?', 
                                 (record['Fund Value'], record['Date']))
                    records_updated += 1
                else:
                    # Insert new record
                    cursor.execute('INSERT INTO valuation_dates ("Date", "Fund Value") VALUES (?, ?)', 
                                 (record['Date'], record['Fund Value']))
                    records_added += 1
                    
            except Exception as e:
                logger.warning(f"Error inserting record for date {record['Date']}: {str(e)}")
                continue
        
        return records_added, records_updated
    
    def update_database(self, file_path: str) -> Tuple[bool, str]:
        """
        Load valuation dates from a CSV file into the database.
        
        Args:
            file_path: Path to the CSV file containing valuation dates
        
        Returns:
            Tuple of (success, message)
        """
        try:
            # Validate file exists
            if not os.path.exists(file_path):
                return False, f"File not found: {file_path}"
            
            if not file_path.lower().endswith('.csv'):
                return False, f"File is not a CSV file: {file_path}"
            
            # Process the file
            valuation_records = self.process_file(file_path)
            if not valuation_records:
                return False, "Failed to process file or no valid valuation records found"
            
            # Connect to database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create table if it doesn't exist
                self._create_database_table(cursor)
                
                # Insert records
                records_added, records_updated = self._insert_valuation_records(cursor, valuation_records)
                
                conn.commit()
            
            # Rebuild overall table to reflect the new valuation dates
            if records_added > 0 or records_updated > 0:
                overall_table_manager = OverallTableManager(self.db_path)
                overall_table_manager.build_overall_table()
            
            message = f"Successfully processed {len(valuation_records)} records: {records_added} new valuation dates added, {records_updated} existing valuation dates updated"
            return True, message
            
        except Exception as e:
            logger.error(f"Error updating database: {e}")
            return False, f"Error loading valuation dates from CSV: {str(e)}"
    
    def add_valuation_date(self, date_str: str, fund_value: Optional[float] = None) -> Tuple[bool, str]:
        """
        Add a single valuation date to the database.
        
        Args:
            date_str: Date string in MM/DD/YYYY format
            fund_value: Optional fund value for this date
            
        Returns:
            Tuple of (success, message)
        """
        try:
            # Validate date format
            if not self._validate_date(date_str):
                return False, f"Invalid date format '{date_str}'. Expected MM/DD/YYYY."
            
            # Connect to database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create table if it doesn't exist
                self._create_database_table(cursor)
                
                # Check if date already exists
                cursor.execute('SELECT "Date", "Fund Value" FROM valuation_dates WHERE "Date" = ?', (date_str,))
                existing_row = cursor.fetchone()
                
                if existing_row:
                    # Date exists, check if we need to update fund value
                    if fund_value is not None:
                        cursor.execute('UPDATE valuation_dates SET "Fund Value" = ? WHERE "Date" = ?', (fund_value, date_str))
                        message = f"Updated valuation date '{date_str}' with fund value: ${fund_value:,.2f}"
                    else:
                        message = f"Date '{date_str}' is already in the valuation dates list."
                        if existing_row[1] is not None:
                            message += f" Current fund value: ${existing_row[1]:,.2f}"
                else:
                    # Insert the new valuation date
                    cursor.execute('INSERT INTO valuation_dates ("Date", "Fund Value") VALUES (?, ?)', (date_str, fund_value))
                    
                    if fund_value is not None:
                        message = f"Added '{date_str}' to valuation dates list with fund value: ${fund_value:,.2f}"
                    else:
                        message = f"Added '{date_str}' to valuation dates list."
                
                conn.commit()
            
            # Rebuild overall table to reflect the new valuation date
            overall_table_manager = OverallTableManager(self.db_path)
            overall_table_manager.build_overall_table()
            
            return True, message
            
        except Exception as e:
            logger.error(f"Error adding valuation date: {e}")
            return False, f"Error adding valuation date: {str(e)}"
    
    def list_valuation_dates(self) -> Tuple[bool, str]:
        """
        List all custom valuation dates in the database.
        
        Returns:
            Tuple of (success, message)
        """
        try:
            # Connect to database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if table exists and get dates
                cursor.execute('''
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='valuation_dates'
                ''')
                
                if not cursor.fetchone():
                    message = "No custom valuation dates have been added yet.\nNote: The 1st of every month is automatically a valuation date."
                    return True, message
                
                cursor.execute('SELECT "Date", "Fund Value" FROM valuation_dates ORDER BY "Date"')
                dates = cursor.fetchall()
            
            if not dates:
                message = "No custom valuation dates have been added yet.\nNote: The 1st of every month is automatically a valuation date."
            else:
                message = "Custom valuation dates:\n"
                for date_row in dates:
                    if date_row[1] is not None:
                        message += f"  • {date_row[0]} (Fund Value: ${date_row[1]:,.2f})\n"
                    else:
                        message += f"  • {date_row[0]}\n"
                message += "\nNote: The 1st of every month is also automatically a valuation date."
            
            return True, message
            
        except Exception as e:
            logger.error(f"Error listing valuation dates: {e}")
            return False, f"Error listing valuation dates: {str(e)}"
    
    def delete_valuation_date(self, date_str: str) -> Tuple[bool, str]:
        """
        Delete a specific valuation date entry from the database.
        
        Args:
            date_str: Date string in MM/DD/YYYY format
            
        Returns:
            Tuple of (success, message)
        """
        try:
            # Validate date format
            if not self._validate_date(date_str):
                return False, f"Invalid date format '{date_str}'. Expected MM/DD/YYYY."
            
            # Connect to database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if valuation_dates table exists
                cursor.execute('''
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='valuation_dates'
                ''')
                
                if not cursor.fetchone():
                    return True, "No custom valuation dates table exists."
                
                # Check if the date exists
                cursor.execute('SELECT "Date", "Fund Value" FROM valuation_dates WHERE "Date" = ?', (date_str,))
                existing_row = cursor.fetchone()
                
                if not existing_row:
                    return True, f"Valuation date '{date_str}' not found in the database."
                
                # Delete the valuation date
                cursor.execute('DELETE FROM valuation_dates WHERE "Date" = ?', (date_str,))
                conn.commit()
            
            # Rebuild overall table to reflect the change
            overall_table_manager = OverallTableManager(self.db_path)
            overall_table_manager.build_overall_table()
            
            return True, f"Valuation date '{date_str}' has been deleted successfully."
            
        except Exception as e:
            logger.error(f"Error deleting valuation date: {e}")
            return False, f"Error deleting valuation date: {str(e)}"


# Legacy function wrappers for backward compatibility
def validate_date(date_string: str) -> bool:
    """Legacy wrapper for validate_date."""
    processor = ValuationCSVProcessor()
    return processor._validate_date(date_string)


def update_database(csv_file_path: str, database_path: str) -> Tuple[bool, str]:
    """Legacy wrapper for update_database."""
    processor = ValuationCSVProcessor(database_path)
    return processor.update_database(csv_file_path) 