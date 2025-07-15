"""
Broker CSV to SQLite Database Processor

This module processes broker CSV files containing Change in NAV data
and stores the processed information in a SQLite database.

Features:
- Extracts date information from CSV files
- Calculates P&L using multiple methods for verification
- Detects and reports discrepancies in calculations
- Handles accrual reconciliation
- Provides comprehensive error handling and logging
"""

import pandas as pd
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
import logging
from pathlib import Path

# Import the overall_table module for maintaining aggregate data
from overall_table import OverallTableManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BrokerCSVProcessor:
    """
    A class to process broker CSV files and manage database operations.
    """
    
    # Class constants
    TOLERANCE = 0.01  # Tolerance for P&L discrepancy detection
    ACCRUAL_TOLERANCE = 0.10  # 10% tolerance for accrual discrepancies
    
    # Database schema field mapping
    DATABASE_FIELDS = {
        'Date': 'TEXT PRIMARY KEY',
        'P&L': 'REAL',
        'Reporting Error': 'REAL',
        'Cumulative P&L': 'REAL',
        'Mark-to-Market': 'REAL',
        'Change in Dividend Accruals': 'REAL',
        'Interest': 'REAL',
        'Dividends': 'REAL',
        'Deposits & Withdrawals': 'REAL',
        'Change in Interest Accruals': 'REAL',
        'Commissions': 'REAL',
        'Total Broker': 'REAL'
    }
    
    def __init__(self, db_path: str = 'daily_accounting.db'):
        """
        Initialize the processor with database path.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.db_path_obj = Path(db_path)
        
    def _parse_financial_value(self, value: Union[str, float, int]) -> Optional[float]:
        """
        Parse financial value from string, removing currency symbols and commas.
        
        Args:
            value: Raw value to parse
            
        Returns:
            Parsed float value or None if parsing fails
        """
        if value is None:
            return None
            
        try:
            # Convert to string and clean
            value_str = str(value).strip()
            if not value_str or value_str.lower() in ['nan', 'none', '']:
                return None
                
            # Remove currency symbols and commas
            clean_value = value_str.replace('$', '').replace(',', '').strip()
            if not clean_value:
                return None
                
            return float(clean_value)
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to parse financial value '{value}': {e}")
            return None
    
    def extract_date_from_csv(self, file_path: str) -> Optional[str]:
        """
        Extract date from CSV file by looking for the 'Period' field.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Formatted date string (MM/DD/YYYY) or None if not found
        """
        try:
            # Read CSV with error handling
            df = pd.read_csv(file_path, on_bad_lines='skip', encoding='utf-8')
            
            # Find the row where 'Field Name' is 'Period'
            period_row = df[df['Field Name'] == 'Period']
            
            if period_row.empty:
                logger.warning(f"No 'Period' field found in {file_path}")
                return None
            
            # Get the date value and parse it
            date_str = period_row['Field Value'].iloc[0]
            date_obj = datetime.strptime(date_str, '%B %d, %Y')
            
            # Convert to desired format (MM/DD/YYYY)
            return date_obj.strftime('%m/%d/%Y')
            
        except Exception as e:
            logger.error(f"Error extracting date from {file_path}: {e}")
            return None
    
    def _calculate_pnl_method1(self, fields: Dict[str, Optional[float]]) -> float:
        """
        Calculate P&L using sum of components method.
        
        Args:
            fields: Dictionary of processed fields
            
        Returns:
            P&L calculated from components
        """
        components = [
            fields.get('Mark-to-Market') or 0,
            fields.get('Change in Interest Accruals') or 0,
            fields.get('Change in Dividend Accruals') or 0,
            fields.get('Commissions') or 0,
            fields.get('Interest') or 0,
            fields.get('Dividends') or 0
        ]
        
        return sum(components)
    
    def _calculate_pnl_method2(self, starting_value: Optional[float], 
                              ending_value: Optional[float],
                              deposits_withdrawals: float) -> Optional[float]:
        """
        Calculate P&L using ending - starting - deposits/withdrawals method.
        
        Args:
            starting_value: Starting NAV value
            ending_value: Ending NAV value
            deposits_withdrawals: Net deposits and withdrawals
            
        Returns:
            P&L calculated from NAV changes or None if insufficient data
        """
        if starting_value is None or ending_value is None:
            return None
        
        return ending_value - starting_value - deposits_withdrawals
    
    def _detect_pnl_discrepancy(self, pnl_method1: float, pnl_method2: Optional[float],
                               date: str) -> Tuple[float, float]:
        """
        Detect and report P&L discrepancies between calculation methods.
        
        Args:
            pnl_method1: P&L from components sum
            pnl_method2: P&L from NAV changes
            date: Date for reporting
            
        Returns:
            Tuple of (final_pnl, reporting_error)
        """
        if pnl_method2 is None:
            logger.warning(f"Could not verify P&L using NAV method for {date} - missing data")
            return pnl_method1, 0.0
        
        discrepancy = abs(pnl_method1 - pnl_method2)
        
        if discrepancy > self.TOLERANCE:
            logger.warning(f"P&L DISCREPANCY DETECTED for {date}:")
            logger.warning(f"  Method 1 (Sum of Components): ${pnl_method1:.2f}")
            logger.warning(f"  Method 2 (NAV Changes): ${pnl_method2:.2f}")
            logger.warning(f"  Difference: ${discrepancy:.2f}")
            
            return pnl_method1, discrepancy
        else:
            return pnl_method1, 0.0
    
    def _check_accrual_discrepancies(self, fields: Dict[str, Optional[float]], date: str) -> None:
        """
        Check for discrepancies between actual transactions and accrual changes.
        
        Args:
            fields: Dictionary of processed fields
            date: Date for reporting
        """
        # Check Interest vs Change in Interest Accruals
        interest_val = fields.get('Interest')
        interest_accrual = fields.get('Change in Interest Accruals') or 0
        
        if interest_val is not None and interest_val != 0 and interest_accrual != 0:
            expected_accrual = -interest_val
            discrepancy_ratio = abs(interest_accrual - expected_accrual) / abs(interest_val)
            
            if discrepancy_ratio > self.ACCRUAL_TOLERANCE:
                logger.warning(f"INTEREST ACCRUAL DISCREPANCY DETECTED for {date}:")
                logger.warning(f"  Interest Transaction: ${interest_val:.2f}")
                logger.warning(f"  Change in Interest Accruals: ${interest_accrual:.2f}")
                logger.warning(f"  Expected Accrual Change: ${expected_accrual:.2f}")
                logger.warning(f"  Discrepancy: {discrepancy_ratio:.1%} (>{self.ACCRUAL_TOLERANCE:.0%} threshold)")
        
        # Check Dividends vs Change in Dividend Accruals
        dividend_val = fields.get('Dividends')
        dividend_accrual = fields.get('Change in Dividend Accruals') or 0
        
        if dividend_val is not None and dividend_val != 0 and dividend_accrual != 0:
            expected_accrual = -dividend_val
            discrepancy_ratio = abs(dividend_accrual - expected_accrual) / abs(dividend_val)
            
            if discrepancy_ratio > self.ACCRUAL_TOLERANCE:
                logger.warning(f"DIVIDEND ACCRUAL DISCREPANCY DETECTED for {date}:")
                logger.warning(f"  Dividend Transaction: ${dividend_val:.2f}")
                logger.warning(f"  Change in Dividend Accruals: ${dividend_accrual:.2f}")
                logger.warning(f"  Expected Accrual Change: ${expected_accrual:.2f}")
                logger.warning(f"  Discrepancy: {discrepancy_ratio:.1%} (>{self.ACCRUAL_TOLERANCE:.0%} threshold)")
    
    def process_file(self, file_path: str) -> Optional[Dict[str, Optional[float]]]:
        """
        Process a single CSV file and extract financial data.
        
        Args:
            file_path: Path to the CSV file to process
            
        Returns:
            Dictionary of processed fields or None if processing fails
        """
        try:
            file_name = os.path.basename(file_path)
            logger.info(f"Processing file: {file_name}")
            
            # Extract date from the CSV
            date = self.extract_date_from_csv(file_path)
            if not date:
                logger.error(f"No 'Period' field found in {file_name}")
                return None
            
            # Read the CSV file with error handling
            try:
                df = pd.read_csv(file_path, on_bad_lines='skip', encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, on_bad_lines='skip', encoding='latin1')
            
            # Filter for rows where 'Statement' is 'Change in NAV'
            nav_section = df[df['Statement'] == 'Change in NAV']
            nav_data = nav_section[['Field Name', 'Field Value']].iloc[1:]  # Skip first row
            
            # Initialize fields dictionary
            fields = {field: None for field in self.DATABASE_FIELDS.keys()}
            fields['Date'] = date
            
            # Process NAV data
            starting_value = None
            ending_value = None
            deposits_withdrawals = 0
            
            for _, row in nav_data.iterrows():
                field_name = row['Field Name']
                field_value = row['Field Value']
                
                if field_name == 'Starting Value':
                    starting_value = self._parse_financial_value(field_value)
                    
                elif field_name == 'Ending Value':
                    ending_value = self._parse_financial_value(field_value)
                    fields['Total Broker'] = ending_value
                    
                elif field_name == 'Deposits & Withdrawals':
                    deposits_withdrawals = self._parse_financial_value(field_value) or 0
                    fields['Deposits & Withdrawals'] = deposits_withdrawals
                    
                elif field_name in fields:
                    fields[field_name] = self._parse_financial_value(field_value)
            
            # Calculate P&L using both methods
            pnl_method1 = self._calculate_pnl_method1(fields)
            pnl_method2 = self._calculate_pnl_method2(starting_value, ending_value, deposits_withdrawals)
            
            # Detect discrepancies and set final P&L
            final_pnl, reporting_error = self._detect_pnl_discrepancy(pnl_method1, pnl_method2, date)
            fields['P&L'] = final_pnl
            fields['Reporting Error'] = reporting_error
            
            # Check for accrual discrepancies
            self._check_accrual_discrepancies(fields, date)
            
            logger.info(f"Successfully processed {file_name} for date {date}")
            return fields
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return None
    
    def _create_database_table(self, cursor: sqlite3.Cursor) -> None:
        """
        Create the broker table if it doesn't exist.
        
        Args:
            cursor: SQLite cursor object
        """
        field_definitions = [f'"{field}" {field_type}' for field, field_type in self.DATABASE_FIELDS.items()]
        create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS broker (
                {', '.join(field_definitions)}
            )
        '''
        cursor.execute(create_table_sql)
    
    def _insert_record(self, cursor: sqlite3.Cursor, data: Dict[str, Optional[float]]) -> None:
        """
        Insert or replace a record in the broker table.
        
        Args:
            cursor: SQLite cursor object
            data: Dictionary of field values to insert
        """
        fields = list(self.DATABASE_FIELDS.keys())
        placeholders = ', '.join(['?' for _ in fields])
        field_names = ', '.join([f'"{field}"' for field in fields])
        
        insert_sql = f'''
            INSERT OR REPLACE INTO broker ({field_names})
            VALUES ({placeholders})
        '''
        
        values = [data.get(field) for field in fields]
        cursor.execute(insert_sql, values)
    
    def update_database(self, file_path: str) -> Tuple[bool, str]:
        """
        Update the database with data from a single CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Tuple of (success, message)
        """
        try:
            # Validate file exists
            if not os.path.exists(file_path):
                return False, f"File not found: {file_path}"
            
            # Process the file
            data = self.process_file(file_path)
            if not data:
                return False, "Failed to process file"
            
            # Connect to database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create table if it doesn't exist
                self._create_database_table(cursor)
                
                # Insert data
                self._insert_record(cursor, data)
                
                conn.commit()
            
            # Rebuild overall table to keep aggregates in sync
            overall_table_manager = OverallTableManager(self.db_path)
            overall_table_manager.build_overall_table()
            
            return True, f"Successfully updated database with data from {os.path.basename(file_path)}"
            
        except Exception as e:
            logger.error(f"Error updating database: {e}")
            return False, f"Error updating database: {str(e)}"
    
    def process_all_files(self, folder_path: str) -> Tuple[bool, str]:
        """
        Process all CSV files in the specified folder and store data in SQLite database.
        
        Args:
            folder_path: Path to the folder containing CSV files
            
        Returns:
            Tuple of (success, message)
        """
        try:
            # Validate folder exists
            if not os.path.exists(folder_path):
                return False, f"Folder not found: {folder_path}"
            
            if not os.path.isdir(folder_path):
                return False, f"Path is not a directory: {folder_path}"
            
            logger.info(f"Processing files in folder: {folder_path}")
            logger.info(f"Database file: {self.db_path}")
            
            # Get list of CSV files
            csv_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.csv')]
            
            if not csv_files:
                return False, "No CSV files found in the specified folder"
            
            # Initialize database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                self._create_database_table(cursor)
                
                # Process each CSV file
                files_processed = 0
                for filename in csv_files:
                    file_path = os.path.join(folder_path, filename)
                    success, _ = self.update_database(file_path)
                    if success:
                        files_processed += 1
                    else:
                        logger.warning(f"Failed to process {filename}")
                
                conn.commit()
            
            # Rebuild overall table once after processing all files
            overall_table_manager = OverallTableManager(self.db_path)
            overall_table_manager.build_overall_table()
            
            message = f"Successfully processed {files_processed} out of {len(csv_files)} files"
            logger.info(message)
            
            return True, message
            
        except Exception as e:
            logger.error(f"Error processing files: {e}")
            return False, f"Error processing files: {str(e)}"


# Legacy function wrappers for backward compatibility
def extract_date_from_csv(file_path: str) -> Optional[str]:
    """Legacy wrapper for extract_date_from_csv."""
    processor = BrokerCSVProcessor()
    return processor.extract_date_from_csv(file_path)


def process_file(file_path: str) -> Optional[Dict[str, Optional[float]]]:
    """Legacy wrapper for process_file."""
    processor = BrokerCSVProcessor()
    return processor.process_file(file_path)


def update_database(file_path: str, db_path: str = 'daily_accounting.db') -> Tuple[bool, str]:
    """Legacy wrapper for update_database."""
    processor = BrokerCSVProcessor(db_path)
    return processor.update_database(file_path)


def process_all_files(folder_path: str, db_path: str = 'daily_accounting.db') -> Tuple[bool, str]:
    """Legacy wrapper for process_all_files."""
    processor = BrokerCSVProcessor(db_path)
    return processor.process_all_files(folder_path)


if __name__ == '__main__':
    # Configuration - these would normally be command line arguments
    import argparse
    
    parser = argparse.ArgumentParser(description='Process broker CSV files')
    parser.add_argument('--folder', '-f', help='Folder containing CSV files')
    parser.add_argument('--file', help='Single CSV file to process')
    parser.add_argument('--database', '-d', default='daily_accounting.db', help='Database file path')
    
    args = parser.parse_args()
    
    processor = BrokerCSVProcessor(args.database)
    
    if args.folder:
        success, message = processor.process_all_files(args.folder)
        print(message)
        if not success:
            exit(1)
    elif args.file:
        success, message = processor.update_database(args.file)
        print(message)
        if not success:
            exit(1)
    else:
        print("Please specify either --folder or --file")
        exit(1)