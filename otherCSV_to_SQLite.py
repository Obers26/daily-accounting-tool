"""
Other Transactions CSV to SQLite Database Processor

This module processes other transactions CSV files and stores 
the processed information in a SQLite database.

Features:
- Processes transaction data from CSV files
- Handles various date formats
- Validates data types and formats
- Provides comprehensive error handling and logging
- Maintains database integrity with unique constraints
"""

import pandas as pd
import os
from datetime import datetime
import sqlite3
from typing import Dict, List, Optional, Tuple, Union
import logging
from overall_table import OverallTableManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OtherCSVProcessor:
    """
    A class to process other transactions CSV files and manage database operations.
    """
    
    # Database schema field mapping
    DATABASE_FIELDS = {
        'Date': 'TEXT NOT NULL',
        'Amount': 'REAL',
        'Account Description': 'TEXT',
        'Transaction Description': 'TEXT',
        'Counted in P&L': 'BOOLEAN',
        'Overnight': 'BOOLEAN',
        'Additional Info': 'TEXT'
    }
    
    # Required columns that must exist in CSV files
    REQUIRED_COLUMNS = [
        'Date', 'Amount', 'Account Description', 'Transaction Description',
        'Counted in P&L', 'Overnight', 'Additional Info'
    ]
    
    def __init__(self, db_path: str = 'daily_accounting.db'):
        """
        Initialize the processor with database path.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        
    def _parse_date(self, date_str: str) -> Optional[str]:
        """
        Parse and format date from string, handling multiple formats.
        
        Args:
            date_str: Raw date string to parse
            
        Returns:
            Formatted date string (MM/DD/YYYY) or None if parsing fails
        """
        try:
            date_str = str(date_str).strip()
            if not date_str or date_str.lower() == 'nan':
                return None
            
            # Try different date formats
            date_formats = [
                '%m/%d/%Y',
                '%Y-%m-%d',
                '%B %d, %Y'
            ]
            
            for fmt in date_formats:
                try:
                    date_obj = datetime.strptime(date_str, fmt)
                    return date_obj.strftime('%m/%d/%Y')
                except ValueError:
                    continue
            
            logger.warning(f"Could not parse date '{date_str}' with any known format")
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing date '{date_str}': {e}")
            return None
    
    def _parse_amount(self, amount_str: str) -> float:
        """
        Parse financial amount from string, removing currency symbols and commas.
        
        Args:
            amount_str: Raw amount string to parse
            
        Returns:
            Parsed float amount
        """
        try:
            amount_str = str(amount_str).replace('$', '').replace(',', '').strip()
            return float(amount_str) if amount_str and amount_str != 'nan' else 0.0
        except ValueError:
            return 0.0
    
    def _parse_boolean(self, bool_str: str) -> bool:
        """
        Parse boolean from string.
        
        Args:
            bool_str: Raw boolean string to parse
            
        Returns:
            Parsed boolean value
        """
        return str(bool_str).strip().lower() in ['true', '1', 'yes', 'y']
    
    def _parse_string(self, str_value: str) -> str:
        """
        Parse string field, handling NaN values.
        
        Args:
            str_value: Raw string value to parse
            
        Returns:
            Cleaned string value
        """
        if pd.isna(str_value):
            return ''
        return str(str_value).strip()
    
    def process_file(self, file_path: str) -> Optional[List[Dict]]:
        """
        Process a single CSV file with other transaction data.
        
        Args:
            file_path: Path to the CSV file
        
        Returns:
            List of transaction dictionaries, or None if error
        """
        try:
            file_name = os.path.basename(file_path)
            logger.info(f"Processing file: {file_name}")
            
            # Read the CSV file with error handling
            try:
                df = pd.read_csv(file_path, on_bad_lines='skip', encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, on_bad_lines='skip', encoding='latin1')
            
            # Check if required columns exist
            missing_columns = [col for col in self.REQUIRED_COLUMNS if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return None
            
            transactions = []
            
            # Process each row
            for row_idx, row in df.iterrows():
                try:
                    # Parse and format date
                    formatted_date = self._parse_date(row['Date'])
                    if not formatted_date:
                        logger.warning(f"Skipping row {row_idx + 1}: Invalid or empty date")
                        continue
                    
                    # Parse amount
                    amount = self._parse_amount(row['Amount'])
                    
                    # Parse boolean fields
                    counted_in_pl = self._parse_boolean(row['Counted in P&L'])
                    overnight = self._parse_boolean(row['Overnight'])
                    
                    # Parse string fields
                    account_desc = self._parse_string(row['Account Description'])
                    transaction_desc = self._parse_string(row['Transaction Description'])
                    additional_info = self._parse_string(row['Additional Info'])
                    
                    # Create transaction record
                    transaction = {
                        'Date': formatted_date,
                        'Amount': amount,
                        'Account Description': account_desc,
                        'Transaction Description': transaction_desc,
                        'Counted in P&L': counted_in_pl,
                        'Overnight': overnight,
                        'Additional Info': additional_info
                    }
                    
                    transactions.append(transaction)
                    
                except Exception as e:
                    logger.warning(f"Error processing row {row_idx + 1}: {str(e)}")
                    continue
            
            logger.info(f"Successfully processed {len(transactions)} transactions from {file_name}")
            return transactions
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return None
    
    def _create_database_table(self, cursor: sqlite3.Cursor) -> None:
        """
        Create the other_transactions table if it doesn't exist.
        
        Args:
            cursor: SQLite cursor object
        """
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
    
    def _insert_transactions(self, cursor: sqlite3.Cursor, transactions: List[Dict]) -> Tuple[int, int]:
        """
        Insert transactions into the database.
        
        Args:
            cursor: SQLite cursor object
            transactions: List of transaction dictionaries
            
        Returns:
            Tuple of (rows_inserted, rows_updated)
        """
        rows_inserted = 0
        rows_updated = 0
        
        for transaction in transactions:
            try:
                cursor.execute('''
                    INSERT INTO other_transactions 
                    ("Date", "Amount", "Account Description", "Transaction Description", 
                     "Counted in P&L", "Overnight", "Additional Info")
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    transaction['Date'],
                    transaction['Amount'],
                    transaction['Account Description'],
                    transaction['Transaction Description'],
                    transaction['Counted in P&L'],
                    transaction['Overnight'],
                    transaction['Additional Info']
                ))
                rows_inserted += 1
            except sqlite3.IntegrityError:
                # Handle duplicate entries by updating
                cursor.execute('''
                    UPDATE other_transactions 
                    SET "Counted in P&L" = ?, "Overnight" = ?, "Additional Info" = ?
                    WHERE "Date" = ? AND "Account Description" = ? 
                    AND "Transaction Description" = ? AND "Amount" = ?
                ''', (
                    transaction['Counted in P&L'],
                    transaction['Overnight'],
                    transaction['Additional Info'],
                    transaction['Date'],
                    transaction['Account Description'],
                    transaction['Transaction Description'],
                    transaction['Amount']
                ))
                rows_updated += 1
        
        return rows_inserted, rows_updated
    
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
            
            if not file_path.lower().endswith('.csv'):
                return False, f"File is not a CSV file: {file_path}"
            
            # Process the file
            transactions = self.process_file(file_path)
            if not transactions:
                return False, "Failed to process file or no valid transactions found"
            
            # Connect to database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create table if it doesn't exist
                self._create_database_table(cursor)
                
                # Insert transactions
                rows_inserted, rows_updated = self._insert_transactions(cursor, transactions)
                
                conn.commit()
            
            # Rebuild overall table since other transactions have changed
            overall_table_manager = OverallTableManager(self.db_path)
            overall_table_manager.build_overall_table()
            
            message = f"Successfully processed {len(transactions)} transactions: {rows_inserted} inserted, {rows_updated} updated"
            return True, message
            
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
                conn.commit()
            
            # Process each CSV file
            files_processed = 0
            total_transactions = 0
            
            for filename in csv_files:
                file_path = os.path.join(folder_path, filename)
                success, message = self.update_database(file_path)
                
                if success:
                    files_processed += 1
                    # Extract number of transactions from message
                    if "processed" in message:
                        try:
                            trans_count = int(message.split()[2])
                            total_transactions += trans_count
                        except (IndexError, ValueError):
                            pass
                    logger.info(f"✓ {filename}: {message}")
                else:
                    logger.warning(f"✗ {filename}: {message}")
            
            # Rebuild overall table once after processing all files
            overall_table_manager = OverallTableManager(self.db_path)
            overall_table_manager.build_overall_table()
            
            final_message = f"Successfully processed {files_processed} files with {total_transactions} total transactions"
            logger.info(final_message)
            return True, final_message
            
        except Exception as e:
            logger.error(f"Error processing files: {e}")
            return False, f"Error processing files: {str(e)}"


# Legacy function wrappers for backward compatibility
def process_file(file_path: str) -> Optional[List[Dict]]:
    """Legacy wrapper for process_file."""
    processor = OtherCSVProcessor()
    return processor.process_file(file_path)


def update_database(file_path: str, db_path: str = 'daily_accounting.db') -> Tuple[bool, str]:
    """Legacy wrapper for update_database."""
    processor = OtherCSVProcessor(db_path)
    return processor.update_database(file_path)


def process_all_files(folder_path: str, db_path: str = 'daily_accounting.db') -> Tuple[bool, str]:
    """Legacy wrapper for process_all_files."""
    processor = OtherCSVProcessor(db_path)
    return processor.process_all_files(folder_path)


if __name__ == '__main__':
    # Configuration - customize these paths as needed
    csv_folder_path = r"C:\Users\owent\Documents\Link Signis Internship\Other Transactions CSV"
    database_path = r"C:\Users\owent\Documents\Link Signis Internship\daily_accounting.db"
    
    # Process all files
    processor = OtherCSVProcessor(database_path)
    success, message = processor.process_all_files(csv_folder_path)
    
    if success:
        print(f"\n{message}")
    else:
        print(f"Processing failed: {message}")
        exit(1)
