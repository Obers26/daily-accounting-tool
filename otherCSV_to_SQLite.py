import pandas as pd
import os
from datetime import datetime
import sqlite3
import overall_table

def process_file(file_path):
    """
    Process a single CSV file with other transaction data.
    
    Args:
        file_path (str): Path to the CSV file
    
    Returns:
        list: List of transaction dictionaries, or None if error
    """
    try:
        print(f"Processing file: {os.path.basename(file_path)}")
        
        # Read the CSV file with error handling
        try:
            df = pd.read_csv(file_path, on_bad_lines='skip', encoding='utf-8')
        except:
            df = pd.read_csv(file_path, on_bad_lines='skip')
        
        # Check if required columns exist
        required_columns = ['Date', 'Amount', 'Account Description', 'Transaction Description', 
                           'Counted in P&L', 'Overnight', 'Additional Info']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing required columns: {missing_columns}")
            return None
        
        transactions = []
        
        # Process each row
        for _, row in df.iterrows():
            try:
                # Parse and format date
                date_str = str(row['Date']).strip()
                if date_str and date_str != 'nan':
                    try:
                        # Try different date formats
                        if '/' in date_str:
                            date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                        elif '-' in date_str:
                            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                        else:
                            date_obj = datetime.strptime(date_str, '%B %d, %Y')
                        
                        formatted_date = date_obj.strftime('%m/%d/%Y')
                    except ValueError:
                        print(f"Warning: Could not parse date '{date_str}', skipping row")
                        continue
                else:
                    print(f"Warning: Empty date field, skipping row")
                    continue
                
                # Parse amount
                amount_str = str(row['Amount']).replace('$', '').replace(',', '').strip()
                try:
                    amount = float(amount_str) if amount_str and amount_str != 'nan' else 0.0
                except ValueError:
                    amount = 0.0
                
                # Parse boolean for "Counted in P&L"
                counted_in_pl_str = str(row['Counted in P&L']).strip().lower()
                counted_in_pl = counted_in_pl_str in ['true', '1', 'yes', 'y']
                
                # Parse boolean for "Overnight"
                overnight_str = str(row['Overnight']).strip().lower()
                overnight = overnight_str in ['true', '1', 'yes', 'y']
                
                # Get other string fields
                account_desc = str(row['Account Description']).strip() if pd.notna(row['Account Description']) else ''
                transaction_desc = str(row['Transaction Description']).strip() if pd.notna(row['Transaction Description']) else ''
                additional_info = str(row['Additional Info']).strip() if pd.notna(row['Additional Info']) else ''
                
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
                print(f"Warning: Error processing row: {str(e)}")
                continue
        
        print(f"Successfully processed {len(transactions)} transactions from {os.path.basename(file_path)}")
        return transactions
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return None

def update_database(file_path, db_path='daily_accounting.db'):
    """
    Update the database with data from a single CSV file.
    
    Args:
        file_path (str): Path to the CSV file
        db_path (str): Path to the SQLite database file (default: 'daily_accounting.db')
    
    Returns:
        bool: True if successful, False otherwise
        str: Success message or error message
    """
    try:
        # Process the file
        transactions = process_file(file_path)
        
        if not transactions:
            return False, "Failed to process file or no valid transactions found"
        
        # Connect to database
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
        
        # Insert transactions
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
        
        conn.commit()
        conn.close()
        
        # Rebuild overall table since other transactions have changed
        overall_table.build_overall_table(db_path)

        message = f"Successfully processed {len(transactions)} transactions: {rows_inserted} inserted, {rows_updated} updated"
        return True, message
        
    except Exception as e:
        return False, f"Error updating database: {str(e)}"

def process_all_files(folder_path, db_path='daily_accounting.db'):
    """
    Process all CSV files in the specified folder and store data in SQLite database.
    
    Args:
        folder_path (str): Path to the folder containing CSV files
        db_path (str): Path to the SQLite database file (default: 'daily_accounting.db')
    
    Returns:
        bool: True if successful, False otherwise
        str: Success message or error message
    """
    try:
        print(f"Processing files in folder: {folder_path}")
        print(f"Database file: {db_path}")
        
        if not os.path.exists(folder_path):
            return False, f"Folder does not exist: {folder_path}"
        
        # Initialize database
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
        
        conn.close()
        
        # Process each CSV file in the folder
        files_processed = 0
        total_transactions = 0
        
        csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
        
        if not csv_files:
            return False, "No CSV files found in the specified folder"
        
        for filename in csv_files:
            file_path = os.path.join(folder_path, filename)
            success, message = update_database(file_path, db_path)
            
            if success:
                files_processed += 1
                # Extract number of transactions from message
                if "processed" in message:
                    try:
                        trans_count = int(message.split()[2])
                        total_transactions += trans_count
                    except:
                        pass
                print(f"✓ {filename}: {message}")
            else:
                print(f"✗ {filename}: {message}")
        
        # All individual updates finished, rebuild aggregate table once now
        overall_table.build_overall_table(db_path)

        final_message = f"Successfully processed {files_processed} files with {total_transactions} total transactions"
        print(final_message)
        return True, final_message
        
    except Exception as e:
        error_message = f"Error processing files: {str(e)}"
        print(error_message)
        return False, error_message

if __name__ == '__main__':
    # Configuration - customize these paths as needed
    csv_folder_path = r"C:\Users\owent\Documents\Link Signis Internship\Other Transactions CSV"
    database_path = r"C:\Users\owent\Documents\Link Signis Internship\daily_accounting.db"
    
    # Process all files
    success, message = process_all_files(csv_folder_path, database_path)
    
    if success:
        print(f"\n{message}")
    else:
        print(f"Processing failed: {message}")
        exit(1)
