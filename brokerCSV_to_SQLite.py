import pandas as pd
import os
from datetime import datetime
import sqlite3
import overall_table

def extract_date_from_csv(file_path):
    try:
        # Read the CSV file
        df = pd.read_csv(file_path, on_bad_lines='skip', encoding='utf-8')
        
        # Find the row where 'Field Name' is 'Period'
        period_row = df[df['Field Name'] == 'Period']
        
        if period_row.empty:
            return None
        
        # Get the date value and parse it
        date_str = period_row['Field Value'].iloc[0]
        date_obj = datetime.strptime(date_str, '%B %d, %Y')
        
        # Convert to desired format (MM/DD/YYYY)
        formatted_date = date_obj.strftime('%m/%d/%Y')
        
        return formatted_date
    except Exception as e:
        return None

def process_file(file_path):
    try:
        print(f"Processing file: {os.path.basename(file_path)}")
        
        # Extract date from the CSV using the dedicated function
        date = extract_date_from_csv(file_path)
        if not date:
            print("No 'Period' field found in CSV")
            return None
        
        # Read the CSV file with error handling
        try:
            df = pd.read_csv(file_path, on_bad_lines='skip', encoding='utf-8')
        except:
            df = pd.read_csv(file_path, on_bad_lines='skip')
        
        # Filter for rows where 'Statement' is 'Change in NAV'
        nav_section = df[df['Statement'] == 'Change in NAV']
        nav_data = nav_section[['Field Name', 'Field Value']].iloc[1:]  # Skip first row
        
        # Initialize fields dictionary
        fields = {
            'Date': date,
            'P&L': None,
            'Reporting Error': None,
            'Cumulative P&L': None,
            'Mark-to-Market': None,
            'Change in Dividend Accruals': None,
            'Interest': None,
            'Dividends': None,
            'Deposits & Withdrawals': None,
            'Change in Interest Accruals': None,
            'Commissions': None,
            'Total Broker': None
        }
        
        # Get Starting Value and Ending Value
        starting_value = None
        ending_value = None
        deposits_withdrawals = 0
        
        # Fill in the values
        for _, row in nav_data.iterrows():
            field_name = row['Field Name']
            field_value = row['Field Value']
            
            if field_name == 'Starting Value':
                try:
                    starting_value = float(str(field_value).replace('$', '').replace(',', ''))
                except (ValueError, AttributeError):
                    pass
                    
            elif field_name == 'Ending Value':
                try:
                    ending_value = float(str(field_value).replace('$', '').replace(',', ''))
                    fields['Total Broker'] = ending_value
                except (ValueError, AttributeError):
                    pass

            if field_name == 'Deposits & Withdrawals':
                try:
                    deposits_withdrawals = float(str(field_value).replace('$', '').replace(',', ''))
                    fields['Deposits & Withdrawals'] = deposits_withdrawals
                except (ValueError, AttributeError):
                    pass

            elif field_name in fields:
                try:
                    # Remove currency symbols and commas
                    value_str = str(field_value).replace('$', '').replace(',', '')
                    value = float(value_str)
                    fields[field_name] = value
                except (ValueError, AttributeError):
                    fields[field_name] = None
        
        # Calculate P&L using two methods
        pnl_method1 = None  # Sum of components method
        pnl_method2 = None  # Traditional method: ending - starting - deposits/withdrawals
        
        # Method 1: Sum of P&L components (treat None/blank as 0)
        components = [
            fields.get('Mark-to-Market') or 0,
            fields.get('Change in Interest Accruals') or 0, 
            fields.get('Change in Dividend Accruals') or 0,
            fields.get('Commissions') or 0
        ]
        
        # Calculate method 1 (always possible now since None values become 0)
        pnl_method1 = sum(components)
        
        # Method 2: Traditional calculation (subtract interest and dividends too)
        if starting_value is not None and ending_value is not None:
            interest = fields.get('Interest') or 0
            dividends = fields.get('Dividends') or 0
            pnl_method2 = ending_value - starting_value - deposits_withdrawals - interest - dividends
        
        # Compare methods and detect discrepancies
        pnl_discrepancy = False
        tolerance = 0.01  # Allow for small rounding differences
        
        if pnl_method1 is not None and pnl_method2 is not None:
            # Both methods available - compare them
            if abs(pnl_method1 - pnl_method2) > tolerance:
                pnl_discrepancy = True
                print(f"P&L DISCREPANCY DETECTED for {date}:")
                print(f"  Method 1 (Sum of Components): ${pnl_method1:.2f}")
                print(f"  Method 2 (Ending - Starting - Deposits/Withdrawals - Interest - Dividends): ${pnl_method2:.2f}")
                print(f"  Difference: ${abs(pnl_method1 - pnl_method2):.2f}")
                
                # Use method 1 as primary but flag the error
                fields['P&L'] = pnl_method1
                fields['Reporting Error'] = abs(pnl_method1 - pnl_method2)
            else:
                # Methods agree - use method 1
                fields['P&L'] = pnl_method1
                fields['Reporting Error'] = 0.0
        elif pnl_method1 is not None:
            # Only method 1 available
            fields['P&L'] = pnl_method1
            print(f"Warning: Could not verify P&L using traditional method for {date} - missing starting/ending values")
        elif pnl_method2 is not None:
            # Only method 2 available
            fields['P&L'] = pnl_method2
            print(f"Warning: Using traditional P&L for {date} - missing component data")
        else:
            # Neither method available
            print(f"Error: Could not calculate P&L for {date} - insufficient data")
        
        return fields
        
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
        data = process_file(file_path)
        
        if not data:
            return False, "Failed to process file"
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist using exact CSV field names
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS broker (
                "Date" TEXT PRIMARY KEY,
                "P&L" REAL,
                "Reporting Error" REAL,
                "Cumulative P&L" REAL,
                "Mark-to-Market" REAL,
                "Change in Dividend Accruals" REAL,
                "Interest" REAL,
                "Dividends" REAL,
                "Deposits & Withdrawals" REAL,
                "Change in Interest Accruals" REAL,
                "Commissions" REAL,
                "Total Broker" REAL
            )
        ''')
        
        # Insert or update data
        cursor.execute('''
            INSERT OR REPLACE INTO broker 
            ("Date", "P&L", "Reporting Error", "Cumulative P&L", "Mark-to-Market",
             "Change in Dividend Accruals", "Interest", "Dividends", "Deposits & Withdrawals",
             "Change in Interest Accruals", "Commissions", "Total Broker")
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data['Date'],
            data['P&L'],
            data['Reporting Error'],
            data['Cumulative P&L'],
            data['Mark-to-Market'],
            data['Change in Dividend Accruals'],
            data['Interest'],
            data['Dividends'],
            data['Deposits & Withdrawals'],
            data['Change in Interest Accruals'],
            data['Commissions'],
            data['Total Broker']
        ))
        
        conn.commit()
        conn.close()

        # Rebuild overall table to keep aggregates in sync
        overall_table.build_overall_table(db_path)

        return True, "Data successfully updated in database"
        
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
        
        # Initialize database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist (preserve existing data)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS broker (
                "Date" TEXT PRIMARY KEY,
                "P&L" REAL,
                "Reporting Error" REAL,
                "Cumulative P&L" REAL,
                "Mark-to-Market" REAL,
                "Change in Dividend Accruals" REAL,
                "Interest" REAL,
                "Dividends" REAL,
                "Deposits & Withdrawals" REAL,
                "Change in Interest Accruals" REAL,
                "Commissions" REAL,
                "Total Broker" REAL
            )
        ''')
        
        # Process each CSV file in the folder
        files_processed = 0
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(folder_path, filename)
                update_database(file_path, db_path)
                files_processed += 1
        conn.commit()
        conn.close()

        # Rebuild overall table once after processing all files
        overall_table.build_overall_table(db_path)

        print(f"Successfully processed {files_processed} files")
        return True, f"Successfully processed {files_processed} files"
        
    except Exception as e:
        print(f"Error processing files: {str(e)}")
        return False, f"Error processing files: {str(e)}"

if __name__ == '__main__':
    # Configuration - customize these paths as needed
    csv_folder_path = r"C:\Users\owent\Documents\Link Signis Internship\2023-03\CSV"
    database_path = r"C:\Users\owent\Documents\Link Signis Internship\daily_accounting.db"
    
    # Process all files
    success, message = process_all_files(csv_folder_path, database_path)
    if not success:
        print(message)
        exit(1)