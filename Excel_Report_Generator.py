import pandas as pd
import sqlite3
from datetime import datetime
import openpyxl

def generate_excel_report(start_date, end_date, output_path, db_path='daily_accounting.db'):
    """
    Generate an Excel report for the specified date range.
    
    Args:
        start_date (str): Start date in format 'MM/DD/YYYY'
        end_date (str): End date in format 'MM/DD/YYYY'
        output_path (str): Path where the Excel file should be saved
        db_path (str): Path to the SQLite database file (default: 'daily_accounting.db')
    
    Returns:
        bool: True if successful, False otherwise
        str: Success message or error message
    """
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Convert input dates to datetime objects for comparison
        try:
            start_dt = datetime.strptime(start_date, '%m/%d/%Y')
            end_dt = datetime.strptime(end_date, '%m/%d/%Y')
        except ValueError as e:
            return False, "Invalid date format"
        
        # Query data for the date range from broker table
        query_broker = '''
            SELECT * FROM broker 
            WHERE "Date" BETWEEN ? AND ?
            ORDER BY "Date"
        '''
        
        df_broker = pd.read_sql_query(query_broker, conn, params=(start_date, end_date))
        
        if df_broker.empty:
            return False, "No broker data found for the specified date range"
        
        # Query data for the date range from overall table
        query_overall = '''
            SELECT * FROM overall 
            WHERE "Date" BETWEEN ? AND ?
            ORDER BY "Date"
        '''
        
        df_overall = pd.read_sql_query(query_overall, conn, params=(start_date, end_date))
        
        # Query data for the date range from other_transactions table
        query_other = '''
            SELECT * FROM other_transactions 
            WHERE "Date" BETWEEN ? AND ?
            ORDER BY "Date"
        '''
        
        df_other = pd.read_sql_query(query_other, conn, params=(start_date, end_date))
        
        # Remove unwanted columns if they exist
        if not df_other.empty and 'id' in df_other.columns:
            df_other = df_other.drop('id', axis=1)
        
        # Convert "Counted in P&L" column from 1/0 to Yes/No
        if not df_other.empty and 'Counted in P&L' in df_other.columns:
            df_other['Counted in P&L'] = df_other['Counted in P&L'].map({1: 'Yes', 0: 'No'})
        
        # Convert "Overnight" column from 1/0 to Yes/No
        if not df_other.empty and 'Overnight' in df_other.columns:
            df_other['Overnight'] = df_other['Overnight'].map({1: 'Yes', 0: 'No'})
        
        # Set date as index for all dataframes
        df_broker.set_index('Date', inplace=True)
        if not df_overall.empty:
            df_overall.set_index('Date', inplace=True)
            # Add Period Cumulative Return column (will be populated with formulas later)
            df_overall['Period Cumulative Return'] = 0.0
        if not df_other.empty:
            df_other.set_index('Date', inplace=True)
        
        # Create Excel writer object
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Write the Overall sheet first
            if not df_overall.empty:
                df_overall.to_excel(writer, sheet_name='Overall')
                
                # Format the Overall sheet
                worksheet_overall = writer.sheets['Overall']
                
                # Set Date column width
                worksheet_overall.column_dimensions['A'].width = 12
                
                # Add Daily Fund Return calculation formula
                # Formula: Daily Fund Return = (Total P&L / Start of Day Fund Value) 
                # Use current day's Start of Day Fund Value as denominator
                for row in range(2, len(df_overall) + 2):  # Start from row 2 (after header and first row)
                    # Get column positions based on actual schema:
                    # Date (A), Broker P&L (B), Total Broker (C), Other P&L (D), Total Other (E), Total P&L (F), Period Starting NAV (G), Start of Day Fund Value (H), Total Fund Value (I), Daily Fund Return (J), Period Cumulative Return (K)
                    total_pl_col = 'F'  # Total P&L column
                    period_starting_nav_col = 'G'  # Period Starting NAV column
                    start_of_day_fund_value_col = 'H'  # Start of Day Fund Value column
                    total_fund_value_col = 'I'  # Total Fund Value column
                    daily_fund_return_col = 'J'  # Daily Fund Return column
                    period_cumulative_return_col = 'K'  # Period Cumulative Return column
                    
                    # Use Start of Day Fund Value as denominator for daily return calculation
                    denominator = f'{start_of_day_fund_value_col}{row}'
                    
                    # Add the Daily Fund Return formula 
                    daily_return_cell = f'{daily_fund_return_col}{row}'
                    numerator = f'{total_pl_col}{row}'
                    worksheet_overall[daily_return_cell] = f'=({numerator}/{denominator})'
                    # Format as percentage with 2 decimal places
                    worksheet_overall[daily_return_cell].number_format = '0.00%'
                    
                    # Add the Period Cumulative Return formula
                    # Formula: Cumulative P&L since last valuation date / Period Starting NAV
                    cumulative_return_cell = f'{period_cumulative_return_col}{row}'
                    
                    if row == 2:  # First data row
                        # Same as Daily Fund Return for the first row
                        worksheet_overall[cumulative_return_cell] = f'=({total_pl_col}{row}/{period_starting_nav_col}{row})'
                    else:
                        # Check if Period Starting NAV changed from previous day (new valuation date)
                        current_nav = f'{period_starting_nav_col}{row}'
                        prev_nav = f'{period_starting_nav_col}{row-1}'
                        prev_cumulative = f'{period_cumulative_return_col}{row-1}'
                        
                        # If NAV changed, reset cumulative return to current day's return
                        # If NAV same, add current day's P&L to previous cumulative P&L amount
                        # Formula: IF(NAV changed, current P&L / current NAV, (previous cumulative % * previous NAV + current P&L) / current NAV)
                        worksheet_overall[cumulative_return_cell] = f'=IF({current_nav}<>{prev_nav},{total_pl_col}{row}/{current_nav},({prev_cumulative}*{prev_nav}+{total_pl_col}{row})/{current_nav})'
                    
                    # Format as percentage with 2 decimal places
                    worksheet_overall[cumulative_return_cell].number_format = '0.00%'
                
                # Auto-adjust column widths and format numbers
                for idx, col in enumerate(df_overall.columns):
                    max_length = max(
                        df_overall[col].astype(str).apply(len).max(),
                        len(str(col))
                    )
                    worksheet_overall.column_dimensions[chr(65 + idx + 1)].width = max_length + 2
                    
                    # Format numbers in the column
                    for row in range(2, len(df_overall) + 2):
                        cell = worksheet_overall.cell(row=row, column=idx + 2)
                        if isinstance(cell.value, (int, float)):
                            # Special formatting for percentage columns
                            if col in ['Daily Fund Return', 'Period Cumulative Return']:
                                cell.number_format = '0.00%'
                            else:
                                # All other numbers rounded to whole numbers
                                cell.number_format = '#,##0;(#,##0)'
            
            # Write the Brokerage Account sheet second
            df_broker.to_excel(writer, sheet_name='Brokerage Account')
            
            # Get the workbook and the worksheet for broker data
            worksheet_broker = writer.sheets['Brokerage Account']
            
            # Store original P&L values for comparison
            original_pnl_values = df_broker['P&L'].copy()
            
            # Add P&L calculation formula
            # Formula: P&L = Total Broker - Previous Day's Total Broker - Deposits & Withdrawals
            for row in range(3, len(df_broker) + 2):  # Start from row 3 (after header and first row)
                # Get the cell references for Total Broker (column L since we have all the original columns)
                current_total = f'L{row}'  # Column L is Total Broker
                prev_total = f'L{row-1}'   # Previous day's Total Broker
                deposits_withdrawals = f'I{row}'  # Column I is Deposits & Withdrawals
                dividends = f'H{row}'  # Column H is Dividends
                interest = f'G{row}'  # Column G is Interest
                
                # Add the P&L formula (column B is P&L)
                pnl_cell = f'B{row}'  # Column B is P&L
                worksheet_broker[pnl_cell] = f'={current_total}-{prev_total}-{deposits_withdrawals}-{dividends}-{interest}'
                # Format the P&L formula cell to display as whole numbers
                worksheet_broker[pnl_cell].number_format = '#,##0;(#,##0)'
            
            # Validate P&L calculations against database values
            print("Validating P&L calculations...")
            tolerance = 0.01  # Allow for small rounding differences
            discrepancies_found = False
            
            for idx, (date_idx, row_data) in enumerate(df_broker.iterrows()):
                if idx == 0:  # Skip first row as it has no previous day to compare
                    continue
                    
                # Calculate P&L manually using the same formula as Excel
                current_total = row_data['Total Broker'] or 0
                prev_total = df_broker.iloc[idx-1]['Total Broker'] or 0
                deposits_withdrawals = row_data['Deposits & Withdrawals'] or 0
                dividends = row_data['Dividends'] or 0
                interest = row_data['Interest'] or 0
                
                calculated_value = current_total - prev_total - deposits_withdrawals - dividends - interest
                
                # Get the original database value
                database_value = original_pnl_values.iloc[idx]
                
                # Compare values
                if database_value is not None:
                    if abs(calculated_value - database_value) > tolerance:
                        discrepancies_found = True
                        print(f"P&L DISCREPANCY for {date_idx}:")
                        print(f"  Database P&L: ${database_value:.2f}")
                        print(f"  Formula P&L: ${calculated_value:.2f}")
                        print(f"  Difference: ${abs(calculated_value - database_value):.2f}")
                        print(f"  Components: Total=${current_total:.2f}, PrevTotal=${prev_total:.2f}, Deposits=${deposits_withdrawals:.2f}, Dividends=${dividends:.2f}, Interest=${interest:.2f}")
                else:
                    print(f"P&L DISCREPANCY for {date_idx}: Database P&L is None")
            
            if not discrepancies_found:
                print("✓ All P&L calculations match between database and Excel formulas")
            else:
                print("⚠ P&L discrepancies detected - please review the data")
            
            # Adjust column widths and format numbers for Brokerage Account sheet
            # First, set the Date column (column A) width specifically
            worksheet_broker.column_dimensions['A'].width = 12  # Set Date column width to accommodate MM/DD/YYYY format
            
            for idx, col in enumerate(df_broker.columns):
                max_length = max(
                    df_broker[col].astype(str).apply(len).max(),
                    len(str(col))
                )
                worksheet_broker.column_dimensions[chr(65 + idx + 1)].width = max_length + 2
                
                # Format numbers in the column
                for row in range(2, len(df_broker) + 2):  # Start from 2 to skip header
                    cell = worksheet_broker.cell(row=row, column=idx + 2)  # +2 because Excel is 1-based and we have an index column
                    if isinstance(cell.value, (int, float)):
                        cell.number_format = '#,##0;(#,##0)'  # Format with parentheses for negative numbers, rounded to nearest whole number
            
            # Write the Other Transactions sheet third
            if not df_other.empty:
                df_other.to_excel(writer, sheet_name='Other Transactions')
                
                # Format the Other Transactions sheet
                worksheet_other = writer.sheets['Other Transactions']
                
                # Set Date column width
                worksheet_other.column_dimensions['A'].width = 12
                
                # Auto-adjust column widths and format numbers
                for idx, col in enumerate(df_other.columns):
                    max_length = max(
                        df_other[col].astype(str).apply(len).max(),
                        len(str(col))
                    )
                    worksheet_other.column_dimensions[chr(65 + idx + 1)].width = max_length + 2
                    
                    # Format numbers in the column (specifically for Amount column)
                    for row in range(2, len(df_other) + 2):
                        cell = worksheet_other.cell(row=row, column=idx + 2)
                        if isinstance(cell.value, (int, float)):
                            # Round all numbers to whole numbers
                            cell.number_format = '#,##0;(#,##0)'
        
        conn.close()
        
        # Create success message
        sheets_created = []
        if not df_overall.empty:
            sheets_created.append('Overall')
        sheets_created.append('Brokerage Account')  # Always present
        if not df_other.empty:
            sheets_created.append('Other Transactions')
        
        success_message = f"Excel report generated successfully with sheets: {', '.join(sheets_created)}"
        return True, success_message
        
    except Exception as e:
        return False, f"Error generating report: {str(e)}"
    
if __name__ == '__main__':
    # Generate report
    success, result = generate_excel_report('02/05/2023', '03/14/2023', r'C:\Users\owent\Documents\Link Signis Internship\nav_changes_test.xlsx', r'C:\Users\owent\Documents\Link Signis Internship\Daily Accounting Tool\daily_accounting.db')
    if not success:
        print(result)
        exit(1)
    else:
        print(result)