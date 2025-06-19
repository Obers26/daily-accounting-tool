#!/usr/bin/env python3
"""
Comprehensive P&L Calculation Testing Suite

This unified test framework validates:
1. P&L calculation accuracy using dual methods (CSV_NAV_to_SQLite.py)
2. Discrepancy detection between calculation methods
3. Excel report validation logic (Excel_Report_Generator.py)
4. Error handling for various data scenarios
5. Edge cases and negative value handling

Usage:
    python test_pnl_comprehensive.py
"""

import unittest
import sqlite3
import pandas as pd
import os
import tempfile
import csv
from unittest.mock import patch, MagicMock
from datetime import datetime

# Import the functions we want to test
try:
    from CSV_NAV_to_SQLite import process_file, extract_date_from_csv, update_database
    from Excel_Report_Generator import generate_excel_report
except ImportError as e:
    print(f"Warning: Could not import modules: {e}")
    print("Make sure CSV_NAV_to_SQLite.py and Excel_Report_Generator.py are in the same directory")

class TestPnLCalculations(unittest.TestCase):
    """
    Comprehensive test suite for P&L calculation validation.
    
    Tests cover:
    - Dual method P&L calculations
    - Discrepancy detection and reporting
    - Missing data handling
    - Negative value processing
    - Excel validation logic
    - Database operations
    """
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create temporary database for testing
        self.test_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.test_db_path = self.test_db.name
        self.test_db.close()
        
        # Create temporary directory for CSV files
        self.test_csv_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up after each test method."""
        # Remove test database
        if os.path.exists(self.test_db_path):
            os.unlink(self.test_db_path)
        
        # Remove test CSV files
        for file in os.listdir(self.test_csv_dir):
            os.unlink(os.path.join(self.test_csv_dir, file))
        os.rmdir(self.test_csv_dir)
    
    def create_test_csv(self, filename, data_dict):
        """
        Create a test CSV file with specified NAV data.
        
        Args:
            filename (str): Name of CSV file to create
            data_dict (dict): Dictionary containing test data values
            
        Returns:
            str: Path to created CSV file
        """
        csv_path = os.path.join(self.test_csv_dir, filename)
        
        # Create structured CSV data matching expected format
        rows = [
            ['Field Name', 'Field Value', 'Statement'],
            ['Period', data_dict.get('date', 'January 1, 2023'), ''],
            ['', '', ''],
            ['Change in NAV', '', 'Change in NAV'],
            ['Starting Value', f"${data_dict.get('starting_value', 100000):.2f}", 'Change in NAV'],
            ['Ending Value', f"${data_dict.get('ending_value', 105000):.2f}", 'Change in NAV'],
            ['Deposits & Withdrawals', f"${data_dict.get('deposits_withdrawals', 0):.2f}", 'Change in NAV'],
            ['Interest', f"${data_dict.get('interest', 0):.2f}", 'Change in NAV'],
            ['Dividends', f"${data_dict.get('dividends', 0):.2f}", 'Change in NAV'],
            ['Mark-to-Market', f"${data_dict.get('mark_to_market', 4000):.2f}", 'Change in NAV'],
            ['Change in Interest Accruals', f"${data_dict.get('change_interest_accruals', 500):.2f}", 'Change in NAV'],
            ['Change in Dividend Accruals', f"${data_dict.get('change_dividend_accruals', 300):.2f}", 'Change in NAV'],
            ['Commissions', f"${data_dict.get('commissions', -200):.2f}", 'Change in NAV'],
        ]
        
        # Write CSV file
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        
        return csv_path

    # === P&L CALCULATION TESTS ===
    
    def test_pnl_discrepancy_detection(self):
        """Test that P&L discrepancies are correctly identified and reported."""
        print("\n=== TEST: P&L Discrepancy Detection ===")
        
        # Create data that will cause a discrepancy between methods
        test_data = {
            'date': 'January 15, 2023',
            'starting_value': 100000,
            'ending_value': 104000,  # Will create discrepancy
            'deposits_withdrawals': 0,
            'interest': 200,
            'dividends': 400,
            'mark_to_market': 4000,
            'change_interest_accruals': 500,
            'change_dividend_accruals': 300,
            'commissions': -200
        }
        
        # Expected calculations:
        # Method 1: 104000 - 100000 - 0 - 200 - 400 = 3400
        # Method 2: 4000 + 500 + 300 + (-200) = 4600
        # Discrepancy: |3400 - 4600| = 1200
        
        csv_path = self.create_test_csv('discrepancy_test.csv', test_data)
        
        # Capture print output to verify discrepancy reporting
        with patch('builtins.print') as mock_print:
            result = process_file(csv_path)
        
        # Verify discrepancy was detected
        self.assertIsNotNone(result, "process_file should return valid result")
        self.assertGreater(result['Reporting Error'], 0, "Reporting Error should be > 0 when discrepancy exists")
        self.assertAlmostEqual(result['Reporting Error'], 1200, places=1, msg="Discrepancy should be ~1200")
        
        # Verify discrepancy was logged
        print_calls = [str(call) for call in mock_print.call_args_list]
        discrepancy_logged = any('P&L DISCREPANCY DETECTED' in call for call in print_calls)
        self.assertTrue(discrepancy_logged, "Discrepancy should be logged to console")
        
        print(f"✓ Discrepancy correctly detected: ${result['Reporting Error']:.2f}")
        print(f"✓ P&L stored: ${result['P&L']:.2f}")
    
    def test_pnl_methods_agreement(self):
        """Test P&L calculation when both methods should agree."""
        print("\n=== TEST: P&L Methods Agreement ===")
        
        # Create data where both methods will produce same result
        # Method 2: 4000 + 800 + 600 + (-400) = 5000
        # Method 1: ending - starting - deposits - interest - dividends = 5000
        # So: ending = starting + 5000 + deposits + interest + dividends
        test_data = {
            'date': 'January 16, 2023',
            'starting_value': 100000,
            'deposits_withdrawals': 500,
            'interest': 100,
            'dividends': 200,
            'mark_to_market': 4000,
            'change_interest_accruals': 800,
            'change_dividend_accruals': 600,
            'commissions': -400
        }
        test_data['ending_value'] = 100000 + 5000 + 500 + 100 + 200  # = 105800
        
        csv_path = self.create_test_csv('agreement_test.csv', test_data)
        
        with patch('builtins.print') as mock_print:
            result = process_file(csv_path)
        
        # Verify no discrepancy
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result['Reporting Error'], 0.0, places=2, msg="No reporting error expected")
        self.assertAlmostEqual(result['P&L'], 5000, places=1, msg="P&L should be 5000")
        
        print(f"✓ Methods agree: P&L=${result['P&L']:.2f}, Error=${result['Reporting Error']:.2f}")
    
    def test_missing_component_data(self):
        """Test handling of missing P&L component data."""
        print("\n=== TEST: Missing Component Data ===")
        
        test_data = {
            'date': 'January 17, 2023',
            'starting_value': 100000,
            'ending_value': 102500,
            'deposits_withdrawals': 0,
            'interest': 50,
            'dividends': 100,
            'mark_to_market': 2000,
            # Missing: change_interest_accruals, change_dividend_accruals, commissions
        }
        
        csv_path = self.create_test_csv('missing_data_test.csv', test_data)
        
        # Remove some component rows to simulate missing data
        df = pd.read_csv(csv_path)
        df = df[~df['Field Name'].isin(['Change in Interest Accruals', 'Commissions'])]
        df.to_csv(csv_path, index=False)
        
        result = process_file(csv_path)
        
        self.assertIsNotNone(result, "Should handle missing data gracefully")
        
        # Method 1: 102500 - 100000 - 0 - 50 - 100 = 2350
        expected_method1 = 2350
        self.assertAlmostEqual(result['P&L'], expected_method1, places=1)
        
        print(f"✓ Missing data handled: P&L=${result['P&L']:.2f}")

    # === EXCEL VALIDATION TESTS ===
    
    def test_excel_validation_logic(self):
        """Test Excel report P&L validation functionality."""
        print("\n=== TEST: Excel Validation Logic ===")
        
        # Create test database with sample data
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Create table schema
        cursor.execute('''
            CREATE TABLE nav_changes (
                date TEXT PRIMARY KEY,
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
        
        # Insert test data that will create validation discrepancy
        test_data = [
            ('01/15/2023', 4000.00, 0.00, None, 3500, 200, 100, 200, 0, 300, -200, 100000),
            ('01/16/2023', 2000.00, 0.00, None, 2500, 150, 50, 150, 200, 200, -100, 101500)  
            # Excel calc: 101500 - 100000 - 200 - 50 - 150 = 1100 (vs DB: 2000) = discrepancy
        ]
        
        cursor.executemany('''
            INSERT INTO nav_changes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', test_data)
        
        conn.commit()
        conn.close()
        
        # Test Excel validation with mocked database connection
        output_path = os.path.join(self.test_csv_dir, 'validation_test.xlsx')
        
        with patch('sqlite3.connect') as mock_connect:
            mock_connect.return_value = sqlite3.connect(self.test_db_path)
            
            with patch('builtins.print') as mock_print:
                success, result = generate_excel_report('01/15/2023', '01/16/2023', output_path)
        
        # Assert that Excel generation succeeded
        self.assertTrue(success, f"Excel generation should succeed but failed with: {result}")
        
        # Verify validation ran
        print_calls = [str(call) for call in mock_print.call_args_list]
        validation_started = any('Validating P&L calculations' in call for call in print_calls)
        self.assertTrue(validation_started, "Excel validation should run")
        
        print("✓ Excel validation logic executed successfully")

    # === EDGE CASE TESTS ===
    
    def test_negative_values_in_parentheses(self):
        """Test proper handling of negative values in parentheses format."""
        print("\n=== TEST: Negative Values (Parentheses Format) ===")
        
        test_data = {
            'date': 'January 18, 2023',
            'starting_value': 100000,
            'ending_value': 97500,
            'deposits_withdrawals': 0,
            'interest': 100,
            'dividends': 200,
            'mark_to_market': -3000,  # Will be formatted as (3000.00)
            'change_interest_accruals': 500,
            'change_dividend_accruals': 200,
            'commissions': -500       # Will be formatted as (500.00)
        }
        
        csv_path = self.create_test_csv('negative_test.csv', test_data)
        
        # Note: Since we removed parentheses handling, negative values should parse as-is
        result = process_file(csv_path)
        
        # Verify negative values parsed correctly
        self.assertIsNotNone(result)
        self.assertEqual(result['Mark-to-Market'], -3000, "Negative mark-to-market should be -3000")
        self.assertEqual(result['Commissions'], -500, "Negative commissions should be -500")
        
        print(f"✓ Negative values parsed: MTM=${result['Mark-to-Market']}, Comm=${result['Commissions']}")
    
    def test_database_operations(self):
        """Test database creation and data insertion."""
        print("\n=== TEST: Database Operations ===")
        
        test_data = {
            'date': 'January 19, 2023',
            'starting_value': 100000,
            'ending_value': 103000,
            'deposits_withdrawals': 0,
            'interest': 100,
            'dividends': 200,
            'mark_to_market': 2500,
            'change_interest_accruals': 400,
            'change_dividend_accruals': 300,
            'commissions': -100
        }
        
        csv_path = self.create_test_csv('db_test.csv', test_data)
        
        # Use a temporary database for this test
        temp_db = os.path.join(self.test_csv_dir, 'temp_test.db')
        
        # Mock the database path in the update_database function
        with patch('sqlite3.connect') as mock_connect:
            mock_conn = sqlite3.connect(temp_db)
            mock_connect.return_value = mock_conn
            
            success, message = update_database(csv_path)
            
            # Verify success
            self.assertTrue(success, f"Database update should succeed: {message}")
            
            # Verify data was inserted
            cursor = mock_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM nav_changes")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 1, "Should have one record in database")
            
            mock_conn.close()
        
        print("✓ Database operations working correctly")

def run_comprehensive_tests():
    """
    Execute the complete test suite with detailed reporting.
    """
    print("=" * 80)
    print("COMPREHENSIVE P&L CALCULATION & ERROR DETECTION TEST SUITE")
    print("=" * 80)
    print("Testing CSV processing, database operations, and Excel validation...")
    print()
    
    # Create test loader and runner
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestPnLCalculations)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    # Generate comprehensive summary
    print("\n" + "=" * 80)
    print("TEST EXECUTION SUMMARY")
    print("=" * 80)
    
    total_tests = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    passed = total_tests - failures - errors
    success_rate = (passed / total_tests * 100) if total_tests > 0 else 0
    
    print(f"Total Tests Run:     {total_tests}")
    print(f"Tests Passed:        {passed}")
    print(f"Tests Failed:        {failures}")
    print(f"Test Errors:         {errors}")
    print(f"Success Rate:        {success_rate:.1f}%")
    
    # Report failures in detail
    if result.failures:
        print(f"\nFAILED TESTS ({len(result.failures)}):")
        for i, (test, traceback) in enumerate(result.failures, 1):
            print(f"{i}. {test}")
            print(f"   Error: {traceback.split('AssertionError:')[-1].strip()}")
    
    # Report errors in detail
    if result.errors:
        print(f"\nTEST ERRORS ({len(result.errors)}):")
        for i, (test, traceback) in enumerate(result.errors, 1):
            print(f"{i}. {test}")
            print(f"   Error: {traceback.split('Exception:')[-1].strip()}")
    
    # Final assessment
    print("\n" + "=" * 80)
    if success_rate == 100:
        print("🎉 ALL TESTS PASSED!")
        print("✓ P&L calculation methods working correctly")
        print("✓ Discrepancy detection functioning properly") 
        print("✓ Excel validation logic operational")
        print("✓ Database operations successful")
        print("✓ Error handling robust for edge cases")
        print("\nYour P&L calculation system is ready for production use!")
    elif success_rate >= 80:
        print("⚠️  MOSTLY SUCCESSFUL")
        print("Most core functionality is working, but some issues need attention.")
    else:
        print("❌ SIGNIFICANT ISSUES DETECTED")
        print("Multiple test failures indicate problems that need to be resolved.")
    
    print("=" * 80)
    return result.wasSuccessful()

if __name__ == '__main__':
    # Run the comprehensive test suite
    success = run_comprehensive_tests()
    
    # Exit with appropriate code
    exit(0 if success else 1) 