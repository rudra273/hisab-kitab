#!/usr/bin/env python3
"""
Test script for SMS to Transaction conversion.
This script helps you test the conversion process before running it on all data.
"""

import os
import sys
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from convert import SMSToTransactionConverter, get_db_connection

def test_single_conversion():
    """Test the conversion of a single SMS message."""
    print("Testing SMS to Transaction Conversion...")
    
    try:
        # Initialize converter
        converter = SMSToTransactionConverter()
        print("✓ Converter initialized successfully")
        
        # Test cases based on your sample data
        test_cases = [
    {
        "address": "AX-HDFCBK-S",
        "body": "Sent Rs.36.00\nFrom HDFC Bank A/C *8206\nTo BMTC BUS KA57F2456\nOn 10/08/25\nRef 677927937758\nNot You?\nCall 18002586161/SMS BLOCK UPI to 7308080808"
    },
    {
        "address": "VM-HDFCBK-S",
        "body": "Sent Rs.260.00\nFrom HDFC Bank A/C *8206\nTo BADAL  MEHER\nOn 10/08/25\nRef 516059125345\nNot You?\nCall 18002586161/SMS BLOCK UPI to 7308080808"
    },
    {
        "address": "VD-HDFCBN-P",
        "body": "HDFC Bank:\nEnjoy freedom from high EMIs with a pre-approved Personal Loan at reduced rates. Check EMI: https://hdfcbk.io/HDFCBK/s/7dkLjLAB"
    },
    {
        "address": "JK-AXISBK-S",
        "body": "INR 1.00 credited\nA/c no. XX9624\n10-08-25, 00:01:25 IST\nUPI/P2A/839457076434/BADAL MEH/ICICI Ban - Axis Bank "
    }
]

        
        for i, test_case in enumerate(test_cases, 1):
            print(f"\n--- Test Case {i} ---")
            print(f"Address: {test_case['address']}")
            print(f"Body: {test_case['body'][:100]}...")
            
            # Test individual extraction methods first
            print("\nRule-based extraction:")
            bank = converter.extract_bank_from_address(test_case['address'])
            amount = converter.extract_amount(test_case['body'])
            transaction_type = converter.extract_transaction_type(test_case['body'])
            merchant = converter.extract_merchant(test_case['body'])
            
            print(f"  Bank from address: {bank}")
            print(f"  Amount from body: {amount}")
            print(f"  Transaction type: {transaction_type}")
            print(f"  Merchant: {merchant}")
            
            # Now test full conversion
            print("\nFull conversion result:")
            result = converter.convert_sms_to_transaction(
                test_case['body'], 
                test_case['address']
            )
            
            for key, value in result.items():
                status = "✓" if value is not None else "✗"
                print(f"  {status} {key}: {value}")
            
            # Check if conversion was successful
            success_count = sum(1 for v in result.values() if v is not None)
            print(f"  Success rate: {success_count}/4 fields extracted")
                
    except Exception as e:
        print(f"✗ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def test_database_connection():
    """Test database connection and check tables."""
    print("\nTesting database connection...")
    
    try:
        conn = get_db_connection()
        print("✓ Database connection successful")
        
        with conn.cursor() as cur:
            # Check if sms_messages table exists
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'sms_messages'
            """)
            if cur.fetchone()[0] > 0:
                print("✓ sms_messages table exists")
                
                # Check for unprocessed messages
                cur.execute("SELECT COUNT(*) FROM sms_messages WHERE is_processed = FALSE")
                unprocessed_count = cur.fetchone()[0]
                print(f"  Unprocessed messages: {unprocessed_count}")
            else:
                print("✗ sms_messages table not found")
                
            # Check if transactions table exists
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'transactions'
            """)
            if cur.fetchone()[0] > 0:
                print("✓ transactions table exists")
                
                # Check existing transactions
                cur.execute("SELECT COUNT(*) FROM transactions")
                transaction_count = cur.fetchone()[0]
                print(f"  Existing transactions: {transaction_count}")
            else:
                print("✗ transactions table not found")
                
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False

def check_environment():
    """Check if all required environment variables are set."""
    print("Checking environment variables...")
    
    required_vars = ["DB_URL", "GEMINI_APIKEY"]
    all_set = True
    
    for var in required_vars:
        if os.getenv(var):
            print(f"✓ {var} is set")
        else:
            print(f"✗ {var} is not set")
            all_set = False
    
    return all_set

if __name__ == "__main__":
    print("SMS to Transaction Conversion Test")
    print("=" * 50)
    
    # Load environment variables
    load_dotenv()
    
    # Run tests
    env_ok = check_environment()
    if not env_ok:
        print("\n❌ Environment variables not properly configured")
        sys.exit(1)
    
    db_ok = test_database_connection()
    if not db_ok:
        print("\n❌ Database connection issues")
        sys.exit(1)
    
    conversion_ok = test_single_conversion()
    if not conversion_ok:
        print("\n❌ Conversion test failed")
        sys.exit(1)
    
    print("\n✅ All tests passed! You can now run the full conversion process.")
    print("\nTo convert all messages, call the /convert API endpoint or run:")
    print("python -c 'from convert import convert_all_messages; print(convert_all_messages())'")