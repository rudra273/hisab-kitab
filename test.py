# test_detailed.py
"""
Enhanced test script that shows exactly what AI extracts vs rule-based extraction.
Updated to work with LangChain LLM providers.
"""

import os
import sys
from dotenv import load_dotenv
import json

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from convert import TestingSMSConverter, get_db_connection


class EnhancedTestingSMSConverter(TestingSMSConverter):
    """Enhanced testing converter with detailed provider tracking."""
    
    def __init__(self):
        super().__init__()
        self.primary_attempts = 0
        self.secondary_attempts = 0
        self.provider_used = None
        self.primary_failed = False
        self.secondary_failed = False
    
    def convert_sms_to_transaction_with_detailed_tracking(self, sms_body: str, address: str):
        """Convert SMS with detailed tracking including LLM provider details."""
        self.ai_was_called = False
        self.ai_result = None
        self.rule_based_result = None
        self.provider_used = None
        self.primary_attempts = 0
        self.secondary_attempts = 0
        self.primary_failed = False
        self.secondary_failed = False
        
        try:
            print(f"🔍 Starting conversion for address: {address}")
            
            # First try rule-based extraction
            bank = self.extract_bank_from_address(address)
            amount = self.extract_amount(sms_body)
            transaction_type = self.extract_transaction_type(sms_body)
            merchant = self.extract_merchant(sms_body)
            
            # Store rule-based results
            self.rule_based_result = {
                'bank': bank,
                'amount': amount,
                'transaction_type': transaction_type,
                'merchant': merchant
            }
            
            print("\n📋 Rule-based extraction results:")
            for key, value in self.rule_based_result.items():
                status = "✓" if value is not None else "✗"
                print(f"  {status} {key}: {value}")
            
            # Check if rule-based extraction got everything
            rule_based_complete = all([bank, amount, transaction_type, merchant])
            
            if rule_based_complete:
                print("\n✅ Rule-based extraction COMPLETE - AI call SKIPPED")
                return self.rule_based_result
            else:
                # Show what's missing
                missing_fields = []
                if not bank: missing_fields.append("bank")
                if not amount: missing_fields.append("amount")
                if not transaction_type: missing_fields.append("transaction_type")
                if not merchant: missing_fields.append("merchant")
                
                print(f"\n❌ Rule-based extraction INCOMPLETE")
                print(f"   Missing fields: {', '.join(missing_fields)}")
                print("🤖 Making AI call to extract missing data...")
                
                # Call AI with enhanced tracking
                self.ai_was_called = True
                
                prompt = f"""
Extract financial transaction information from this SMS:

Address: {address}
Message: {sms_body}

Return ONLY a JSON object with these fields:
{{
    "bank": "bank name (HDFC, AXIS, SBI, etc.)",
    "amount": "numeric amount without currency symbols",
    "transaction_type": "debited or credited",
    "merchant": "recipient or merchant name"
}}

Rules:
- Extract bank from address patterns (AX-HDFCBK-S means HDFC, VM-HDFCBK-S means HDFC)
- Amount should be just the number (36.00 not Rs.36.00)
- "Sent" means debited, "Received" means credited
- Merchant is the "To" recipient
- Use null for missing data
- Return ONLY valid JSON, no other text

Example: {{"bank": "HDFC", "amount": 36.00, "transaction_type": "debited", "merchant": "BMTC BUS KA57F2456"}}
"""

                # Track LLM provider attempts
                ai_response = self._track_llm_attempts(prompt)
                
                if ai_response:
                    self.ai_result = self.parse_ai_response(ai_response)
                    
                    print(f"\n🤖 AI extraction results (using {self.provider_used}):")
                    for key, value in self.ai_result.items():
                        status = "✓" if value is not None else "✗"
                        print(f"  {status} {key}: {value}")
                    
                    # Show provider usage details
                    self._show_provider_details()
                    
                    # Combine rule-based and AI results
                    final_result = {
                        'bank': bank or self.ai_result.get('bank'),
                        'amount': amount or self.ai_result.get('amount'),
                        'transaction_type': transaction_type or self.ai_result.get('transaction_type'),
                        'merchant': merchant or self.ai_result.get('merchant')
                    }
                    
                    print("\n🔧 Final combined results:")
                    for key, value in final_result.items():
                        rule_val = self.rule_based_result.get(key)
                        ai_val = self.ai_result.get(key)
                        
                        if rule_val is not None:
                            print(f"  📋 {key}: {value} (from rules)")
                        elif ai_val is not None:
                            print(f"  🤖 {key}: {value} (from {self.provider_used})")
                        else:
                            print(f"  ❌ {key}: {value} (not extracted)")
                    
                    return final_result
                else:
                    print("\n❌ All AI providers FAILED - using rule-based results only")
                    self._show_provider_details()
                    return self.rule_based_result
                    
        except Exception as e:
            print(f"\n💥 Error during conversion: {e}")
            return self._get_empty_transaction()
    
    def _track_llm_attempts(self, prompt: str):
        """Track LLM attempts with detailed logging."""
        # Override the LLM provider's generate_response to track attempts
        original_try_llm = self.llm_provider._try_llm
        
        def tracked_try_llm(llm, provider_name, prompt_text):
            if provider_name == self.llm_provider.primary_provider:
                self.primary_attempts += 1
                print(f"🔄 Trying PRIMARY ({provider_name}) - Attempt {self.primary_attempts}")
            else:
                self.secondary_attempts += 1
                print(f"🔄 Trying SECONDARY ({provider_name}) - Attempt {self.secondary_attempts}")
            
            result = original_try_llm(llm, provider_name, prompt_text)
            
            if result:
                self.provider_used = provider_name
                print(f"✅ SUCCESS with {provider_name}")
            else:
                if provider_name == self.llm_provider.primary_provider:
                    self.primary_failed = True
                    print(f"❌ PRIMARY ({provider_name}) FAILED")
                else:
                    self.secondary_failed = True
                    print(f"❌ SECONDARY ({provider_name}) FAILED")
            
            return result
        
        # Temporarily override the method
        self.llm_provider._try_llm = tracked_try_llm
        
        try:
            return self.llm_provider.generate_response(prompt)
        finally:
            # Restore original method
            self.llm_provider._try_llm = original_try_llm
    
    def _show_provider_details(self):
        """Show detailed provider usage information."""
        print(f"\n📊 LLM Provider Usage Details:")
        print(f"   🔹 Primary ({self.llm_provider.primary_provider}): {self.primary_attempts} attempts")
        if self.primary_failed:
            print(f"   🔴 Primary FAILED after {self.primary_attempts} attempts")
        
        print(f"   🔹 Secondary ({self.llm_provider.secondary_provider}): {self.secondary_attempts} attempts")
        if self.secondary_failed:
            print(f"   🔴 Secondary FAILED after {self.secondary_attempts} attempts")
        
        if self.provider_used:
            print(f"   ✅ Final success with: {self.provider_used}")
        else:
            print(f"   ❌ Both providers failed")


def test_conversion_with_enhanced_tracking():
    """Test conversion with detailed AI usage and provider tracking."""
    print("Testing SMS to Transaction Conversion with Enhanced LLM Provider Tracking")
    print("=" * 80)
    
    try:
        # Initialize enhanced testing converter
        converter = EnhancedTestingSMSConverter()
        print("✓ Enhanced testing converter initialized successfully")
        print(f"✓ Primary LLM: {converter.llm_provider.primary_provider}")
        print(f"✓ Secondary LLM: {converter.llm_provider.secondary_provider}")
        
        # Test cases
        test_cases = [
            {
                "name": "Complete HDFC transaction (should skip AI)",
                "address": "AX-HDFCBK-S",
                "body": "Sent Rs.36.00\nFrom HDFC Bank A/C *8206\nTo BMTC BUS KA57F2456\nOn 10/08/25\nRef 677927937758\nNot You?\nCall 18002586161/SMS BLOCK UPI to 7308080808"
            },
            {
                "name": "Another HDFC transaction (should skip AI)",
                "address": "VM-HDFCBK-S",
                "body": "Sent Rs.260.00\nFrom HDFC Bank A/C *8206\nTo BADAL  MEHER\nOn 10/08/25\nRef 516059125345\nNot You?\nCall 18002586161/SMS BLOCK UPI to 7308080808"
            },
            {
                "name": "Promotional SMS (should use AI)",
                "address": "VD-HDFCBN-P",
                "body": "HDFC Bank:\nEnjoy freedom from high EMIs with a pre-approved Personal Loan at reduced rates. Check EMI: https://hdfcbk.io/HDFCBK/s/7dkLjLAB"
            },
            {
                "name": "Credit transaction (should skip AI)",
                "address": "JK-AXISBK-S",
                "body": "INR 1.00 credited\nA/c no. XX9624\n10-08-25, 00:01:25 IST\nUPI/P2A/839457076434/BADAL MEH/ICICI Ban - Axis Bank "
            },
            {
                "name": "Unknown format (should use AI with provider fallback)",
                "address": "UNKNOWN-BANK",
                "body": "Payment of 500 rupees made to Coffee Shop yesterday"
            }
        ]
        
        ai_calls_made = 0
        ai_calls_skipped = 0
        primary_used = 0
        secondary_used = 0
        both_failed = 0
        
        for i, test_case in enumerate(test_cases, 1):
            print(f"\n{'='*80}")
            print(f"TEST CASE {i}: {test_case['name']}")
            print(f"{'='*80}")
            print(f"Address: {test_case['address']}")
            print(f"Message: {test_case['body'][:150]}...")
            
            # Run conversion with enhanced tracking
            result = converter.convert_sms_to_transaction_with_detailed_tracking(
                test_case['body'],
                test_case['address']
            )
            
            # Track AI and provider usage
            if converter.ai_was_called:
                ai_calls_made += 1
                print(f"\n📊 RESULT: AI was called for this message")
                
                if converter.provider_used == converter.llm_provider.primary_provider:
                    primary_used += 1
                elif converter.provider_used == converter.llm_provider.secondary_provider:
                    secondary_used += 1
                else:
                    both_failed += 1
            else:
                ai_calls_skipped += 1
                print(f"\n📊 RESULT: AI call was skipped (rule-based sufficient)")
            
            # Show final success rate
            success_count = sum(1 for v in result.values() if v is not None)
            print(f"📈 Final success rate: {success_count}/4 fields extracted")
            print(f"🎯 Overall extraction quality: {'GOOD' if success_count >= 3 else 'POOR'}")
        
        # Enhanced Summary
        print(f"\n{'='*80}")
        print("ENHANCED CONVERSION SUMMARY")
        print(f"{'='*80}")
        print(f"Total test cases: {len(test_cases)}")
        print(f"🤖 AI calls made: {ai_calls_made}")
        print(f"📋 AI calls skipped: {ai_calls_skipped}")
        print(f"💰 Cost efficiency: {(ai_calls_skipped/len(test_cases))*100:.1f}% calls avoided")
        print(f"\n🔀 LLM Provider Usage:")
        print(f"   🔹 Primary ({converter.llm_provider.primary_provider}) used: {primary_used} times")
        print(f"   🔹 Secondary ({converter.llm_provider.secondary_provider}) used: {secondary_used} times")
        print(f"   🔴 Both failed: {both_failed} times")
        
        if ai_calls_made > 0:
            primary_success_rate = (primary_used / ai_calls_made) * 100
            secondary_success_rate = (secondary_used / ai_calls_made) * 100
            print(f"\n📊 Provider Success Rates:")
            print(f"   Primary success rate: {primary_success_rate:.1f}%")
            print(f"   Secondary success rate: {secondary_success_rate:.1f}%")
            print(f"   Fallback effectiveness: {((primary_used + secondary_used) / ai_calls_made) * 100:.1f}%")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_environment():
    """Check if all required environment variables are set."""
    print("Checking environment variables...")
    
    required_vars = ["DB_URL", "GEMINI_APIKEY", "OPENAI_APIKEY"]
    all_set = True
    
    for var in required_vars:
        if os.getenv(var):
            print(f"✓ {var} is set")
        else:
            print(f"✗ {var} is not set")
            all_set = False
    
    return all_set


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Check environment
    if not check_environment():
        print("\n❌ Environment variables not properly configured")
        print("Required: DB_URL, GEMINI_APIKEY, OPENAI_API_KEY")
        sys.exit(1)
    
    # Run enhanced testing
    success = test_conversion_with_enhanced_tracking()
    
    if success:
        print("\n✅ All enhanced tests completed successfully!")
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)