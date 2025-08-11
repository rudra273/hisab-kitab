# convert.py
import os
import json
import re
import time
from typing import Dict, List, Optional
from dotenv import load_dotenv
from logging_config import get_logger
from db import get_db_connection, setup_database
from llm_provider import LLMProvider

# Load environment variables
load_dotenv()
logger = get_logger("sms_sync.convert")


class SMSToTransactionConverter:
    """Converts SMS messages to transaction data using LLM providers."""
    
    def __init__(self):
        """Initialize the converter with LLM provider."""
        self.llm_provider = LLMProvider()
    
    def extract_bank_from_address(self, address: str) -> Optional[str]:
        """Extract bank name from SMS address using pattern matching."""
        if not address:
            return None
            
        address_upper = address.upper()
        
        # Common bank patterns in SMS addresses
        bank_patterns = {
            'HDFC': ['HDFC', 'HDFCBK'],
            'AXIS': ['AXIS', 'AXISBK'],
            'SBI': ['SBI', 'SBIIN'],
            'ICICI': ['ICICI', 'ICICIB'],
            'KOTAK': ['KOTAK', 'KOTAKB'],
            'PNB': ['PNB', 'PNBIN'],
            'BOB': ['BOB', 'BOBCARD'],
            'CANARA': ['CANARA', 'CANBK'],
            'UNION': ['UNION', 'UBIN'],
            'IDBI': ['IDBI', 'IDBIB']
        }
        
        for bank_name, patterns in bank_patterns.items():
            for pattern in patterns:
                if pattern in address_upper:
                    return bank_name
        
        return None
    
    def extract_amount(self, text: str) -> Optional[float]:
        """Extract amount from SMS text."""
        # Pattern to match currency amounts
        patterns = [
            r'Rs\.?\s*(\d+(?:,\d+)*(?:\.\d{2})?)',  # Rs.36.00 or Rs 36.00
            r'INR\s*(\d+(?:,\d+)*(?:\.\d{2})?)',    # INR 36.00
            r'₹\s*(\d+(?:,\d+)*(?:\.\d{2})?)',      # ₹36.00
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                # Take the first match and clean it
                amount_str = matches[0].replace(',', '')
                try:
                    return float(amount_str)
                except ValueError:
                    continue
        
        return None
    
    # def extract_transaction_type(self, text: str) -> Optional[str]:
    #     """Extract transaction type from SMS text."""
    #     text_lower = text.lower()
        
    #     # Debit indicators
    #     debit_keywords = ['sent', 'debited', 'paid', 'transferred', 'withdrawn', 'purchase']
    #     # Credit indicators
    #     credit_keywords = ['received', 'credited', 'deposited', 'refund', 'cashback']
        
    #     for keyword in debit_keywords:
    #         if keyword in text_lower:
    #             return 'debited'
                
    #     for keyword in credit_keywords:
    #         if keyword in text_lower:
    #             return 'credited'
        
    #     return None

    def extract_transaction_type(self, text: str) -> Optional[str]:
        """Extract transaction type from SMS text."""
        text_lower = text.lower()

        # Exclusion keywords (non-transactional)
        exclusion_keywords = [
            "invest", "fd", "fixed deposit", "loan offer", "book now", 
            "apply now", "mandate created", "mandate has been created", 
            "towards", "scheduled", "authorization", 
            "pre-approved", "otp", "reminder"
        ]

        # If any exclusion keyword exists, return None (treat as 'other')
        if any(keyword in text_lower for keyword in exclusion_keywords):
            return None

        # Debit indicators
        debit_keywords = ['sent', 'debited', 'paid', 'transferred', 'withdrawn', 'purchase']
        # Credit indicators
        credit_keywords = ['received', 'credited', 'deposited', 'refund', 'cashback']

        for keyword in debit_keywords:
            if keyword in text_lower:
                return 'debited'
                    
        for keyword in credit_keywords:
            if keyword in text_lower:
                return 'credited'
            
        return None

    
    def extract_merchant(self, text: str) -> Optional[str]:
        """Extract merchant/recipient from SMS text."""
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            # Look for "To" patterns
            if line.lower().startswith('to '):
                merchant = line[3:].strip()
                # Clean up common suffixes
                merchant = re.sub(r'\s+on\s+\d+/\d+/\d+.*', '', merchant, flags=re.IGNORECASE)
                return merchant
            
            # Look for UPI transaction patterns
            # Pattern: UPI/P2A/reference/MERCHANT_NAME/bank
            upi_pattern = r'UPI/[^/]+/[^/]+/([^/]+)/'
            upi_match = re.search(upi_pattern, line, re.IGNORECASE)
            if upi_match:
                merchant = upi_match.group(1).strip()
                # Clean up common suffixes and extra spaces
                merchant = re.sub(r'\s+', ' ', merchant)
                return merchant
            
            # Look for other merchant patterns
            if 'merchant' in line.lower():
                parts = line.split(':')
                if len(parts) > 1:
                    return parts[1].strip()
        
        return None
    
    def convert_sms_to_transaction(self, sms_body: str, address: str) -> Dict:
        """Convert a single SMS to transaction data."""
        try:
            logger.info(f"Converting SMS from address: {address}")
            logger.debug(f"SMS body: {sms_body[:100]}...")
            
            # First try rule-based extraction for better reliability
            bank = self.extract_bank_from_address(address)
            amount = self.extract_amount(sms_body)
            transaction_type = self.extract_transaction_type(sms_body)
            merchant = self.extract_merchant(sms_body)
            
            # If rule-based extraction got everything, use it (skip AI call)
            if all([bank, amount, transaction_type, merchant]):
                result = {
                    'bank': bank,
                    'amount': amount,
                    'transaction_type': transaction_type,
                    'merchant': merchant
                }
                logger.info(f"Rule-based extraction successful (no AI call needed): {result}")
                return result
            
            # Otherwise, use AI to fill in missing parts
            logger.info("Using AI to extract missing transaction data")
            
#             prompt = f"""
# Extract financial transaction information from this SMS:

# Address: {address}
# Message: {sms_body}

# Return ONLY a JSON object with these fields:
# {{
#     "bank": "bank name (HDFC, AXIS, SBI, etc.)",
#     "amount": "numeric amount without currency symbols",
#     "transaction_type": "debited or credited",
#     "merchant": "recipient or merchant name"
# }}

# Rules:
# - Extract bank from address patterns (AX-HDFCBK-S means HDFC, VM-HDFCBK-S means HDFC)
# - Amount should be just the number (36.00 not Rs.36.00)
# - "Sent" means debited, "Received" means credited
# - Merchant is the "To" recipient 
# - Use null for missing data
# - Return ONLY valid JSON, no other text

# Example: {{"bank": "HDFC", "amount": 36.00, "transaction_type": "debited", "merchant": "BMTC BUS KA57F2456"}}
# """


            prompt = f"""
Extract financial transaction information from this SMS:

Address: {address}
Message: {sms_body}

Return ONLY a JSON object with these fields:
{{
    "bank": "bank name (HDFC, AXIS, SBI, etc.)",
    "amount": "numeric amount without currency symbols",
    "transaction_type": "debited, credited, or other",
    "merchant": "other party involved in the transaction (shop, business, service, or individual), or null if not a transaction"
}}

Rules:
- Extract bank from address patterns (AX-HDFCBK-S means HDFC, VM-HDFCBK-S means HDFC)
- Amount should be just the number (36.00 not Rs.36.00)
- "transaction_type" rules:
    * "debited" ONLY if the message clearly confirms money was sent, paid, withdrawn, deducted, or spent from the account.
    * "credited" ONLY if the message clearly confirms money was received, deposited, or added to the account.
    * "other" if ANY of these are true:
        - The message is promotional, informational, or about future/potential transactions.
        - The message contains any of these keywords (case-insensitive): 
          ["invest", "FD", "fixed deposit", "loan offer", "book now", "apply now", "mandate created", "mandate has been created", "towards", "scheduled", "will be", "authorization", "pre-approved", "OTP", "reminder"].
        - The message does not explicitly confirm that money has already moved.
- Merchant should only be extracted if the message is a confirmed debit or credit transaction. For non-transaction messages, set merchant to null.
- Use null for missing data
- Return ONLY valid JSON, no other text

Example: {{"bank": "HDFC", "amount": 36.00, "transaction_type": "debited", "merchant": "BMTC BUS KA57F2456"}}
"""

            ai_response = self.llm_provider.generate_response(prompt)
            
            if ai_response:
                ai_result = self.parse_ai_response(ai_response)
                
                # Combine rule-based and AI results, preferring rule-based
                final_result = {
                    'bank': bank or ai_result.get('bank'),
                    'amount': amount or ai_result.get('amount'),
                    'transaction_type': transaction_type or ai_result.get('transaction_type'),
                    'merchant': merchant or ai_result.get('merchant')
                }
                
                logger.info(f"Combined extraction result: {final_result}")
                return final_result
            else:
                logger.error("Failed to get response from LLM providers after retries")
                # Fall back to rule-based extraction only
                result = {
                    'bank': bank,
                    'amount': amount,
                    'transaction_type': transaction_type,
                    'merchant': merchant
                }
                logger.info(f"Using rule-based extraction only: {result}")
                return result
                
        except Exception as e:
            logger.error(f"Error converting SMS to transaction: {e}")
            # Fall back to rule-based extraction
            return {
                'bank': self.extract_bank_from_address(address),
                'amount': self.extract_amount(sms_body),
                'transaction_type': self.extract_transaction_type(sms_body),
                'merchant': self.extract_merchant(sms_body)
            }
    
    def parse_ai_response(self, text: str) -> Dict:
        """Parse AI response to extract transaction data."""
        try:
            # Clean the response text
            text = text.strip()
            
            # Remove markdown code blocks if present
            if text.startswith('```'):
                text = re.sub(r'^```(?:json)?\s*', '', text)
                text = re.sub(r'```\s*$', '', text)
            
            # Try to find JSON in the text
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                result = json.loads(json_str)
                
                # Validate and clean the result
                cleaned_result = {}
                
                # Clean amount - convert to float if it's a string
                if 'amount' in result:
                    amount = result['amount']
                    if isinstance(amount, str):
                        # Remove currency symbols and convert
                        amount_clean = re.sub(r'[^\d.]', '', amount)
                        try:
                            cleaned_result['amount'] = float(amount_clean) if amount_clean else None
                        except ValueError:
                            cleaned_result['amount'] = None
                    else:
                        cleaned_result['amount'] = amount
                
                # Clean other fields
                for field in ['bank', 'transaction_type', 'merchant']:
                    if field in result:
                        value = result[field]
                        if value and value.lower() not in ['null', 'none', '']:
                            cleaned_result[field] = str(value).strip()
                        else:
                            cleaned_result[field] = None
                    else:
                        cleaned_result[field] = None
                
                return cleaned_result
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
        except Exception as e:
            logger.error(f"Error parsing AI response: {e}")
        
        return self._get_empty_transaction()
    
    def _get_empty_transaction(self) -> Dict:
        """Return empty transaction data structure."""
        return {
            'bank': None,
            'amount': None,
            'transaction_type': None,
            'merchant': None
        }


def get_unprocessed_messages() -> List[Dict]:
    """Get all unprocessed SMS messages from the database."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT user_name, sms_id, address, body, date_received, created_at
                FROM sms_messages
                WHERE is_processed = FALSE
                ORDER BY created_at ASC
            """)
            rows = cur.fetchall()
            
            messages = []
            for row in rows:
                messages.append({
                    'user_name': row[0],
                    'sms_id': row[1],
                    'address': row[2],
                    'body': row[3],
                    'date_received': row[4],
                    'created_at': row[5]
                })
            
            logger.info(f"Found {len(messages)} unprocessed messages")
            return messages
    except Exception as e:
        logger.error(f"Error fetching unprocessed messages: {e}")
        raise
    finally:
        conn.close()


def save_transaction(user_name: str, sms_id: int, address: str, transaction_data: Dict, date_received: int, created_at) -> bool:
    """Save transaction data to the database."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO transactions (user_name, sms_id, address, bank, amount, transaction_type, merchant, date_received, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sms_id, user_name) DO NOTHING
            """, (
                user_name,
                sms_id,
                address,
                transaction_data.get('bank'),
                transaction_data.get('amount'),
                transaction_data.get('transaction_type'),
                transaction_data.get('merchant'),
                date_received,
                created_at
            ))
            
            inserted = cur.rowcount > 0
        conn.commit()
        return inserted
    except Exception as e:
        logger.error(f"Error saving transaction: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def mark_message_as_processed(sms_id: int, user_name: str) -> bool:
    """Mark an SMS message as processed."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE sms_messages
                SET is_processed = TRUE
                WHERE sms_id = %s AND user_name = %s
            """, (sms_id, user_name))
            
            updated = cur.rowcount > 0
        conn.commit()
        return updated
    except Exception as e:
        logger.error(f"Error marking message as processed: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def convert_all_messages() -> Dict:
    """Convert all unprocessed SMS messages to transactions."""
    logger.info("Starting SMS to transaction conversion process")
    
    try:
        # Ensure all required tables exist
        setup_database()
        
        # Initialize converter
        converter = SMSToTransactionConverter()
        
        # Get unprocessed messages
        messages = get_unprocessed_messages()
        
        if not messages:
            logger.info("No unprocessed messages found")
            return {
                "status": "success",
                "message": "No unprocessed messages found",
                "processed_count": 0,
                "failed_count": 0
            }
        
        processed_count = 0
        failed_count = 0
        ai_calls_made = 0
        ai_calls_skipped = 0
        
        logger.info(f"Starting to process {len(messages)} messages with rate limiting...")
        
        for i, message in enumerate(messages, 1):
            try:
                logger.info(f"Processing message {i}/{len(messages)} (ID: {message['sms_id']}) for user {message['user_name']}")
                
                # Convert SMS to transaction
                transaction_data = converter.convert_sms_to_transaction(
                    message['body'],
                    message['address']
                )
                
                # Check if conversion was successful (at least some data extracted)
                if any(v is not None for v in transaction_data.values()):
                    # Save transaction with date_received
                    if save_transaction(
                        message['user_name'],
                        message['sms_id'],
                        message['address'],
                        transaction_data,
                        message['date_received'],
                        message['created_at']
                    ):
                        # Mark as processed
                        if mark_message_as_processed(message['sms_id'], message['user_name']):
                            processed_count += 1
                            logger.info(f"Successfully processed message {message['sms_id']} ({i}/{len(messages)})")
                        else:
                            logger.error(f"Failed to mark message {message['sms_id']} as processed")
                            failed_count += 1
                    else:
                        logger.error(f"Failed to save transaction for message {message['sms_id']}")
                        failed_count += 1
                else:
                    logger.warning(f"No data extracted from message {message['sms_id']}, marking as processed anyway")
                    mark_message_as_processed(message['sms_id'], message['user_name'])
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing message {message['sms_id']}: {e}")
                failed_count += 1
        
        result = {
            "status": "success",
            "message": f"Conversion completed. Processed: {processed_count}, Failed: {failed_count}",
            "processed_count": processed_count,
            "failed_count": failed_count,
            "total_messages": len(messages),
            "ai_calls_made": ai_calls_made,
            "ai_calls_skipped": ai_calls_skipped
        }
        
        logger.info(f"Conversion process completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error in conversion process: {e}")
        return {
            "status": "error",
            "message": f"Conversion process failed: {str(e)}",
            "processed_count": 0,
            "failed_count": 0
        }


class TestingSMSConverter(SMSToTransactionConverter):
    """Extended converter class that tracks AI usage for testing."""
    
    def __init__(self):
        super().__init__()
        self.ai_was_called = False
        self.ai_result = None
        self.rule_based_result = None
        self.provider_used = None
    
    def convert_sms_to_transaction_with_tracking(self, sms_body: str, address: str):
        """Convert SMS with detailed tracking of rule-based vs AI extraction."""
        self.ai_was_called = False
        self.ai_result = None
        self.rule_based_result = None
        self.provider_used = None
        
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
                
                # Call AI
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

                ai_response = self.llm_provider.generate_response(prompt)
                
                if ai_response:
                    self.ai_result = self.parse_ai_response(ai_response)
                    
                    print("\n🤖 AI extraction results:")
                    for key, value in self.ai_result.items():
                        status = "✓" if value is not None else "✗"
                        print(f"  {status} {key}: {value}")
                    
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
                            print(f"  🤖 {key}: {value} (from AI)")
                        else:
                            print(f"  ❌ {key}: {value} (not extracted)")
                    
                    return final_result
                else:
                    print("\n❌ AI call FAILED - using rule-based results only")
                    return self.rule_based_result
                    
        except Exception as e:
            print(f"\n💥 Error during conversion: {e}")
            return self._get_empty_transaction()


if __name__ == "__main__":
    # Test the conversion process
    result = convert_all_messages()
    print(json.dumps(result, indent=2))