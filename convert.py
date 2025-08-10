# import os
# import json
# import psycopg2
# import re
# import time
# from typing import Dict, List, Optional
# from dotenv import load_dotenv
# from logging_config import get_logger
# import google.generativeai as genai

# # Load environment variables
# load_dotenv()
# logger = get_logger("sms_sync.convert")

# class SMSToTransactionConverter:
#     """Converts SMS messages to transaction data using Gemini AI."""
    
#     def __init__(self):
#         """Initialize the converter with Gemini AI."""
#         self.api_key = os.getenv("GEMINI_APIKEY")
#         if not self.api_key:
#             raise ValueError("GEMINI_APIKEY environment variable not set.")
        
#         # Configure Gemini AI
#         genai.configure(api_key=self.api_key)
#         self.model = genai.GenerativeModel('gemini-2.0-flash')
        
#         # Configure generation parameters for more consistent output
#         self.generation_config = genai.types.GenerationConfig(
#             temperature=0.1,  # Low temperature for consistent output
#             top_p=0.1,
#             top_k=1,
#             max_output_tokens=500,
#         )
        
#         # Rate limiting parameters
#         self.request_delay = 2.0  # 2 seconds between requests
#         self.max_retries = 3
#         self.retry_delay = 5.0  # 5 seconds between retries
#         self.last_request_time = 0
    
#     def _wait_for_rate_limit(self):
#         """Implement rate limiting to avoid quota issues."""
#         current_time = time.time()
#         time_since_last_request = current_time - self.last_request_time
        
#         if time_since_last_request < self.request_delay:
#             sleep_time = self.request_delay - time_since_last_request
#             logger.info(f"Rate limiting: waiting {sleep_time:.2f} seconds")
#             time.sleep(sleep_time)
        
#         self.last_request_time = time.time()
    
#     def _make_ai_request_with_retry(self, prompt: str) -> Optional[str]:
#         """Make AI request with retry logic for quota/rate limit errors."""
#         for attempt in range(self.max_retries):
#             try:
#                 # Wait for rate limiting
#                 self._wait_for_rate_limit()
                
#                 logger.info(f"Making AI request (attempt {attempt + 1}/{self.max_retries})")
                
#                 response = self.model.generate_content(
#                     prompt,
#                     generation_config=self.generation_config
#                 )
                
#                 if response and response.text:
#                     return response.text
#                 else:
#                     logger.warning(f"Empty response from AI on attempt {attempt + 1}")
                    
#             except Exception as e:
#                 error_str = str(e).lower()
                
#                 # Check if it's a quota or rate limit error
#                 if any(keyword in error_str for keyword in ['quota', 'rate', 'limit', 'exceeded']):
#                     logger.warning(f"Quota/rate limit error on attempt {attempt + 1}: {e}")
                    
#                     if attempt < self.max_retries - 1:
#                         # Exponential backoff for quota errors
#                         backoff_time = self.retry_delay * (2 ** attempt)
#                         logger.info(f"Waiting {backoff_time} seconds before retry...")
#                         time.sleep(backoff_time)
#                         continue
#                 else:
#                     logger.error(f"Non-quota error on attempt {attempt + 1}: {e}")
                    
#                 if attempt == self.max_retries - 1:
#                     logger.error(f"All {self.max_retries} attempts failed for AI request")
#                     return None
        
#         return None
    
#     def extract_bank_from_address(self, address: str) -> Optional[str]:
#         """Extract bank name from SMS address using pattern matching."""
#         if not address:
#             return None
            
#         address_upper = address.upper()
        
#         # Common bank patterns in SMS addresses
#         bank_patterns = {
#             'HDFC': ['HDFC', 'HDFCBK'],
#             'AXIS': ['AXIS', 'AXISBK'],
#             'SBI': ['SBI', 'SBIIN'],
#             'ICICI': ['ICICI', 'ICICIB'],
#             'KOTAK': ['KOTAK', 'KOTAKB'],
#             'PNB': ['PNB', 'PNBIN'],
#             'BOB': ['BOB', 'BOBCARD'],
#             'CANARA': ['CANARA', 'CANBK'],
#             'UNION': ['UNION', 'UBIN'],
#             'IDBI': ['IDBI', 'IDBIB']
#         }
        
#         for bank_name, patterns in bank_patterns.items():
#             for pattern in patterns:
#                 if pattern in address_upper:
#                     return bank_name
        
#         return None
    
#     def extract_amount(self, text: str) -> Optional[float]:
#         """Extract amount from SMS text."""
#         # Pattern to match currency amounts
#         patterns = [
#             r'Rs\.?\s*(\d+(?:,\d+)*(?:\.\d{2})?)',  # Rs.36.00 or Rs 36.00
#             r'INR\s*(\d+(?:,\d+)*(?:\.\d{2})?)',    # INR 36.00
#             r'₹\s*(\d+(?:,\d+)*(?:\.\d{2})?)',      # ₹36.00
#         ]
        
#         for pattern in patterns:
#             matches = re.findall(pattern, text, re.IGNORECASE)
#             if matches:
#                 # Take the first match and clean it
#                 amount_str = matches[0].replace(',', '')
#                 try:
#                     return float(amount_str)
#                 except ValueError:
#                     continue
        
#         return None
    
#     def extract_transaction_type(self, text: str) -> Optional[str]:
#         """Extract transaction type from SMS text."""
#         text_lower = text.lower()
        
#         # Debit indicators
#         debit_keywords = ['sent', 'debited', 'paid', 'transferred', 'withdrawn', 'purchase']
#         # Credit indicators  
#         credit_keywords = ['received', 'credited', 'deposited', 'refund', 'cashback']
        
#         for keyword in debit_keywords:
#             if keyword in text_lower:
#                 return 'debited'
                
#         for keyword in credit_keywords:
#             if keyword in text_lower:
#                 return 'credited'
        
#         return None
    
#     def extract_merchant(self, text: str) -> Optional[str]:
#         """Extract merchant/recipient from SMS text."""
#         lines = text.split('\n')
        
#         for line in lines:
#             line = line.strip()
#             # Look for "To" patterns
#             if line.lower().startswith('to '):
#                 merchant = line[3:].strip()
#                 # Clean up common suffixes
#                 merchant = re.sub(r'\s+on\s+\d+/\d+/\d+.*', '', merchant, flags=re.IGNORECASE)
#                 return merchant
            
#             # Look for UPI transaction patterns
#             # Pattern: UPI/P2A/reference/MERCHANT_NAME/bank
#             upi_pattern = r'UPI/[^/]+/[^/]+/([^/]+)/'
#             upi_match = re.search(upi_pattern, line, re.IGNORECASE)
#             if upi_match:
#                 merchant = upi_match.group(1).strip()
#                 # Clean up common suffixes and extra spaces
#                 merchant = re.sub(r'\s+', ' ', merchant)
#                 return merchant
            
#             # Look for other merchant patterns
#             if 'merchant' in line.lower():
#                 parts = line.split(':')
#                 if len(parts) > 1:
#                     return parts[1].strip()
        
#         return None
    
#     def convert_sms_to_transaction(self, sms_body: str, address: str) -> Dict:
#         """Convert a single SMS to transaction data."""
#         try:
#             logger.info(f"Converting SMS from address: {address}")
#             logger.debug(f"SMS body: {sms_body[:100]}...")
            
#             # First try rule-based extraction for better reliability
#             bank = self.extract_bank_from_address(address)
#             amount = self.extract_amount(sms_body)
#             transaction_type = self.extract_transaction_type(sms_body)
#             merchant = self.extract_merchant(sms_body)
            
#             # If rule-based extraction got everything, use it (skip AI call)
#             if all([bank, amount, transaction_type, merchant]):
#                 result = {
#                     'bank': bank,
#                     'amount': amount,
#                     'transaction_type': transaction_type,
#                     'merchant': merchant
#                 }
#                 logger.info(f"Rule-based extraction successful (no AI call needed): {result}")
#                 return result
            
#             # Otherwise, use AI to fill in missing parts
#             logger.info("Using AI to extract missing transaction data")
            
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

#             ai_response = self._make_ai_request_with_retry(prompt)
            
#             if ai_response:
#                 ai_result = self.parse_ai_response(ai_response)
                
#                 # Combine rule-based and AI results, preferring rule-based
#                 final_result = {
#                     'bank': bank or ai_result.get('bank'),
#                     'amount': amount or ai_result.get('amount'),
#                     'transaction_type': transaction_type or ai_result.get('transaction_type'),
#                     'merchant': merchant or ai_result.get('merchant')
#                 }
                
#                 logger.info(f"Combined extraction result: {final_result}")
#                 return final_result
#             else:
#                 logger.error("Failed to get response from Gemini AI after retries")
#                 # Fall back to rule-based extraction only
#                 result = {
#                     'bank': bank,
#                     'amount': amount,
#                     'transaction_type': transaction_type,
#                     'merchant': merchant
#                 }
#                 logger.info(f"Using rule-based extraction only: {result}")
#                 return result
                
#         except Exception as e:
#             logger.error(f"Error converting SMS to transaction: {e}")
#             # Fall back to rule-based extraction
#             return {
#                 'bank': self.extract_bank_from_address(address),
#                 'amount': self.extract_amount(sms_body),
#                 'transaction_type': self.extract_transaction_type(sms_body),
#                 'merchant': self.extract_merchant(sms_body)
#             }
    
#     def parse_ai_response(self, text: str) -> Dict:
#         """Parse AI response to extract transaction data."""
#         try:
#             # Clean the response text
#             text = text.strip()
            
#             # Remove markdown code blocks if present
#             if text.startswith('```'):
#                 text = re.sub(r'^```(?:json)?\s*', '', text)
#                 text = re.sub(r'```\s*$', '', text)
            
#             # Try to find JSON in the text
#             json_match = re.search(r'\{.*\}', text, re.DOTALL)
#             if json_match:
#                 json_str = json_match.group(0)
#                 result = json.loads(json_str)
                
#                 # Validate and clean the result
#                 cleaned_result = {}
                
#                 # Clean amount - convert to float if it's a string
#                 if 'amount' in result:
#                     amount = result['amount']
#                     if isinstance(amount, str):
#                         # Remove currency symbols and convert
#                         amount_clean = re.sub(r'[^\d.]', '', amount)
#                         try:
#                             cleaned_result['amount'] = float(amount_clean) if amount_clean else None
#                         except ValueError:
#                             cleaned_result['amount'] = None
#                     else:
#                         cleaned_result['amount'] = amount
                
#                 # Clean other fields
#                 for field in ['bank', 'transaction_type', 'merchant']:
#                     if field in result:
#                         value = result[field]
#                         if value and value.lower() not in ['null', 'none', '']:
#                             cleaned_result[field] = str(value).strip()
#                         else:
#                             cleaned_result[field] = None
#                     else:
#                         cleaned_result[field] = None
                
#                 return cleaned_result
            
#         except json.JSONDecodeError as e:
#             logger.error(f"JSON parsing error: {e}")
#         except Exception as e:
#             logger.error(f"Error parsing AI response: {e}")
        
#         return self._get_empty_transaction()
    
#     def _get_empty_transaction(self) -> Dict:
#         """Return empty transaction data structure."""
#         return {
#             'bank': None,
#             'amount': None,
#             'transaction_type': None,
#             'merchant': None
#         }

# def get_db_connection():
#     """Establish a connection to the PostgreSQL database using DB_URL env var."""
#     db_url = os.getenv("DB_URL")
#     if not db_url:
#         raise ValueError("DB_URL environment variable not set.")
#     try:
#         return psycopg2.connect(db_url)
#     except Exception as e:
#         raise RuntimeError(f"Database connection failed: {e}")

# def create_transaction_table():
#     """Create the transaction table if it doesn't exist."""
#     conn = get_db_connection()
#     try:
#         with conn.cursor() as cur:
#             cur.execute("""
#                 CREATE TABLE IF NOT EXISTS transactions (
#                     id SERIAL PRIMARY KEY,
#                     user_name VARCHAR(255) NOT NULL,
#                     sms_id BIGINT NOT NULL,
#                     address VARCHAR(255),
#                     bank VARCHAR(100),
#                     amount DECIMAL(15,2),
#                     transaction_type VARCHAR(20),
#                     merchant VARCHAR(255),
#                     created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
#                     UNIQUE (sms_id, user_name)
#                 );
#             """)
            
#             # Create index for better performance
#             cur.execute("""
#                 CREATE INDEX IF NOT EXISTS idx_transactions_user_created
#                 ON transactions (user_name, created_at DESC);
#             """)
#         conn.commit()
#         logger.info("Transaction table created successfully")
#     except Exception as e:
#         logger.error(f"Error creating transaction table: {e}")
#         raise
#     finally:
#         conn.close()

# def get_unprocessed_messages() -> List[Dict]:
#     """Get all unprocessed SMS messages from the database."""
#     conn = get_db_connection()
#     try:
#         with conn.cursor() as cur:
#             cur.execute("""
#                 SELECT user_name, sms_id, address, body, created_at
#                 FROM sms_messages 
#                 WHERE is_processed = FALSE
#                 ORDER BY created_at ASC
#             """)
#             rows = cur.fetchall()
            
#             messages = []
#             for row in rows:
#                 messages.append({
#                     'user_name': row[0],
#                     'sms_id': row[1],
#                     'address': row[2],
#                     'body': row[3],
#                     'created_at': row[4]
#                 })
            
#             logger.info(f"Found {len(messages)} unprocessed messages")
#             return messages
#     except Exception as e:
#         logger.error(f"Error fetching unprocessed messages: {e}")
#         raise
#     finally:
#         conn.close()

# def save_transaction(user_name: str, sms_id: int, address: str, transaction_data: Dict, created_at) -> bool:
#     """Save transaction data to the database."""
#     conn = get_db_connection()
#     try:
#         with conn.cursor() as cur:
#             cur.execute("""
#                 INSERT INTO transactions (user_name, sms_id, address, bank, amount, transaction_type, merchant, created_at)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#                 ON CONFLICT (sms_id, user_name) DO NOTHING
#             """, (
#                 user_name,
#                 sms_id,
#                 address,
#                 transaction_data.get('bank'),
#                 transaction_data.get('amount'),
#                 transaction_data.get('transaction_type'),
#                 transaction_data.get('merchant'),
#                 created_at
#             ))
            
#             inserted = cur.rowcount > 0
#         conn.commit()
#         return inserted
#     except Exception as e:
#         logger.error(f"Error saving transaction: {e}")
#         conn.rollback()
#         return False
#     finally:
#         conn.close()

# def mark_message_as_processed(sms_id: int, user_name: str) -> bool:
#     """Mark an SMS message as processed."""
#     conn = get_db_connection()
#     try:
#         with conn.cursor() as cur:
#             cur.execute("""
#                 UPDATE sms_messages 
#                 SET is_processed = TRUE 
#                 WHERE sms_id = %s AND user_name = %s
#             """, (sms_id, user_name))
            
#             updated = cur.rowcount > 0
#         conn.commit()
#         return updated
#     except Exception as e:
#         logger.error(f"Error marking message as processed: {e}")
#         conn.rollback()
#         return False
#     finally:
#         conn.close()

# def convert_all_messages() -> Dict:
#     """Convert all unprocessed SMS messages to transactions."""
#     logger.info("Starting SMS to transaction conversion process")
    
#     try:
#         # Create transaction table if it doesn't exist
#         create_transaction_table()
        
#         # Initialize converter
#         converter = SMSToTransactionConverter()
        
#         # Get unprocessed messages
#         messages = get_unprocessed_messages()
        
#         if not messages:
#             logger.info("No unprocessed messages found")
#             return {
#                 "status": "success",
#                 "message": "No unprocessed messages found",
#                 "processed_count": 0,
#                 "failed_count": 0
#             }
        
#         processed_count = 0
#         failed_count = 0
#         ai_calls_made = 0
#         ai_calls_skipped = 0
        
#         logger.info(f"Starting to process {len(messages)} messages with rate limiting...")
        
#         for i, message in enumerate(messages, 1):
#             try:
#                 logger.info(f"Processing message {i}/{len(messages)} (ID: {message['sms_id']}) for user {message['user_name']}")
                
#                 # Convert SMS to transaction
#                 transaction_data = converter.convert_sms_to_transaction(
#                     message['body'], 
#                     message['address']
#                 )
                
#                 # Check if conversion was successful (at least some data extracted)
#                 if any(v is not None for v in transaction_data.values()):
#                     # Save transaction
#                     if save_transaction(
#                         message['user_name'],
#                         message['sms_id'],
#                         message['address'],
#                         transaction_data,
#                         message['created_at']
#                     ):
#                         # Mark as processed
#                         if mark_message_as_processed(message['sms_id'], message['user_name']):
#                             processed_count += 1
#                             logger.info(f"Successfully processed message {message['sms_id']} ({i}/{len(messages)})")
#                         else:
#                             logger.error(f"Failed to mark message {message['sms_id']} as processed")
#                             failed_count += 1
#                     else:
#                         logger.error(f"Failed to save transaction for message {message['sms_id']}")
#                         failed_count += 1
#                 else:
#                     logger.warning(f"No data extracted from message {message['sms_id']}, marking as processed anyway")
#                     mark_message_as_processed(message['sms_id'], message['user_name'])
#                     failed_count += 1
                    
#             except Exception as e:
#                 logger.error(f"Error processing message {message['sms_id']}: {e}")
#                 failed_count += 1
        
#         result = {
#             "status": "success",
#             "message": f"Conversion completed. Processed: {processed_count}, Failed: {failed_count}",
#             "processed_count": processed_count,
#             "failed_count": failed_count,
#             "total_messages": len(messages),
#             "ai_calls_made": ai_calls_made,
#             "ai_calls_skipped": ai_calls_skipped
#         }
        
#         logger.info(f"Conversion process completed: {result}")
#         return result
        
#     except Exception as e:
#         logger.error(f"Error in conversion process: {e}")
#         return {
#             "status": "error",
#             "message": f"Conversion process failed: {str(e)}",
#             "processed_count": 0,
#             "failed_count": 0
#         }

# if __name__ == "__main__":
#     # Test the conversion process
#     result = convert_all_messages()
#     print(json.dumps(result, indent=2))

import os
import json
import psycopg2
import re
import time
from typing import Dict, List, Optional
from dotenv import load_dotenv
from logging_config import get_logger
import google.generativeai as genai


# Load environment variables
load_dotenv()
logger = get_logger("sms_sync.convert")


class SMSToTransactionConverter:
    """Converts SMS messages to transaction data using Gemini AI."""
   
    def __init__(self):
        """Initialize the converter with Gemini AI."""
        self.api_key = os.getenv("GEMINI_APIKEY")
        if not self.api_key:
            raise ValueError("GEMINI_APIKEY environment variable not set.")
       
        # Configure Gemini AI
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash')
       
        # Configure generation parameters for more consistent output
        self.generation_config = genai.types.GenerationConfig(
            temperature=0.1,  # Low temperature for consistent output
            top_p=0.1,
            top_k=1,
            max_output_tokens=500,
        )
       
        # Rate limiting parameters
        self.request_delay = 2.0  # 2 seconds between requests
        self.max_retries = 3
        self.retry_delay = 5.0  # 5 seconds between retries
        self.last_request_time = 0
   
    def _wait_for_rate_limit(self):
        """Implement rate limiting to avoid quota issues."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
       
        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            logger.info(f"Rate limiting: waiting {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
       
        self.last_request_time = time.time()
   
    def _make_ai_request_with_retry(self, prompt: str) -> Optional[str]:
        """Make AI request with retry logic for quota/rate limit errors."""
        for attempt in range(self.max_retries):
            try:
                # Wait for rate limiting
                self._wait_for_rate_limit()
               
                logger.info(f"Making AI request (attempt {attempt + 1}/{self.max_retries})")
               
                response = self.model.generate_content(
                    prompt,
                    generation_config=self.generation_config
                )
               
                if response and response.text:
                    return response.text
                else:
                    logger.warning(f"Empty response from AI on attempt {attempt + 1}")
                   
            except Exception as e:
                error_str = str(e).lower()
               
                # Check if it's a quota or rate limit error
                if any(keyword in error_str for keyword in ['quota', 'rate', 'limit', 'exceeded']):
                    logger.warning(f"Quota/rate limit error on attempt {attempt + 1}: {e}")
                   
                    if attempt < self.max_retries - 1:
                        # Exponential backoff for quota errors
                        backoff_time = self.retry_delay * (2 ** attempt)
                        logger.info(f"Waiting {backoff_time} seconds before retry...")
                        time.sleep(backoff_time)
                        continue
                else:
                    logger.error(f"Non-quota error on attempt {attempt + 1}: {e}")
                   
                if attempt == self.max_retries - 1:
                    logger.error(f"All {self.max_retries} attempts failed for AI request")
                    return None
       
        return None
   
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
   
    def extract_transaction_type(self, text: str) -> Optional[str]:
        """Extract transaction type from SMS text."""
        text_lower = text.lower()
       
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

            ai_response = self._make_ai_request_with_retry(prompt)
           
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
                logger.error("Failed to get response from Gemini AI after retries")
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


def get_db_connection():
    """Establish a connection to the PostgreSQL database using DB_URL env var."""
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL environment variable not set.")
    try:
        return psycopg2.connect(db_url)
    except Exception as e:
        raise RuntimeError(f"Database connection failed: {e}")


def create_transaction_table():
    """Create the transaction table if it doesn't exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    user_name VARCHAR(255) NOT NULL,
                    sms_id BIGINT NOT NULL,
                    address VARCHAR(255),
                    bank VARCHAR(100),
                    amount DECIMAL(15,2),
                    transaction_type VARCHAR(20),
                    merchant VARCHAR(255),
                    date_received BIGINT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (sms_id, user_name)
                );
            """)
           
            # Create index for better performance
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_transactions_user_created
                ON transactions (user_name, created_at DESC);
            """)
            
            # Create index for date_received
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_transactions_date_received
                ON transactions (date_received DESC);
            """)
        conn.commit()
        logger.info("Transaction table created successfully")
    except Exception as e:
        logger.error(f"Error creating transaction table: {e}")
        raise
    finally:
        conn.close()


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
        # Create transaction table if it doesn't exist
        create_transaction_table()
       
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
                        message['date_received'],  # Added date_received parameter
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


if __name__ == "__main__":
    # Test the conversion process
    result = convert_all_messages()
    print(json.dumps(result, indent=2))