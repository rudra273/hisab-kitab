import os
import json
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from logging_config import get_logger
from db import get_db_connection
from langchain.tools import Tool
from langchain.agents import create_openai_functions_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import SystemMessage
from langchain_openai import ChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI

logger = get_logger("sms_sync.chat")


class TransactionChatSystem:
    """Advanced chat system with LLM function calling for transaction queries."""
    
    def __init__(self):
        """Initialize the chat system with LLM and tools."""
        self._setup_llm()
        self._setup_tools()
        self._setup_agent()
        
    def _setup_llm(self):
        """Setup the LLM with function calling capabilities."""
        openai_api_key = os.getenv("OPENAI_APIKEY")
        if openai_api_key:
            self.llm = ChatOpenAI(
                model="gpt-4o-mini",
                api_key=openai_api_key,
                temperature=0.1,
                max_tokens=1000
            )
            logger.info("Using OpenAI LLM for chat system")
        else:
            gemini_api_key = os.getenv("GEMINI_APIKEY")
            if not gemini_api_key:
                raise ValueError("Either OPENAI_APIKEY or GEMINI_APIKEY must be set")
            
            self.llm = ChatGoogleGenerativeAI(
                model="gemini-2.0-flash",
                google_api_key=gemini_api_key,
                temperature=0.1,
                max_output_tokens=1000
            )
            logger.info("Using Gemini LLM for chat system")
    
    def _get_timestamp_range(self, period: str) -> Tuple[int, int]:
        """Get timestamp range for different periods."""
        now = datetime.now()
        
        if period == "today":
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        elif period == "yesterday":
            yesterday = now - timedelta(days=1)
            start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
            end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        elif period == "this_week":
            days_since_monday = now.weekday()
            start = (now - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=6, hours=23, minutes=59, seconds=59)
        elif period == "last_week":
            start_of_this_week = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            start = start_of_this_week - timedelta(days=7)
            end = start_of_this_week - timedelta(microseconds=1)
        elif period == "this_month":
            start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if now.month == 12:
                next_month = now.replace(year=now.year + 1, month=1, day=1)
            else:
                next_month = now.replace(month=now.month + 1, day=1)
            end = next_month - timedelta(microseconds=1)
        elif period == "last_month":
            first_day_of_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            end = first_day_of_this_month - timedelta(microseconds=1)
            start = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        elif "last_" in period and "days" in period:
            try:
                days = int(period.split("_")[1])
                start = (now - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
                end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            except (ValueError, IndexError):
                start = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
                end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        else:
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        return int(start.timestamp() * 1000), int(end.timestamp() * 1000)

    def _execute_sql_query(self, query: str, params: tuple, user_name: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return formatted results."""
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                logger.debug(f"Executing SQL for '{user_name}': {cur.mogrify(query, params).decode('utf-8')}")
                cur.execute(query, params)
                
                if cur.description is None:
                    return []
                    
                rows = cur.fetchall()
                logger.info(f"Query returned {len(rows)} row(s).")
                if not rows:
                    return []

                column_names = [desc[0] for desc in cur.description]
                results = []
                for row in rows:
                    row_dict = {}
                    for i, value in enumerate(row):
                        col_name = column_names[i]
                        
                        if isinstance(value, Decimal):
                            row_dict[col_name] = float(value)
                        elif col_name == 'date_received' and value:
                            try:
                                date_object = datetime.fromtimestamp(value / 1000)
                                row_dict[col_name] = date_object.strftime("%Y-%m-%d %H:%M:%S")
                            except (ValueError, TypeError):
                                row_dict[col_name] = value
                        elif col_name == 'amount' and value is not None:
                            row_dict[col_name] = float(value)
                        else:
                            row_dict[col_name] = value
                    results.append(row_dict)

                logger.debug(f"Formatted results sample: {json.dumps(results[:2], indent=2)}")
                return results
        except Exception as e:
            logger.error(f"Error executing query for user '{user_name}': {e}", exc_info=True)
            try:
                with get_db_connection().cursor() as temp_cur:
                    failed_query = temp_cur.mogrify(query, params).decode('utf-8')
                    logger.error(f"Failed Query: {failed_query}")
            except Exception as mogrify_error:
                logger.error(f"Could not mogrify failing query. Raw: {query}, Params: {params}, Error: {mogrify_error}")
            return []
        finally:
            if conn:
                conn.close()

    def _setup_tools(self):
        """Setup tools for the LLM agent."""
        
        def search_merchants(user_name: str, search_term: str) -> str:
            """Search for merchants that match or contain the search term."""
            query = """
                SELECT DISTINCT merchant, COUNT(*) as transaction_count
                FROM transactions 
                WHERE user_name = %s 
                AND merchant IS NOT NULL 
                AND merchant != ''
                AND LOWER(merchant) LIKE LOWER(%s)
                AND transaction_type IN ('debited', 'credited')
                GROUP BY merchant
                ORDER BY transaction_count DESC, merchant
                LIMIT 10;
                """
            try:
                results = self._execute_sql_query(query, (user_name, f"%{search_term}%"), user_name)
                if not results:
                    return f"No merchants found matching '{search_term}'. Try a different search term."
                return json.dumps(results)
            except Exception as e:
                logger.error(f"Error searching merchants: {e}")
                return f"Error searching merchants: {str(e)}"
        
        def get_all_merchants(user_name: str) -> str:
            """Get all unique merchants for the user."""
            query = """
                SELECT DISTINCT merchant, COUNT(*) as transaction_count
                FROM transactions 
                WHERE user_name = %s 
                AND merchant IS NOT NULL 
                AND merchant != ''
                AND transaction_type IN ('debited', 'credited')
                GROUP BY merchant
                ORDER BY transaction_count DESC
                LIMIT 20;
                """
            try:
                results = self._execute_sql_query(query, (user_name,), user_name)
                return json.dumps(results)
            except Exception as e:
                logger.error(f"Error fetching merchants: {e}")
                return f"Error fetching merchants: {str(e)}"

        def get_all_banks(user_name: str) -> str:
            """Get all banks for the user."""
            query = """
                SELECT DISTINCT bank, COUNT(*) as transaction_count
                FROM transactions 
                WHERE user_name = %s 
                AND bank IS NOT NULL 
                AND bank != ''
                AND transaction_type IN ('debited', 'credited')
                GROUP BY bank
                ORDER BY transaction_count DESC;
                """
            try:
                results = self._execute_sql_query(query, (user_name,), user_name)
                return json.dumps(results)
            except Exception as e:
                logger.error(f"Error fetching banks: {e}")
                return f"Error fetching banks: {str(e)}"

        def query_transactions(user_name: str, filters: str) -> str:
            """Query transactions with flexible filters."""
            try:
                filters_dict = json.loads(filters)
                
                where_clauses = ["user_name = %s", "transaction_type IN ('debited', 'credited')"]
                params = [user_name]
                
                if filters_dict.get('merchant'):
                    where_clauses.append("LOWER(merchant) LIKE LOWER(%s)")
                    params.append(f"%{filters_dict['merchant']}%")
                
                if filters_dict.get('bank'):
                    where_clauses.append("LOWER(bank) LIKE LOWER(%s)")
                    params.append(f"%{filters_dict['bank']}%")
                
                if filters_dict.get('transaction_type'):
                    ttype = filters_dict['transaction_type'].lower()
                    if 'debit' in ttype:
                        where_clauses.append("transaction_type = 'debited'")
                    elif 'credit' in ttype:
                         where_clauses.append("transaction_type = 'credited'")

                if filters_dict.get('min_amount'):
                    where_clauses.append("ABS(amount) >= %s")
                    params.append(filters_dict['min_amount'])
                
                if filters_dict.get('max_amount'):
                    where_clauses.append("ABS(amount) <= %s")
                    params.append(filters_dict['max_amount'])
                
                if filters_dict.get('date_range'):
                    start_ts, end_ts = self._get_timestamp_range(filters_dict['date_range'])
                    where_clauses.append("date_received BETWEEN %s AND %s")
                    params.extend([start_ts, end_ts])
                
                where_sql = " AND ".join(where_clauses)
                
                summary_query = f"""
                SELECT 
                    COUNT(*) as total_transactions,
                    SUM(ABS(amount)) as total_amount,
                    SUM(CASE WHEN transaction_type = 'debited' THEN ABS(amount) ELSE 0 END) as total_debits,
                    SUM(CASE WHEN transaction_type = 'credited' THEN ABS(amount) ELSE 0 END) as total_credits
                FROM transactions 
                WHERE {where_sql};
                """
                summary_results = self._execute_sql_query(summary_query, tuple(params), user_name)
                
                transactions_query = f"""
                SELECT bank, amount, transaction_type, merchant, date_received, address
                FROM transactions 
                WHERE {where_sql}
                ORDER BY date_received DESC 
                LIMIT 20;
                """
                transaction_list = self._execute_sql_query(transactions_query, tuple(params), user_name)
                
                summary = summary_results[0] if summary_results else {
                    "total_transactions": 0, "total_amount": 0, "total_debits": 0, "total_credits": 0
                }
                summary["transactions_sample"] = transaction_list
                
                return json.dumps(summary, default=str)
                
            except Exception as e:
                logger.error(f"Error querying transactions: {e}", exc_info=True)
                return f"Error querying transactions: {str(e)}"
        
        def calculate_spending_summary(user_name: str, date_range: str, groupby: str = None) -> str:
            """Calculate spending summary for a date range, optionally grouped by merchant or bank."""
            try:
                start_ts, end_ts = self._get_timestamp_range(date_range)
                params = (user_name, start_ts, end_ts)

                if groupby == "merchant":
                    query = """
                    SELECT merchant, SUM(ABS(amount)) as total_amount, COUNT(*) as transaction_count
                    FROM transactions 
                    WHERE user_name = %s 
                    AND transaction_type = 'debited'
                    AND date_received BETWEEN %s AND %s
                    AND merchant IS NOT NULL AND merchant != ''
                    GROUP BY merchant
                    ORDER BY total_amount DESC
                    LIMIT 10;
                    """
                elif groupby == "bank":
                    query = """
                    SELECT bank, SUM(ABS(amount)) as total_amount, COUNT(*) as transaction_count
                    FROM transactions 
                    WHERE user_name = %s 
                    AND transaction_type = 'debited'
                    AND date_received BETWEEN %s AND %s
                    AND bank IS NOT NULL AND bank != ''
                    GROUP BY bank
                    ORDER BY total_amount DESC;
                    """
                else:
                    query = """
                    SELECT 
                        SUM(CASE WHEN transaction_type = 'debited' THEN ABS(amount) ELSE 0 END) as total_spent,
                        SUM(CASE WHEN transaction_type = 'credited' THEN ABS(amount) ELSE 0 END) as total_received,
                        COUNT(CASE WHEN transaction_type = 'debited' THEN 1 END) as debit_count,
                        COUNT(CASE WHEN transaction_type = 'credited' THEN 1 END) as credit_count
                    FROM transactions 
                    WHERE user_name = %s 
                    AND date_received BETWEEN %s AND %s
                    AND transaction_type IN ('debited', 'credited');
                    """
                
                results = self._execute_sql_query(query, params, user_name)
                return json.dumps(results)
                
            except Exception as e:
                logger.error(f"Error calculating spending summary: {e}", exc_info=True)
                return f"Error calculating spending summary: {str(e)}"
        
        self.tools = [
            Tool(
                name="search_merchants",
                description="Search for merchants that match a search term. Use this when user mentions a specific merchant name or when you need to find merchants similar to what user mentioned.",
                func=lambda search_term: search_merchants(self.current_user, search_term)
            ),
            Tool(
                name="get_all_merchants", 
                description="Get all available merchants for the user. Use this to show user what merchants are available.",
                func=lambda _: get_all_merchants(self.current_user)
            ),
            Tool(
                name="get_all_banks",
                description="Get all available banks for the user. Use this when user asks about banks or needs to see available banks.",
                func=lambda _: get_all_banks(self.current_user)
            ),
            Tool(
                name="query_transactions",
                description="Query transactions with filters. Pass filters as JSON string with keys: merchant, bank, transaction_type, min_amount, max_amount, date_range. The transaction_type can be 'debit' or 'credit'.",
                func=lambda filters: query_transactions(self.current_user, filters)
            ),
            Tool(
                name="calculate_spending_summary",
                description="Calculate spending summary for a date range. Optional groupby parameter can be 'merchant' or 'bank'. Date ranges: today, yesterday, this_week, last_week, this_month, last_month, last_X_days",
                func=lambda date_range, groupby=None: calculate_spending_summary(self.current_user, date_range, groupby)
            )
        ]
    
    def _setup_agent(self):
        """Setup the LangChain agent with tools."""
        
        prompt = ChatPromptTemplate.from_messages([
            SystemMessage(content="""You are a helpful financial assistant that can analyze transaction data. 
            You have access to tools to search merchants, query transactions, and calculate spending summaries.
            
            When a user asks about spending or transactions:
            1. If they mention a specific merchant/company name, use search_merchants first to find the exact name.
            2. If they ask about general spending ("How much did I spend?"), use calculate_spending_summary. 
            3. If they want to see specific transactions ("Show me my transactions"), use query_transactions.
            4. Always provide clear, helpful responses with actual numbers and insights from the tool output.
            
            Date ranges you can use: today, yesterday, this_week, last_week, this_month, last_month, last_X_days (e.g., last_7_days).
            
            When presenting transaction summaries, clearly state the total amounts and transaction counts. If you show a list of transactions, mention that it's just a sample of the most recent ones.
            Be conversational and helpful. Always explain what you found in plain language."""),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad")
        ])
        
        agent = create_openai_functions_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=prompt
        )
        
        self.agent_executor = AgentExecutor(
            agent=agent,
            tools=self.tools,
            verbose=True,
            return_intermediate_steps=True,
            max_iterations=5,
            handle_parsing_errors=True
        )
    
    def chat(self, message: str, user_name: str, chat_history: List = None) -> Dict[str, Any]:
        """Process a chat message and return response."""
        
        self.current_user = user_name
        
        try:
            if chat_history is None:
                chat_history = []
            
            logger.info(f"Processing chat message from {user_name}: {message}")
            
            result = self.agent_executor.invoke({
                "input": message,
                "chat_history": chat_history
            })
            
            response_text = result.get("output", "I couldn't process your request.")
            intermediate_steps = result.get("intermediate_steps", [])
            
            tools_used = [step[0].tool for step in intermediate_steps if step[0].tool]
            logger.info(f"Tools used: {tools_used}")
            
            return {
                "success": True,
                "message": response_text,
                "tools_used": tools_used,
                "intermediate_steps": len(intermediate_steps)
            }
            
        except Exception as e:
            logger.error(f"Error processing chat message: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"I encountered an error while processing your request: {str(e)}",
                "tools_used": [],
                "intermediate_steps": 0
            }

chat_system = TransactionChatSystem()