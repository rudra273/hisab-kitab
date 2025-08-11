# llm_provider.py
import os
import time
from typing import Optional
from logging_config import get_logger
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_openai import ChatOpenAI

logger = get_logger("sms_sync.llm_provider")

class LLMProvider:
    """LLM provider with primary and secondary fallback support."""
    
    def __init__(self):
        """Initialize LLM providers with fallback logic."""
        
        # Primary LLM (Gemini)
        self.primary_provider = "gemini"
        gemini_api_key = os.getenv("GEMINI_APIKEY")
        if not gemini_api_key:
            raise ValueError("GEMINI_APIKEY environment variable not set.")
        
        self.primary_llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            google_api_key=gemini_api_key,
            temperature=0.1,
            max_output_tokens=1000
        )
        
        # Secondary LLM (OpenAI GPT-4)
        self.secondary_provider = "openai"
        openai_api_key = os.getenv("OPENAI_APIKEY")
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set.")
        
        self.secondary_llm = ChatOpenAI(
            model="gpt-4o-mini",
            api_key=openai_api_key,
            temperature=0.1,
            max_tokens=700
        )
        
        # Rate limiting
        self.request_delay = 2.0
        self.max_retries = 2
        self.retry_delay = 5.0
        self.last_request_time = 0
    
    def _wait_for_rate_limit(self):
        """Implement rate limiting."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            logger.info(f"Rate limiting: waiting {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def generate_response(self, prompt: str) -> Optional[str]:
        """Generate response with primary-secondary fallback logic."""
        
        # Try primary LLM first
        response = self._try_llm(self.primary_llm, self.primary_provider, prompt)
        if response:
            return response
        
        logger.warning(f"Primary LLM ({self.primary_provider}) failed, trying secondary")
        
        # Try secondary LLM
        response = self._try_llm(self.secondary_llm, self.secondary_provider, prompt)
        if response:
            return response
        
        logger.error("Both primary and secondary LLMs failed")
        return None
    
    def _try_llm(self, llm, provider_name: str, prompt: str) -> Optional[str]:
        """Try a specific LLM with retry logic."""
        
        for attempt in range(self.max_retries):
            try:
                self._wait_for_rate_limit()
                logger.info(f"Trying {provider_name} (attempt {attempt + 1}/{self.max_retries})")
                
                response = llm.invoke(prompt)
                
                if response and hasattr(response, 'content') and response.content:
                    logger.info(f"{provider_name} responded successfully")
                    return response.content
                else:
                    logger.warning(f"Empty response from {provider_name} on attempt {attempt + 1}")
                    
            except Exception as e:
                error_str = str(e).lower()
                logger.warning(f"{provider_name} error on attempt {attempt + 1}: {e}")
                
                # Check for rate limit/quota errors
                if any(keyword in error_str for keyword in ['quota', 'rate', 'limit', 'exceeded']):
                    if attempt < self.max_retries - 1:
                        backoff_time = self.retry_delay * (2 ** attempt)
                        logger.info(f"Quota error, waiting {backoff_time} seconds...")
                        time.sleep(backoff_time)
                        continue
                
                if attempt == self.max_retries - 1:
                    logger.error(f"{provider_name} failed after {self.max_retries} attempts")
        
        return None