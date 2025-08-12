from fastapi import APIRouter, Depends, HTTPException, status, Request
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from auth import basic_auth
from chat import chat_system
from logging_config import get_logger
from fastapi.templating import Jinja2Templates

logger = get_logger("sms_sync.chat_routes")

# Create router
chat_router = APIRouter(prefix="/chat", tags=["Chat"])

templates = Jinja2Templates(directory="templates")


class ChatMessage(BaseModel):
    """Individual chat message."""
    role: str  # "user" or "assistant" 
    content: str
    timestamp: Optional[str] = None


class ChatRequest(BaseModel):
    """Request model for chat."""
    message: str
    chat_history: Optional[List[ChatMessage]] = []


class ChatResponse(BaseModel):
    """Response model for chat."""
    success: bool
    message: str
    tools_used: List[str] = []
    intermediate_steps: int = 0


@chat_router.post("/",
                  summary="Chat with your financial data",
                  description="Have a conversation with AI about your transactions using natural language",
                  response_model=ChatResponse)
def chat_with_transactions(
    request: ChatRequest,
    auth_user: str = Depends(basic_auth)
):
    """
    Chat with your transaction data using natural language.
    
    The AI can help you with queries like:
    - "How much did I spend on Swiggy last week?"
    - "Show me my biggest expenses this month"  
    - "What did I spend on yesterday?"
    - "How much did I receive from HDFC bank?"
    - "Find transactions from Amazon"
    - "What are my total expenses this month?"
    
    The AI will automatically:
    - Search for merchant names even if you misspell them
    - Find the right date ranges
    - Calculate totals and summaries
    - Show you relevant transaction details
    """
    
    try:
        if not request.message.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Message cannot be empty"
            )
        
        logger.info(f"Chat request from user {auth_user}: {request.message}")
        
        # Convert chat history to the format expected by LangChain
        langchain_history = []
        for msg in request.chat_history:
            if msg.role == "user":
                langchain_history.append(("human", msg.content))
            elif msg.role == "assistant":  
                langchain_history.append(("ai", msg.content))
        
        # Process the chat message
        result = chat_system.chat(
            message=request.message,
            user_name=auth_user,
            chat_history=langchain_history
        )
        
        logger.info(f"Chat processed. Tools used: {result.get('tools_used', [])}")
        
        return ChatResponse(
            success=result["success"],
            message=result["message"], 
            tools_used=result.get("tools_used", []),
            intermediate_steps=result.get("intermediate_steps", 0)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while processing your message: {str(e)}"
        )



@chat_router.get("/health",
                summary="Check chat system health",
                description="Check if the chat system is working properly")
def chat_health_check():
    """Check if the chat system is healthy."""
    try:
        # Simple health check
        return {
            "status": "healthy",
            "chat_system_ready": hasattr(chat_system, 'llm'),
            "tools_available": len(chat_system.tools),
            "message": "Chat system is ready to help you analyze your transactions!"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy", 
            "error": str(e),
            "message": "Chat system is experiencing issues"
        }


@chat_router.get("/", summary="Chat UI", include_in_schema=False)
def chat_ui(request: Request):
    """Serve the chat HTML UI."""
    return templates.TemplateResponse("chat.html", {"request": request})