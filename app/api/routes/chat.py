"""
Chat API Routes
Endpoints for AI-powered chat queries
"""
from fastapi import APIRouter, HTTPException, Depends
import traceback
from app.models.schemas import ChatRequest, ChatResponse
from app.services.ai import AIService, get_ai_service

router = APIRouter(prefix="/chat", tags=["chat"])


@router.post("", response_model=ChatResponse)
async def chat(
    request: ChatRequest,
    ai: AIService = Depends(get_ai_service)
):
    """
    Chat with AI about tickets
    
    Send natural language queries to search and analyze tickets.
    
    **Example queries:**
    - "Show me all high priority tickets"
    - "How many open tickets does Acme Corp have?"
    - "What are the ticket stats by company?"
    - "Find all critical tickets from last week"
    
    **Request body:**
    - **message**: Your natural language query (required)
    - **conversation_history**: Previous messages in conversation (optional)
    - **session_id**: Session identifier for tracking (optional)
    
    **Response:**
    - **answer**: Natural language summary of results
    - **tickets**: List of matching tickets (up to 50)
    - **ticket_count**: Total number of matching tickets
    - **companies**: List of companies (if applicable)
    - **company**: Specific company data (if applicable)
    - **error**: Error message if something went wrong
    """
    try:
        if not request.message or not request.message.strip():
            raise HTTPException(
                status_code=400,
                detail="Message cannot be empty"
            )
        
        result = await ai.chat_with_tickets(
            user_message=request.message,
            conversation_history=request.conversation_history,
            session_id=request.session_id
        )
        
        return ChatResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Chat error: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process chat request: {str(e)}"
        )
