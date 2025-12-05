"""
MCP Chat Endpoint - n8n Webhook + MCP Fallback
"""

import json
import logging
import httpx
from typing import Dict, List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/mcp", tags=["MCP Chat"])


# ==================== CONFIG ====================
N8N_WEBHOOK_URL = "https://tlitatx.app.n8n.cloud/webhook/a10b5217-8945-4eb6-8f1b-1799d37bc95b"
N8N_WEBHOOK_TEST_URL = "https://tlitatx.app.n8n.cloud/webhook-test/a10b5217-8945-4eb6-8f1b-1799d37bc95b"
N8N_TIMEOUT = 120.0  # 2 minutes for AI agent


# ==================== MODELS ====================
class ChatMessage(BaseModel):
    role: str
    content: str


class MCPChatRequest(BaseModel):
    message: str
    conversation_history: List[ChatMessage] = []
    session_id: Optional[str] = None
    use_test: bool = False  # Use test webhook
    force_mcp: bool = False  # Skip n8n, use MCP directly


class MCPChatResponse(BaseModel):
    answer: str
    source: str  # "n8n" or "mcp"
    tickets: List[Dict] = []
    ticket_count: int = 0
    metadata: Dict = {}
    error: Optional[str] = None


# ==================== N8N WEBHOOK CALL ====================
async def call_n8n_webhook(
    message: str,
    conversation_history: List[Dict] = None,
    session_id: str = None,
    use_test: bool = False
) -> Dict:
    """Call n8n AI agent webhook"""
    
    url = N8N_WEBHOOK_TEST_URL if use_test else N8N_WEBHOOK_URL
    
    payload = {
        "message": message,
        "conversation_history": conversation_history or [],
        "session_id": session_id or "anonymous"
    }
    
    logger.info(f"🔗 Calling n8n: {url}")
    
    async with httpx.AsyncClient(timeout=N8N_TIMEOUT) as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()
        return response.json()


# ==================== MCP FALLBACK ====================
async def call_mcp_fallback(
    message: str,
    conversation_history: List[Dict] = None,
    session_id: str = None
) -> Dict:
    """Fallback to local AI service (MCP)"""
    
    logger.info("🔄 Using MCP fallback (local AI service)")
    
    try:
        from app.services.ai import get_ai_service
        from app.models.schemas import ChatMessage as AIChatMessage
        
        ai_service = get_ai_service()
        
        # Convert history
        history = [
            AIChatMessage(role=msg.get("role", "user"), content=msg.get("content", ""))
            for msg in (conversation_history or [])
        ]
        
        result = await ai_service.chat_with_tickets(
            user_message=message,
            conversation_history=history,
            session_id=session_id
        )
        
        return result
        
    except Exception as e:
        logger.error(f"❌ MCP fallback error: {e}")
        raise


# ==================== ENDPOINTS ====================
@router.post("/chat", response_model=MCPChatResponse)
async def mcp_chat(request: MCPChatRequest):
    """
    MCP Chat Endpoint
    
    - Primary: n8n AI Agent webhook
    - Fallback: Local AI service (MCP)
    
    Set `use_test=true` to use n8n test webhook
    Set `force_mcp=true` to skip n8n and use MCP directly
    """
    
    source = "unknown"
    result = None
    error = None
    
    # Convert history to dict
    history = [{"role": m.role, "content": m.content} for m in request.conversation_history]
    
    # Force MCP mode
    if request.force_mcp:
        try:
            result = await call_mcp_fallback(request.message, history, request.session_id)
            source = "mcp"
        except Exception as e:
            error = str(e)
            raise HTTPException(status_code=500, detail=f"MCP failed: {e}")
    
    else:
        # Try n8n first
        try:
            result = await call_n8n_webhook(
                message=request.message,
                conversation_history=history,
                session_id=request.session_id,
                use_test=request.use_test
            )
            source = "n8n"
            logger.info("✅ n8n successful")
            
        except Exception as n8n_error:
            logger.warning(f"⚠️ n8n failed: {n8n_error}, trying MCP...")
            
            # Fallback to MCP
            try:
                result = await call_mcp_fallback(request.message, history, request.session_id)
                source = "mcp"
                logger.info("✅ MCP fallback successful")
            except Exception as mcp_error:
                error = f"n8n: {n8n_error}, MCP: {mcp_error}"
                raise HTTPException(status_code=500, detail=error)
    
    # Return response
    if result:
        return MCPChatResponse(
            answer=result.get("answer", "No response"),
            source=source,
            tickets=result.get("tickets", []),
            ticket_count=result.get("ticket_count", 0),
            metadata=result.get("metadata", {}),
            error=None
        )
    
    raise HTTPException(status_code=500, detail="No response from n8n or MCP")


@router.get("/health")
async def mcp_health():
    """Check n8n webhook connectivity"""
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Just check if n8n is reachable (HEAD request)
            response = await client.get(N8N_WEBHOOK_TEST_URL.replace("/webhook-test/", "/"))
            return {
                "status": "ok",
                "n8n_reachable": response.status_code < 500,
                "webhook_url": N8N_WEBHOOK_URL
            }
    except Exception as e:
        return {
            "status": "degraded",
            "n8n_reachable": False,
            "error": str(e),
            "fallback": "MCP available"
        }