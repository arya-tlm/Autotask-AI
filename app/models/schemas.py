"""
Pydantic models for API requests and responses
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from datetime import datetime


# ==================== REQUEST MODELS ====================

class ChatMessage(BaseModel):
    """Single chat message"""
    role: str = Field(..., description="Message role: 'user' or 'assistant'")
    content: str = Field(..., description="Message content")


class ChatRequest(BaseModel):
    """Chat request payload"""
    message: str = Field(..., description="User message", min_length=1)
    conversation_history: List[ChatMessage] = Field(
        default=[],
        description="Previous conversation messages"
    )
    session_id: Optional[str] = Field(
        None,
        description="Session identifier for tracking"
    )


class SyncRequest(BaseModel):
    """Sync request parameters"""
    company_id: Optional[int] = Field(None, description="Filter by company ID")
    max_tickets: int = Field(500, ge=1, le=1000, description="Max tickets per batch")
    concurrent_limit: int = Field(5, ge=1, le=10, description="Concurrent API calls")


class CustomSyncRequest(SyncRequest):
    """Custom date range sync request"""
    start_date: str = Field(..., description="Start date (ISO format)")
    end_date: str = Field(..., description="End date (ISO format)")


class SearchTicketsRequest(BaseModel):
    """Ticket search parameters"""
    company_id: Optional[int] = None
    status: Optional[int] = Field(None, description="Status code (1=New, 5=Complete, 8=Cancelled)")
    priority: Optional[int] = Field(None, description="Priority code (1=Critical, 2=High, 3=Medium, 4=Low)")
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    limit: int = Field(100, ge=1, le=1000)


# ==================== RESPONSE MODELS ====================

class TicketResponse(BaseModel):
    """Ticket data response"""
    id: int
    ticket_number: Optional[str]
    title: Optional[str]
    description: Optional[str]
    status: Optional[int]
    priority: Optional[int]
    ticket_type: Optional[int]
    ticket_category: Optional[int]
    create_date: Optional[str]
    due_date_time: Optional[str]
    completed_date: Optional[str]
    company_id: Optional[int]
    contact_id: Optional[int]
    assigned_resource_id: Optional[int]
    resolution: Optional[str]


class NoteResponse(BaseModel):
    """Ticket note response"""
    id: int
    ticket_id: int
    title: Optional[str]
    description: Optional[str]
    note_type: Optional[int]
    create_date_time: Optional[str]


class TimeEntryResponse(BaseModel):
    """Time entry response"""
    id: int
    ticket_id: int
    date_worked: Optional[str]
    hours_worked: Optional[float]
    summary_notes: Optional[str]
    resource_id: Optional[int]


class SyncStats(BaseModel):
    """Sync operation statistics"""
    tickets_processed: int = 0
    tickets_inserted: int = 0
    notes_inserted: int = 0
    time_entries_inserted: int = 0
    errors: List[str] = []


class SyncResponse(BaseModel):
    """Sync operation response"""
    status: str
    date_range: Dict[str, str]
    statistics: SyncStats


class ChatResponse(BaseModel):
    """Chat response"""
    answer: str
    tickets: List[Dict[str, Any]] = []
    ticket_count: int = 0
    companies: List[Dict[str, Any]] = []
    company: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    database: str
    openai: str
    timestamp: str


class StatsResponse(BaseModel):
    """Database statistics response"""
    status: str
    statistics: Dict[str, int]
    timestamp: str


class ErrorResponse(BaseModel):
    """Error response"""
    detail: str
    status_code: int
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
