"""
Health & Stats API Routes
System health checks and database statistics
"""
from fastapi import APIRouter, HTTPException, Depends
from datetime import datetime
from app.models.schemas import HealthResponse, StatsResponse
from app.services.database import DatabaseService, get_database_service
from app.config import get_settings

router = APIRouter(tags=["health"])
settings = get_settings()


@router.get("/health", response_model=HealthResponse)
async def health_check(db: DatabaseService = Depends(get_database_service)):
    """
    Health check endpoint
    
    Returns the operational status of:
    - API service
    - Database connection
    - OpenAI configuration
    """
    try:
        database_status = "connected" if db.health_check() else "disconnected"
        
        openai_status = "configured" if settings.openai_api_key else "not configured"
        
        overall_status = "healthy" if database_status == "connected" else "unhealthy"
        
        return HealthResponse(
            status=overall_status,
            database=database_status,
            openai=openai_status,
            timestamp=datetime.now().isoformat()
        )
    except Exception as e:
        return HealthResponse(
            status="unhealthy",
            database="error",
            openai="unknown",
            timestamp=datetime.now().isoformat()
        )


@router.get("/stats/database", response_model=StatsResponse)
async def get_database_stats(db: DatabaseService = Depends(get_database_service)):
    """
    Get comprehensive database statistics
    
    Returns counts of:
    - Tickets: Total tickets in the system
    - Notes: Total ticket notes
    - Time Entries: Total time entries
    - Companies: Total client companies
    - Resources: Total technicians/employees
    - Contacts: Total contact persons
    
    Example Response:
    {
        "status": "success",
        "statistics": {
            "tickets": 502,
            "notes": 4614,
            "time_entries": 1250,
            "companies": 100,
            "resources": 45,
            "contacts": 1100
        },
        "timestamp": "2025-11-13T10:30:00"
    }
    """
    try:
        stats = db.get_database_stats()
        
        return StatsResponse(
            status="success",
            statistics=stats,
            timestamp=datetime.now().isoformat()
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve statistics: {str(e)}"
        )