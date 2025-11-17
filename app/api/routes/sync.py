"""
Sync API Routes
Endpoints for syncing data from Autotask
"""
from fastapi import APIRouter, HTTPException, Depends
from datetime import datetime, timedelta
import traceback
from app.models.schemas import SyncResponse, SyncRequest, CustomSyncRequest
from app.services.autotask import AutotaskService, get_autotask_service
from app.services.database import DatabaseService, get_database_service

router = APIRouter(prefix="/sync", tags=["sync"])


@router.post("/last-7-days", response_model=SyncResponse)
async def sync_last_7_days(
    request: SyncRequest = SyncRequest(),
    autotask: AutotaskService = Depends(get_autotask_service),
    db: DatabaseService = Depends(get_database_service)
):
    """
    Sync tickets from the last 7 days
    
    - **company_id**: Optional company filter
    - **max_tickets**: Maximum tickets per batch (1-1000)
    - **concurrent_limit**: Concurrent API calls (1-10)
    """
    try:
        end_date = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
        start_date = (end_date - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        print(f"\n{'='*60}")
        print(f"Syncing last 7 days: {start_date} to {end_date}")
        print(f"{'='*60}")
        
        tickets = await autotask.fetch_tickets_with_details(
            start_date=start_date,
            end_date=end_date,
            company_id=request.company_id,
            max_tickets=request.max_tickets,
            concurrent_limit=request.concurrent_limit
        )
        
        stats = await db.store_tickets_with_details(tickets)
        
        return SyncResponse(
            status="success",
            date_range={
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            statistics=stats
        )
    except Exception as e:
        print(f"ERROR: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")


@router.post("/last-30-days", response_model=SyncResponse)
async def sync_last_30_days(
    request: SyncRequest = SyncRequest(),
    autotask: AutotaskService = Depends(get_autotask_service),
    db: DatabaseService = Depends(get_database_service)
):
    """
    Sync tickets from the last 30 days
    
    - **company_id**: Optional company filter
    - **max_tickets**: Maximum tickets per batch (1-1000)
    - **concurrent_limit**: Concurrent API calls (1-10)
    """
    try:
        end_date = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
        start_date = (end_date - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        print(f"\n{'='*60}")
        print(f"Syncing last 30 days: {start_date} to {end_date}")
        print(f"{'='*60}")
        
        tickets = await autotask.fetch_tickets_with_details(
            start_date=start_date,
            end_date=end_date,
            company_id=request.company_id,
            max_tickets=request.max_tickets,
            concurrent_limit=request.concurrent_limit
        )
        
        stats = await db.store_tickets_with_details(tickets)
        
        return SyncResponse(
            status="success",
            date_range={
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            statistics=stats
        )
    except Exception as e:
        print(f"ERROR: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")


@router.post("/custom", response_model=SyncResponse)
async def sync_custom(
    request: CustomSyncRequest,
    autotask: AutotaskService = Depends(get_autotask_service),
    db: DatabaseService = Depends(get_database_service)
):
    """
    Sync tickets with custom date range
    
    - **start_date**: Start date (ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
    - **end_date**: End date (ISO format)
    - **company_id**: Optional company filter
    - **max_tickets**: Maximum tickets per batch (1-1000)
    - **concurrent_limit**: Concurrent API calls (1-10)
    """
    try:
        try:
            start = datetime.fromisoformat(request.start_date)
            end = datetime.fromisoformat(request.end_date)
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid date format. Use ISO format (YYYY-MM-DD): {str(e)}"
            )
        
        if start >= end:
            raise HTTPException(
                status_code=400,
                detail="Start date must be before end date"
            )
        
        print(f"\n{'='*60}")
        print(f"Syncing custom range: {start} to {end}")
        print(f"{'='*60}")
        
        tickets = await autotask.fetch_tickets_with_details(
            start_date=start,
            end_date=end,
            company_id=request.company_id,
            max_tickets=request.max_tickets,
            concurrent_limit=request.concurrent_limit
        )
        
        stats = await db.store_tickets_with_details(tickets)
        
        return SyncResponse(
            status="success",
            date_range={
                "start": start.isoformat(),
                "end": end.isoformat()
            },
            statistics=stats
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")
