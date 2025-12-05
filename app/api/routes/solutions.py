"""
Solutions API Routes
Endpoints for searching and viewing ticket solutions/resolutions
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List, Dict, Optional
from openai import AsyncOpenAI
from app.config import get_settings
from app.services.database import DatabaseService, get_database_service

router = APIRouter(prefix="/solutions", tags=["solutions"])
settings = get_settings()
openai_client = AsyncOpenAI(api_key=settings.openai_api_key)

# In-memory cache for AI suggestions (to avoid repeated OpenAI calls)
_suggestions_cache = {
    "suggestions": [],
    "timestamp": None,
    "cache_ttl_seconds": 3600  # Cache for 1 hour
}


# ==================== REQUEST/RESPONSE MODELS ====================

class SolutionSearchRequest(BaseModel):
    """Search for ticket solutions"""
    ticket_number: Optional[str] = Field(None, description="Search by ticket number")
    ticket_id: Optional[int] = Field(None, description="Search by ticket ID")
    company_id: Optional[int] = Field(None, description="Filter by company ID")
    has_resolution: bool = Field(True, description="Only show tickets with solutions")
    limit: int = Field(50, ge=1, le=500, description="Maximum results")


class SolutionResponse(BaseModel):
    """Ticket with its solution"""
    id: int
    ticket_number: Optional[str]
    title: Optional[str]
    description: Optional[str]
    resolution: Optional[str]
    status: Optional[int]
    priority: Optional[int]
    create_date: Optional[str]
    completed_date: Optional[str]
    resolved_date_time: Optional[str]
    company_id: Optional[int]
    assigned_resource_id: Optional[int]


class SolutionsListResponse(BaseModel):
    """List of solutions"""
    solutions: List[SolutionResponse]
    total_count: int
    has_resolution_count: int


# ==================== ENDPOINTS ====================

@router.post("/search", response_model=SolutionsListResponse)
async def search_solutions(
    request: SolutionSearchRequest,
    db: DatabaseService = Depends(get_database_service)
):
    """
    Search for ticket solutions

    Find tickets with their resolutions/solutions. You can search by:
    - Ticket number (exact match)
    - Ticket ID
    - Company ID
    - Only tickets with resolutions

    **Example requests:**
    ```json
    {
      "ticket_number": "T20240001"
    }
    ```

    ```json
    {
      "company_id": 123,
      "has_resolution": true,
      "limit": 100
    }
    ```
    """
    try:
        query = db.client.table("tickets").select("*")

        # Filter by ticket number (exact match)
        if request.ticket_number:
            query = query.eq("ticket_number", request.ticket_number)

        # Filter by ticket ID
        if request.ticket_id:
            query = query.eq("id", request.ticket_id)

        # Filter by company
        if request.company_id:
            query = query.eq("company_id", request.company_id)

        # Filter only tickets with resolutions
        if request.has_resolution:
            query = query.not_.is_("resolution", "null")
            query = query.neq("resolution", "")

        # Order by most recently resolved first
        query = query.order("resolved_date_time", desc=True)

        # Limit results
        query = query.limit(request.limit)

        result = query.execute()
        tickets = result.data or []

        # Count tickets with resolutions
        has_resolution_count = len([t for t in tickets if t.get("resolution")])

        # Transform to SolutionResponse
        solutions = [
            SolutionResponse(
                id=t.get("id"),
                ticket_number=t.get("ticket_number"),
                title=t.get("title"),
                description=t.get("description"),
                resolution=t.get("resolution"),
                status=t.get("status"),
                priority=t.get("priority"),
                create_date=t.get("create_date"),
                completed_date=t.get("completed_date"),
                resolved_date_time=t.get("resolved_date_time"),
                company_id=t.get("company_id"),
                assigned_resource_id=t.get("assigned_resource_id")
            )
            for t in tickets
        ]

        return SolutionsListResponse(
            solutions=solutions,
            total_count=len(solutions),
            has_resolution_count=has_resolution_count
        )

    except Exception as e:
        print(f"Solutions search error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to search solutions: {str(e)}"
        )


@router.get("/ticket/{ticket_number}")
async def get_solution_by_ticket_number(
    ticket_number: str,
    db: DatabaseService = Depends(get_database_service)
):
    """
    Get AI-formatted solution for a specific ticket number     

    **Example:**
    - GET /solutions/ticket/T20251027.0005

    Returns AI-formatted response with ticket details and solution
    """
    try:
        # Fetch ticket from database
        result = db.client.table("tickets")\
            .select("*")\
            .eq("ticket_number", ticket_number)\
            .limit(1)\
            .execute()

        if not result.data or len(result.data) == 0:
            raise HTTPException(
                status_code=404,
                detail=f"Ticket {ticket_number} not found in database"
            )

        ticket = result.data[0]

        # Check if ticket has a resolution
        has_resolution = bool(ticket.get("resolution") and ticket.get("resolution").strip())

        # Prepare ticket data for AI
        ticket_info = f"""
Ticket Number: {ticket.get('ticket_number', 'N/A')}
Title: {ticket.get('title', 'No title')}
Description: {ticket.get('description', 'No description available')}
Resolution/Solution: {ticket.get('resolution', 'No resolution recorded yet')}
Status: {ticket.get('status', 'Unknown')}
Priority: {ticket.get('priority', 'Unknown')}
Created Date: {ticket.get('create_date', 'N/A')}
Completed Date: {ticket.get('completed_date', 'Not completed')}
Company ID: {ticket.get('company_id', 'N/A')}
"""

        # Send to OpenAI for formatting and solution generation
        system_prompt = """You are an expert IT support assistant that provides step-by-step solutions for technical problems.

When given a ticket, you should:
1. Display the ticket number and title prominently
2. Summarize the problem clearly
3. Provide a detailed STEP-BY-STEP solution:
   - If a resolution is provided in the ticket, format it as numbered steps
   - If NO resolution is provided, analyze the problem description and generate practical troubleshooting steps
4. Include any relevant metadata (status, priority, dates)

IMPORTANT: Always provide actionable, step-by-step solutions. Even if no resolution is recorded, use your IT expertise to suggest troubleshooting steps based on the problem description.

Format your response with clear markdown:
- Use **bold** for important points
- Use numbered lists for step-by-step instructions
- Use bullet points for additional tips
- Keep solutions practical and easy to follow"""

        user_prompt = f"""Analyze this support ticket and provide a comprehensive step-by-step solution:

{ticket_info}

Generate a clear, actionable solution with step-by-step instructions to resolve this issue. If a resolution is provided, format it properly. If not, use the problem description to suggest troubleshooting steps."""

        # Call OpenAI
        response = await openai_client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.5  # Increased for more detailed, creative solutions
        )

        ai_answer = response.choices[0].message.content

        # Return formatted response
        return {
            "ticket_number": ticket.get("ticket_number"),
            "title": ticket.get("title"),
            "answer": ai_answer,
            "has_resolution": has_resolution,
            "ticket": ticket
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Get solution error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get solution: {str(e)}"
        )


@router.get("/stats")
async def get_solution_stats(
    db: DatabaseService = Depends(get_database_service)
):
    """
    Get statistics about solutions

    Returns counts of:
    - Total tickets
    - Tickets with solutions
    - Tickets without solutions
    - Resolution rate
    """
    try:
        # Get total tickets
        total_result = db.client.table("tickets")\
            .select("id", count="exact")\
            .limit(1)\
            .execute()
        total_tickets = total_result.count or 0

        # Get tickets with solutions
        with_solution_result = db.client.table("tickets")\
            .select("id", count="exact")\
            .not_.is_("resolution", "null")\
            .neq("resolution", "")\
            .limit(1)\
            .execute()
        with_solutions = with_solution_result.count or 0

        without_solutions = total_tickets - with_solutions
        resolution_rate = (with_solutions / total_tickets * 100) if total_tickets > 0 else 0

        return {
            "status": "success",
            "stats": {
                "total_tickets": total_tickets,
                "tickets_with_solutions": with_solutions,
                "tickets_without_solutions": without_solutions,
                "resolution_rate_percentage": round(resolution_rate, 2)
            }
        }

    except Exception as e:
        print(f"Solution stats error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get solution stats: {str(e)}"
        )


@router.get("/ai-suggestions")
async def get_ai_suggestions(
    db: DatabaseService = Depends(get_database_service),
    force_refresh: bool = False
):
    """
    Get AI-generated suggestions for open tickets (with caching)

    Uses OpenAI to analyze recent OPEN tickets (status != 5 / NOT Complete) and generate
    intelligent solution suggestions for common unresolved issues.

    Results are cached for 1 hour to avoid repeated OpenAI API calls.
    Use ?force_refresh=true to bypass cache.
    """
    try:
        # Check cache first (unless force_refresh is True)
        from datetime import datetime, timedelta

        if not force_refresh and _suggestions_cache["timestamp"]:
            cache_age = datetime.now() - _suggestions_cache["timestamp"]
            if cache_age.total_seconds() < _suggestions_cache["cache_ttl_seconds"]:
                print(f"Returning cached suggestions (age: {int(cache_age.total_seconds())}s)")
                return {
                    "status": "success",
                    "suggestions": _suggestions_cache["suggestions"],
                    "analyzed_tickets": _suggestions_cache.get("analyzed_tickets", 0),
                    "sample_tickets": _suggestions_cache.get("sample_tickets", []),
                    "cached": True,
                    "cache_age_seconds": int(cache_age.total_seconds())
                }

        print("Generating fresh AI suggestions...")

        # Fetch recent tickets with status != 5 (NOT Complete) - ALIGNED WITH DASHBOARD
        result = db.client.table("tickets")\
            .select("ticket_number, title, description, resolution, priority, create_date, status")\
            .neq("status", 5)\
            .order("create_date", desc=True)\
            .limit(200)\
            .execute()

        tickets = result.data or []

        print(f"Found {len(tickets)} open tickets (status != 5) out of 200 fetched")

        if len(tickets) < 3:
            return {
                "status": "success",
                "suggestions": [],
                "message": f"Not enough open tickets found (need at least 3, found {len(tickets)}). Open tickets are those with status != 5 (Complete)"
            }

        # Prepare ticket data for AI
        tickets_summary = []
        for ticket in tickets:
            # Handle None values properly
            description = ticket.get("description") or ""
            title = ticket.get("title") or "No Title"

            tickets_summary.append({
                "ticket_number": ticket.get("ticket_number"),
                "title": title,
                "description": description[:250],  # Truncate for API limits
                "priority": ticket.get("priority"),
                "status": ticket.get("status")
            })

        # Ask OpenAI to analyze and generate suggestions
        system_prompt = """You are an IT support expert analyzing OPEN support tickets that need solutions.

Analyze the provided open tickets and identify:
1. Common problems/issues that appear frequently across multiple tickets
2. Patterns in the types of issues being reported
3. Priority levels and urgency indicators

Generate 5-8 actionable solution suggestions that would help resolve these open tickets.

For each suggestion, provide:
- A clear, concise title describing the problem type
- A brief description of the common issue you identified
- A recommended solution/fix that would resolve this type of ticket
- Why implementing this solution would be impactful

Focus on practical, implementable solutions that could resolve multiple similar tickets.

Format as JSON with a "suggestions" key containing an array:
{
  "suggestions": [
    {
      "title": "Email Configuration Issues",
      "problem": "Multiple users reporting email sync problems on mobile devices",
      "solution": "Create step-by-step guide for reconfiguring email accounts, verify server settings are correct",
      "usefulness": "Would resolve 5+ similar open tickets and prevent future issues"
    }
  ]
}"""

        user_prompt = f"""Analyze these OPEN support tickets that currently have NO resolution:

{tickets_summary}

Generate 5-8 smart solution suggestions that would help resolve these common open issues."""

        # Call OpenAI
        response = await openai_client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            response_format={"type": "json_object"}
        )

        ai_response = response.choices[0].message.content
        print(f"AI Response: {ai_response[:500]}")  # Debug print

        # Parse JSON response
        import json
        try:
            suggestions_data = json.loads(ai_response)
            print(f"Parsed data type: {type(suggestions_data)}")
            print(f"Parsed data keys: {suggestions_data.keys() if isinstance(suggestions_data, dict) else 'Not a dict'}")

            # Handle both array and object responses
            if isinstance(suggestions_data, dict):
                # Check if it has a "suggestions" key (expected format)
                if "suggestions" in suggestions_data:
                    suggestions = suggestions_data["suggestions"]
                # Check if it's a single suggestion object (has title, problem, solution)
                elif "title" in suggestions_data and "problem" in suggestions_data:
                    # Wrap single suggestion in an array
                    suggestions = [suggestions_data]
                    print("Wrapped single suggestion object in array")
                # Try other possible array keys
                else:
                    suggestions = (
                        suggestions_data.get("recommendations") or
                        suggestions_data.get("items") or
                        []
                    )
            elif isinstance(suggestions_data, list):
                suggestions = suggestions_data
            else:
                suggestions = []

            print(f"Found {len(suggestions) if isinstance(suggestions, list) else 0} suggestions")
        except Exception as e:
            print(f"JSON parsing error: {e}")
            print(f"AI Response was: {ai_response}")
            suggestions = []

        # Update cache with fresh suggestions
        _suggestions_cache["suggestions"] = suggestions if isinstance(suggestions, list) else []
        _suggestions_cache["timestamp"] = datetime.now()
        _suggestions_cache["analyzed_tickets"] = len(tickets)
        _suggestions_cache["sample_tickets"] = tickets[:10]  # Cache sample tickets too
        print(f"Cache updated with {len(_suggestions_cache['suggestions'])} suggestions")

        return {
            "status": "success",
            "suggestions": _suggestions_cache["suggestions"],
            "analyzed_tickets": len(tickets),
            "sample_tickets": tickets[:10],  # Return first 10 tickets as examples
            "cached": False,
            "raw_response": ai_response[:200] if not suggestions else None  # Debug info
        }

    except Exception as e:
        print(f"AI suggestions error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate AI suggestions: {str(e)}"
        )
