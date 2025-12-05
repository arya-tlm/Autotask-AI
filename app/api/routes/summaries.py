"""
Ticket Summaries API Routes
Generates and displays AI summaries of tickets including notes and time entries
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from typing import List, Dict, Optional
from datetime import datetime
from openai import AsyncOpenAI
from app.config import get_settings
from app.services.database import DatabaseService, get_database_service

router = APIRouter(prefix="/summaries", tags=["summaries"])
settings = get_settings()
openai_client = AsyncOpenAI(api_key=settings.openai_api_key)


# ==================== REQUEST/RESPONSE MODELS ====================

class TicketSummaryResponse(BaseModel):
    """Ticket with AI-generated summary"""
    id: int
    ticket_number: Optional[str]
    title: Optional[str]
    status: Optional[int]
    is_closed: bool
    status_text: str
    priority: Optional[int]
    create_date: Optional[str]
    completed_date: Optional[str]
    resolution: Optional[str]
    summary: Optional[str]
    notes_count: int
    time_entries_count: int
    total_hours: float

    # Additional fields
    company_name: Optional[str] = None
    assigned_to: Optional[str] = None
    created_by: Optional[str] = None
    closed_by: Optional[str] = None
    time_to_resolution: Optional[str] = None  # Human-readable duration


class SummariesListResponse(BaseModel):
    """List of ticket summaries"""
    summaries: List[TicketSummaryResponse]
    total_count: int
    generated_count: int
    cached_count: int


# ==================== HELPER FUNCTIONS ====================

def calculate_time_to_resolution(create_date: str, completed_date: str) -> str:
    """Calculate human-readable time difference between creation and completion"""
    try:
        created = datetime.fromisoformat(create_date.replace('Z', '+00:00'))
        completed = datetime.fromisoformat(completed_date.replace('Z', '+00:00'))
        diff = completed - created

        days = diff.days
        hours = diff.seconds // 3600
        minutes = (diff.seconds % 3600) // 60

        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0 or (days == 0 and hours == 0):
            parts.append(f"{minutes}m")

        return " ".join(parts) if parts else "0m"
    except:
        return "N/A"


# ==================== ENDPOINTS ====================

@router.get("/list", response_model=SummariesListResponse)
async def get_summaries_list(
    limit: int = Query(200, ge=1, le=500, description="Number of tickets to fetch"),
    force_regenerate: bool = Query(False, description="Force regenerate all summaries"),
    with_summary_only: bool = Query(False, description="Only return tickets that already have summaries"),
    db: DatabaseService = Depends(get_database_service)
):
    """
    Get list of ticket summaries for the latest tickets

    - Fetches 200 latest tickets by default
    - Generates summaries on-demand if not cached (unless with_summary_only=True)
    - Includes notes and time entries summary
    - Shows status (open/closed) and resolution if closed
    - Use with_summary_only=True to quickly load cached summaries first
    """
    try:
        # Fetch latest tickets ordered by create_date descending
        query = db.client.table("tickets")\
            .select("*")\
            .order("create_date", desc=True)

        # Filter for tickets with summaries if requested
        if with_summary_only:
            query = query.not_.is_("summary", "null")

        tickets_result = query.limit(limit).execute()

        tickets = tickets_result.data or []
        print(f"Fetched {len(tickets)} latest tickets (with_summary_only={with_summary_only})")

        if not tickets:
            return SummariesListResponse(
                summaries=[],
                total_count=0,
                generated_count=0,
                cached_count=0
            )

        # Extract all IDs for bulk queries
        ticket_ids = [t["id"] for t in tickets]
        company_ids = list(set([t.get("company_id") for t in tickets if t.get("company_id")]))
        resource_ids = list(set(
            [t.get("assigned_resource_id") for t in tickets if t.get("assigned_resource_id")] +
            [t.get("creator_resource_id") for t in tickets if t.get("creator_resource_id")] +
            [t.get("completed_by_resource_id") for t in tickets if t.get("completed_by_resource_id")]
        ))

        # Bulk fetch companies
        companies_map = {}
        if company_ids:
            companies_result = db.client.table("companies")\
                .select("id", "company_name")\
                .in_("id", company_ids)\
                .execute()
            companies_map = {c["id"]: c.get("company_name") for c in (companies_result.data or [])}

        # Bulk fetch resources
        resources_map = {}
        if resource_ids:
            resources_result = db.client.table("resources")\
                .select("id", "first_name", "last_name")\
                .in_("id", resource_ids)\
                .execute()
            resources_map = {
                r["id"]: f"{r.get('first_name', '')} {r.get('last_name', '')}".strip()
                for r in (resources_result.data or [])
            }

        # Bulk fetch notes counts (group by ticket_id)
        notes_map = {}
        try:
            notes_result = db.client.table("ticket_notes")\
                .select("ticket_id")\
                .in_("ticket_id", ticket_ids)\
                .execute()
            notes_data = notes_result.data or []
            for ticket_id in ticket_ids:
                notes_map[ticket_id] = sum(1 for n in notes_data if n.get("ticket_id") == ticket_id)
        except:
            pass

        # Bulk fetch time entries
        time_entries_map = {}
        try:
            time_entries_result = db.client.table("time_entries")\
                .select("ticket_id", "hours_worked")\
                .in_("ticket_id", ticket_ids)\
                .execute()
            time_entries_data = time_entries_result.data or []
            for ticket_id in ticket_ids:
                entries = [te for te in time_entries_data if te.get("ticket_id") == ticket_id]
                time_entries_map[ticket_id] = {
                    "count": len(entries),
                    "total_hours": sum(float(te.get("hours_worked", 0) or 0) for te in entries)
                }
        except:
            pass

        summaries = []
        generated_count = 0
        cached_count = 0

        for ticket in tickets:
            ticket_id = ticket["id"]

            # Check if summary exists and force_regenerate is False
            if ticket.get("summary") and not force_regenerate:
                summary_text = ticket["summary"]
                cached_count += 1
            else:
                # Generate new summary
                summary_text = await generate_ticket_summary(db, ticket)
                generated_count += 1

                # Store summary in database
                try:
                    db.client.table("tickets")\
                        .update({"summary": summary_text})\
                        .eq("id", ticket_id)\
                        .execute()
                    print(f"  ✓ Generated and saved summary for ticket {ticket_id}")
                except Exception as e:
                    print(f"  ⚠ Failed to save summary for ticket {ticket_id}: {str(e)}")

            # Get data from bulk queries
            notes_count = notes_map.get(ticket_id, 0)
            time_entry_info = time_entries_map.get(ticket_id, {"count": 0, "total_hours": 0})
            time_entries_count = time_entry_info["count"]
            total_hours = time_entry_info["total_hours"]

            # Determine status
            status = ticket.get("status")
            is_closed = status == 5
            status_text = "Closed" if is_closed else "Open"

            # Get company name from bulk query
            company_name = companies_map.get(ticket.get("company_id"))

            # Get resource names from bulk query
            assigned_to = resources_map.get(ticket.get("assigned_resource_id"))
            created_by = resources_map.get(ticket.get("creator_resource_id"))
            closed_by = resources_map.get(ticket.get("completed_by_resource_id")) if is_closed else None

            # Calculate time to resolution
            time_to_resolution = None
            if is_closed and ticket.get("create_date") and ticket.get("completed_date"):
                time_to_resolution = calculate_time_to_resolution(
                    ticket.get("create_date"),
                    ticket.get("completed_date")
                )

            summaries.append(TicketSummaryResponse(
                id=ticket_id,
                ticket_number=ticket.get("ticket_number"),
                title=ticket.get("title"),
                status=status,
                is_closed=is_closed,
                status_text=status_text,
                priority=ticket.get("priority"),
                create_date=ticket.get("create_date"),
                completed_date=ticket.get("completed_date"),
                resolution=ticket.get("resolution") if is_closed else None,
                summary=summary_text,
                notes_count=notes_count,
                time_entries_count=time_entries_count,
                total_hours=round(total_hours, 2),
                company_name=company_name,
                assigned_to=assigned_to,
                created_by=created_by,
                closed_by=closed_by,
                time_to_resolution=time_to_resolution
            ))

        print(f"✓ Processed {len(summaries)} tickets ({cached_count} cached, {generated_count} generated)")

        return SummariesListResponse(
            summaries=summaries,
            total_count=len(summaries),
            generated_count=generated_count,
            cached_count=cached_count
        )

    except Exception as e:
        print(f"Summaries list error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get summaries: {str(e)}"
        )


async def generate_ticket_summary(db: DatabaseService, ticket: Dict) -> str:
    """
    Generate AI summary for a ticket including notes and time entries

    Args:
        db: Database service
        ticket: Ticket data dict

    Returns:
        AI-generated summary text
    """
    ticket_id = ticket["id"]
    ticket_number = ticket.get("ticket_number", "N/A")

    # Fetch all notes for this ticket
    notes_result = db.client.table("ticket_notes")\
        .select("*")\
        .eq("ticket_id", ticket_id)\
        .order("create_date_time", desc=False)\
        .execute()
    notes = notes_result.data or []

    # Fetch all time entries for this ticket
    time_entries_result = db.client.table("time_entries")\
        .select("*")\
        .eq("ticket_id", ticket_id)\
        .order("date_worked", desc=False)\
        .execute()
    time_entries = time_entries_result.data or []

    # Prepare notes text
    notes_text = []
    for note in notes:
        note_title = note.get("title", "")
        note_desc = note.get("description", "")
        note_date = note.get("create_date_time", "")
        notes_text.append(f"[{note_date}] {note_title}: {note_desc}")

    notes_summary = "\n".join(notes_text) if notes_text else "No notes available"

    # Prepare time entries text
    time_text = []
    total_hours = 0
    for entry in time_entries:
        hours = float(entry.get("hours_worked", 0) or 0)
        total_hours += hours
        date_worked = entry.get("date_worked", "")
        summary_notes = entry.get("summary_notes", "")
        time_text.append(f"[{date_worked}] {hours}h - {summary_notes}")

    time_summary = "\n".join(time_text) if time_text else "No time entries recorded"

    # Determine status
    status = ticket.get("status")
    is_closed = status == 5
    status_text = "CLOSED" if is_closed else "OPEN"
    resolution = ticket.get("resolution", "")

    # Create prompt for OpenAI
    system_prompt = """You are an IT support analyst creating concise ticket summaries.

Your summary should include:
1. **Status**: Clearly state if the ticket is OPEN or CLOSED
2. **Notes Summary**: Summarize what was discussed/reported (from ticket notes)
3. **Work Done**: Summarize the actual work performed (from time entries)
4. **Resolution**: If closed, include the final resolution
5. **Outcome**: Brief conclusion about the ticket

Format your response in clear sections with emojis:
- 📋 STATUS:
- 📝 NOTES SUMMARY:
- ⏱️ WORK DONE:
- ✅ RESOLUTION: (only if closed)
- 📊 OUTCOME:

Keep it concise but informative. Focus on WHAT happened and WHAT was done."""

    user_prompt = f"""Create a summary for this ticket:

**Ticket:** {ticket_number}
**Title:** {ticket.get('title', 'No title')}
**Status:** {status_text}
**Priority:** {ticket.get('priority', 'Unknown')}
**Created:** {ticket.get('create_date', 'N/A')}
**Description:** {ticket.get('description', 'No description')}

**NOTES FROM AUTOTASK:**
{notes_summary}

**TIME ENTRIES (Total: {total_hours}h):**
{time_summary}

**RESOLUTION (if closed):**
{resolution if is_closed else "Ticket is still open"}

Generate a concise summary of this ticket."""

    try:
        # Call OpenAI
        response = await openai_client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.5,
            max_tokens=800
        )

        summary = response.choices[0].message.content
        return summary

    except Exception as e:
        print(f"OpenAI summary generation error for ticket {ticket_id}: {str(e)}")
        # Return a basic summary if AI fails
        return f"""📋 STATUS: {status_text}

📝 NOTES: {len(notes)} notes available
⏱️ WORK DONE: {total_hours}h across {len(time_entries)} time entries
📊 OUTCOME: Summary generation failed - view ticket details for full information"""


@router.post("/regenerate/{ticket_id}")
async def regenerate_summary(
    ticket_id: int,
    db: DatabaseService = Depends(get_database_service)
):
    """
    Regenerate summary for a specific ticket

    Useful when ticket has been updated and summary needs to be refreshed
    """
    try:
        # Fetch ticket
        ticket_result = db.client.table("tickets")\
            .select("*")\
            .eq("id", ticket_id)\
            .limit(1)\
            .execute()

        if not ticket_result.data:
            raise HTTPException(status_code=404, detail=f"Ticket {ticket_id} not found")

        ticket = ticket_result.data[0]

        # Generate new summary
        summary = await generate_ticket_summary(db, ticket)

        # Save to database
        db.client.table("tickets")\
            .update({"summary": summary})\
            .eq("id", ticket_id)\
            .execute()

        return {
            "status": "success",
            "ticket_id": ticket_id,
            "summary": summary
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Regenerate summary error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to regenerate summary: {str(e)}"
        )
