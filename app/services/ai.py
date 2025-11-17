"""
COMPLETE FLEXIBLE AI Service - NO MORE HARDCODED PHRASES!
✅ All methods included
✅ Understands natural language queries
✅ "summary of corinthians ticket" → searches Corinthians company
"""
import json
import logging
import math
from typing import List, Dict, Optional, Any
from enum import IntEnum
from openai import AsyncOpenAI
from app.config import get_settings
from app.services.database import get_database_service
from app.models.schemas import ChatMessage

logger = logging.getLogger(__name__)
settings = get_settings()


# ==================== LOOKUP CACHE ====================
class LookupCache:
    """Cache for lookup table data - loads dynamically from DB"""
    
    def __init__(self, db_client):
        self.db_client = db_client
        self._cache = {}
        self._load_all()
    
    def _load_all(self):
        """Load all lookup tables into memory"""
        tables = [
            'ticket_status', 'ticket_priority', 'ticket_type', 
            'ticket_category', 'issue_type', 'subissue_type', 'ticket_queue'
        ]
        
        for table in tables:
            try:
                result = self.db_client.table(table).select("*").eq("is_active", True).execute()
                self._cache[table] = {item['id']: item for item in result.data}
                logger.info(f"✅ Loaded {len(result.data)} items from {table}")
            except Exception as e:
                logger.warning(f"⚠️ Could not load {table}: {e}")
                self._cache[table] = {}
    
    def get_label(self, table: str, id: int) -> str:
        """Get label for an ID"""
        item = self._cache.get(table, {}).get(id)
        return item['label'] if item else f"Unknown ({id})"
    
    def get_all(self, table: str) -> List[Dict]:
        """Get all items from a table"""
        return list(self._cache.get(table, {}).values())
    
    def is_open_status(self, status_id: int) -> bool:
        """Check if status is open (NOT 5 = Complete)"""
        return status_id != 5
    
    def get_id_by_label(self, table: str, label: str) -> Optional[int]:
        """Get ID by label (case-insensitive partial match)"""
        label_lower = label.lower()
        for id, item in self._cache.get(table, {}).items():
            if label_lower in item['label'].lower():
                return id
        return None
    
    def refresh(self):
        """Refresh cache"""
        self._load_all()


# ==================== CONSTANTS ====================
class TicketStatus(IntEnum):
    NEW = 1
    COMPLETE = 5
    WAITING_CUSTOMER = 7
    CUSTOMER_NOTE_ADDED = 8
    SCHEDULED = 10
    HELP_DESK = 12
    FOLLOW_UP = 13
    WAITING_MATERIALS = 14
    IN_PROGRESS = 15
    WAITING_VENDOR = 16
    WAITING_CUSTOMER_2 = 17
    CLIENT_NON_RESPONSIVE = 22
    PENDING_CUSTOMER_CONFIRM = 31
    WAITING_CUSTOMER_3 = 34
    REQUIRES_ONSITE_VISIT = 35
    CUSTOMER_REOPENED = 36
    STUCK = 37
    CONDITION_RESET = 38
    ASSIGNED = 39
    
    @classmethod
    def get_name(cls, status_code: int) -> str:
        names = {
            1: "New", 5: "Complete", 7: "Waiting Customer", 8: "Customer note added",
            10: "Scheduled", 12: "Help Desk", 13: "Follow Up", 14: "Waiting Materials",
            15: "In Progress", 16: "Waiting Vendor", 17: "Waiting Customer 2",
            22: "Client Non-Responsive", 31: "Pending Customer Confirm",
            34: "Waiting Customer 3", 35: "Requires OnSite Visit",
            36: "Customer Reopened", 37: "Stuck", 38: "Condition Reset", 39: "Assigned"
        }
        return names.get(status_code, f"Status {status_code}")
    
    @classmethod
    def is_open(cls, status_code: int) -> bool:
        return status_code != 5


class TicketPriority(IntEnum):
    HIGH = 1
    MEDIUM = 2
    LOW = 3
    CRITICAL = 4
    
    @classmethod
    def get_name(cls, priority_code: int) -> str:
        names = {1: "High", 2: "Medium", 3: "Low", 4: "Critical"}
        return names.get(priority_code, f"Priority {priority_code}")


class QueryLimits:
    MAX_DISPLAY = 1000
    DEFAULT_LIMIT = 100
    BATCH_SIZE = 1000
    TOP_COUNT = 5
    MAX_ISSUES_ANALYSIS = 500


# ==================== FLEXIBLE PROMPT ====================
def get_system_prompt() -> str:
    """FLEXIBLE AI system prompt - understands intent, not just keywords"""
    return """You are an AI assistant for a ticket management database.

DATABASE SCHEMA:
- tickets: Main ticket table with contact_name and assigned_resource_name columns
- companies: Customer companies
- contacts: Contact persons (linked to tickets via contact_id)
- resources: Technicians/employees (linked to tickets via assigned_resource_id)
- time_entries: Time tracking (hours_worked, ticket_id, resource_id, date_worked)

IMPORTANT: Tickets have BOTH IDs and NAMES:
- assigned_resource_id + assigned_resource_name (technician assigned to ticket)
- contact_id + contact_name (person who created/reported ticket)
- company_id + company_name (company the ticket belongs to)

OPEN vs CLOSED TICKETS:
- CLOSED: Status 5 (Complete) ONLY
- OPEN: Any status that is NOT 5 (Complete)

STATUSES: 1=New, 5=Complete, 7=Waiting Customer, 8=Customer note added, 10=Scheduled, 
12=Help Desk, 13=Follow Up, 14=Waiting Materials, 15=In Progress, 16=Waiting Vendor,
17=Waiting Customer 2, 22=Client Non-Responsive, 31=Pending Customer Confirm,
34=Waiting Customer 3, 35=Requires OnSite Visit, 36=Customer Reopened, 37=Stuck,
38=Condition Reset, 39=Assigned

PRIORITIES: 1=High, 2=Medium, 3=Low, 4=Critical

QUEUES: 5=Client Portal, 6=Post Sale, 8=Monitoring Alert, 14046773=Help Desk, 
29682858=Triage, 29682859=Escalation, 29682861=Alerts, 29682863=AHD,
29682866=Project Tasks, 29682867=Inputiv, 29682869=Co-Managed, 29682870=System Maintenance

=============================================================================
🔥 FLEXIBLE QUERY UNDERSTANDING - UNDERSTAND USER INTENT 🔥
=============================================================================

When the user mentions ANY company/person/tech name + wants info about their tickets:
→ Use analyze_common_issues with the appropriate filter

USER SAYS ANY OF THESE:
- "summary of [company] ticket(s)" → analyze_common_issues with company_name
- "tell me about [company] tickets" → analyze_common_issues with company_name
- "what's going on with [company]" → analyze_common_issues with company_name
- "show me [company] issues" → analyze_common_issues with company_name
- "what problems does [company] have" → analyze_common_issues with company_name
- "what's happening with [tech] tickets" → analyze_common_issues with assigned_resource_name
- "summarize [contact] requests" → analyze_common_issues with contact_name

EXAMPLES:
User: "summary of corinthians ticket"
→ {"action": "analyze_common_issues", "params": {"company_name": "corinthians"}}

User: "what's going on with acme corp"
→ {"action": "analyze_common_issues", "params": {"company_name": "acme corp"}}

User: "tell me about john's tickets"
→ {"action": "analyze_common_issues", "params": {"assigned_resource_name": "john"}}

User: "common issues"
→ {"action": "analyze_common_issues", "params": {"is_open": true}}

=============================================================================

COUNTING:
- "How many technicians?" → count_entities, entity="resources"
- "How many tickets?" → count_tickets
- "How many tickets in Help Desk?" → count_tickets, params: {"queue_id": 14046773}

TIME ANALYSIS:
- "Which ticket took most time?" → aggregate_time, group_by="ticket_id"
- "Total hours by tech?" → aggregate_time, group_by="resource_id"

AGGREGATION:
- "Who has most tickets?" → aggregate_tickets, group_by: ["assigned_resource_name"]
- "Tickets by queue" → aggregate_tickets, group_by: ["queue_id"]

Respond with JSON:
{
  "action": "count_entities|list_entities|count_tickets|search_tickets|aggregate_tickets|search_resources|search_contacts|semantic_search|aggregate_time|analyze_common_issues",
  "params": {"company_name": "<string>", "assigned_resource_name": "<string>", "contact_name": "<string>", "is_open": <boolean>, ...},
  "entity": "resources|contacts|companies"
}

CRITICAL: Be FLEXIBLE in understanding user intent!
"""


# ==================== FILTER BUILDER ====================
class QueryFilterBuilder:
    def __init__(self, db_client, lookups=None):
        self.db_client = db_client
        self.lookups = lookups
    
    def apply_filters(self, query, params: Dict) -> Any:
        if params.get("company_id"):
            query = query.eq("company_id", params["company_id"])
        elif params.get("company_name"):
            query = query.ilike("company_name", f"%{params['company_name']}%")
        
        if params.get("status") is not None:
            query = query.eq("status", params["status"])
        elif params.get("is_open") is not None:
            if params["is_open"]:
                query = query.neq("status", 5)
            else:
                query = query.eq("status", 5)
        
        if params.get("priority") is not None:
            query = query.eq("priority", params["priority"])
        if params.get("ticket_type") is not None:
            query = query.eq("ticket_type", params["ticket_type"])
        if params.get("ticket_category") is not None:
            query = query.eq("ticket_category", params["ticket_category"])
        if params.get("issue_type") is not None:
            query = query.eq("issue_type", params["issue_type"])
        if params.get("sub_issue_type") is not None:
            query = query.eq("sub_issue_type", params["sub_issue_type"])
        if params.get("queue_id") is not None:
            query = query.eq("queue_id", params["queue_id"])
        
        if params.get("assigned_resource_id"):
            query = query.eq("assigned_resource_id", params["assigned_resource_id"])
        elif params.get("assigned_resource_name"):
            query = query.ilike("assigned_resource_name", f"%{params['assigned_resource_name']}%")
        
        if params.get("contact_id"):
            query = query.eq("contact_id", params["contact_id"])
        elif params.get("contact_name"):
            query = query.ilike("contact_name", f"%{params['contact_name']}%")
        
        if params.get("start_date"):
            query = query.gte("create_date", params["start_date"])
        if params.get("end_date"):
            query = query.lte("create_date", params["end_date"])
        
        return query
    
    def describe_filters(self, params: Dict) -> str:
        filters = []
        
        if params.get("company_name"):
            filters.append(f"company: {params['company_name']}")
        elif params.get("company_id"):
            filters.append(f"company_id={params['company_id']}")
        
        if params.get("is_open") is not None:
            filters.append("open" if params["is_open"] else "closed")
        elif params.get("status") is not None:
            status_label = self.lookups.get_label('ticket_status', params['status']) if self.lookups else TicketStatus.get_name(params['status'])
            filters.append(f"status={status_label}")
        
        if params.get("assigned_resource_name"):
            filters.append(f"assigned to: {params['assigned_resource_name']}")
        if params.get("contact_name"):
            filters.append(f"contact: {params['contact_name']}")
        
        return f" ({', '.join(filters)})" if filters else ""


# ==================== RESULT ENHANCER ====================
class ResultEnhancer:
    def __init__(self, db_client, lookups=None):
        self.db_client = db_client
        self.lookups = lookups
    
    async def enhance(self, results: List[Dict], group_by: List[str]) -> List[Dict]:
        for result in results:
            if self.lookups:
                if "status" in result:
                    result["status_name"] = self.lookups.get_label('ticket_status', result["status"])
                    result["is_open"] = self.lookups.is_open_status(result["status"])
                if "priority" in result:
                    result["priority_name"] = self.lookups.get_label('ticket_priority', result["priority"])
                if "queue_id" in result:
                    result["queue_name"] = self.lookups.get_label('ticket_queue', result["queue_id"])
        
        if "company_id" in group_by and "company_name" not in group_by:
            await self._add_company_names(results)
        if "assigned_resource_id" in group_by and "assigned_resource_name" not in group_by:
            await self._add_resource_names(results)
        if "contact_id" in group_by and "contact_name" not in group_by:
            await self._add_contact_names(results)
        
        return results
    
    async def _add_company_names(self, results: List[Dict]):
        ids = list(set(r.get("company_id") for r in results if r.get("company_id")))
        if not ids:
            return
        try:
            data = self.db_client.table("companies").select("id, company_name").in_("id", ids).execute()
            name_map = {item["id"]: item["company_name"] for item in data.data}
            for result in results:
                if result.get("company_id"):
                    result["company_name"] = name_map.get(result["company_id"], "Unknown")
        except Exception as e:
            logger.error(f"Error fetching company names: {e}")
    
    async def _add_resource_names(self, results: List[Dict]):
        ids = list(set(r.get("assigned_resource_id") for r in results if r.get("assigned_resource_id")))
        if not ids:
            return
        try:
            data = self.db_client.table("resources").select("id, first_name, last_name").in_("id", ids).execute()
            name_map = {r["id"]: f"{r['first_name']} {r['last_name']}".strip() for r in data.data}
            for result in results:
                if result.get("assigned_resource_id"):
                    result["assigned_resource_name"] = name_map.get(result["assigned_resource_id"], "Unassigned")
        except Exception as e:
            logger.error(f"Error fetching resource names: {e}")
    
    async def _add_contact_names(self, results: List[Dict]):
        ids = list(set(r.get("contact_id") for r in results if r.get("contact_id")))
        if not ids:
            return
        try:
            data = self.db_client.table("contacts").select("id, first_name, last_name").in_("id", ids).execute()
            name_map = {c["id"]: f"{c['first_name']} {c['last_name']}".strip() for c in data.data}
            for result in results:
                if result.get("contact_id"):
                    result["contact_name"] = name_map.get(result["contact_id"], "Unknown")
        except Exception as e:
            logger.error(f"Error fetching contact names: {e}")


# ==================== SUMMARY GENERATOR ====================
class SummaryGenerator:
    def __init__(self, client: AsyncOpenAI, lookups=None):
        self.client = client
        self.lookups = lookups
    
    async def generate_aggregation_summary(self, results: List[Dict], group_by: List[str]) -> str:
        if not results:
            return "No tickets found."
        
        total = sum(r.get("count", 0) for r in results)
        top = results[:QueryLimits.TOP_COUNT]
        lines = [f"Total: {total:,} tickets across {len(results)} groups."]
        
        if "company_name" in group_by and top:
            lines.append("\nTop companies:")
            for i, r in enumerate(top, 1):
                lines.append(f"{i}. {r.get('company_name', 'Unknown')}: {r['count']:,}")
        elif "assigned_resource_name" in group_by and top:
            lines.append("\nTop technicians:")
            for i, r in enumerate(top, 1):
                lines.append(f"{i}. {r.get('assigned_resource_name', 'Unassigned')}: {r['count']:,}")
        elif "status" in group_by and top and "status_name" in top[0]:
            lines.append("\nBy status:")
            for r in top:
                open_tag = " (open)" if r.get("is_open") else " (closed)"
                lines.append(f"• {r.get('status_name', 'Unknown')}{open_tag}: {r['count']:,}")
        
        return "\n".join(lines)
    
    async def generate_ticket_summary(self, tickets: List[Dict], context: str) -> str:
        if not tickets:
            return context
        
        try:
            sample = []
            for t in tickets[:3]:
                status_name = self.lookups.get_label('ticket_status', t.get("status", 0)) if self.lookups else TicketStatus.get_name(t.get("status", 0))
                priority_name = self.lookups.get_label('ticket_priority', t.get("priority", 0)) if self.lookups else TicketPriority.get_name(t.get("priority", 0))
                
                sample.append({
                    "ticket_number": t.get("ticket_number"),
                    "title": t.get("title"),
                    "status": status_name,
                    "priority": priority_name,
                    "company": t.get("company_name"),
                    "assigned_to": t.get("assigned_resource_name"),
                    "created": t.get("create_date")
                })
            
            prompt = f"{context}\n\nSample:\n{json.dumps(sample, indent=2, default=str)}\n\nBrief summary (2-3 sentences):"
            
            response = await self.client.chat.completions.create(
                model=settings.openai_mini_model,
                messages=[
                    {"role": "system", "content": "Summarize ticket data concisely and helpfully."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.5,
                max_tokens=300
            )
            
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Summary error: {e}")
            return context


# ==================== MAIN SERVICE ====================
class AIService:
    def __init__(self):
        self.client = AsyncOpenAI(api_key=settings.openai_api_key)
        self.db_service = get_database_service()
        
        try:
            self.lookups = LookupCache(self.db_service.client)
        except Exception as e:
            logger.warning(f"⚠️ Lookup cache initialization failed: {e}")
            self.lookups = None
        
        self.filter_builder = QueryFilterBuilder(self.db_service.client, self.lookups)
        self.enhancer = ResultEnhancer(self.db_service.client, self.lookups)
        self.summary = SummaryGenerator(self.client, self.lookups)
        
        try:
            from app.services.embedding_service import get_embedding_service
            self.embedding_service = get_embedding_service()
            self.has_embeddings = True
        except:
            self.has_embeddings = False
    
    async def chat_with_tickets(
        self, 
        user_message: str, 
        conversation_history: List[ChatMessage],
        session_id: Optional[str] = None
    ) -> Dict:
        messages = [
            {"role": "system", "content": get_system_prompt()},
            *[{"role": m.role, "content": m.content} for m in conversation_history],
            {"role": "user", "content": f"JSON: {user_message}"}
        ]
        
        try:
            response = await self.client.chat.completions.create(
                model=settings.openai_model,
                messages=messages,
                temperature=0.3,
                response_format={"type": "json_object"}
            )
            
            ai_response = json.loads(response.choices[0].message.content)
            logger.info(f"AI: {json.dumps(ai_response)}")
            
            action = ai_response.get("action")
            if not action:
                raise ValueError("Missing action")
            
            return await self._execute(action, ai_response)
            
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
            return {"answer": "Error processing request.", "tickets": [], "ticket_count": 0}
    
    async def _execute(self, action: str, ai_response: Dict) -> Dict:
        handlers = {
            "count_tickets": self._count,
            "count_entities": self._count_entities,
            "list_entities": self._list_entities,
            "aggregate_tickets": self._aggregate,
            "aggregate_time": self._aggregate_time,
            "search_tickets": self._search,
            "search_resources": self._search_resources,
            "search_contacts": self._search_contacts,
            "search_companies": self._search_companies,
            "semantic_search": self._semantic_search,
            "analyze_common_issues": self._analyze_common_issues
        }
        
        handler = handlers.get(action)
        if not handler:
            return {"answer": f"Unknown action: {action}", "tickets": [], "ticket_count": 0}
        
        return await handler(ai_response)
    
    async def _count(self, ai_response: Dict) -> Dict:
        params = ai_response.get("params", {})
        
        query = self.db_service.client.table("tickets").select("id", count="exact")
        query = self.filter_builder.apply_filters(query, params)
        
        result = query.execute()
        count = result.count or 0
        desc = self.filter_builder.describe_filters(params)
        
        return {
            "answer": f"Found {count:,} tickets{desc}.",
            "tickets": [],
            "ticket_count": count,
            "filters": params
        }
    
    async def _count_entities(self, ai_response: Dict) -> Dict:
        entity = ai_response.get("entity", "resources")
        params = ai_response.get("params", {})
        
        entity_map = {
            "resources": "resources",
            "technicians": "resources",
            "contacts": "contacts",
            "companies": "companies"
        }
        
        table = entity_map.get(entity.lower(), entity)
        query = self.db_service.client.table(table).select("id", count="exact")
        
        if params.get("is_active") is not None:
            query = query.eq("is_active", params["is_active"])
        
        count = query.execute().count or 0
        entity_name = {"resources": "technicians", "contacts": "contacts", "companies": "companies"}.get(table, table)
        
        return {
            "answer": f"There are {count:,} {entity_name}.",
            "tickets": [],
            "ticket_count": count
        }
    
    async def _list_entities(self, ai_response: Dict) -> Dict:
        entity = ai_response.get("entity", "resources")
        params = ai_response.get("params", {})
        
        entity_map = {"resources": "resources", "contacts": "contacts", "companies": "companies"}
        table = entity_map.get(entity.lower(), entity)
        
        query = self.db_service.client.table(table).select("*")
        if params.get("is_active") is not None:
            query = query.eq("is_active", params["is_active"])
        
        items = query.limit(50).execute().data or []
        
        formatted = []
        for item in items[:20]:
            if table == "resources":
                name = f"{item.get('first_name', '')} {item.get('last_name', '')}".strip()
                status = "✓" if item.get('is_active') else "✗"
                formatted.append(f"{status} {name}")
            elif table == "contacts":
                name = f"{item.get('first_name', '')} {item.get('last_name', '')}".strip()
                formatted.append(f"• {name} - {item.get('email_address', 'No email')}")
            elif table == "companies":
                formatted.append(f"• {item.get('company_name', 'Unknown')}")
        
        answer = f"Found {len(items)} {table}:\n" + "\n".join(formatted[:20])
        
        return {"answer": answer, f"{table}": items, "ticket_count": len(items)}
    
    async def _aggregate(self, ai_response: Dict) -> Dict:
        params = ai_response.get("params", {})
        group_by = ai_response.get("aggregation", {}).get("group_by", [])
        
        if not group_by:
            return {"answer": "Specify group_by fields", "tickets": [], "ticket_count": 0}
        
        query = self.db_service.client.table("tickets").select(", ".join(["id"] + group_by))
        query = self.filter_builder.apply_filters(query, params)
        
        all_tickets = []
        offset = 0
        
        while True:
            batch = query.range(offset, offset + QueryLimits.BATCH_SIZE - 1).execute()
            if not batch.data:
                break
            all_tickets.extend(batch.data)
            if len(batch.data) < QueryLimits.BATCH_SIZE:
                break
            offset += QueryLimits.BATCH_SIZE
        
        agg = {}
        for ticket in all_tickets:
            key = tuple(ticket.get(f) for f in group_by)
            if key not in agg:
                agg[key] = {**{f: ticket.get(f) for f in group_by}, "count": 0}
            agg[key]["count"] += 1
        
        results = sorted(agg.values(), key=lambda x: x["count"], reverse=True)
        results = await self.enhancer.enhance(results, group_by)
        answer = await self.summary.generate_aggregation_summary(results, group_by)
        
        return {
            "answer": answer,
            "aggregation_results": results,
            "ticket_count": len(results),
            "total_tickets": len(all_tickets),
            "grouped_by": group_by
        }
    
    async def _aggregate_time(self, ai_response: Dict) -> Dict:
        time_agg = ai_response.get("time_aggregation", {})
        group_by = time_agg.get("group_by", "ticket_id")
        limit = time_agg.get("limit", 10)
        
        try:
            query = self.db_service.client.table("time_entries").select("*")
            
            all_entries = []
            offset = 0
            
            while True:
                batch = query.range(offset, offset + 999).execute()
                if not batch.data:
                    break
                all_entries.extend(batch.data)
                if len(batch.data) < 1000:
                    break
                offset += 1000
            
            agg = {}
            for entry in all_entries:
                key = entry.get(group_by)
                if not key:
                    continue
                
                if key not in agg:
                    agg[key] = {group_by: key, "total_hours": 0, "entry_count": 0}
                
                hours = entry.get("hours_worked") or 0
                try:
                    agg[key]["total_hours"] += float(hours)
                    agg[key]["entry_count"] += 1
                except:
                    continue
            
            results = sorted(agg.values(), key=lambda x: x["total_hours"], reverse=True)[:limit]
            
            if not results:
                return {"answer": "No time entries found", "results": [], "ticket_count": 0}
            
            if group_by == "ticket_id":
                ticket_ids = [r["ticket_id"] for r in results]
                tickets_data = self.db_service.client.table("tickets").select("id, ticket_number, title").in_("id", ticket_ids).execute().data or []
                ticket_map = {t["id"]: t for t in tickets_data}
                
                for result in results:
                    tid = result["ticket_id"]
                    if tid in ticket_map:
                        result["ticket_number"] = ticket_map[tid].get("ticket_number")
                        result["title"] = ticket_map[tid].get("title")
            
            lines = [f"Top {len(results)} by total hours:\n"]
            for i, r in enumerate(results, 1):
                hours = r["total_hours"]
                if group_by == "ticket_id":
                    ticket_num = r.get("ticket_number", "Unknown")
                    title = r.get("title", "No title")[:50]
                    lines.append(f"{i}. Ticket #{ticket_num}: {hours:.1f} hours - {title}")
                else:
                    lines.append(f"{i}. ID {r[group_by]}: {hours:.1f} hours")
            
            return {"answer": "\n".join(lines), "results": results, "ticket_count": len(results)}
            
        except Exception as e:
            logger.error(f"Time aggregation error: {e}", exc_info=True)
            return {"answer": f"Error: {str(e)}", "results": [], "ticket_count": 0}
    
    async def _search(self, ai_response: Dict) -> Dict:
        params = ai_response.get("params", {})
        
        count_query = self.db_service.client.table("tickets").select("id", count="exact")
        count_query = self.filter_builder.apply_filters(count_query, params)
        count = count_query.execute().count or 0
        
        if count > QueryLimits.MAX_DISPLAY:
            desc = self.filter_builder.describe_filters(params)
            return {
                "answer": f"Found {count:,} tickets{desc}. Too many! Add more filters.",
                "tickets": [],
                "ticket_count": count,
                "warning": "too_many_results"
            }
        
        limit = min(count, QueryLimits.DEFAULT_LIMIT)
        query = self.db_service.client.table("tickets").select("*")
        query = self.filter_builder.apply_filters(query, params)
        query = query.limit(limit)
        
        tickets = query.execute().data or []
        desc = self.filter_builder.describe_filters(params)
        context = f"Showing {len(tickets)} of {count:,} tickets{desc}"
        answer = await self.summary.generate_ticket_summary(tickets, context)
        
        return {"answer": answer, "tickets": tickets, "ticket_count": count, "showing": len(tickets)}
    
    async def _search_resources(self, ai_response: Dict) -> Dict:
        search_text = ai_response.get("search_text", "")
        if not search_text:
            return {"answer": "Please specify search text", "resources": [], "ticket_count": 0}
        
        query = self.db_service.client.table("resources").select("*")
        query = query.or_(
            f"first_name.ilike.%{search_text}%,"
            f"last_name.ilike.%{search_text}%,"
            f"email.ilike.%{search_text}%"
        )
        
        resources = query.limit(50).execute().data or []
        
        if not resources:
            return {"answer": f"No resources found matching '{search_text}'", "resources": [], "ticket_count": 0}
        
        formatted = []
        for r in resources[:10]:
            name = f"{r.get('first_name', '')} {r.get('last_name', '')}".strip()
            formatted.append(f"• {name} ({r.get('email', 'No email')})")
        
        answer = f"Found {len(resources)} resources matching '{search_text}':\n" + "\n".join(formatted)
        
        return {"answer": answer, "resources": resources, "ticket_count": len(resources)}
    
    async def _search_contacts(self, ai_response: Dict) -> Dict:
        search_text = ai_response.get("search_text", "")
        if not search_text:
            return {"answer": "Please specify search text", "contacts": [], "ticket_count": 0}
        
        query = self.db_service.client.table("contacts").select("*")
        query = query.or_(
            f"first_name.ilike.%{search_text}%,"
            f"last_name.ilike.%{search_text}%,"
            f"email_address.ilike.%{search_text}%"
        )
        
        contacts = query.limit(50).execute().data or []
        
        if not contacts:
            return {"answer": f"No contacts found matching '{search_text}'", "contacts": [], "ticket_count": 0}
        
        formatted = []
        for c in contacts[:10]:
            name = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
            formatted.append(f"• {name} ({c.get('email_address', 'No email')})")
        
        answer = f"Found {len(contacts)} contacts matching '{search_text}':\n" + "\n".join(formatted)
        
        return {"answer": answer, "contacts": contacts, "ticket_count": len(contacts)}
    
    async def _search_companies(self, ai_response: Dict) -> Dict:
        search_text = ai_response.get("search_text", "") or ai_response.get("params", {}).get("company_name", "")
        
        query = self.db_service.client.table("companies").select("*")
        if search_text:
            query = query.ilike("company_name", f"%{search_text}%")
        
        companies = query.limit(50).execute().data or []
        
        if not companies:
            return {"answer": "No companies found", "companies": [], "ticket_count": 0}
        
        formatted = [f"• {c.get('company_name', 'Unknown')}" for c in companies[:10]]
        answer = f"Found {len(companies)} companies:\n" + "\n".join(formatted)
        
        return {"answer": answer, "companies": companies, "ticket_count": len(companies)}
    
    async def _semantic_search(self, ai_response: Dict) -> Dict:
        if not self.has_embeddings:
            search_text = ai_response.get("search_params", {}).get("query", "")
            return await self._search_resources({"search_text": search_text})
        
        return {"answer": "Semantic search not fully implemented", "tickets": [], "ticket_count": 0}
    
    async def _analyze_common_issues(self, ai_response: Dict) -> Dict:
        """Analyze common issues in tickets - SAFE & FLEXIBLE"""
        params = ai_response.get("params", {})
    
        try:
            count_query = self.db_service.client.table("tickets").select("id", count="exact")
            count_query = self.filter_builder.apply_filters(count_query, params)
            total_count = count_query.execute().count or 0
        
            if total_count == 0:
                desc = self.filter_builder.describe_filters(params)
                return {"answer": f"No tickets found{desc}.", "tickets": [], "ticket_count": 0}
        
            limit = min(total_count, QueryLimits.MAX_ISSUES_ANALYSIS)
        
            query = self.db_service.client.table("tickets").select(
                "id, ticket_number, title, description, status, priority, company_name, "
                "assigned_resource_name, contact_name, create_date, queue_id"
            )
            query = self.filter_builder.apply_filters(query, params)
            query = query.order("create_date", desc=True).limit(limit)
        
            tickets = query.execute().data or []
        
            if not tickets:
                return {"answer": "No tickets found to analyze.", "tickets": [], "ticket_count": 0}

            def safe_text(text, fallback="No text provided", max_length=None):
                if text is None or text == "null" or not isinstance(text, str):
                    result = fallback
                else:
                    result = text.strip()
                    if not result:
                        result = fallback
                return result[:max_length] if max_length else result

            ticket_summaries = []
            for t in tickets:
                status_name = (
                    self.lookups.get_label('ticket_status', t.get("status", 0))
                    if self.lookups else TicketStatus.get_name(t.get("status", 0))
                )
                priority_name = (
                    self.lookups.get_label('ticket_priority', t.get("priority", 0))
                    if self.lookups else TicketPriority.get_name(t.get("priority", 0))
                )

                ticket_summaries.append({
                    "ticket_number": t.get("ticket_number", "Unknown"),
                    "title": safe_text(t.get("title"), "No title", 200),
                    "description": safe_text(t.get("description"), "No description", 500),
                    "status": status_name,
                    "priority": priority_name,
                    "company": t.get("company_name") or "Unknown",
                    "assigned_to": t.get("assigned_resource_name") or "Unassigned",
                    "created": str(t.get("create_date", "") or "")[:10]
                })
        
            desc = self.filter_builder.describe_filters(params)
            analysis_prompt = f"""You are a senior technical support analyst.
Analyze these {len(tickets)} support tickets{desc} and identify the most common real-world issues customers are facing.

Tickets:
{json.dumps(ticket_summaries, indent=2, default=str)}

Provide a clear, actionable report with:
1. Top 5 most frequent issues (based on titles & descriptions)
2. Any patterns (e.g. specific software, hardware, user error, network, etc.)
3. Affected companies or queues (if any stand out)
4. Recommendations to reduce these tickets

Be specific, practical, and focus on root causes."""

            response = await self.client.chat.completions.create(
                model=settings.openai_model,
                messages=[
                    {"role": "system", "content": "You are an expert support analyst who finds patterns in ticket data."},
                    {"role": "user", "content": analysis_prompt}
                ],
                temperature=0.4,
                max_tokens=2000
            )
        
            analysis = response.choices[0].message.content.strip()
        
            context_info = f"\n\n---\nAnalysis based on {len(tickets):,} recent ticket(s)"
            if len(tickets) < total_count:
                context_info += f" sampled from {total_count:,} total matching tickets"
            context_info += desc

            return {
                "answer": analysis + context_info,
                "tickets": tickets[:20],
                "ticket_count": total_count,
                "analyzed_count": len(tickets)
            }
        
        except Exception as e:
            logger.error(f"Analysis error: {e}", exc_info=True)
            return {"answer": f"Error: {str(e)}", "tickets": [], "ticket_count": 0}
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        try:
            if len(vec1) != len(vec2):
                return 0.0
            dot = sum(float(a) * float(b) for a, b in zip(vec1, vec2))
            mag1 = math.sqrt(sum(float(a) * float(a) for a in vec1))
            mag2 = math.sqrt(sum(float(b) * float(b) for b in vec2))
            return dot / (mag1 * mag2) if mag1 and mag2 else 0.0
        except:
            return 0.0


def get_ai_service() -> AIService:
    return AIService()