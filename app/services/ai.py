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


# ==================== NEW: LOOKUP CACHE ====================
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


# ==================== CONSTANTS (NOW USING LOOKUP CACHE DYNAMICALLY) ====================
class TicketStatus(IntEnum):
    """Ticket status codes - these are examples, actual values loaded from DB"""
    NEW = 1
    COMPLETE = 5  # CLOSED
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
        """Fallback status names (will be overridden by lookup cache)"""
        names = {
            1: "New",
            5: "Complete",
            7: "Waiting Customer",
            8: "Customer note added",
            10: "Scheduled",
            12: "Help Desk",
            13: "Follow Up",
            14: "Waiting Materials",
            15: "In Progress",
            16: "Waiting Vendor",
            17: "Waiting Customer 2",
            22: "Client Non-Responsive",
            31: "Pending Customer Confirm",
            34: "Waiting Customer 3",
            35: "Requires OnSite Visit",
            36: "Customer Reopened",
            37: "Stuck",
            38: "Condition Reset",
            39: "Assigned"
        }
        return names.get(status_code, f"Status {status_code}")
    
    @classmethod
    def is_open(cls, status_code: int) -> bool:
        """Status NOT 5 (Complete) = OPEN"""
        return status_code != 5


class TicketPriority(IntEnum):
    """Ticket priority codes"""
    HIGH = 1
    MEDIUM = 2
    LOW = 3
    CRITICAL = 4
    
    @classmethod
    def get_name(cls, priority_code: int) -> str:
        names = {1: "High", 2: "Medium", 3: "Low", 4: "Critical"}
        return names.get(priority_code, f"Priority {priority_code}")


class QueryLimits:
    """Query limits"""
    MAX_DISPLAY = 1000
    DEFAULT_LIMIT = 100
    BATCH_SIZE = 1000
    TOP_COUNT = 5
    MAX_ISSUES_ANALYSIS = 500  # NEW: Max tickets to analyze for common issues


# ==================== PROMPTS ====================
def get_system_prompt() -> str:
    """AI system prompt with full schema knowledge"""
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
- status (ID), priority (ID), ticket_type (ID), ticket_category (ID), issue_type (ID), sub_issue_type (ID), queue_id (ID)

OPEN vs CLOSED TICKETS:
- CLOSED: Status 5 (Complete) ONLY
- OPEN: Any status that is NOT 5 (Complete)

STATUSES (IMPORTANT - USE THESE IDs):
1=New, 5=Complete, 7=Waiting Customer, 8=Customer note added, 10=Scheduled, 
12=Help Desk, 13=Follow Up, 14=Waiting Materials, 15=In Progress, 16=Waiting Vendor,
17=Waiting Customer 2, 22=Client Non-Responsive, 31=Pending Customer Confirm,
34=Waiting Customer 3, 35=Requires OnSite Visit, 36=Customer Reopened, 37=Stuck,
38=Condition Reset, 39=Assigned

PRIORITIES: 1=High, 2=Medium, 3=Low, 4=Critical

QUEUES (IMPORTANT - USE THESE IDs):
5=Client Portal, 6=Post Sale, 8=Monitoring Alert, 14046773=Help Desk, 
29682858=Triage, 29682859=Escalation, 29682861=Alerts, 29682863=AHD,
29682866=Project Tasks, 29682867=Inputiv, 29682869=Co-Managed, 29682870=System Maintenance

TICKET TYPES: 1=Service Request, 2=Incident, 3=Problem, 4=Change Request, 5=Alert

CATEGORIES: 2=AEM Alert, 3=Standard, 4=Datto Alert, 5=RMA, 100=Employee On-boarding, 101=Co-Managed

CRITICAL RULES FOR COUNTING:
- "How many technicians/resources?" → use count_entities with entity="resources"
- "How many contacts?" → use count_entities with entity="contacts"
- "How many companies?" → use count_entities with entity="companies"
- "How many tickets?" → use count_tickets
- "How many tickets in Help Desk queue?" → use count_tickets with params: {"queue_id": 14046773}
- "How many tickets with status Help Desk?" → use count_tickets with params: {"status": 12}
- "How many scheduled tickets?" → use count_tickets with params: {"status": 10}
- "Find/search technician NAME" → use search_resources with search_text
- "List all technicians" → use list_entities with entity="resources"

TIME & ANALYSIS QUERIES (use aggregate_time):
- "Which ticket took most time?" → use aggregate_time with group_by="ticket_id"
- "Which ticket has maximum hours?" → use aggregate_time with group_by="ticket_id"
- "Most time-consuming tickets?" → use aggregate_time with group_by="ticket_id"
- "Total hours by technician?" → use aggregate_time with group_by="resource_id"
- "Time spent per company?" → use aggregate_time with group_by="company_id"

CRITICAL - NAME-SPECIFIC TIME QUERIES (ALWAYS filter by name when a person is mentioned):
- "How much time did [NAME] work?" → aggregate_time with group_by="resource_id", resource_name="[NAME]"
- "Hours worked by [NAME]?" → aggregate_time with group_by="resource_id", resource_name="[NAME]"
- "[NAME]'s time on tickets" → aggregate_time with group_by="ticket_id", resource_name="[NAME]"
- "Time [NAME] spent" → aggregate_time with group_by="ticket_id", resource_name="[NAME]"
- "What did [NAME] work on?" → aggregate_time with group_by="ticket_id", resource_name="[NAME]"

When ANY person's name appears in a time query (e.g., "Ashish", "Alex", "Uttam", "JC"), 
you MUST include resource_name="[that name]" in time_aggregation params. Never ignore the name!

TICKET ANALYSIS & CONTENT QUERIES (FLEXIBLE - understands many ways to ask):
- "What are common issues?" → use analyze_common_issues
- "Most frequent problems?" → use analyze_common_issues
- "What issues does company X have?" → use analyze_common_issues with company_name filter
- "Summary of [company] tickets" → use analyze_common_issues with company_name filter
- "Tell me about [company] tickets" → use analyze_common_issues with company_name filter
- "What's going on with [company/tech/contact]?" → use analyze_common_issues with appropriate filter
- "Give me [company] summary" → use analyze_common_issues with company_name filter
- "Common problems in open tickets?" → use analyze_common_issues with is_open: true
- "What are people complaining about?" → use analyze_common_issues
- "Analyze ticket descriptions" → use analyze_common_issues

Respond with JSON:
{
  "action": "count_entities" | "list_entities" | "count_tickets" | "search_tickets" | "aggregate_tickets" | "search_resources" | "search_contacts" | "semantic_search" | "aggregate_time" | "analyze_common_issues",
  "params": {
    "company_id": <number>,
    "company_name": "<string>",
    "status": <number>,
    "priority": <number>,
    "ticket_type": <number>,
    "ticket_category": <number>,
    "issue_type": <number>,
    "sub_issue_type": <number>,
    "queue_id": <number>,
    "is_open": <boolean>,
    "assigned_resource_id": <number>,
    "assigned_resource_name": "<string>",
    "contact_id": <number>,
    "contact_name": "<string>",
    "start_date": "YYYY-MM-DD",
    "end_date": "YYYY-MM-DD",
    "is_active": <boolean>
  }, 

  "aggregation": {
    "group_by": ["status", "priority", "queue_id", "ticket_type", "company_name", "assigned_resource_name", "contact_name"]
  },
  "time_aggregation": {
    "group_by": "ticket_id" | "resource_id" | "company_id",
    "limit": 10,
    "resource_name": "<string - filter by technician name>",
    "company_name": "<string - filter by company name>"
  },
  "search_text": "<string>",
  "entity": "resources" | "contacts" | "companies",
  "search_params": {
    "query": "<text>",
    "tables": ["tickets", "resources", "contacts"],
    "limit": 10
  }
}

EXAMPLES:
COUNTING ENTITIES:
- "How many technicians?" → {"action": "count_entities", "entity": "resources"}
- "How many contacts?" → {"action": "count_entities", "entity": "contacts"}

COUNTING TICKETS BY QUEUE/STATUS:
- "How many tickets in Help Desk queue?" → {"action": "count_tickets", "params": {"queue_id": 14046773}}
- "How many tickets in Triage?" → {"action": "count_tickets", "params": {"queue_id": 29682858}}
- "How many tickets with status Help Desk?" → {"action": "count_tickets", "params": {"status": 12}}
- "How many scheduled tickets?" → {"action": "count_tickets", "params": {"status": 10}}
- "How many stuck tickets?" → {"action": "count_tickets", "params": {"status": 37}}

TIME QUERIES WITH NAMES:
- "How much time did Ashish work?" → {"action": "aggregate_time", "time_aggregation": {"group_by": "resource_id", "resource_name": "Ashish"}}
- "What tickets did Alex work on?" → {"action": "aggregate_time", "time_aggregation": {"group_by": "ticket_id", "resource_name": "Alex"}}
- "Hours JC spent last month?" → {"action": "aggregate_time", "time_aggregation": {"group_by": "ticket_id", "resource_name": "JC"}}
- "Uttam's time entries" → {"action": "aggregate_time", "time_aggregation": {"group_by": "ticket_id", "resource_name": "Uttam"}}

AGGREGATION QUERIES:
- "How many open tickets?" → {"action": "count_tickets", "params": {"is_open": true}}
- "Who has the most tickets?" → {"action": "aggregate_tickets", "aggregation": {"group_by": ["assigned_resource_name"]}}
- "Which technician solved the most tickets?" → {"action": "aggregate_tickets", "params": {"status": 5}, "aggregation": {"group_by": ["assigned_resource_name"]}}
- "Which tech closed most tickets?" → {"action": "aggregate_tickets", "params": {"status": 5}, "aggregation": {"group_by": ["assigned_resource_name"]}}
- "Tickets by queue" → {"action": "aggregate_tickets", "aggregation": {"group_by": ["queue_id"]}}
- "Tickets by status" → {"action": "aggregate_tickets", "aggregation": {"group_by": ["status"]}}

TICKETS:
- "How many open tickets?" → {"action": "count_tickets", "params": {"is_open": true}}
- "Who has the most tickets?" → {"action": "aggregate_tickets", "aggregation": {"group_by": ["assigned_resource_name"]}}
- "Tickets by queue" → {"action": "aggregate_tickets", "aggregation": {"group_by": ["queue_id"]}}
- "Tickets by status" → {"action": "aggregate_tickets", "aggregation": {"group_by": ["status"]}}
"""


# ==================== FILTER BUILDER ====================
class QueryFilterBuilder:
    """Builds database query filters"""
    
    def __init__(self, db_client, lookups=None):
        self.db_client = db_client
        self.lookups = lookups
    
    def apply_filters(self, query, params: Dict) -> Any:
        """Apply filters to query - includes name-based filters"""
        
        # Company filters
        if params.get("company_id"):
            query = query.eq("company_id", params["company_id"])
        elif params.get("company_name"):
            query = query.ilike("company_name", f"%{params['company_name']}%")
        
        # Status filters
        if params.get("status") is not None:
            query = query.eq("status", params["status"])
        elif params.get("is_open") is not None:
            if params["is_open"]:
                query = query.neq("status", 5)  # Open = NOT Complete
            else:
                query = query.eq("status", 5)  # Closed = Complete
        
        # Priority filter
        if params.get("priority") is not None:
            query = query.eq("priority", params["priority"])
        
        # Ticket type filter
        if params.get("ticket_type") is not None:
            query = query.eq("ticket_type", params["ticket_type"])
        
        # Ticket category filter
        if params.get("ticket_category") is not None:
            query = query.eq("ticket_category", params["ticket_category"])
        
        # Issue type filter
        if params.get("issue_type") is not None:
            query = query.eq("issue_type", params["issue_type"])
        
        # Sub-issue type filter
        if params.get("sub_issue_type") is not None:
            query = query.eq("sub_issue_type", params["sub_issue_type"])
        
        # Queue filter
        if params.get("queue_id") is not None:
            query = query.eq("queue_id", params["queue_id"])
        
        # Resource (technician) filters - by ID or NAME
        if params.get("assigned_resource_id"):
            query = query.eq("assigned_resource_id", params["assigned_resource_id"])
        elif params.get("assigned_resource_name"):
            query = query.ilike("assigned_resource_name", f"%{params['assigned_resource_name']}%")
        
        # Contact filters - by ID or NAME
        if params.get("contact_id"):
            query = query.eq("contact_id", params["contact_id"])
        elif params.get("contact_name"):
            query = query.ilike("contact_name", f"%{params['contact_name']}%")
        
        # Date range filters
        if params.get("start_date"):
            query = query.gte("create_date", params["start_date"])
        
        if params.get("end_date"):
            query = query.lte("create_date", params["end_date"])
        
        return query
    
    def describe_filters(self, params: Dict) -> str:
        """Human-readable filter description"""
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
        
        if params.get("priority") is not None:
            priority_label = self.lookups.get_label('ticket_priority', params['priority']) if self.lookups else TicketPriority.get_name(params['priority'])
            filters.append(f"priority={priority_label}")
        
        if params.get("queue_id") is not None:
            queue_label = self.lookups.get_label('ticket_queue', params['queue_id']) if self.lookups else f"Queue {params['queue_id']}"
            filters.append(f"queue={queue_label}")
        
        if params.get("ticket_type") is not None:
            type_label = self.lookups.get_label('ticket_type', params['ticket_type']) if self.lookups else f"Type {params['ticket_type']}"
            filters.append(f"type={type_label}")
        
        if params.get("assigned_resource_name"):
            filters.append(f"assigned to: {params['assigned_resource_name']}")
        elif params.get("assigned_resource_id"):
            filters.append(f"assigned_resource_id={params['assigned_resource_id']}")
        
        if params.get("contact_name"):
            filters.append(f"contact: {params['contact_name']}")
        elif params.get("contact_id"):
            filters.append(f"contact_id={params['contact_id']}")
        
        if params.get("start_date"):
            filters.append(f"after {params['start_date']}")
        
        if params.get("end_date"):
            filters.append(f"before {params['end_date']}")
        
        return f" ({', '.join(filters)})" if filters else ""


# ==================== RESULT ENHANCER ====================
class ResultEnhancer:
    """Enhances aggregation results with additional data"""
    
    def __init__(self, db_client, lookups=None):
        self.db_client = db_client
        self.lookups = lookups
    
    async def enhance(self, results: List[Dict], group_by: List[str]) -> List[Dict]:
        """Enhance results with labels from lookup tables"""
        
        # Add labels from lookup tables
        for result in results:
            if self.lookups:
                # Add status info
                if "status" in result:
                    result["status_name"] = self.lookups.get_label('ticket_status', result["status"])
                    result["is_open"] = self.lookups.is_open_status(result["status"])
                
                # Add priority info
                if "priority" in result:
                    result["priority_name"] = self.lookups.get_label('ticket_priority', result["priority"])
                
                # Add ticket type info
                if "ticket_type" in result:
                    result["type_name"] = self.lookups.get_label('ticket_type', result["ticket_type"])
                
                # Add category info
                if "ticket_category" in result:
                    result["category_name"] = self.lookups.get_label('ticket_category', result["ticket_category"])
                
                # Add issue type info
                if "issue_type" in result:
                    result["issue_type_name"] = self.lookups.get_label('issue_type', result["issue_type"])
                
                # Add sub-issue type info
                if "sub_issue_type" in result:
                    result["sub_issue_type_name"] = self.lookups.get_label('subissue_type', result["sub_issue_type"])
                
                # Add queue info
                if "queue_id" in result:
                    result["queue_name"] = self.lookups.get_label('ticket_queue', result["queue_id"])
            else:
                # Fallback to hardcoded names
                if "status" in result:
                    result["status_name"] = TicketStatus.get_name(result["status"])
                    result["is_open"] = TicketStatus.is_open(result["status"])
                if "priority" in result:
                    result["priority_name"] = TicketPriority.get_name(result["priority"])
        
        # For company_id grouping, fetch company names if not already present
        if "company_id" in group_by and "company_name" not in group_by:
            await self._add_company_names(results)
        
        # For resource_id grouping, fetch names if not already present
        if "assigned_resource_id" in group_by and "assigned_resource_name" not in group_by:
            await self._add_resource_names(results)
        
        # For contact_id grouping, fetch names if not already present
        if "contact_id" in group_by and "contact_name" not in group_by:
            await self._add_contact_names(results)
        
        return results
    
    async def _add_company_names(self, results: List[Dict]):
        """Add company names from companies table"""
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
        """Add resource names from resources table"""
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
        """Add contact names from contacts table"""
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
    """Generates summaries"""
    
    def __init__(self, client: AsyncOpenAI, lookups=None):
        self.client = client
        self.lookups = lookups
    
    async def generate_aggregation_summary(self, results: List[Dict], group_by: List[str]) -> str:
        """Generate aggregation summary with name support"""
        if not results:
            return "No tickets found."
        
        total = sum(r.get("count", 0) for r in results)
        top = results[:QueryLimits.TOP_COUNT]
        
        lines = [f"Total: {total:,} tickets across {len(results)} groups."]
        
        # Group by company name (direct from tickets table)
        if "company_name" in group_by and top:
            lines.append("\nTop companies:")
            for i, r in enumerate(top, 1):
                lines.append(f"{i}. {r.get('company_name', 'Unknown')}: {r['count']:,}")
        
        # Group by company ID (needs lookup)
        elif "company_id" in group_by and top and "company_name" in top[0]:
            lines.append("\nTop companies:")
            for i, r in enumerate(top, 1):
                lines.append(f"{i}. {r.get('company_name', 'Unknown')}: {r['count']:,}")
        
        # Group by technician name (direct from tickets table)
        elif "assigned_resource_name" in group_by and top:
            lines.append("\nTop technicians:")
            for i, r in enumerate(top, 1):
                name = r.get('assigned_resource_name', 'Unassigned')
                lines.append(f"{i}. {name}: {r['count']:,}")
        
        # Group by technician ID (needs lookup)
        elif "assigned_resource_id" in group_by and top and "assigned_resource_name" in top[0]:
            lines.append("\nTop technicians:") 
            for i, r in enumerate(top, 1):
                lines.append(f"{i}. {r.get('assigned_resource_name', 'Unassigned')}: {r['count']:,}")
        
        # Group by contact name (direct from tickets table)
        elif "contact_name" in group_by and top:
            lines.append("\nTop contacts:")
            for i, r in enumerate(top, 1):
                name = r.get('contact_name', 'Unknown')
                lines.append(f"{i}. {name}: {r['count']:,}")
        
        # Group by contact ID (needs lookup)
        elif "contact_id" in group_by and top and "contact_name" in top[0]:
            lines.append("\nTop contacts:")
            for i, r in enumerate(top, 1):
                lines.append(f"{i}. {r.get('contact_name', 'Unknown')}: {r['count']:,}")
        
        # Group by status
        elif "status" in group_by and top and "status_name" in top[0]:
            lines.append("\nBy status:")
            for r in top:
                status = r.get('status_name', 'Unknown')
                open_tag = " (open)" if r.get("is_open") else " (closed)"
                lines.append(f"• {status}{open_tag}: {r['count']:,}")
        
        # Group by priority
        elif "priority" in group_by and top and "priority_name" in top[0]:
            lines.append("\nBy priority:")
            for r in top:
                lines.append(f"• {r.get('priority_name', 'Unknown')}: {r['count']:,}")
        
        # Group by queue
        elif "queue_id" in group_by and top and "queue_name" in top[0]:
            lines.append("\nBy queue:")
            for r in top:
                lines.append(f"• {r.get('queue_name', 'Unknown')}: {r['count']:,}")
        
        return "\n".join(lines)
    
    async def generate_ticket_summary(self, tickets: List[Dict], context: str) -> str:
        """Generate AI summary"""
        if not tickets:
            return context
        
        try:
            # Include names in the sample for better summaries
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
                    "contact": t.get("contact_name"),
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
                max_tokens=300  # Increased from 150
            )
            
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Summary error: {e}")
            return context


# ==================== MAIN SERVICE ====================
class AIService:
    """Improved AI service with full resource/contact support + vector search + lookup tables + common issues analysis"""
    
    def __init__(self):
        self.client = AsyncOpenAI(api_key=settings.openai_api_key)
        self.db_service = get_database_service()
        
        # NEW: Initialize lookup cache
        try:
            self.lookups = LookupCache(self.db_service.client)
        except Exception as e:
            logger.warning(f"⚠️ Lookup cache initialization failed: {e}")
            self.lookups = None
        
        self.filter_builder = QueryFilterBuilder(self.db_service.client, self.lookups)
        self.enhancer = ResultEnhancer(self.db_service.client, self.lookups)
        self.summary = SummaryGenerator(self.client, self.lookups)
        
        # Try to load embedding service for vector search
        try:
            from app.services.embedding_service import get_embedding_service
            self.embedding_service = get_embedding_service()
            self.has_embeddings = True
            logger.info("✅ Vector search enabled")
        except Exception as e:
            logger.warning(f"⚠️ Vector search not available: {e}")
            self.has_embeddings = False
    
    async def chat_with_tickets(
        self, 
        user_message: str, 
        conversation_history: List[ChatMessage],
        session_id: Optional[str] = None
    ) -> Dict:
        """Process user message"""
        
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
        """Execute action"""
        handlers = {
            "count_tickets": self._count,
            "count_entities": self._count_entities,
            "list_entities": self._list_entities,
            "aggregate_tickets": self._aggregate,
            "aggregate_time": self._aggregate_time,
            "search_tickets": self._search,
            "search_by_name": self._search,
            "search_resources": self._search_resources,
            "search_contacts": self._search_contacts,
            "search_companies": self._search_companies,
            "semantic_search": self._semantic_search,
            "analyze_common_issues": self._analyze_common_issues  # NEW
        }
        
        handler = handlers.get(action)
        if not handler:
            return {"answer": f"Unknown action: {action}", "tickets": [], "ticket_count": 0}
        
        return await handler(ai_response)
    
    async def _count(self, ai_response: Dict) -> Dict:
        """Count tickets"""
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
        """Count resources/contacts/companies"""
        entity = ai_response.get("entity", "resources")
        params = ai_response.get("params", {})
        
        entity_map = {
            "resources": "resources",
            "technicians": "resources",
            "techs": "resources",
            "contacts": "contacts",
            "companies": "companies",
            "clients": "companies"
        }
        
        table = entity_map.get(entity.lower(), entity)
        
        query = self.db_service.client.table(table).select("id", count="exact")
        
        if params.get("is_active") is not None:
            query = query.eq("is_active", params["is_active"])
        
        count = query.execute().count or 0
        
        entity_names = {
            "resources": "technicians",
            "contacts": "contacts",
            "companies": "companies"
        }
        
        entity_name = entity_names.get(table, table)
        active_text = " active" if params.get("is_active") else ""
        
        return {
            "answer": f"There are {count:,}{active_text} {entity_name}.",
            "tickets": [],
            "ticket_count": count
        }
    
    async def _list_entities(self, ai_response: Dict) -> Dict:
        """List resources/contacts/companies"""
        entity = ai_response.get("entity", "resources")
        params = ai_response.get("params", {})
        
        entity_map = {
            "resources": "resources",
            "technicians": "resources",
            "contacts": "contacts",
            "companies": "companies"
        }
        
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
                formatted.append(f"{status} {name} - {item.get('title', 'N/A')}")
            elif table == "contacts":
                name = f"{item.get('first_name', '')} {item.get('last_name', '')}".strip()
                status = "✓" if item.get('is_active') == 1 else "✗"
                formatted.append(f"{status} {name} - {item.get('email_address', 'No email')}")
            elif table == "companies":
                status = "✓" if item.get('is_active') else "✗"
                formatted.append(f"{status} {item.get('company_name', 'Unknown')}")
        
        answer = f"Found {len(items)} {table}:\n" + "\n".join(formatted[:20])
        
        return {
            "answer": answer,
            f"{table}": items,
            "ticket_count": len(items)
        }
    
    async def _aggregate(self, ai_response: Dict) -> Dict:
        """Aggregate tickets"""
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
        """Aggregate time entries with optional name filters"""
        time_agg = ai_response.get("time_aggregation", {})
        group_by = time_agg.get("group_by", "ticket_id")
        limit = time_agg.get("limit", 10)
        resource_name = time_agg.get("resource_name")
        company_name = time_agg.get("company_name")
        
        logger.info(f"⏱️ Aggregating time by {group_by}, resource_name={resource_name}")
        
        try:
            query = self.db_service.client.table("time_entries").select("*")
            
            # Filter by resource name if provided
            resource_ids = None
            matched_resource_name = None
            if resource_name:
                resource_query = self.db_service.client.table("resources").select("id, first_name, last_name")
                resource_query = resource_query.or_(
                    f"first_name.ilike.%{resource_name}%,"
                    f"last_name.ilike.%{resource_name}%"
                )
                resource_result = resource_query.execute()
                
                if not resource_result.data:
                    return {
                        "answer": f"No technician found matching '{resource_name}'",
                        "results": [],
                        "ticket_count": 0
                    }
                
                resource_ids = [r["id"] for r in resource_result.data]
                matched_resource_name = f"{resource_result.data[0]['first_name']} {resource_result.data[0]['last_name']}".strip()
                query = query.in_("resource_id", resource_ids)
                logger.info(f"🔍 Filtering for {matched_resource_name} (IDs: {resource_ids})")
            
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
            
            logger.info(f"📊 Processing {len(all_entries)} time entries")
            
            if not all_entries:
                if resource_name:
                    return {
                        "answer": f"No time entries found for '{resource_name}'",
                        "results": [],
                        "ticket_count": 0
                    }
                return {"answer": "No time entries found", "results": [], "ticket_count": 0}
            
            # Aggregate
            agg = {}
            total_hours = 0
            for entry in all_entries:
                key = entry.get(group_by)
                if not key:
                    continue
                
                if key not in agg:
                    agg[key] = {
                        group_by: key,
                        "total_hours": 0,
                        "entry_count": 0
                    }
                
                hours = entry.get("hours_worked") or 0
                try:
                    agg[key]["total_hours"] += float(hours)
                    agg[key]["entry_count"] += 1
                    total_hours += float(hours)
                except:
                    continue
            
            results = sorted(agg.values(), key=lambda x: x["total_hours"], reverse=True)[:limit]
            
            if not results:
                return {"answer": "No time entries found", "results": [], "ticket_count": 0}
            
            # Enrich with names
            if group_by == "ticket_id":
                ticket_ids = [r["ticket_id"] for r in results]
                tickets_data = self.db_service.client.table("tickets")\
                    .select("id, ticket_number, title")\
                    .in_("id", ticket_ids)\
                    .execute().data or []
                
                ticket_map = {t["id"]: t for t in tickets_data}
                
                for result in results:
                    tid = result["ticket_id"]
                    if tid in ticket_map:
                        result["ticket_number"] = ticket_map[tid].get("ticket_number")
                        result["title"] = ticket_map[tid].get("title")
            
            elif group_by == "resource_id":
                res_ids = [r["resource_id"] for r in results]
                resources_data = self.db_service.client.table("resources")\
                    .select("id, first_name, last_name")\
                    .in_("id", res_ids)\
                    .execute().data or []
                
                resource_map = {r["id"]: f"{r['first_name']} {r['last_name']}".strip() for r in resources_data}
                
                for result in results:
                    rid = result["resource_id"]
                    if rid in resource_map:
                        result["resource_name"] = resource_map[rid]
            
            # Build response
            lines = []
            
            # If searching for a specific person, show their summary first
            if resource_name and matched_resource_name:
                lines.append(f"📊 **{matched_resource_name}** - Time Summary:\n")
                lines.append(f"Total: {total_hours:.1f} hours across {len(all_entries)} time entries\n")
            
            if group_by == "ticket_id":
                lines.append(f"Top {len(results)} tickets by hours:\n")
                for i, r in enumerate(results, 1):
                    ticket_num = r.get("ticket_number", "Unknown")
                    title = r.get("title", "No title")[:50]
                    lines.append(f"{i}. Ticket #{ticket_num}: {r['total_hours']:.1f} hours - {title}")
            
            elif group_by == "resource_id":
                if not resource_name:
                    lines.append(f"Top {len(results)} technicians by hours:\n")
                for i, r in enumerate(results, 1):
                    name = r.get("resource_name", "Unknown")
                    lines.append(f"{i}. {name}: {r['total_hours']:.1f} hours ({r['entry_count']} entries)")
            
            else:
                lines.append(f"Top {len(results)} by hours:\n")
                for i, r in enumerate(results, 1):
                    lines.append(f"{i}. ID {r[group_by]}: {r['total_hours']:.1f} hours")
            
            return {
                "answer": "\n".join(lines),
                "results": results,
                "ticket_count": len(results),
                "total_entries": len(all_entries),
                "total_hours": total_hours
            }
            
        except Exception as e:
            logger.error(f"❌ Time aggregation error: {e}", exc_info=True)
            return {"answer": f"Error aggregating time: {str(e)}", "results": [], "ticket_count": 0}
    
    async def _search(self, ai_response: Dict) -> Dict:
        """Search tickets"""
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
        
        return {
            "answer": answer,
            "tickets": tickets,
            "ticket_count": count,
            "showing": len(tickets)
        }
    
    async def _search_resources(self, ai_response: Dict) -> Dict:
        """Search in resources table"""
        search_text = ai_response.get("search_text", "")
        if not search_text:
            return {"answer": "Please specify search text", "resources": [], "ticket_count": 0}
        
        query = self.db_service.client.table("resources").select("*")
        query = query.or_(
            f"first_name.ilike.%{search_text}%,"
            f"last_name.ilike.%{search_text}%,"
            f"email.ilike.%{search_text}%,"
            f"user_name.ilike.%{search_text}%"
        )
        
        resources = query.limit(50).execute().data or []
        
        if not resources:
            return {"answer": f"No resources found matching '{search_text}'", "resources": [], "ticket_count": 0}
        
        formatted = []
        for r in resources[:10]:
            name = f"{r.get('first_name', '')} {r.get('last_name', '')}".strip()
            formatted.append(f"• {name} ({r.get('email', 'No email')}) - {'Active' if r.get('is_active') else 'Inactive'}")
        
        answer = f"Found {len(resources)} resources matching '{search_text}':\n" + "\n".join(formatted)
        
        return {
            "answer": answer,
            "resources": resources,
            "ticket_count": len(resources)
        }
    
    async def _search_contacts(self, ai_response: Dict) -> Dict:
        """Search in contacts table"""
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
            email = c.get('email_address', 'No email')
            formatted.append(f"• {name} ({email}) - {'Active' if c.get('is_active') == 1 else 'Inactive'}")
        
        answer = f"Found {len(contacts)} contacts matching '{search_text}':\n" + "\n".join(formatted)
        
        return {
            "answer": answer,
            "contacts": contacts,
            "ticket_count": len(contacts)
        }
    
    async def _search_companies(self, ai_response: Dict) -> Dict:
        """Search companies"""
        search_text = ai_response.get("search_text", "") or ai_response.get("params", {}).get("company_name", "")
        
        query = self.db_service.client.table("companies").select("*")
        
        if search_text:
            query = query.ilike("company_name", f"%{search_text}%")
        
        companies = query.limit(50).execute().data or []
        
        if not companies:
            msg = f"No companies found matching '{search_text}'" if search_text else "No companies found"
            return {"answer": msg, "companies": [], "ticket_count": 0}
        
        formatted = []
        for c in companies[:10]:
            name = c.get('company_name', 'Unknown')
            formatted.append(f"• {name} - {'Active' if c.get('is_active') else 'Inactive'}")
        
        answer = f"Found {len(companies)} companies" + (f" matching '{search_text}'" if search_text else "") + ":\n" + "\n".join(formatted)
        
        return {
            "answer": answer,
            "companies": companies,
            "ticket_count": len(companies)
        }
    
    async def _semantic_search(self, ai_response: Dict) -> Dict:
        """Semantic search using vector embeddings"""
        
        if not self.has_embeddings:
            search_text = ai_response.get("search_params", {}).get("query", "")
            tables = ai_response.get("search_params", {}).get("tables", ["resources"])
            
            if "resources" in tables:
                return await self._search_resources({"search_text": search_text})
            elif "contacts" in tables:
                return await self._search_contacts({"search_text": search_text})
            else:
                return {"answer": "Vector search not available", "tickets": [], "ticket_count": 0}
        
        search_params = ai_response.get("search_params", {})
        query_text = search_params.get("query", "")
        tables = search_params.get("tables", ["tickets"])
        limit = search_params.get("limit", 10)
        threshold = 0.60
        
        if not query_text:
            return {"answer": "No query", "tickets": [], "ticket_count": 0}
        
        logger.info(f"🔍 Vector search: '{query_text}' in {tables}")
        
        try:
            query_embedding = await self.embedding_service.generate_embedding(query_text)
            results = []
            
            for table in tables:
                try:
                    records = self.db_service.client.table(table).select("*").not_.is_("embedding", "null").limit(1000).execute().data or []
                    
                    for record in records:
                        embedding_data = record.get("embedding")
                        if not embedding_data:
                            continue
                        
                        try:
                            if isinstance(embedding_data, str):
                                embedding_data = embedding_data.strip('[]')
                                record_embedding = [float(x.strip()) for x in embedding_data.split(',')]
                            elif isinstance(embedding_data, list):
                                record_embedding = embedding_data
                            else:
                                continue
                            
                            sim = self._cosine_similarity(query_embedding, record_embedding)
                            
                            if sim >= threshold:
                                record["similarity_score"] = sim
                                record["source_table"] = table
                                results.append(record)
                        except:
                            continue
                except Exception as e:
                    logger.error(f"Error searching {table}: {e}")
            
            results.sort(key=lambda x: x.get("similarity_score", 0), reverse=True)
            results = results[:limit]
            
            if not results:
                return {"answer": f"No results for '{query_text}'", "tickets": [], "ticket_count": 0}
            
            lines = [f"Found {len(results)} results for '{query_text}':\n"]
            
            for i, r in enumerate(results, 1):
                score = r.get("similarity_score", 0)
                
                if r["source_table"] == "tickets":
                    lines.append(f"{i}. Ticket #{r.get('ticket_number')} - {r.get('title', 'N/A')[:50]} ({score:.1%})")
                elif r["source_table"] == "resources":
                    name = f"{r.get('first_name', '')} {r.get('last_name', '')}".strip()
                    lines.append(f"{i}. {name} - {r.get('title', 'N/A')} ({score:.1%})")
                elif r["source_table"] == "contacts":
                    name = f"{r.get('first_name', '')} {r.get('last_name', '')}".strip()
                    lines.append(f"{i}. {name} - {r.get('email_address', 'N/A')} ({score:.1%})")
            
            return {
                "answer": "\n".join(lines),
                "results": results,
                "tickets": [r for r in results if r["source_table"] == "tickets"],
                "ticket_count": len(results)
            }
        except Exception as e:
            logger.error(f"Vector search error: {e}", exc_info=True)
            return {"answer": f"Search error: {str(e)}", "tickets": [], "ticket_count": 0}
    
    async def _analyze_common_issues(self, ai_response: Dict) -> Dict:
        """
        FIXED & SAFE: Analyze common issues in tickets
        Now 100% safe against NoneType, null, or non-string description/title
        """
        params = ai_response.get("params", {})
    
        logger.info(f"Analyzing common issues with filters: {params}")
    
        try:
            # Count total matching tickets
            count_query = self.db_service.client.table("tickets").select("id", count="exact")
            count_query = self.filter_builder.apply_filters(count_query, params)
            total_count = count_query.execute().count or 0
        
            if total_count == 0:
                desc = self.filter_builder.describe_filters(params)
                return {
                    "answer": f"No tickets found{desc}.",
                    "tickets": [],
                    "ticket_count": 0
                }
        
            # Fetch tickets - limit to avoid token overflow
            limit = min(total_count, QueryLimits.MAX_ISSUES_ANALYSIS)
        
            query = self.db_service.client.table("tickets").select(
                "id, ticket_number, title, description, status, priority, company_name, "
                "assigned_resource_name, contact_name, create_date, queue_id"
            )
            query = self.filter_builder.apply_filters(query, params)
            query = query.order("create_date", desc=True).limit(limit)
        
            tickets = query.execute().data or []
        
            if not tickets:
                return {
                    "answer": "No tickets found to analyze.",
                    "tickets": [],
                    "ticket_count": 0
                }
        
            logger.info(f"Analyzing {len(tickets)} tickets (out of {total_count} total)")

            # HELPER: Safe string extractor (handles None, null, numbers, objects, etc.)
            def safe_text(text, fallback="No text provided", max_length=None):
                if text is None or text == "null" or not isinstance(text, str):
                    result = fallback
                else:
                    result = text.strip()
                    if not result:
                        result = fallback
                return result[:max_length] if max_length else result

            # Prepare clean, safe ticket summaries
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
                queue_name = (
                    self.lookups.get_label('ticket_queue', t.get("queue_id"))
                    if self.lookups and t.get("queue_id") else "Unknown"
                )

                ticket_summaries.append({
                    "ticket_number": t.get("ticket_number", "Unknown"),
                    "title": safe_text(t.get("title"), "No title", 200),
                    "description": safe_text(t.get("description"), "No description provided", 500),
                    "status": status_name,
                    "priority": priority_name,
                    "queue": queue_name,
                    "company": t.get("company_name") or "Unknown Company",
                    "assigned_to": t.get("assigned_resource_name") or "Unassigned",
                    "contact": t.get("contact_name") or "Unknown Contact",
                    "created": str(t.get("create_date", "") or "")[:10]
                })
        
            # Generate analysis prompt
            desc = self.filter_builder.describe_filters(params)
            analysis_prompt = f"""You are a senior technical support analyst.
    Analyze these {len(tickets)} support tickets{desc} and identify the most common real-world issues customers are facing.

    Tickets (title + description are most important):
    {json.dumps(ticket_summaries, indent=2, default=str)}

    Provide a clear, actionable report with:
    1. Top 5 most frequent issues (based on titles & descriptions)
    2. Any patterns (e.g. specific software, hardware, user error, network, etc.)
    3. Affected companies or queues (if any stand out)
    4. Recommendations to reduce these tickets

    Be specific, practical, and focus on root causes from the actual text."""

            response = await self.client.chat.completions.create(
                model=settings.openai_model,
                messages=[
                    {"role": "system", "content": "You are an expert support analyst who finds patterns in messy ticket data."},
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
                "analyzed_count": len(tickets),
                "analysis_type": "common_issues"
            }
        
        except Exception as e:
            logger.error(f"Common issues analysis error: {e}", exc_info=True)
            return {
                "answer": f"Sorry, an error occurred during analysis: {str(e)}",
                "tickets": [],
                "ticket_count": 0
            }
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Compute cosine similarity"""
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
    """Get service instance"""
    return AIService()