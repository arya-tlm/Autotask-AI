# # """
# # Database Service
# # Handles all Supabase database operations
# # """
# # from typing import List, Dict, Optional
# # from supabase import create_client, Client
# # from app.config import get_settings
# # from app.models.schemas import SyncStats

# # settings = get_settings()


# # class DatabaseService:
# #     """Service for database operations with Supabase"""
    
# #     def __init__(self):
# #         self.client: Client = create_client(settings.supabase_url, settings.supabase_key)
    
# #     @staticmethod
# #     def transform_ticket(ticket: Dict) -> Dict:
# #         """Transform Autotask ticket to database schema"""
# #         return {
# #             "id": ticket.get("id"),
# #             "ticket_number": ticket.get("ticketNumber"),
# #             "title": ticket.get("title"),
# #             "description": ticket.get("description", ""),
# #             "status": ticket.get("status"),
# #             "priority": ticket.get("priority"),
# #             "ticket_type": ticket.get("ticketType"),
# #             "ticket_category": ticket.get("ticketCategory"),
# #             "create_date": ticket.get("createDate"),
# #             "due_date_time": ticket.get("dueDateTime"),
# #             "completed_date": ticket.get("completedDate"),
# #             "resolved_date_time": ticket.get("resolvedDateTime"),
# #             "last_activity_date": ticket.get("lastActivityDate"),
# #             "company_id": ticket.get("companyID"),
# #             "contact_id": ticket.get("contactID"),
# #             "assigned_resource_id": ticket.get("assignedResourceID"),
# #             "resolution": ticket.get("resolution", ""),
# #         }
    
# #     @staticmethod
# #     def transform_note(note: Dict) -> Dict:
# #         """Transform Autotask note to database schema"""
# #         return {
# #             "id": note.get("id"),
# #             "ticket_id": note.get("ticketID"),
# #             "title": note.get("title", ""),
# #             "description": note.get("description", ""),
# #             "note_type": note.get("noteType"),
# #             "create_date_time": note.get("createDateTime"),
# #         }
    
# #     @staticmethod
# #     def transform_time_entry(entry: Dict) -> Dict:
# #         """Transform Autotask time entry to database schema"""
# #         return {
# #             "id": entry.get("id"),
# #             "ticket_id": entry.get("ticketID"),
# #             "date_worked": entry.get("dateWorked"),
# #             "hours_worked": entry.get("hoursWorked"),
# #             "summary_notes": entry.get("summaryNotes", ""),
# #             "resource_id": entry.get("resourceID"),
# #         }
    
# #     async def store_tickets_with_details(self, tickets_data: List[Dict]) -> SyncStats:
# #         """
# #         Store tickets, notes, and time entries in Supabase
        
# #         Args:
# #             tickets_data: List of tickets with notes and time entries
            
# #         Returns:
# #             SyncStats with operation statistics
# #         """
# #         stats = SyncStats()
        
# #         print(f"\nStoring {len(tickets_data)} tickets in database...")
        
# #         for idx, ticket_data in enumerate(tickets_data, 1):
# #             ticket_id = ticket_data.get("id", "unknown")
            
# #             try:
# #                 # Store ticket
# #                 ticket = self.transform_ticket(ticket_data)
                
# #                 try:
# #                     result = self.client.table("tickets").upsert(ticket, on_conflict="id").execute()
# #                     stats.tickets_processed += 1
# #                     if result.data:
# #                         stats.tickets_inserted += 1
# #                         print(f"  ✓ Stored ticket {ticket_id} ({idx}/{len(tickets_data)})")
# #                 except Exception as e:
# #                     error_msg = f"Ticket {ticket_id}: {str(e)}"
# #                     stats.errors.append(error_msg)
# #                     print(f"  ✗ {error_msg}")
# #                     continue
                
# #                 # Store notes
# #                 notes = ticket_data.get("notes", [])
# #                 if notes:
# #                     try:
# #                         transformed_notes = [self.transform_note(n) for n in notes]
# #                         notes_result = self.client.table("ticket_notes").upsert(
# #                             transformed_notes, on_conflict="id"
# #                         ).execute()
# #                         if notes_result.data:
# #                             stats.notes_inserted += len(notes_result.data)
# #                             print(f"    → Stored {len(notes_result.data)} notes")
# #                     except Exception as e:
# #                         error_msg = f"Notes for ticket {ticket_id}: {str(e)}"
# #                         stats.errors.append(error_msg)
# #                         print(f"    ⚠ {error_msg}")
                
# #                 # Store time entries
# #                 time_entries = ticket_data.get("time_entries", [])
# #                 if time_entries:
# #                     try:
# #                         transformed_entries = [self.transform_time_entry(e) for e in time_entries]
# #                         entries_result = self.client.table("time_entries").upsert(
# #                             transformed_entries, on_conflict="id"
# #                         ).execute()
# #                         if entries_result.data:
# #                             stats.time_entries_inserted += len(entries_result.data)
# #                             print(f"    → Stored {len(entries_result.data)} time entries")
# #                     except Exception as e:
# #                         error_msg = f"Time entries for ticket {ticket_id}: {str(e)}"
# #                         stats.errors.append(error_msg)
# #                         print(f"    ⚠ {error_msg}")
                    
# #             except Exception as e:
# #                 error_msg = f"Processing ticket {ticket_id}: {str(e)}"
# #                 stats.errors.append(error_msg)
# #                 print(f"  ✗ {error_msg}")
        
# #         print(f"\n{'='*60}")
# #         print(f"Storage Complete!")
# #         print(f"{'='*60}")
# #         print(f"Tickets: {stats.tickets_inserted}/{stats.tickets_processed}")
# #         print(f"Notes: {stats.notes_inserted}")
# #         print(f"Time entries: {stats.time_entries_inserted}")
# #         print(f"Errors: {len(stats.errors)}")
# #         print(f"{'='*60}\n")
        
# #         return stats

# #     def search_tickets(self, params: Dict) -> List[Dict]:
# #         """
# #         Search tickets with filters
        
# #         Args:
# #             params: Search parameters (company_id, status, priority, dates, limit, offset)
            
# #         Returns:
# #             List of matching tickets
# #         """
# #         query = self.client.table("tickets").select("*")
        
# #         if "company_id" in params and params["company_id"]:
# #             query = query.eq("company_id", params["company_id"])
        
# #         if "status" in params and params["status"] is not None:
# #             query = query.eq("status", params["status"])
        
# #         if "priority" in params and params["priority"] is not None:
# #             query = query.eq("priority", params["priority"])
        
# #         if "start_date" in params and params["start_date"]:
# #             query = query.gte("create_date", params["start_date"])
        
# #         if "end_date" in params and params["end_date"]:
# #             query = query.lte("create_date", params["end_date"])
        
# #         # Handle pagination
# #         limit = params.get("limit", settings.default_search_limit)
# #         offset = params.get("offset", 0)
        
# #         # Use range for pagination
# #         end_range = offset + limit - 1
# #         query = query.range(offset, end_range)
        
# #         result = query.execute()
# #         return result.data
    
# #     def count_tickets(self, params: Dict) -> int:
# #         """
# #         Count tickets matching filters WITHOUT fetching data
        
# #         Args:
# #             params: Search parameters
            
# #         Returns:
# #             Count of matching tickets
# #         """
# #         query = self.client.table("tickets").select("id", count="exact")
        
# #         if "company_id" in params and params["company_id"]:
# #             query = query.eq("company_id", params["company_id"])
        
# #         if "status" in params and params["status"] is not None:
# #             query = query.eq("status", params["status"])
        
# #         if "priority" in params and params["priority"] is not None:
# #             query = query.eq("priority", params["priority"])
        
# #         if "start_date" in params and params["start_date"]:
# #             query = query.gte("create_date", params["start_date"])
        
# #         if "end_date" in params and params["end_date"]:
# #             query = query.lte("create_date", params["end_date"])
        
# #         # Only get count, no data
# #         result = query.limit(1).execute()
# #         return result.count or 0
    
# #     def get_tickets_batch(
# #         self, 
# #         params: Dict, 
# #         batch_size: int = 1000, 
# #         offset: int = 0
# #     ) -> tuple[List[Dict], bool]:
# #         """
# #         Get a batch of tickets with pagination
        
# #         Args:
# #             params: Search parameters
# #             batch_size: Number of tickets per batch
# #             offset: Starting offset
            
# #         Returns:
# #             Tuple of (tickets, has_more)
# #         """
# #         query = self.client.table("tickets").select("*")
        
# #         if "company_id" in params and params["company_id"]:
# #             query = query.eq("company_id", params["company_id"])
        
# #         if "status" in params and params["status"] is not None:
# #             query = query.eq("status", params["status"])
        
# #         if "priority" in params and params["priority"] is not None:
# #             query = query.eq("priority", params["priority"])
        
# #         if "start_date" in params and params["start_date"]:
# #             query = query.gte("create_date", params["start_date"])
        
# #         if "end_date" in params and params["end_date"]:
# #             query = query.lte("create_date", params["end_date"])
        
# #         # Fetch one extra to check if there are more
# #         query = query.range(offset, offset + batch_size)
# #         result = query.execute()
        
# #         tickets = result.data
# #         has_more = len(tickets) > batch_size
        
# #         if has_more:
# #             tickets = tickets[:batch_size]
        
# #         return tickets, has_more
    
# #     def get_database_stats(self) -> Dict[str, int]:
# #         """
# #         Get database statistics
        
# #         Returns:
# #             Dictionary with counts of tickets, notes, and time entries
# #         """
# #         tickets = self.client.table("tickets").select("id", count="exact").limit(1).execute()
# #         notes = self.client.table("ticket_notes").select("id", count="exact").limit(1).execute()
# #         entries = self.client.table("time_entries").select("id", count="exact").limit(1).execute()
        
# #         return {
# #             "total_tickets": tickets.count or 0,
# #             "total_notes": notes.count or 0,
# #             "total_time_entries": entries.count or 0
# #         }
    
# #     def get_ticket_stats_by_status(self) -> List[Dict]:
# #         """
# #         Get ticket counts grouped by status
        
# #         Returns:
# #             List of status statistics
# #         """
# #         result = self.client.table("tickets")\
# #             .select("status")\
# #             .execute()
        
# #         # Aggregate in Python
# #         stats = {}
# #         for ticket in result.data:
# #             status = ticket.get("status", "Unknown")
# #             stats[status] = stats.get(status, 0) + 1
        
# #         return [{"status": k, "count": v} for k, v in stats.items()]
    
# #     def get_ticket_stats_by_priority(self) -> List[Dict]:
# #         """
# #         Get ticket counts grouped by priority
        
# #         Returns:
# #             List of priority statistics
# #         """
# #         result = self.client.table("tickets")\
# #             .select("priority")\
# #             .execute()
        
# #         # Aggregate in Python
# #         stats = {}
# #         for ticket in result.data:
# #             priority = ticket.get("priority", "Unknown")
# #             stats[priority] = stats.get(priority, 0) + 1
        
# #         return [{"priority": k, "count": v} for k, v in stats.items()]
    
# #     def health_check(self) -> bool:
# #         """
# #         Check if database connection is healthy
        
# #         Returns:
# #             True if connection is healthy, False otherwise
# #         """
# #         try:
# #             self.client.table("tickets").select("id").limit(1).execute()
# #             return True
# #         except Exception as e:
# #             print(f"Database health check failed: {str(e)}")
# #             return False


# # # Singleton instance
# # _db_service: Optional[DatabaseService] = None


# # def get_database_service() -> DatabaseService:
# #     """Dependency injection for database service"""
# #     global _db_service
# #     if _db_service is None:
# #         _db_service = DatabaseService()
# #     return _db_service

# """
# Database Service
# Handles all Supabase database operations
# """
# from typing import List, Dict, Optional
# from supabase import create_client, Client
# from app.config import get_settings
# from app.models.schemas import SyncStats

# settings = get_settings()


# class DatabaseService:
#     """Service for database operations with Supabase"""
    
#     def __init__(self):
#         self.client: Client = create_client(settings.supabase_url, settings.supabase_key)
    
#     @staticmethod
#     def transform_ticket(ticket: Dict) -> Dict:
#         """Transform Autotask ticket to database schema"""
#         return {
#             "id": ticket.get("id"),
#             "ticket_number": ticket.get("ticketNumber"),
#             "title": ticket.get("title"),
#             "description": ticket.get("description", ""),
#             "status": ticket.get("status"),
#             "priority": ticket.get("priority"),
#             "ticket_type": ticket.get("ticketType"),
#             "ticket_category": ticket.get("ticketCategory"),
#             "create_date": ticket.get("createDate"),
#             "due_date_time": ticket.get("dueDateTime"),
#             "completed_date": ticket.get("completedDate"),
#             "resolved_date_time": ticket.get("resolvedDateTime"),
#             "last_activity_date": ticket.get("lastActivityDate"),
#             "company_id": ticket.get("companyID"),
#             "contact_id": ticket.get("contactID"),
#             "assigned_resource_id": ticket.get("assignedResourceID"),
#             "resolution": ticket.get("resolution", ""),
#         }
    
#     @staticmethod
#     def transform_note(note: Dict) -> Dict:
#         """Transform Autotask note to database schema"""
#         return {
#             "id": note.get("id"),
#             "ticket_id": note.get("ticketID"),
#             "title": note.get("title", ""),
#             "description": note.get("description", ""),
#             "note_type": note.get("noteType"),
#             "create_date_time": note.get("createDateTime"),
#         }
    
#     @staticmethod
#     def transform_time_entry(entry: Dict) -> Dict:
#         """Transform Autotask time entry to database schema"""
#         return {
#             "id": entry.get("id"),
#             "ticket_id": entry.get("ticketID"),
#             "date_worked": entry.get("dateWorked"),
#             "hours_worked": entry.get("hoursWorked"),
#             "summary_notes": entry.get("summaryNotes", ""),
#             "resource_id": entry.get("resourceID"),
#         }
    
#     async def store_tickets_with_details(self, tickets_data: List[Dict]) -> SyncStats:
#         """
#         Store tickets, notes, and time entries in Supabase
        
#         Args:
#             tickets_data: List of tickets with notes and time entries
            
#         Returns:
#             SyncStats with operation statistics
#         """
#         stats = SyncStats()
        
#         print(f"\nStoring {len(tickets_data)} tickets in database...")
        
#         for idx, ticket_data in enumerate(tickets_data, 1):
#             ticket_id = ticket_data.get("id", "unknown")
            
#             try:
#                 # Store ticket
#                 ticket = self.transform_ticket(ticket_data)
                
#                 try:
#                     result = self.client.table("tickets").upsert(ticket, on_conflict="id").execute()
#                     stats.tickets_processed += 1
#                     if result.data:
#                         stats.tickets_inserted += 1
#                         print(f"  ✓ Stored ticket {ticket_id} ({idx}/{len(tickets_data)})")
#                 except Exception as e:
#                     error_msg = f"Ticket {ticket_id}: {str(e)}"
#                     stats.errors.append(error_msg)
#                     print(f"  ✗ {error_msg}")
#                     continue
                
#                 # Store notes
#                 notes = ticket_data.get("notes", [])
#                 if notes:
#                     try:
#                         transformed_notes = [self.transform_note(n) for n in notes]
#                         notes_result = self.client.table("ticket_notes").upsert(
#                             transformed_notes, on_conflict="id"
#                         ).execute()
#                         if notes_result.data:
#                             stats.notes_inserted += len(notes_result.data)
#                             print(f"    → Stored {len(notes_result.data)} notes")
#                     except Exception as e:
#                         error_msg = f"Notes for ticket {ticket_id}: {str(e)}"
#                         stats.errors.append(error_msg)
#                         print(f"    ⚠ {error_msg}")
                
#                 # Store time entries
#                 time_entries = ticket_data.get("time_entries", [])
#                 if time_entries:
#                     try:
#                         transformed_entries = [self.transform_time_entry(e) for e in time_entries]
#                         entries_result = self.client.table("time_entries").upsert(
#                             transformed_entries, on_conflict="id"
#                         ).execute()
#                         if entries_result.data:
#                             stats.time_entries_inserted += len(entries_result.data)
#                             print(f"    → Stored {len(entries_result.data)} time entries")
#                     except Exception as e:
#                         error_msg = f"Time entries for ticket {ticket_id}: {str(e)}"
#                         stats.errors.append(error_msg)
#                         print(f"    ⚠ {error_msg}")
                    
#             except Exception as e:
#                 error_msg = f"Processing ticket {ticket_id}: {str(e)}"
#                 stats.errors.append(error_msg)
#                 print(f"  ✗ {error_msg}")
        
#         print(f"\n{'='*60}")
#         print(f"Storage Complete!")
#         print(f"{'='*60}")
#         print(f"Tickets: {stats.tickets_inserted}/{stats.tickets_processed}")
#         print(f"Notes: {stats.notes_inserted}")
#         print(f"Time entries: {stats.time_entries_inserted}")
#         print(f"Errors: {len(stats.errors)}")
#         print(f"{'='*60}\n")
        
#         return stats

#     def search_tickets(self, params: Dict) -> List[Dict]:
#         """
#         Search tickets with filters
        
#         Args:
#             params: Search parameters (company_id, status, priority, dates, limit, offset)
            
#         Returns:
#             List of matching tickets
#         """
#         query = self.client.table("tickets").select("*")
        
#         if "company_id" in params and params["company_id"]:
#             query = query.eq("company_id", params["company_id"])
        
#         if "status" in params and params["status"] is not None:
#             query = query.eq("status", params["status"])
        
#         if "priority" in params and params["priority"] is not None:
#             query = query.eq("priority", params["priority"])
        
#         if "start_date" in params and params["start_date"]:
#             query = query.gte("create_date", params["start_date"])
        
#         if "end_date" in params and params["end_date"]:
#             query = query.lte("create_date", params["end_date"])
        
#         # Handle pagination
#         limit = params.get("limit", settings.default_search_limit)
#         offset = params.get("offset", 0)
        
#         # Use range for pagination
#         end_range = offset + limit - 1
#         query = query.range(offset, end_range)
        
#         result = query.execute()
#         return result.data
    
#     def count_tickets(self, params: Dict) -> int:
#         """
#         Count tickets matching filters WITHOUT fetching data
        
#         Args:
#             params: Search parameters
            
#         Returns:
#             Count of matching tickets
#         """
#         query = self.client.table("tickets").select("id", count="exact")
        
#         if "company_id" in params and params["company_id"]:
#             query = query.eq("company_id", params["company_id"])
        
#         if "status" in params and params["status"] is not None:
#             query = query.eq("status", params["status"])
        
#         if "priority" in params and params["priority"] is not None:
#             query = query.eq("priority", params["priority"])
        
#         if "start_date" in params and params["start_date"]:
#             query = query.gte("create_date", params["start_date"])
        
#         if "end_date" in params and params["end_date"]:
#             query = query.lte("create_date", params["end_date"])
        
#         # Only get count, no data
#         result = query.limit(1).execute()
#         return result.count or 0
    
#     def get_tickets_batch(
#         self, 
#         params: Dict, 
#         batch_size: int = 1000, 
#         offset: int = 0
#     ) -> tuple[List[Dict], bool]:
#         """
#         Get a batch of tickets with pagination
        
#         Args:
#             params: Search parameters
#             batch_size: Number of tickets per batch
#             offset: Starting offset
            
#         Returns:
#             Tuple of (tickets, has_more)
#         """
#         query = self.client.table("tickets").select("*")
        
#         if "company_id" in params and params["company_id"]:
#             query = query.eq("company_id", params["company_id"])
        
#         if "status" in params and params["status"] is not None:
#             query = query.eq("status", params["status"])
        
#         if "priority" in params and params["priority"] is not None:
#             query = query.eq("priority", params["priority"])
        
#         if "start_date" in params and params["start_date"]:
#             query = query.gte("create_date", params["start_date"])
        
#         if "end_date" in params and params["end_date"]:
#             query = query.lte("create_date", params["end_date"])
        
#         # Fetch one extra to check if there are more
#         query = query.range(offset, offset + batch_size)
#         result = query.execute()
        
#         tickets = result.data
#         has_more = len(tickets) > batch_size
        
#         if has_more:
#             tickets = tickets[:batch_size]
        
#         return tickets, has_more
    
#     def get_database_stats(self) -> Dict[str, int]:
#         """
#         Get database statistics
        
#         Returns:
#             Dictionary with counts of tickets, notes, time entries, companies, resources, and contacts
#         """
#         tickets = self.client.table("tickets").select("id", count="exact").limit(1).execute()
#         notes = self.client.table("ticket_notes").select("id", count="exact").limit(1).execute()
#         entries = self.client.table("time_entries").select("id", count="exact").limit(1).execute()
#         companies = self.client.table("companies").select("id", count="exact").limit(1).execute()
#         resources = self.client.table("resources").select("id", count="exact").limit(1).execute()
#         contacts = self.client.table("contacts").select("id", count="exact").limit(1).execute()
        
#         return {
#             "tickets": tickets.count or 0,
#             "notes": notes.count or 0,
#             "time_entries": entries.count or 0,
#             "companies": companies.count or 0,
#             "resources": resources.count or 0,
#             "contacts": contacts.count or 0
#         }
    
#     def get_ticket_stats_by_status(self) -> List[Dict]:
#         """
#         Get ticket counts grouped by status
        
#         Returns:
#             List of status statistics
#         """
#         result = self.client.table("tickets")\
#             .select("status")\
#             .execute()
        
#         # Aggregate in Python
#         stats = {}
#         for ticket in result.data:
#             status = ticket.get("status", "Unknown")
#             stats[status] = stats.get(status, 0) + 1
        
#         return [{"status": k, "count": v} for k, v in stats.items()]
    
#     def get_ticket_stats_by_priority(self) -> List[Dict]:
#         """
#         Get ticket counts grouped by priority
        
#         Returns:
#             List of priority statistics
#         """
#         result = self.client.table("tickets")\
#             .select("priority")\
#             .execute()
        
#         # Aggregate in Python
#         stats = {}
#         for ticket in result.data:
#             priority = ticket.get("priority", "Unknown")
#             stats[priority] = stats.get(priority, 0) + 1
        
#         return [{"priority": k, "count": v} for k, v in stats.items()]
    
#     def health_check(self) -> bool:
#         """
#         Check if database connection is healthy
        
#         Returns:
#             True if connection is healthy, False otherwise
#         """
#         try:
#             self.client.table("tickets").select("id").limit(1).execute()
#             return True
#         except Exception as e:
#             print(f"Database health check failed: {str(e)}")
#             return False


# # Singleton instance
# _db_service: Optional[DatabaseService] = None


# def get_database_service() -> DatabaseService:
#     """Dependency injection for database service"""
#     global _db_service
#     if _db_service is None:
#         _db_service = DatabaseService()
#     return _db_service
"""
Database Service
Handles all Supabase database operations
"""
from typing import List, Dict, Optional
from supabase import create_client, Client
from app.config import get_settings
from app.models.schemas import SyncStats

settings = get_settings()


class DatabaseService:
    """Service for database operations with Supabase"""
    
    def __init__(self):
        self.client: Client = create_client(settings.supabase_url, settings.supabase_key)
    
    @staticmethod
    def transform_ticket(ticket: Dict) -> Dict:
        """Transform Autotask ticket to database schema"""
        # Handle sub_issue_type - set to None if not provided or if it might be invalid
        # This prevents foreign key constraint violations
        sub_issue_type = ticket.get("subIssueType")
        if sub_issue_type == 0 or sub_issue_type == "":
            sub_issue_type = None

        return {
            "id": ticket.get("id"),
            "ticket_number": ticket.get("ticketNumber"),
            "title": ticket.get("title"),
            "description": ticket.get("description", ""),
            "status": ticket.get("status"),
            "priority": ticket.get("priority"),
            "ticket_type": ticket.get("ticketType"),
            "ticket_category": ticket.get("ticketCategory"),
            "create_date": ticket.get("createDate"),
            "due_date_time": ticket.get("dueDateTime"),
            "completed_date": ticket.get("completedDate"),
            "resolved_date_time": ticket.get("resolvedDateTime"),
            "last_activity_date": ticket.get("lastActivityDate"),
            "company_id": ticket.get("companyID"),
            "contact_id": ticket.get("contactID"),
            "assigned_resource_id": ticket.get("assignedResourceID"),
            "resolution": ticket.get("resolution", ""),
            "source": ticket.get("source"),
            "issue_type": ticket.get("issueType"),
            "sub_issue_type": sub_issue_type,  # Now handles None gracefully
            "queue_id": ticket.get("queueID"),
        }
    
    @staticmethod
    def transform_note(note: Dict) -> Dict:
        """Transform Autotask note to database schema"""
        return {
            "id": note.get("id"),
            "ticket_id": note.get("ticketID"),
            "title": note.get("title", ""),
            "description": note.get("description", ""),
            "note_type": note.get("noteType"),
            "create_date_time": note.get("createDateTime"),
        }
    
    @staticmethod
    def transform_time_entry(entry: Dict) -> Dict:
        """Transform Autotask time entry to database schema"""
        return {
            "id": entry.get("id"),
            "ticket_id": entry.get("ticketID"),
            "date_worked": entry.get("dateWorked"),
            "hours_worked": entry.get("hoursWorked"),
            "summary_notes": entry.get("summaryNotes", ""),
            "resource_id": entry.get("resourceID"),
        }
    
    async def store_tickets_with_details(self, tickets_data: List[Dict]) -> SyncStats:
        """
        Store tickets, notes, and time entries in Supabase

        Args:
            tickets_data: List of tickets with notes and time entries

        Returns:
            SyncStats with operation statistics
        """
        stats = SyncStats()

        print(f"\nStoring {len(tickets_data)} tickets in database...")

        for idx, ticket_data in enumerate(tickets_data, 1):
            ticket_id = ticket_data.get("id", "unknown")

            try:
                # Store ticket
                ticket = self.transform_ticket(ticket_data)

                # Retry logic for network errors (SSL handshake, timeouts, etc.)
                max_retries = 3
                retry_count = 0
                ticket_stored = False

                while retry_count < max_retries and not ticket_stored:
                    try:
                        result = self.client.table("tickets").upsert(ticket, on_conflict="id").execute()
                        stats.tickets_processed += 1
                        if result.data:
                            stats.tickets_inserted += 1
                            print(f"  ✓ Stored ticket {ticket_id} ({idx}/{len(tickets_data)})")
                            ticket_stored = True
                    except Exception as e:
                        retry_count += 1
                        error_str = str(e)

                        # Check if it's a network/SSL error that might succeed on retry
                        is_retriable = any(keyword in error_str.lower() for keyword in
                                         ['ssl', 'handshake', 'timeout', 'connection', '525', '503', '502'])

                        if is_retriable and retry_count < max_retries:
                            print(f"  ⚠ Ticket {ticket_id}: Network error, retrying ({retry_count}/{max_retries})...")
                            import asyncio
                            await asyncio.sleep(2 ** retry_count)  # Exponential backoff: 2s, 4s, 8s
                        else:
                            # Not retriable or max retries reached
                            error_msg = f"Ticket {ticket_id}: {error_str}"
                            stats.errors.append(error_msg)
                            print(f"  ✗ {error_msg}")
                            break
                
                # Store notes
                notes = ticket_data.get("notes", [])
                if notes:
                    try:
                        transformed_notes = [self.transform_note(n) for n in notes]
                        notes_result = self.client.table("ticket_notes").upsert(
                            transformed_notes, on_conflict="id"
                        ).execute()
                        if notes_result.data:
                            stats.notes_inserted += len(notes_result.data)
                            print(f"    → Stored {len(notes_result.data)} notes")
                    except Exception as e:
                        error_msg = f"Notes for ticket {ticket_id}: {str(e)}"
                        stats.errors.append(error_msg)
                        print(f"    ⚠ {error_msg}")
                
                # Store time entries
                time_entries = ticket_data.get("time_entries", [])
                if time_entries:
                    try:
                        transformed_entries = [self.transform_time_entry(e) for e in time_entries]
                        entries_result = self.client.table("time_entries").upsert(
                            transformed_entries, on_conflict="id"
                        ).execute()
                        if entries_result.data:
                            stats.time_entries_inserted += len(entries_result.data)
                            print(f"    → Stored {len(entries_result.data)} time entries")
                    except Exception as e:
                        error_msg = f"Time entries for ticket {ticket_id}: {str(e)}"
                        stats.errors.append(error_msg)
                        print(f"    ⚠ {error_msg}")
                    
            except Exception as e:
                error_msg = f"Processing ticket {ticket_id}: {str(e)}"
                stats.errors.append(error_msg)
                print(f"  ✗ {error_msg}")
        
        print(f"\n{'='*60}")
        print(f"Storage Complete!")
        print(f"{'='*60}")
        print(f"Tickets: {stats.tickets_inserted}/{stats.tickets_processed}")
        print(f"Notes: {stats.notes_inserted}")
        print(f"Time entries: {stats.time_entries_inserted}")
        print(f"Errors: {len(stats.errors)}")
        print(f"{'='*60}\n")
        
        return stats

    def search_tickets(self, params: Dict) -> List[Dict]:
        """
        Search tickets with filters
        
        Args:
            params: Search parameters (company_id, status, priority, dates, limit, offset)
            
        Returns:
            List of matching tickets
        """
        query = self.client.table("tickets").select("*")
        
        if "company_id" in params and params["company_id"]:
            query = query.eq("company_id", params["company_id"])
        
        if "status" in params and params["status"] is not None:
            query = query.eq("status", params["status"])
        
        if "priority" in params and params["priority"] is not None:
            query = query.eq("priority", params["priority"])
        
        if "start_date" in params and params["start_date"]:
            query = query.gte("create_date", params["start_date"])
        
        if "end_date" in params and params["end_date"]:
            query = query.lte("create_date", params["end_date"])
        
        # Handle pagination
        limit = params.get("limit", settings.default_search_limit)
        offset = params.get("offset", 0)
        
        # Use range for pagination
        end_range = offset + limit - 1
        query = query.range(offset, end_range)
        
        result = query.execute()
        return result.data
    
    def count_tickets(self, params: Dict) -> int:
        """
        Count tickets matching filters WITHOUT fetching data
        
        Args:
            params: Search parameters
            
        Returns:
            Count of matching tickets
        """
        query = self.client.table("tickets").select("id", count="exact")
        
        if "company_id" in params and params["company_id"]:
            query = query.eq("company_id", params["company_id"])
        
        if "status" in params and params["status"] is not None:
            query = query.eq("status", params["status"])
        
        if "priority" in params and params["priority"] is not None:
            query = query.eq("priority", params["priority"])
        
        if "start_date" in params and params["start_date"]:
            query = query.gte("create_date", params["start_date"])
        
        if "end_date" in params and params["end_date"]:
            query = query.lte("create_date", params["end_date"])
        
        # Only get count, no data
        result = query.limit(1).execute()
        return result.count or 0
    
    def get_tickets_batch(
        self, 
        params: Dict, 
        batch_size: int = 1000, 
        offset: int = 0
    ) -> tuple[List[Dict], bool]:
        """
        Get a batch of tickets with pagination
        
        Args:
            params: Search parameters
            batch_size: Number of tickets per batch
            offset: Starting offset
            
        Returns:
            Tuple of (tickets, has_more)
        """
        query = self.client.table("tickets").select("*")
        
        if "company_id" in params and params["company_id"]:
            query = query.eq("company_id", params["company_id"])
        
        if "status" in params and params["status"] is not None:
            query = query.eq("status", params["status"])
        
        if "priority" in params and params["priority"] is not None:
            query = query.eq("priority", params["priority"])
        
        if "start_date" in params and params["start_date"]:
            query = query.gte("create_date", params["start_date"])
        
        if "end_date" in params and params["end_date"]:
            query = query.lte("create_date", params["end_date"])
        
        # Fetch one extra to check if there are more
        query = query.range(offset, offset + batch_size)
        result = query.execute()
        
        tickets = result.data
        has_more = len(tickets) > batch_size
        
        if has_more:
            tickets = tickets[:batch_size]
        
        return tickets, has_more
    
    def get_database_stats(self) -> Dict[str, int]:
        """
        Get database statistics
        
        Returns:
            Dictionary with counts of tickets, notes, time entries, companies, resources, and contacts
        """
        tickets = self.client.table("tickets").select("id", count="exact").limit(1).execute()
        notes = self.client.table("ticket_notes").select("id", count="exact").limit(1).execute()
        entries = self.client.table("time_entries").select("id", count="exact").limit(1).execute()
        companies = self.client.table("companies").select("id", count="exact").limit(1).execute()
        resources = self.client.table("resources").select("id", count="exact").limit(1).execute()
        contacts = self.client.table("contacts").select("id", count="exact").limit(1).execute()
        
        return {
            "tickets": tickets.count or 0,
            "notes": notes.count or 0,
            "time_entries": entries.count or 0,
            "companies": companies.count or 0,
            "resources": resources.count or 0,
            "contacts": contacts.count or 0
        }
    
    def get_ticket_stats_by_status(self) -> List[Dict]:
        """
        Get ticket counts grouped by status
        
        Returns:
            List of status statistics
        """
        result = self.client.table("tickets")\
            .select("status")\
            .execute()
        
        # Aggregate in Python
        stats = {}
        for ticket in result.data:
            status = ticket.get("status", "Unknown")
            stats[status] = stats.get(status, 0) + 1
        
        return [{"status": k, "count": v} for k, v in stats.items()]
    
    def get_ticket_stats_by_priority(self) -> List[Dict]:
        """
        Get ticket counts grouped by priority
        
        Returns:
            List of priority statistics
        """
        result = self.client.table("tickets")\
            .select("priority")\
            .execute()
        
        # Aggregate in Python
        stats = {}
        for ticket in result.data:
            priority = ticket.get("priority", "Unknown")
            stats[priority] = stats.get(priority, 0) + 1
        
        return [{"priority": k, "count": v} for k, v in stats.items()]
    
    def health_check(self) -> bool:
        """
        Check if database connection is healthy
        
        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            self.client.table("tickets").select("id").limit(1).execute()
            return True
        except Exception as e:
            print(f"Database health check failed: {str(e)}")
            return False
    
    # ==================== NEW: LOOKUP TABLE METHODS (ADDED 2 METHODS) ====================
    
    def get_lookup_table(self, table_name: str, active_only: bool = True) -> List[Dict]:
        """Get all values from a lookup table"""
        try:
            query = self.client.table(table_name).select("*")
            if active_only:
                query = query.eq("is_active", True)
            query = query.order("sort_order")
            result = query.execute()
            return result.data or []
        except Exception as e:
            print(f"Error fetching {table_name}: {e}")
            return []
    
    def sync_lookup_table(self, table_name: str, data: List[Dict]) -> int:
        """Sync lookup table data from Autotask"""
        if not data:
            return 0
        try:
            result = self.client.table(table_name).upsert(data, on_conflict="id").execute()
            return len(result.data) if result.data else 0
        except Exception as e:
            print(f"Error syncing {table_name}: {str(e)}")
            return 0


# Singleton instance
_db_service: Optional[DatabaseService] = None


def get_database_service() -> DatabaseService:
    """Dependency injection for database service"""
    global _db_service
    if _db_service is None:
        _db_service = DatabaseService()
    return _db_service