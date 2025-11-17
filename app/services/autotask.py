"""
Autotask API Service
Handles all interactions with the Autotask REST API
"""
import httpx
import asyncio
from typing import List, Dict, Optional
from datetime import datetime
from app.config import get_settings

settings = get_settings()


class AutotaskService:
    """Service for interacting with Autotask API"""
    
    def __init__(self):
        self.username = settings.autotask_username
        self.password = settings.autotask_password
        self.integration_code = settings.autotask_integration_code
        self.zone_url = settings.autotask_zone_url
        self.base_url = f"{self.zone_url}/atservicesrest/v1.0"
    
    def _get_headers(self) -> Dict[str, str]:
        """Generate authentication headers for Autotask API"""
        return {
            "UserName": self.username,
            "Secret": self.password,
            "APIIntegrationcode": self.integration_code,
            "Content-Type": "application/json"
        }

    async def fetch_tickets_with_details(
        self,
        start_date: datetime,
        end_date: datetime,
        company_id: Optional[int] = None,
        max_tickets: int = None,
        concurrent_limit: int = None
    ) -> List[Dict]:
        """
        Fetch tickets with their associated notes and time entries
        
        Args:
            start_date: Start date for ticket creation filter
            end_date: End date for ticket creation filter
            company_id: Optional company ID filter
            max_tickets: Maximum tickets per batch
            concurrent_limit: Maximum concurrent API calls
            
        Returns:
            List of tickets with notes and time entries
        """
        max_tickets = max_tickets or settings.max_tickets_per_request
        concurrent_limit = concurrent_limit or settings.max_concurrent_requests
        
        all_tickets_with_details = []
        last_ticket_id = 0

        print(f"Fetching tickets from {start_date.isoformat()} to {end_date.isoformat()}...")
        print(f"Concurrency limit: {concurrent_limit} simultaneous requests")
        
        semaphore = asyncio.Semaphore(concurrent_limit)

        while True:
            filter_params = [
                {"field": "createDate", "op": "gte", "value": start_date.isoformat()},
                {"field": "createDate", "op": "lte", "value": end_date.isoformat()},
                {"field": "id", "op": "gt", "value": last_ticket_id}
            ]
            
            if company_id:
                filter_params.append({"field": "companyID", "op": "eq", "value": company_id})
            
            payload = {
                "MaxRecords": max_tickets,
                "IncludeFields": [],
                "Filter": filter_params
            }

            async with httpx.AsyncClient(timeout=60.0) as client:
                try:
                    response = await client.post(
                        f"{self.base_url}/Tickets/query",
                        json=payload,
                        headers=self._get_headers()
                    )
                    response.raise_for_status()
                    data = response.json()
                    tickets = data.get("items", [])
                except httpx.HTTPError as e:
                    print(f"✗ HTTP Error fetching tickets: {str(e)}")
                    raise
                except Exception as e:
                    print(f"✗ Error fetching tickets: {str(e)}")
                    raise

                if not tickets:
                    break

                print(f"Fetched {len(tickets)} tickets. Now fetching their notes and time entries...")

                async def fetch_ticket_details_limited(ticket):
                    """Fetch notes and time entries for a single ticket"""
                    async with semaphore:
                        ticket_id = ticket["id"]
                        
                        try:
                            # Rate limiting
                            await asyncio.sleep(0.1)
                            
                            # Parallel fetch of notes and time entries
                            notes_task = client.post(
                                f"{self.base_url}/TicketNotes/query",
                                json={
                                    "MaxRecords": 500,
                                    "Filter": [{"field": "ticketID", "op": "eq", "value": ticket_id}]
                                },
                                headers=self._get_headers()
                            )
                            
                            time_task = client.post(
                                f"{self.base_url}/TimeEntries/query",
                                json={
                                    "MaxRecords": 500,
                                    "Filter": [{"field": "ticketID", "op": "eq", "value": ticket_id}]
                                },
                                headers=self._get_headers()
                            )
                            
                            notes_response, time_response = await asyncio.gather(
                                notes_task, time_task, return_exceptions=True
                            )
                            
                            notes = []
                            time_entries = []
                            
                            # Extract notes if successful
                            if isinstance(notes_response, Exception):
                                print(f"  ⚠ Failed to fetch notes for ticket {ticket_id}: {str(notes_response)}")
                            else:
                                try:
                                    notes_response.raise_for_status()
                                    notes = notes_response.json().get("items", [])
                                except Exception as e:
                                    print(f"  ⚠ Failed to parse notes for ticket {ticket_id}: {str(e)}")
                            
                            # Extract time entries if successful
                            if isinstance(time_response, Exception):
                                print(f"  ⚠ Failed to fetch time entries for ticket {ticket_id}: {str(time_response)}")
                            else:
                                try:
                                    time_response.raise_for_status()
                                    time_entries = time_response.json().get("items", [])
                                except Exception as e:
                                    print(f"  ⚠ Failed to parse time entries for ticket {ticket_id}: {str(e)}")
                            
                            print(f"  ✓ Ticket {ticket_id}: {len(notes)} notes, {len(time_entries)} time entries")
                            
                            return {
                                **ticket,
                                "notes": notes,
                                "time_entries": time_entries
                            }
                        except Exception as e:
                            print(f"  ✗ Error fetching details for ticket {ticket_id}: {str(e)}")
                            return {**ticket, "notes": [], "time_entries": []}
                
                # Fetch all ticket details concurrently
                tickets_with_details = await asyncio.gather(*[
                    fetch_ticket_details_limited(ticket) for ticket in tickets
                ])
                
                all_tickets_with_details.extend(tickets_with_details)
                last_ticket_id = tickets[-1]["id"]
                
                print(f"Processed {len(tickets)} tickets (Total: {len(all_tickets_with_details)})")

                # Check if we've fetched all available tickets
                if len(tickets) < max_tickets:
                    break
                
                # Rate limiting between batches
                await asyncio.sleep(0.5)

        print(f"Finished fetching. Total tickets with details: {len(all_tickets_with_details)}")
        return all_tickets_with_details


def get_autotask_service() -> AutotaskService:
    """Dependency injection for Autotask service"""
    return AutotaskService()