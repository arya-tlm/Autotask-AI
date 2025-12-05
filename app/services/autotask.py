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
                    """Fetch notes and time entries for a single ticket with retry logic"""
                    async with semaphore:
                        ticket_id = ticket["id"]

                        async def fetch_with_retry(url, payload, entity_type, max_retries=3):
                            """Fetch data with exponential backoff retry for 429 errors"""
                            for attempt in range(max_retries):
                                try:
                                    response = await client.post(url, json=payload, headers=self._get_headers())
                                    response.raise_for_status()
                                    return response.json().get("items", [])
                                except httpx.HTTPStatusError as e:
                                    if e.response.status_code == 429:
                                        # Rate limit hit - wait and retry
                                        wait_time = (2 ** attempt) * 1.5  # 1.5s, 3s, 6s
                                        print(f"  ⚠ Rate limit (429) for {entity_type} on ticket {ticket_id}, retrying in {wait_time}s...")
                                        await asyncio.sleep(wait_time)
                                        if attempt == max_retries - 1:
                                            print(f"  ✗ Max retries reached for {entity_type} on ticket {ticket_id}")
                                            return []
                                    else:
                                        print(f"  ⚠ HTTP {e.response.status_code} fetching {entity_type} for ticket {ticket_id}")
                                        return []
                                except Exception as e:
                                    print(f"  ⚠ Failed to fetch {entity_type} for ticket {ticket_id}: {str(e)}")
                                    return []
                            return []

                        try:
                            # Rate limiting - increased delay to reduce API pressure
                            await asyncio.sleep(0.3)

                            # Fetch notes with retry logic
                            notes = await fetch_with_retry(
                                f"{self.base_url}/TicketNotes/query",
                                {
                                    "MaxRecords": 500,
                                    "Filter": [{"field": "ticketID", "op": "eq", "value": ticket_id}]
                                },
                                "notes"
                            )

                            # Small delay between notes and time entries
                            await asyncio.sleep(0.2)

                            # Fetch time entries with retry logic
                            time_entries = await fetch_with_retry(
                                f"{self.base_url}/TimeEntries/query",
                                {
                                    "MaxRecords": 500,
                                    "Filter": [{"field": "ticketID", "op": "eq", "value": ticket_id}]
                                },
                                "time_entries"
                            )

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

                # Rate limiting between batches - increased to avoid hitting Autotask limits
                print(f"  Waiting 2 seconds before next batch to respect Autotask rate limits...")
                await asyncio.sleep(2.0)

        print(f"Finished fetching. Total tickets with details: {len(all_tickets_with_details)}")
        return all_tickets_with_details


def get_autotask_service() -> AutotaskService:
    """Dependency injection for Autotask service"""
    return AutotaskService()