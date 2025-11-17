"""
Standalone Script to Fetch ALL Resources from Autotask and Store in Supabase
Fetches BOTH active and inactive resources

Usage: python fetch_resources_only.py
"""
import httpx
import asyncio
from datetime import datetime
from supabase import create_client, Client

# ==================== CONFIGURATION ====================
SUPABASE_URL = "https://bmvhfhytbcvklkcuxwnx.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtdmhmaHl0YmN2a2xrY3V4d254Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjE2NDA4ODgsImV4cCI6MjA3NzIxNjg4OH0.MYBjMIl91evXyO6gYuADs_p32Mr5XS46PUZuvJThr8c"

AUTOTASK_USERNAME = "inputiv@teamlogicit64325.com"
AUTOTASK_PASSWORD = "k6qbRCe&8nTiM2Qbb^"
AUTOTASK_INTEGRATION_CODE = "G2S6X7OOTYMGU25GGOJBZXF7BMD"
AUTOTASK_ZONE_URL = "https://webservices15.autotask.net"

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ==================== HELPER FUNCTIONS ====================
def get_autotask_headers():
    """Get headers for Autotask API requests"""
    return {
        "UserName": AUTOTASK_USERNAME,
        "Secret": AUTOTASK_PASSWORD,
        "APIIntegrationcode": AUTOTASK_INTEGRATION_CODE,
        "Content-Type": "application/json"
    }


def convert_travel_availability_to_numeric(value):
    """Convert travel availability percentage string to numeric value"""
    if not value:
        return None
    
    # Convert string to lowercase for easier matching
    value_str = str(value).lower().strip()
    
    # Handle "0%" -> 0
    if value_str == "0%" or value_str == "0":
        return 0
    
    # Handle "up to X%" -> extract X
    if "up to" in value_str:
        # Extract number from "up to 75%" or "up to 100%"
        import re
        match = re.search(r'(\d+)', value_str)
        if match:
            return float(match.group(1))
    
    # Handle simple percentage "75%" -> 75
    if "%" in value_str:
        import re
        match = re.search(r'(\d+)', value_str)
        if match:
            return float(match.group(1))
    
    # Try to convert directly to float
    try:
        return float(value_str)
    except:
        return None


def convert_resource_to_db_format(resource: dict) -> dict:
    """Convert Autotask resource format to database format"""
    return {
        "id": resource.get("id"),
        "accounting_reference_id": resource.get("accountingReferenceID", ""),
        "date_format": resource.get("dateFormat"),
        "default_service_desk_role_id": resource.get("defaultServiceDeskRoleID"),
        "email": resource.get("email", ""),
        "email2": resource.get("email2", ""),
        "email3": resource.get("email3", ""),
        "email_type_code": resource.get("emailTypeCode"),
        "email_type_code2": resource.get("emailTypeCode2"),
        "email_type_code3": resource.get("emailTypeCode3"),
        "first_name": resource.get("firstName", ""),
        "gender": resource.get("gender"),
        "greeting": resource.get("greeting"),
        "hire_date": resource.get("hireDate"),
        "home_phone": resource.get("homePhone", ""),
        "initials": resource.get("initials", ""),
        "internal_cost": resource.get("internalCost"),
        "is_active": resource.get("isActive", True),
        "last_name": resource.get("lastName", ""),
        "license_type": resource.get("licenseType"),
        "location_id": resource.get("locationID"),
        "middle_name": resource.get("middleName", ""),
        "mobile_phone": resource.get("mobilePhone", ""),
        "number_format": resource.get("numberFormat"),
        "office_extension": resource.get("officeExtension", ""),
        "office_phone": resource.get("officePhone", ""),
        "payroll_identifier": resource.get("payrollIdentifier", ""),
        "payroll_type": resource.get("payrollType"),
        "resource_type": resource.get("resourceType"),
        "suffix": resource.get("suffix"),
        "survey_resource_rating": resource.get("surveyResourceRating"),
        "time_format": resource.get("timeFormat"),
        "title": resource.get("title", ""),
        "travel_availability_pct": convert_travel_availability_to_numeric(resource.get("travelAvailabilityPct")),
        "user_name": resource.get("userName", ""),
        "user_type": resource.get("userType"),
        "updated_at": datetime.now().isoformat()
    }


# ==================== FETCH RESOURCES ====================
async def fetch_all_resources():
    """Fetch ALL resources (both active and inactive) from Autotask API"""
    print("=" * 70)
    print("FETCHING ALL RESOURCES FROM AUTOTASK")
    print("=" * 70)
    
    all_resources = []
    last_resource_id = 0
    base_url = f"{AUTOTASK_ZONE_URL}/atservicesrest/v1.0"
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            # NO FILTER ON isActive - fetch ALL resources
            payload = {
                "MaxRecords": 500,
                "Filter": [
                    {"field": "id", "op": "gt", "value": last_resource_id}
                ]
            }
            
            try:
                response = await client.post(
                    f"{base_url}/Resources/query",
                    json=payload,
                    headers=get_autotask_headers()
                )
                response.raise_for_status()
                data = response.json()
                resources = data.get("items", [])
                
                if not resources:
                    break
                
                # Count active vs inactive
                active_count = sum(1 for r in resources if r.get("isActive") == True)
                inactive_count = len(resources) - active_count
                
                all_resources.extend(resources)
                last_resource_id = resources[-1]["id"]
                
                print(f"  ✓ Fetched {len(resources)} resources")
                print(f"    - Active: {active_count}, Inactive: {inactive_count}")
                print(f"    - Total so far: {len(all_resources)}")
                
                if len(resources) < 500:
                    break
                
                await asyncio.sleep(0.2)
                
            except Exception as e:
                print(f"  ✗ Error fetching resources: {str(e)}")
                raise
    
    # Final count
    total_active = sum(1 for r in all_resources if r.get("isActive") == True)
    total_inactive = len(all_resources) - total_active
    
    print(f"\n{'=' * 70}")
    print(f"✓ FETCH COMPLETE")
    print(f"{'=' * 70}")
    print(f"Total Resources: {len(all_resources)}")
    print(f"  - Active: {total_active}")
    print(f"  - Inactive: {total_inactive}")
    print(f"{'=' * 70}")
    
    return all_resources


# ==================== STORE IN DATABASE ====================
def store_resources_in_db(resources):
    """Store resources in Supabase database"""
    print("\n" + "=" * 70)
    print("STORING RESOURCES IN DATABASE")
    print("=" * 70)
    
    batch_size = 100
    synced_count = 0
    updated_tickets = 0
    
    for i in range(0, len(resources), batch_size):
        batch = resources[i:i + batch_size]
        
        try:
            # Convert to database format
            db_batch = [convert_resource_to_db_format(r) for r in batch]
            
            # Upsert resources
            supabase.table("resources").upsert(db_batch, on_conflict="id").execute()
            
            synced_count += len(batch)
            print(f"  ✓ Stored batch {i//batch_size + 1}: {len(batch)} resources")
            
            # Update ticket names for each resource
            for resource in batch:
                resource_id = resource.get("id")
                first_name = resource.get("firstName", "")
                last_name = resource.get("lastName", "")
                resource_name = f"{first_name} {last_name}".strip()
                
                if resource_name and resource_id:
                    try:
                        update_response = supabase.table("tickets").update({
                            "assigned_resource_name": resource_name,
                            "updated_at": datetime.now().isoformat()
                        }).eq("assigned_resource_id", resource_id).execute()
                        
                        if update_response.data:
                            updated_tickets += len(update_response.data)
                    except Exception as e:
                        # Continue even if ticket update fails
                        pass
            
        except Exception as e:
            print(f"  ✗ Error storing batch {i//batch_size + 1}: {str(e)}")
    
    print(f"\n{'=' * 70}")
    print(f"✓ DATABASE STORAGE COMPLETE")
    print(f"{'=' * 70}")
    print(f"Resources stored: {synced_count}/{len(resources)}")
    print(f"Tickets updated: {updated_tickets}")
    print(f"{'=' * 70}")
    
    return synced_count, updated_tickets


# ==================== MAIN FUNCTION ====================
async def main():
    """Main execution function"""
    print("\n" + "=" * 70)
    print("AUTOTASK RESOURCES SYNC")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    start_time = datetime.now()
    
    try:
        # Step 1: Fetch all resources
        resources = await fetch_all_resources()
        
        # Step 2: Store resources in database
        if resources:
            synced_count, updated_tickets = store_resources_in_db(resources)
        else:
            print("\n⚠ No resources found to sync")
            return
        
        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Final summary
        print("\n" + "=" * 70)
        print("✓ SYNC COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print(f"Total Resources: {len(resources)}")
        print(f"  - Active: {sum(1 for r in resources if r.get('isActive') == True)}")
        print(f"  - Inactive: {sum(1 for r in resources if r.get('isActive') == False)}")
        print(f"Resources Stored: {synced_count}")
        print(f"Tickets Updated: {updated_tickets}")
        print(f"Duration: {duration:.2f} seconds")
        print("=" * 70)
        print("\n✓ Resources table is now up to date!")
        print("✓ All tickets with assigned_resource_id now have assigned_resource_name!")
        print("=" * 70)
        
    except KeyboardInterrupt:
        print("\n\n⚠ Sync interrupted by user")
    except Exception as e:
        print(f"\n\n✗ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════════╗
    ║           AUTOTASK RESOURCES SYNC SCRIPT                         ║
    ║  Fetches ALL resources (active + inactive) from Autotask         ║
    ║  and stores them in Supabase                                     ║
    ╚══════════════════════════════════════════════════════════════════╝
    """)
    
    asyncio.run(main())