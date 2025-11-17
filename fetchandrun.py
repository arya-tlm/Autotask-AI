"""
Standalone Script to Fetch Resources and Contacts from Autotask and Store in Supabase
Run this script independently to sync all resources and contacts

Usage: python fetch_and_store_resources_contacts.py
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
        "travel_availability_pct": resource.get("travelAvailabilityPct"),
        "user_name": resource.get("userName", ""),
        "user_type": resource.get("userType"),
        "updated_at": datetime.now().isoformat()
    }


def convert_contact_to_db_format(contact: dict) -> dict:
    """Convert Autotask contact format to database format"""
    return {
        "id": contact.get("id"),
        "additional_address_information": contact.get("additionalAddressInformation", ""),
        "address_line": contact.get("addressLine", ""),
        "address_line1": contact.get("addressLine1", ""),
        "alternate_phone": contact.get("alternatePhone", ""),
        "api_vendor_id": contact.get("apiVendorID"),
        "bulk_email_opt_out_time": contact.get("bulkEmailOptOutTime"),
        "city": contact.get("city", ""),
        "company_id": contact.get("companyID"),
        "company_location_id": contact.get("companyLocationID"),
        "country_id": contact.get("countryID"),
        "create_date": contact.get("createDate"),
        "email_address": contact.get("emailAddress", ""),
        "email_address2": contact.get("emailAddress2"),
        "email_address3": contact.get("emailAddress3"),
        "extension": contact.get("extension", ""),
        "external_id": contact.get("externalID", ""),
        "facebook_url": contact.get("facebookUrl", ""),
        "fax_number": contact.get("faxNumber", ""),
        "first_name": contact.get("firstName", ""),
        "impersonator_creator_resource_id": contact.get("impersonatorCreatorResourceID"),
        "is_active": contact.get("isActive", 1),
        "is_opted_out_from_bulk_email": contact.get("isOptedOutFromBulkEmail", False),
        "last_activity_date": contact.get("lastActivityDate"),
        "last_modified_date": contact.get("lastModifiedDate"),
        "last_name": contact.get("lastName", ""),
        "linked_in_url": contact.get("linkedInUrl", ""),
        "middle_initial": contact.get("middleInitial"),
        "mobile_phone": contact.get("mobilePhone", ""),
        "name_prefix": contact.get("namePrefix"),
        "name_suffix": contact.get("nameSuffix"),
        "note": contact.get("note", ""),
        "receives_email_notifications": contact.get("receivesEmailNotifications", False),
        "phone": contact.get("phone", ""),
        "primary_contact": contact.get("primaryContact", False),
        "billing_contact": contact.get("billingContact", False),
        "room_number": contact.get("roomNumber", ""),
        "solicitation_opt_out": contact.get("solicitationOptOut", False),
        "solicitation_opt_out_time": contact.get("solicitationOptOutTime"),
        "state": contact.get("state", ""),
        "survey_opt_out": contact.get("surveyOptOut", False),
        "title": contact.get("title", ""),
        "twitter_url": contact.get("twitterUrl", ""),
        "zip_code": contact.get("zipCode", ""),
        "updated_at": datetime.now().isoformat()
    }


# ==================== FETCH RESOURCES ====================
async def fetch_all_resources():
    """Fetch all active resources from Autotask API"""
    print("=" * 70)
    print("FETCHING RESOURCES FROM AUTOTASK")
    print("=" * 70)
    
    all_resources = []
    last_resource_id = 0
    base_url = f"{AUTOTASK_ZONE_URL}/atservicesrest/v1.0"
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            payload = {
                "MaxRecords": 500,
                "Filter": [
                    {"field": "id", "op": "gt", "value": last_resource_id},
                    {"field": "isActive", "op": "eq", "value": True}
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
                
                all_resources.extend(resources)
                last_resource_id = resources[-1]["id"]
                
                print(f"  ✓ Fetched {len(resources)} resources (Total: {len(all_resources)})")
                
                if len(resources) < 500:
                    break
                
                await asyncio.sleep(0.2)
                
            except Exception as e:
                print(f"  ✗ Error fetching resources: {str(e)}")
                raise
    
    print(f"\n✓ Total resources fetched: {len(all_resources)}")
    return all_resources


# ==================== FETCH CONTACTS ====================
async def fetch_all_contacts():
    """Fetch all active contacts from Autotask API"""
    print("\n" + "=" * 70)
    print("FETCHING CONTACTS FROM AUTOTASK")
    print("=" * 70)
    
    all_contacts = []
    last_contact_id = 0
    base_url = f"{AUTOTASK_ZONE_URL}/atservicesrest/v1.0"
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            payload = {
                "MaxRecords": 500,
                "Filter": [
                    {"field": "id", "op": "gt", "value": last_contact_id},
                    {"field": "isActive", "op": "eq", "value": 1}
                ]
            }
            
            try:
                response = await client.post(
                    f"{base_url}/Contacts/query",
                    json=payload,
                    headers=get_autotask_headers()
                )
                response.raise_for_status()
                data = response.json()
                contacts = data.get("items", [])
                
                if not contacts:
                    break
                
                all_contacts.extend(contacts)
                last_contact_id = contacts[-1]["id"]
                
                print(f"  ✓ Fetched {len(contacts)} contacts (Total: {len(all_contacts)})")
                
                if len(contacts) < 500:
                    break
                
                await asyncio.sleep(0.2)
                
            except Exception as e:
                print(f"  ✗ Error fetching contacts: {str(e)}")
                raise
    
    print(f"\n✓ Total contacts fetched: {len(all_contacts)}")
    return all_contacts


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
                    update_response = supabase.table("tickets").update({
                        "assigned_resource_name": resource_name,
                        "updated_at": datetime.now().isoformat()
                    }).eq("assigned_resource_id", resource_id).execute()
                    
                    if update_response.data:
                        updated_tickets += len(update_response.data)
            
        except Exception as e:
            print(f"  ✗ Error storing batch {i//batch_size + 1}: {str(e)}")
    
    print(f"\n✓ Resources stored: {synced_count}")
    print(f"✓ Tickets updated: {updated_tickets}")
    return synced_count, updated_tickets


def store_contacts_in_db(contacts):
    """Store contacts in Supabase database"""
    print("\n" + "=" * 70)
    print("STORING CONTACTS IN DATABASE")
    print("=" * 70)
    
    batch_size = 100
    synced_count = 0
    updated_tickets = 0
    
    for i in range(0, len(contacts), batch_size):
        batch = contacts[i:i + batch_size]
        
        try:
            # Convert to database format
            db_batch = [convert_contact_to_db_format(c) for c in batch]
            
            # Upsert contacts
            supabase.table("contacts").upsert(db_batch, on_conflict="id").execute()
            
            synced_count += len(batch)
            print(f"  ✓ Stored batch {i//batch_size + 1}: {len(batch)} contacts")
            
            # Update ticket names for each contact
            for contact in batch:
                contact_id = contact.get("id")
                first_name = contact.get("firstName", "")
                last_name = contact.get("lastName", "")
                contact_name = f"{first_name} {last_name}".strip()
                
                if contact_name and contact_id:
                    update_response = supabase.table("tickets").update({
                        "contact_name": contact_name,
                        "updated_at": datetime.now().isoformat()
                    }).eq("contact_id", contact_id).execute()
                    
                    if update_response.data:
                        updated_tickets += len(update_response.data)
            
        except Exception as e:
            print(f"  ✗ Error storing batch {i//batch_size + 1}: {str(e)}")
    
    print(f"\n✓ Contacts stored: {synced_count}")
    print(f"✓ Tickets updated: {updated_tickets}")
    return synced_count, updated_tickets


# ==================== MAIN FUNCTION ====================
async def main():
    """Main execution function"""
    print("\n" + "=" * 70)
    print("AUTOTASK RESOURCES & CONTACTS SYNC")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    start_time = datetime.now()
    
    try:
        # Step 1: Fetch resources
        resources = await fetch_all_resources()
        
        # Step 2: Store resources
        if resources:
            store_resources_in_db(resources)
        
        # Step 3: Fetch contacts
        contacts = await fetch_all_contacts()
        
        # Step 4: Store contacts
        if contacts:
            store_contacts_in_db(contacts)
        
        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Final summary
        print("\n" + "=" * 70)
        print("✓ SYNC COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print(f"Resources synced: {len(resources)}")
        print(f"Contacts synced: {len(contacts)}")
        print(f"Duration: {duration:.2f} seconds")
        print("=" * 70)
        print("\nNow when you insert new tickets, the contact_name and")
        print("assigned_resource_name will be automatically populated!")
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
    ║  AUTOTASK RESOURCES & CONTACTS SYNC SCRIPT                       ║
    ║  This will fetch all resources and contacts from Autotask        ║
    ║  and store them in your Supabase database                        ║
    ╚══════════════════════════════════════════════════════════════════╝
    """)
    
    asyncio.run(main())