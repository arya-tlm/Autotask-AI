"""
Main FastAPI Application
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.config import get_settings
from app.api.routes import sync, chat, health
from app.api.routes.solutions import router as solutions_router
from app.api.routes.summaries import router as summaries_router
from app.services.mcp_chat import router as mcp_router

settings = get_settings()

# Initialize FastAPI app 
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Syncing Autotask tickets and AI-powered chat queries",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(sync.router)
app.include_router(chat.router)
app.include_router(health.router)
app.include_router(solutions_router)
app.include_router(summaries_router)
app.include_router(mcp_router)

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": settings.app_name,
        "version": settings.app_version,
        "status": "running",
        "docs": "/docs",
        "endpoints": {
            "sync": {
                "POST /sync/last-7-days": "Sync tickets from last 7 days",
                "POST /sync/last-30-days": "Sync tickets from last 30 days",
                "POST /sync/custom": "Sync tickets with custom date range"
            },
            "chat": {
                "POST /chat": "Chat with AI about tickets"
            },
            "solutions": {
                "POST /solutions/search": "Search for ticket solutions",
                "GET /solutions/ticket/{ticket_number}": "Get solution by ticket number",
                "GET /solutions/stats": "Get solution statistics"
            },
            "summaries": {
                "GET /summaries/list": "Get AI summaries of latest 200 tickets",
                "POST /summaries/regenerate/{ticket_id}": "Regenerate summary for specific ticket"
            }
        }
    }


@app.on_event("startup")
async def startup_event():
    """Run on application startup"""
    print("\n" + "="*60)
    print(f"Starting {settings.app_name} v{settings.app_version}")
    print("="*60)
    print(f"Supabase URL: {settings.supabase_url}")
    print(f"OpenAI: {'Configured' if settings.openai_api_key else 'NOT CONFIGURED'}")
    print(f"Autotask Zone: {settings.autotask_zone_url}")
    print("="*60 + "\n")
    
    # Verify database connection
    from app.services.database import get_database_service
    db = get_database_service()
    if db.health_check():
        print("[OK] Database connection verified")
    else:
        print("[ERROR] WARNING: Database connection failed")


@app.on_event("shutdown")
async def shutdown_event():
    """Run on application shutdown"""
    print("\n" + "="*60)
    print("Shutting down application...") 
    print("="*60 + "\n")
