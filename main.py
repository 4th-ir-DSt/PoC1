"""Main FastAPI application for the Logical Data Modeling Assistant."""

import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from api.v1.routers import conversation, health
from core.config import settings
from core.logging_config import setup_logging, get_logger
from core.exceptions import DataModelingException, LLMServiceException, ValidationException, StorageException

# Setup logging
logger = setup_logging()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    logger.info(f"Starting {settings.app_name} v{settings.app_version}")
    logger.info(f"Environment: {'DEBUG' if settings.debug else 'PRODUCTION'}")
    logger.info(f"Server: {settings.host}:{settings.port}")
    
    yield
    
    # Shutdown
    logger.info(f"Shutting down {settings.app_name}")

# Create FastAPI app with lifespan
app = FastAPI(
    title=settings.app_name,
    description="Generate and iteratively refine logical data models via chat.",
    version=settings.app_version,
    debug=settings.debug,
    lifespan=lifespan
)

# Add security middleware
if not settings.debug:
    app.add_middleware(
        TrustedHostMiddleware, 
        allowed_hosts=["*"]  # Configure appropriately for production
    )

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=settings.cors_credentials,
    allow_methods=settings.cors_methods,
    allow_headers=settings.cors_headers,
)

# Request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """Add processing time to response headers."""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    
    # Log request details
    logger.info(
        f"{request.method} {request.url.path} - "
        f"Status: {response.status_code} - "
        f"Time: {process_time:.3f}s"
    )
    
    return response

# Global exception handler
@app.exception_handler(DataModelingException)
async def data_modeling_exception_handler(request: Request, exc: DataModelingException):
    """Handle custom application exceptions."""
    logger.error(f"Data modeling exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
    )

@app.exception_handler(LLMServiceException)
async def llm_service_exception_handler(request: Request, exc: LLMServiceException):
    """Handle LLM service exceptions."""
    logger.error(f"LLM service exception: {exc}")
    return JSONResponse(
        status_code=503,
        content={"error": "Service temporarily unavailable", "detail": str(exc)}
    )

@app.exception_handler(ValidationException)
async def validation_exception_handler(request: Request, exc: ValidationException):
    """Handle validation exceptions."""
    logger.warning(f"Validation exception: {exc}")
    return JSONResponse(
        status_code=400,
        content={"error": "Bad request", "detail": str(exc)}
    )

@app.exception_handler(StorageException)
async def storage_exception_handler(request: Request, exc: StorageException):
    """Handle storage exceptions."""
    logger.error(f"Storage exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle general exceptions."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": "An unexpected error occurred"}
    )

# Include routers
app.include_router(health.router, prefix="/api/v1")
app.include_router(conversation.router, prefix="/api/v1")

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting application server...")
    uvicorn.run(
        app, 
        host=settings.host, 
        port=settings.port,
        log_level=settings.log_level.lower()
    ) 