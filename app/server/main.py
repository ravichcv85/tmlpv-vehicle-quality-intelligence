"""FastAPI application entry point."""
import os
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from server.routes import complaints, inspections, checklist_agent, pipeline, metrics


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown."""
    print("TMLPV Vehicle Quality Intelligence starting up...")
    yield
    print("TMLPV Vehicle Quality Intelligence shutting down...")


app = FastAPI(
    title="TMLPV Vehicle Quality Intelligence",
    description="Vehicle quality management system for Tata Motors",
    version="1.0.0",
    lifespan=lifespan,
)

# Register API routers
app.include_router(complaints.router)
app.include_router(inspections.router)
app.include_router(checklist_agent.router)
app.include_router(pipeline.router)
app.include_router(metrics.router)


# Health check
@app.get("/api/health")
async def health():
    return {"status": "healthy", "app": "tmlpv-vehicle-quality"}


# Serve React frontend
frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"

if frontend_dist.exists():
    # Serve static assets (JS, CSS, images)
    assets_dir = frontend_dist / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    # Serve other static files at root level (favicon, etc.)
    @app.get("/vite.svg")
    async def vite_svg():
        svg_path = frontend_dist / "vite.svg"
        if svg_path.exists():
            return FileResponse(str(svg_path))

    # SPA fallback - serve index.html for all non-API routes
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        # Don't intercept API calls
        if full_path.startswith("api/"):
            return {"error": "Not found"}, 404
        index_path = frontend_dist / "index.html"
        if index_path.exists():
            return FileResponse(str(index_path))
        return {"error": "Frontend not built. Run: cd frontend && npm run build"}
else:
    @app.get("/")
    async def no_frontend():
        return {
            "message": "TMLPV Vehicle Quality API is running. Frontend not built yet.",
            "docs": "/docs",
        }
