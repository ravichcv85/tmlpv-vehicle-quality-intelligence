"""TMLPV Vehicle Quality Intelligence - FastAPI Application."""
import uvicorn
from server.main import app  # noqa: F401

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
