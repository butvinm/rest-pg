"""Uvicorn entrypoint."""

from app.api_v1.factory import create_app

app = create_app()
