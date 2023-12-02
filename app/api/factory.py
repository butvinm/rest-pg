"""FastAPI instances factory."""


from fastapi import APIRouter, FastAPI


def create_app() -> FastAPI:
    """Create configured FastAPI instance."""
    root_router = APIRouter(prefix='/api')
    app = FastAPI(title='RestPG')
    app.include_router(root_router)
    return app
