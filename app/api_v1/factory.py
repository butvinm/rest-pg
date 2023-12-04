"""FastAPI instances factory."""


from fastapi import APIRouter, FastAPI

from app.api_v1.routes.tables import router as tables_router


def create_app() -> FastAPI:
    """Create configured FastAPI instance."""
    root_router = APIRouter(prefix='/api/v1')
    root_router.include_router(tables_router)

    app = FastAPI(title='RestPG')
    app.include_router(root_router)
    return app
