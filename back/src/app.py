from fastapi import FastAPI
from src.routers import stations, statistiques
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

# Création de l'application FastAPI
app = FastAPI(
    title="API Prix Carburants",
    description="API pour gérer et exposer les données des prix de carburants",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")

# mes routes
app.include_router(stations.router)
app.include_router(statistiques.router)
