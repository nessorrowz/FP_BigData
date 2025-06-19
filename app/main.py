from fastapi import FastAPI
from app.models.user import Base
from app.database import engine
from app.routers import user

Base.metadata.create_all(bind=engine)

app = FastAPI()

app.include_router(user.router)

@app.get("/")
def root():
    return {"message": "API is running"}