from fastapi import FastAPI

import logging_config

from application import data_router, etl_router

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "CfbStats Data Handling API"}


app.include_router(data_router.router)
app.include_router(etl_router.router)
