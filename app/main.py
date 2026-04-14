from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="uc-catalog-mcp")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/mcp")
async def mcp_stub():
    return JSONResponse(status_code=501, content={"detail": "not implemented"})
