# src/routers/parse.py
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pathlib import Path
import tempfile

from src.services.full_statement_xls import parse_full_statement_xls

router = APIRouter(prefix="/parse", tags=["Парсинг отчётов"])


@router.post("/xls")
async def parse_xls(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(('.xls', '.xlsx')):
        raise HTTPException(status_code=400, detail="Поддерживаются только .xls и .xlsx файлы")

    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        result = parse_full_statement_xls(tmp_path)
        if "error" in result.get("meta", {}):
            raise HTTPException(status_code=500, detail=result["meta"]["error"])
        return JSONResponse(content=jsonable_encoder(result))
    finally:
        Path(tmp_path).unlink(missing_ok=True)