# main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from starlette.middleware.cors import CORSMiddleware
from pathlib import Path
import tempfile
from src.services.full_statement_xls import parse_full_statement_xls
from src.utils import logger
from src.routers import parse

app = FastAPI(title="Sberbank Broker Report Parser", version="0.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(parse.router)


@app.get("/health", name='Проверка связи')
def health() -> dict:
    return {"status": "ok"}


@app.post("/parse_statement")
async def parse_statement(file: UploadFile = File(...)):
    """
    Автоматически определяет тип отчета (XLS/XLSX) и парсит его.
    Поддерживает отчеты Сбербанка в форматах Excel.
    """
    filename = file.filename.lower() if file.filename else ""
    file_ext = Path(filename).suffix

    if file_ext in ('.xls', '.xlsx'):
        return await _parse_xls_file(file)
    try:
        content = await file.read()
        await file.seek(0)

        return await _parse_xls_file(file)
    except Exception as e:
        logger.warning("Не удалось определить тип файла по содержимому: %s", e)

    raise HTTPException(
        status_code=400,
        detail=f"Неподдерживаемый формат файла. Поддерживаются только .xls, .xlsx и .xml файлы. Текущее расширение: {file_ext}"
    )


async def _parse_xls_file(file: UploadFile):
    """Внутренняя функция для парсинга XLS/XLSX файлов"""
    if not file.filename.lower().endswith(('.xls', '.xlsx')):
        raise HTTPException(status_code=400, detail="Поддерживаются только .xls и .xlsx файлы")

    logger.info("Получен XLS файл: %s", file.filename)

    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        result = parse_full_statement_xls(tmp_path, original_filename=file.filename)
        if "error" in result.get("meta", {}):
            raise HTTPException(status_code=500, detail=result["meta"]["error"])
        return JSONResponse(content=jsonable_encoder(result))
    finally:
        Path(tmp_path).unlink(missing_ok=True)
