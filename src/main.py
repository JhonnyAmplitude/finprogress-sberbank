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
    Автоматически определяет тип отчета (XLS/XLSX или XML) и парсит его.
    Поддерживает отчеты Альфа-Банка в форматах Excel и XML.
    """
    filename = file.filename.lower() if file.filename else ""
    file_ext = Path(filename).suffix

    if file_ext in ('.xls', '.xlsx'):
        return await _parse_xls_file(file)
    elif file_ext == '.xml':
        return await _parse_xml_file(file)

    try:
        content = await file.read()
        await file.seek(0)

        content_preview = content[:200].decode('utf-8', errors='ignore').lower()
        if '<?xml' in content_preview or '<report' in content_preview or '<broker' in content_preview:
            return await _parse_xml_file(file)

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

async def _parse_xml_file(file: UploadFile):
    """Внутренняя функция для парсинга XML файлов"""
    filename = Path(file.filename).name if file.filename else "uploaded.xml"
    logger.info("Получен XML файл: %s", filename)

    try:
        content = await file.read()
        if not content:
            raise ValueError("Empty file")
    except Exception as e:
        logger.exception("Ошибка чтения XML файла: %s", e)
        raise HTTPException(status_code=400, detail="Не удалось прочитать файл")

    try:
        result = parse_full_statement_xml(content)
    except Exception as e:
        logger.exception("Ошибка парсинга XML: %s", e)
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга: {e}")

    meta = result.get("meta", {})
    ops_count = len(result.get("operations", []))
    trade_parsed = meta.get("trade_ops_stats", {}).get("parsed", 0)
    trade_raw = meta.get("trade_ops_raw_count", 0)
    fin_parsed = meta.get("fin_ops_stats", {}).get("parsed", 0)
    fin_raw = meta.get("fin_ops_raw_count", 0)
    transfer_parsed = meta.get("transfer_ops_stats", {}).get("parsed", 0)
    transfer_raw = meta.get("transfer_ops_raw_count", 0)

    logger.info(
        "%s → операций всего: %s | сделки: %s/%s | фин: %s/%s | конвертации: %s/%s",
        filename,
        ops_count,
        trade_parsed, trade_raw,
        fin_parsed, fin_raw,
        transfer_parsed, transfer_raw,
    )

    return JSONResponse(content=jsonable_encoder(result))