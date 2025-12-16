from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Any, Tuple, Set
from datetime import datetime
import pandas as pd
from src.parsers.xls_parsers.xls_fin_ops import XlsFinancialOperationsParser
from src.parsers.xls_parsers.xls_trades import parse_trades_from_xls
from src.parsers.xls_parsers.xls_transfers import parse_transfers_from_xls
from src.utils import logger, extract_min_max_dates
from src.OperationDTO import OperationDTO


def _extract_account_ids(file_path: str) -> List[str]:
    """
    Извлекает ВСЕ уникальные значения из колонки 'Номер договора'
    в листах 'Движение ДС' и 'Сделки'.
    """
    account_ids: Set[str] = set()
    engine = "openpyxl" if file_path.lower().endswith(".xlsx") else "xlrd"

    try:
        with pd.ExcelFile(file_path, engine=engine) as xls:
            for sheet_name in xls.sheet_names:
                if "движение дс" in sheet_name.lower() or "сделки" in sheet_name.lower():
                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    if df.empty or df.shape[1] == 0:
                        continue

                    headers = [str(col).strip().lower() for col in df.columns]
                    try:
                        col_idx = headers.index("номер договора")
                    except ValueError:
                        col_idx = None
                        for i, h in enumerate(headers):
                            if "номер договора" in h:
                                col_idx = i
                                break
                        if col_idx is None:
                            continue

                    for val in df.iloc[:, col_idx]:
                        if pd.notna(val) and str(val).strip():
                            account_ids.add(str(val).strip())
    except Exception as e:
        logger.debug(f"Не удалось извлечь account_id: {e}")

    return sorted(account_ids)

def _op_key(op: OperationDTO) -> str:
    oid = (op.operation_id or "").strip()
    if oid:
        return f"id:{oid}"
    date_part = ""
    if isinstance(op.date, datetime):
        date_part = op.date.isoformat()
    else:
        date_part = str(op.date or "")
    try:
        sum_part = float(op.payment_sum) if op.payment_sum not in (None, "") else 0.0
    except Exception:
        sum_part = str(op.payment_sum or "")
    return f"auto:{date_part}|{op.operation_type}|{sum_part}|{op.ticker or ''}|{op.isin or ''}"


def _dedupe_ops(ops: List[OperationDTO]) -> Tuple[List[OperationDTO], int]:
    seen = set()
    deduped: List[OperationDTO] = []
    for o in ops:
        k = _op_key(o)
        if k in seen:
            continue
        seen.add(k)
        deduped.append(o)
    return deduped, len(deduped)


def _sort_key_for_operation(op_dict: Dict[str, Any]) -> tuple:
    date_val = op_dict.get("date")
    op_type = op_dict.get("operation_type", "")
    if isinstance(date_val, datetime):
        dt = date_val
    elif isinstance(date_val, str):
        try:
            dt = datetime.fromisoformat(date_val)
        except Exception:
            try:
                dt = datetime.strptime(date_val.split()[0], "%d.%m.%Y")
            except Exception:
                dt = datetime.min
    else:
        dt = datetime.min
    return (dt, op_type)


def parse_full_statement_xls(file_path: str, original_filename: str = "") -> Dict[str, Any]:
    filename_to_log = original_filename or Path(file_path).name
    logger.info(f"Парсинг XLS: {filename_to_log}")

    account_ids = _extract_account_ids(file_path)

    fin_parser = XlsFinancialOperationsParser()
    fin_operations, fin_stats = fin_parser.parse(file_path)

    trades, trade_stats = parse_trades_from_xls(file_path)
    if "error" in trade_stats:
        logger.warning(f"Ошибка сделок: {trade_stats['error']}")
        trades = []
        trade_stats = {"parsed": 0, "total_rows": 0, "total_commission": 0.0}

    transfers, transfer_stats = parse_transfers_from_xls(file_path)
    if "error" in transfer_stats:
        logger.warning(f"Ошибка неторговых операций: {transfer_stats['error']}")
        transfers = []
        transfer_stats = {"parsed": 0, "total_rows": 0}

    deduped_fin, _ = _dedupe_ops(fin_operations)
    deduped_trades, _ = _dedupe_ops(trades)
    deduped_transfers, _ = _dedupe_ops(transfers)
    combined_ops_dto = deduped_fin + deduped_trades + deduped_transfers

    date_start, date_end = extract_min_max_dates(combined_ops_dto)

    operations_dicts = [op.to_dict() for op in combined_ops_dto]
    try:
        operations_dicts.sort(key=_sort_key_for_operation)
    except Exception as e:
        logger.warning(f"Ошибка сортировки: {e}")

    meta = {
        "fin_ops_raw_count": fin_stats.get("total_rows", 0),
        "trade_ops_raw_count": trade_stats.get("total_rows", 0),
        "transfer_ops_raw_count": transfer_stats.get("total_rows", 0),
        "total_ops_count": len(combined_ops_dto),
        "fin_ops_stats": fin_stats,
        "trade_ops_stats": trade_stats,
        "transfer_ops_stats": transfer_stats,
        "detected_sheets": {
            "fin_sheet": fin_stats.get("detected_sheet", ""),
            "trades_sheet": trade_stats.get("detected_sheet", ""),
            "transfers_sheet": transfer_stats.get("detected_sheet", "")
        },
    }

    total_ops = len(combined_ops_dto)
    fin_count = len(deduped_fin)
    trades_count = len(deduped_trades)
    logger.info(
        f"Парсинг завершен: {total_ops} операций "
        f"(фин: {fin_count}, сделки: {trades_count}), "
        f"account_ids: {account_ids}"
    )

    return {
        "account_id": account_ids,
        "date_start": date_start,
        "date_end": date_end,
        "operations": operations_dicts,
        "meta": meta,
    }