import pandas as pd
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional
import re
import logging
from pathlib import Path
from src.OperationDTO import OperationDTO
from src.utils import logger, to_float_safe


class XlsTradesParser:
    """Парсер сделок из Excel отчёта Сбербанка (лист 'Сделки')"""

    SHEET_KEYWORDS = ["сделки", "заявки и сделки"]

    HEADER_KEYWORDS = {
        "trade_id": ["номер сделки"],
        "date_conclusion": ["дата заключения"],
        "operation": ["операция"],
        "isin_or_ticker": ["код финансового инструмента"],
        "asset_type": ["тип финансового инструмента"],  # не используется
        "quantity": ["количество"],
        "price": ["цена"],
        "amount": ["объем сделки"],
        "currency": ["валюта"],
        "commission": ["комиссия"],
    }

    def __init__(self):
        self.stats = {
            "total_rows": 0,
            "parsed": 0,
            "skipped_no_date": 0,
            "skipped_no_qty": 0,
            "skipped_invalid": 0,
            "total_commission": 0.0,
            "detected_sheet": "",
            "column_mapping": {},
        }

    def parse(self, file_path: str) -> Tuple[List[OperationDTO], Dict[str, Any]]:
        """Основной метод парсинга сделок из Excel файла"""
        try:
            engine = self._detect_engine(file_path)
            with pd.ExcelFile(file_path, engine=engine) as excel_file:
                sheet_name = self._find_trades_sheet(excel_file.sheet_names)
                self.stats["detected_sheet"] = sheet_name

                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                df = self._preprocess_dataframe(df)
                column_mapping = self._detect_columns(df)
                self.stats["column_mapping"] = column_mapping

                operations = self._process_rows(df, column_mapping)

            logger.info(
                "Получено %s сделок (проверено %s строк). Итого комиссия=%s",
                self.stats["parsed"],
                self.stats["total_rows"],
                self.stats["total_commission"],
            )
            return operations, self.stats.copy()
        except Exception as e:
            logger.exception("Ошибка при парсинге сделок из Excel: %s", e)
            error_stats = {"error": str(e), **self.stats}
            return [], error_stats

    def _detect_engine(self, file_path: str) -> str:
        if file_path.lower().endswith('.xls'):
            return 'xlrd'
        elif file_path.lower().endswith('.xlsx'):
            return 'openpyxl'
        else:
            raise ValueError(f"Неподдерживаемый формат файла: {Path(file_path).suffix}")

    def _find_trades_sheet(self, sheet_names: List[str]) -> str:
        for name in sheet_names:
            name_lower = name.lower()
            if any(keyword in name_lower for keyword in self.SHEET_KEYWORDS):
                return name
        fallback_names = ["Сделки", "Завершенные сделки", "Trades"]
        for name in fallback_names:
            if name in sheet_names:
                return name
        if sheet_names:
            logger.warning("Не удалось определить лист со сделками. Используется первый лист: %s", sheet_names[0])
            return sheet_names[0]
        raise ValueError("Не найдено ни одного листа в файле Excel")

    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(how='all').reset_index(drop=True)
        return df

    def _detect_columns(self, df: pd.DataFrame) -> Dict[str, int]:
        headers = [str(col).strip().lower() for col in df.columns]
        column_mapping = {}

        for field, keywords in self.HEADER_KEYWORDS.items():
            for idx, h in enumerate(headers):
                if any(kw in h for kw in keywords):
                    column_mapping[field] = idx
                    break

        required = ["date_conclusion", "operation", "isin_or_ticker", "quantity", "amount", "currency"]
        missing = [r for r in required if r not in column_mapping]
        if missing:
            raise ValueError(f"Не найдены обязательные колонки: {missing}. Заголовки: {df.columns.tolist()}")

        return column_mapping

    def _is_isin(self, s: str) -> bool:
        """Проверяет, соответствует ли строка формату ISIN (12 символов, AA + 9 alnum + 1 digit)."""
        if not isinstance(s, str) or len(s) != 12:
            return False
        return bool(re.fullmatch(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$', s))

    def _extract_field(self, row: pd.Series, column_mapping: Dict[str, int], field_name: str) -> str:
        if field_name not in column_mapping:
            return ""
        idx = column_mapping[field_name]
        if idx >= len(row):
            return ""
        val = row.iloc[idx]
        return str(val).strip() if pd.notna(val) else ""

    def _process_rows(self, df: pd.DataFrame, column_mapping: Dict[str, int]) -> List[OperationDTO]:
        operations = []
        for i in range(len(df)):
            row = df.iloc[i]
            self.stats["total_rows"] += 1
            try:
                op = self._process_row(row, column_mapping)
                if op:
                    operations.append(op)
            except Exception as e:
                logger.debug(f"Ошибка в строке сделки {i}: {e}")
                self.stats["skipped_invalid"] += 1
        return operations

    def _process_row(self, row: pd.Series, column_mapping: Dict[str, int]) -> Optional[OperationDTO]:
        # Дата
        date_str = self._extract_field(row, column_mapping, "date_conclusion")
        if not date_str:
            self.stats["skipped_no_date"] += 1
            return None
        try:
            # Поддержка формата "21,12,2023 17:36:41"
            date_clean = date_str.replace(",", ".")
            trade_date = pd.to_datetime(date_clean, dayfirst=True)
        except Exception:
            self.stats["skipped_no_date"] += 1
            return None

        # Основные поля
        qty = to_float_safe(self._extract_field(row, column_mapping, "quantity"))
        if qty == 0:
            self.stats["skipped_no_qty"] += 1
            return None

        amount = to_float_safe(self._extract_field(row, column_mapping, "amount"))
        price = to_float_safe(self._extract_field(row, column_mapping, "price"))
        currency_raw = self._extract_field(row, column_mapping, "currency")
        currency = "RUB" if currency_raw.upper() in ("RUR", "РУБ", "РУБЛЬ") else currency_raw.upper()

        commission = to_float_safe(self._extract_field(row, column_mapping, "commission"))

        # Тип операции — строго по колонке "Операция"
        operation_raw = self._extract_field(row, column_mapping, "operation")
        if "покупка" in operation_raw.lower():
            operation_type = "buy"
        elif "продажа" in operation_raw.lower():
            operation_type = "sale"
        else:
            # Fallback на количество (на случай аномалий)
            operation_type = "buy" if qty > 0 else "sale"

        # Код инструмента: ISIN или тикер
        code = self._extract_field(row, column_mapping, "isin_or_ticker")
        isin = ""
        ticker = ""
        if code:
            if self._is_isin(code):
                isin = code
            else:
                ticker = code

        trade_id = self._extract_field(row, column_mapping, "trade_id")

        dto = OperationDTO(
            date=trade_date,
            operation_type=operation_type,
            payment_sum=abs(amount),
            currency=currency,
            ticker=ticker,
            isin=isin,
            reg_number="",
            price=price,
            quantity=abs(qty),
            aci=0.0,
            comment="",
            operation_id=trade_id,
            commission=commission,
        )

        self.stats["parsed"] += 1
        self.stats["total_commission"] += float(commission or 0.0)
        return dto


def parse_trades_from_xls(file_path: str) -> Tuple[List[OperationDTO], Dict[str, Any]]:
    """Публичный интерфейс для парсинга сделок из XLS/XLSX отчёта"""
    parser = XlsTradesParser()
    return parser.parse(file_path)