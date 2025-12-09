import pandas as pd
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional
import re
import logging
from pathlib import Path
from src.OperationDTO import OperationDTO
from src.utils import logger, to_float_safe


class XlsTradesParser:
    """Парсер сделок из Excel отчетов брокера (лист 'Завершенные сделки')"""

    SHEET_KEYWORDS = ["сделки"]

    HEADER_KEYWORDS = {
        "trade_id": ["№ сделки", "номер сделки", "id сделки"],
        "date_conclusion": ["дата заключения", "заключен"],
        "date_settlement": ["дата расчетов", "расчетов"],
        "place": ["место заключения", "место"],
        "isin": ["isin", "рег.код", "код"],
        "asset_name": ["актив", "наименование"],
        "quantity": ["количество", "шт./грамм", "объем"],
        "price": ["цена"],
        "amount": ["сумма сделки", "стоимость"],
        "nkd": ["нкд", "в т.ч. нкд"],
        "currency": ["валюта расчетов", "валюта"],
        "commission": ["комиссия банка", "комиссия"],
        "commission_currency": ["валюта комиссии"],
        "comment": ["коммент", "примечание"]
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
            "column_mapping": {}
        }

    def parse(self, file_path: str) -> Tuple[List[OperationDTO], Dict[str, Any]]:
        """Основной метод парсинга сделок из Excel файла"""
        try:
            engine = self._detect_engine(file_path)
            excel_file = pd.ExcelFile(file_path, engine=engine)
            sheet_name = self._find_trades_sheet(excel_file.sheet_names)
            self.stats["detected_sheet"] = sheet_name

            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            df = self._preprocess_dataframe(df)
            column_mapping = self._detect_columns(df)
            self.stats["column_mapping"] = column_mapping

            operations = self._process_rows(df, column_mapping)

            logger.info("Получено %s сделок (проверено %s строк). Итого комиссия=%s",
                        self.stats["parsed"], self.stats["total_rows"], self.stats["total_commission"])
            return operations, self.stats.copy()
        except Exception as e:
            logger.exception("Ошибка при парсинге сделок из Excel: %s", e)
            error_stats = {"error": str(e), **self.stats}
            return [], error_stats

    def _detect_engine(self, file_path: str) -> str:
        """Определяет движок для чтения Excel файла"""
        if file_path.lower().endswith('.xls'):
            return 'xlrd'
        elif file_path.lower().endswith('.xlsx'):
            return 'openpyxl'
        else:
            raise ValueError(f"Неподдерживаемый формат файла: {Path(file_path).suffix}")

    def _find_trades_sheet(self, sheet_names: List[str]) -> str:
        """Находит лист с завершенными сделками"""
        for name in sheet_names:
            name_lower = name.lower()
            if any(keyword in name_lower for keyword in self.SHEET_KEYWORDS):
                return name

        fallback_names = ["Завершенные сделки", "Сделки", "Trades"]
        for name in fallback_names:
            if name in sheet_names:
                return name

        if sheet_names:
            logger.warning("Не удалось определить лист со сделками. Используется первый лист: %s", sheet_names[0])
            return sheet_names[0]

        raise ValueError("Не найдено ни одного листа в файле Excel")

    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Предварительная обработка DataFrame"""
        df = df.dropna(how='all').dropna(axis=1, how='all')

        df = df.reset_index(drop=True)

        return df

    def _detect_columns(self, df: pd.DataFrame) -> Dict[str, int]:
        """Определяет назначение колонок по заголовкам"""
        column_mapping = {}
        header_row = self._find_header_row(df)

        if header_row == -1:
            raise ValueError("Не удалось найти строку с заголовками")

        headers = df.iloc[header_row].astype(str).str.lower().tolist()

        for col_idx, header in enumerate(headers):
            for field, keywords in self.HEADER_KEYWORDS.items():
                if field not in column_mapping:
                    for keyword in keywords:
                        if keyword in header:
                            column_mapping[field] = col_idx
                            break

        required_fields = ["date_conclusion", "isin", "asset_name", "quantity", "price", "amount", "currency"]
        missing_fields = [field for field in required_fields if field not in column_mapping]

        if missing_fields:
            logger.warning("Не найдены колонки: %s", missing_fields)

            fallback_positions = {
                "date_conclusion": 2,
                "isin": 4,
                "asset_name": 5,
                "quantity": 7,
                "price": 8,
                "amount": 9,
                "currency": 10,
                "commission": 11
            }

            for field in missing_fields:
                if field in fallback_positions and len(headers) > fallback_positions[field]:
                    column_mapping[field] = fallback_positions[field]

        return column_mapping

    def _find_header_row(self, df: pd.DataFrame) -> int:
        """Ищет строку с заголовками таблицы"""
        for i in range(min(20, len(df))):
            row = df.iloc[i].astype(str).str.lower()
            has_trade_id = any("№ сделки" in str(val) or "номер сделки" in str(val) for val in row)
            has_date = any("дата заключения" in str(val) or "заключен" in str(val) for val in row)
            has_quantity = any("количество" in str(val) for val in row)

            if has_trade_id and has_date and has_quantity:
                return i

        for i in range(min(20, len(df))):
            row = df.iloc[i].astype(str).str.lower()
            if any("isin" in str(val) for val in row):
                return i

        return -1

    def _parse_datetime(self, date_str: str) -> Optional[datetime]:
        """Парсит datetime из строки формата '15.10.2021 23:04:40'"""
        if not date_str or pd.isna(date_str):
            return None

        date_str = str(date_str).strip()

        patterns = [
            r'(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2}):(\d{2})',
            r'(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})',
            r'(\d{2})\.(\d{2})\.(\d{4})',
        ]

        for pattern in patterns:
            match = re.search(pattern, date_str)
            if match:
                try:
                    if len(match.groups()) == 6:
                        day, month, year, hour, minute, second = map(int, match.groups())
                        return datetime(year, month, day, hour, minute, second)
                    elif len(match.groups()) == 5:
                        day, month, year, hour, minute = map(int, match.groups())
                        return datetime(year, month, day, hour, minute)
                    else:
                        day, month, year = map(int, match.groups())
                        return datetime(year, month, day)
                except Exception as e:
                    logger.debug(f"Ошибка при парсинге даты '{date_str}': {e}")
                    continue

        for fmt in ["%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
            try:
                return datetime.strptime(date_str, fmt)
            except Exception:
                continue

        logger.warning(f"Не удалось распарсить дату: '{date_str}'")
        return None

    def _extract_field(self, row: pd.Series, column_mapping: Dict[str, int], field_name: str) -> str:
        """Извлекает значение поля из строки"""
        if field_name not in column_mapping:
            return ""

        try:
            value = row.iloc[column_mapping[field_name]]
            return str(value).strip() if not pd.isna(value) else ""
        except (IndexError, KeyError):
            return ""

    def _process_rows(self, df: pd.DataFrame, column_mapping: Dict[str, int]) -> List[OperationDTO]:
        """Обрабатывает строки с данными о сделках"""
        operations = []
        header_row = self._find_header_row(df)
        start_row = header_row + 1 if header_row != -1 else 0

        for i in range(start_row, min(len(df), start_row + 5000)):
            row = df.iloc[i]
            self.stats["total_rows"] += 1

            operation = self._process_row(row, column_mapping)
            if operation:
                operations.append(operation)

        return operations

    def _process_row(self, row: pd.Series, column_mapping: Dict[str, int]) -> Optional[OperationDTO]:
        """Обрабатывает одну строку с данными о сделке"""
        try:
            date_str = self._extract_field(row, column_mapping, "date_conclusion")
            trade_date = self._parse_datetime(date_str)

            if not trade_date:
                self.stats["skipped_no_date"] += 1
                return None

            qty_str = self._extract_field(row, column_mapping, "quantity")
            qty = to_float_safe(qty_str)

            if qty == 0:
                self.stats["skipped_no_qty"] += 1
                return None

            price_str = self._extract_field(row, column_mapping, "price")
            price = to_float_safe(price_str)

            amount_str = self._extract_field(row, column_mapping, "amount")
            amount = to_float_safe(amount_str)

            nkd_str = self._extract_field(row, column_mapping, "nkd")
            nkd = to_float_safe(nkd_str)

            currency = self._extract_field(row, column_mapping, "currency")
            if currency.upper() in ("RUR", "РУБ", "РУБЛЬ"):
                currency = "RUB"

            commission_str = self._extract_field(row, column_mapping, "commission")
            commission = to_float_safe(commission_str)

            # Извлечение данных об инструменте
            isin = self._extract_field(row, column_mapping, "isin")
            asset_name = self._extract_field(row, column_mapping, "asset_name")
            ticker = self._extract_ticker_from_name(asset_name)

            trade_id = self._extract_field(row, column_mapping, "trade_id")
            trade_id = self._extract_first_trade_id(trade_id)

            comment = self._extract_field(row, column_mapping, "comment")

            operation_type = "buy" if qty > 0 else "sale"

            operation = OperationDTO(
                date=trade_date,
                operation_type=operation_type,
                payment_sum=abs(amount),
                currency=currency,
                ticker="",
                isin=isin,
                reg_number="",
                price=price,
                quantity=abs(qty),
                aci=nkd,
                comment=comment,
                operation_id=trade_id,
                commission=commission,
            )

            self.stats["parsed"] += 1
            self.stats["total_commission"] += float(commission or 0.0)
            return operation

        except Exception as e:
            logger.exception("Ошибка обработки строки сделки: %s", e)
            self.stats["skipped_invalid"] += 1
            return None

    def _extract_ticker_from_name(self, asset_name: str) -> str:
        """Извлекает тикер из названия инструмента"""
        if not asset_name:
            return ""

        clean_name = re.sub(r'[^\w\s\.\-]', '', asset_name).strip()

        parts = clean_name.split()
        if parts:
            first_part = parts[0]
            if re.match(r'^[A-ZА-Я0-9]{1,6}(\.[A-Z]{1,4})?$', first_part):
                return first_part

        match = re.search(r'\(([A-ZА-Я0-9]{1,6}(\.[A-Z]{1,4})?)\)', asset_name)
        if match:
            return match.group(1)

        return ""

    def _extract_first_trade_id(self, trade_id_str: str) -> str:
        """Извлекает первый ID сделки из строки (если их несколько разделены переносом строки)"""
        if not trade_id_str:
            return ""

        parts = [part.strip() for part in trade_id_str.splitlines() if part.strip()]
        return parts[0] if parts else ""


def parse_trades_from_xls(file_path: str) -> Tuple[List[OperationDTO], Dict[str, Any]]:
    """Публичный интерфейс для парсинга сделок из XLS/XLSX отчета"""
    parser = XlsTradesParser()
    return parser.parse(file_path)