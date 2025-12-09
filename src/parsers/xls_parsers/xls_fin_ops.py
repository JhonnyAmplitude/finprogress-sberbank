# parsers/xls_parsers/xls_fin_ops.py
import pandas as pd
from datetime import datetime
from decimal import Decimal
from typing import List, Tuple, Dict, Any, Optional
from pathlib import Path
from src.OperationDTO import OperationDTO
from src.utils import (
    extract_reg_number,
    extract_isin,
    get_logger,
    to_float_safe
)
from src.parsers.operation_classifier import OperationClassifier

logger = get_logger("parser_sber_fin_ops")


class XlsFinancialOperationsParser:
    """Парсер финансовых операций из Excel отчёта Сбербанка (лист 'Движение ДС')"""

    def __init__(self):
        self.stats = {
            "total_rows": 0,
            "parsed": 0,
            "skipped": 0,
            "skipped_not_executed": 0,
            "skipped_no_date": 0,
            "skipped_no_amount": 0,
            "example_comments": [],
            "amounts_by_mapped_type": {},
            "amounts_by_label": {},
            "total_income": Decimal("0"),
            "total_expense": Decimal("0"),
            "detected_sheet": "",
            "column_mapping": {},
        }

    def parse(self, file_path: str) -> Tuple[List[OperationDTO], Dict[str, Any]]:
        try:
            engine = self._detect_engine(file_path)
            logger.debug(f"Используем движок: {engine}")
            excel_file = pd.ExcelFile(file_path, engine=engine)
            logger.debug(f"Листы в файле: {excel_file.sheet_names}")
            sheet_name = self._find_sheet(excel_file.sheet_names)
            logger.info(f"Выбран лист: '{sheet_name}'")
            self.stats["detected_sheet"] = sheet_name

            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            logger.info(f"Прочитано {len(df)} строк. Колонки: {list(df.columns)}")
            if not df.empty:
                logger.debug(f"Пример первой строки: {df.iloc[0].to_dict()}")

            df = self._preprocess_dataframe(df)
            headers = [str(col).strip() for col in df.columns]
            column_mapping = self._map_columns(headers)
            self.stats["column_mapping"] = {k: headers.index(headers[v]) if isinstance(v, str) else v
                                            for k, v in column_mapping.items()}

            operations = self._process_rows(df, column_mapping)
            logger.info(f"✓ Обработано: {len(operations)} операций")
            return operations, self._finalize_stats()
        except Exception as e:
            logger.exception("❌ Ошибка парсинга Сбербанка (фин. операции): %s", e)
            return [], {"error": str(e), **self.stats}

    def _detect_engine(self, file_path: str) -> str:
        if file_path.lower().endswith('.xls'):
            return 'xlrd'
        elif file_path.lower().endswith('.xlsx'):
            return 'openpyxl'
        else:
            raise ValueError(f"Неподдерживаемый формат: {Path(file_path).suffix}")

    def _find_sheet(self, sheet_names: List[str]) -> str:
        for name in sheet_names:
            if "движение дс" in name.lower():
                return name
        for name in sheet_names:
            if "движение" in name.lower() and "дс" in name.lower():
                return name
        if sheet_names:
            logger.warning("Лист 'Движение ДС' не найден. Используется третий: %s", sheet_names[2])
            return sheet_names[2]
        raise ValueError("Нет листов в файле")

    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(how='all').reset_index(drop=True)
        return df

    def _map_columns(self, headers: List[str]) -> Dict[str, int]:
        headers_lower = [h.lower() for h in headers]
        column_mapping = {}
        mapping = {
            "date": ["дата исполнения поручения"],
            "operation_type": ["операция"],
            "amount": ["сумма"],
            "currency": ["валюта операции"],
            "comment": ["содержание операции"],
            "status": ["статус"],
            "instrument_code": ["код финансового инструмента"],
        }
        for field, keywords in mapping.items():
            for idx, h in enumerate(headers_lower):
                if any(kw in h for kw in keywords):
                    column_mapping[field] = idx
                    break
        required = ["date", "operation_type", "amount", "currency", "status"]
        missing = [r for r in required if r not in column_mapping]
        if missing:
            raise ValueError(f"Не найдены колонки: {missing}. Заголовки: {headers}")
        return column_mapping

    def _process_rows(self, df: pd.DataFrame, column_mapping: Dict[str, int]) -> List[OperationDTO]:
        operations = []
        for i, row in df.iterrows():
            self.stats["total_rows"] += 1
            try:
                op = self._process_row(row, column_mapping, i)
                if op:
                    operations.append(op)
            except Exception as e:
                logger.debug(f"Ошибка в строке {i}: {e}")
                self.stats["skipped"] += 1
        return operations

    def _process_row(self, row: pd.Series, column_mapping: Dict[str, int], row_index: int) -> Optional[OperationDTO]:
        # Статус
        status_col = column_mapping["status"]
        status = str(row.iloc[status_col]).strip() if not pd.isna(row.iloc[status_col]) else ""
        if status != "Исполнена":
            self.stats["skipped_not_executed"] += 1
            return None

        # Дата — pandas уже распарсил в datetime, если возможно
        date_val = row.iloc[column_mapping["date"]]
        if pd.isna(date_val):
            self.stats["skipped_no_date"] += 1
            return None
        if isinstance(date_val, datetime):
            execution_date = date_val
        else:
            execution_date = self._parse_date_fallback(str(date_val))

        if not execution_date:
            self.stats["skipped_no_date"] += 1
            return None

        amount_val = row.iloc[column_mapping["amount"]]
        payment_sum = to_float_safe(amount_val)
        if payment_sum == 0.0:
            self.stats["skipped_no_amount"] += 1
            return None

        operation_type_raw = str(row.iloc[column_mapping["operation_type"]]).strip()
        currency_raw = str(row.iloc[column_mapping["currency"]]).strip()
        comment = str(row.iloc[column_mapping["comment"]]).strip() if "comment" in column_mapping else ""
        instrument_code = str(row.iloc[column_mapping["instrument_code"]]).strip() if "instrument_code" in column_mapping else ""

        currency = OperationClassifier.CURRENCY_DICT.get(currency_raw.upper(), currency_raw.upper())
        if currency == "RUR":
            currency = "RUB"

        op_type = OperationClassifier.determine_operation_type(operation_type_raw, comment, payment_sum)
        if OperationClassifier.should_skip_operation(operation_type_raw, comment, operation_type_raw):
            self.stats["skipped"] += 1
            return None

        full_text = f"{comment} {instrument_code}".strip()
        isin = extract_isin(full_text)
        reg_number = extract_reg_number(full_text)

        dto = OperationDTO(
            date=execution_date,
            operation_type=op_type,
            payment_sum=abs(payment_sum),
            currency=currency,
            ticker="",
            isin=isin,
            reg_number=reg_number,
            price=0.0,
            quantity=0.0,
            aci=0.0,
            comment=comment,
            operation_id="",
            commission=0.0,
        )

        amount_dec = Decimal(str(payment_sum))
        self._update_stats(operation_type_raw, comment, amount_dec, op_type)
        self.stats["parsed"] += 1
        return dto

    def _parse_date_fallback(self, date_str: str) -> Optional[datetime]:
        from src.utils import parse_datetime_from_components
        try:
            return parse_datetime_from_components(date_str.replace(",", "."))
        except:
            return None

    def _update_stats(self, oper_type: str, comment: str, amount: Decimal, mapped_type: str):
        label_key = (oper_type or "").strip() or (comment.splitlines()[0].strip() if comment else "")
        if label_key:
            self.stats["amounts_by_label"][label_key] = (
                self.stats["amounts_by_label"].get(label_key, Decimal("0")) + amount
            )
        self.stats["amounts_by_mapped_type"][mapped_type] = (
            self.stats["amounts_by_mapped_type"].get(mapped_type, Decimal("0")) + amount
        )
        if amount > 0:
            self.stats["total_income"] += amount
        else:
            self.stats["total_expense"] += abs(amount)

        if len(self.stats["example_comments"]) < 5 and comment:
            self.stats["example_comments"].append(comment)

    def _finalize_stats(self) -> Dict[str, Any]:
        stats = self.stats.copy()
        stats["total_income"] = str(stats["total_income"])
        stats["total_expense"] = str(stats["total_expense"])
        stats["amounts_by_mapped_type"] = {k: str(v) for k, v in stats["amounts_by_mapped_type"].items()}
        stats["amounts_by_label"] = {k: str(v) for k, v in stats["amounts_by_label"].items()}
        return stats