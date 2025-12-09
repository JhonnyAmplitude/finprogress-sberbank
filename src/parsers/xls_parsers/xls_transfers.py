import pandas as pd
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional
import re
from pathlib import Path
from src.OperationDTO import OperationDTO
from src.utils import logger, to_float_safe, extract_isin_from_attr


class XlsTransfersParser:
    SHEET_KEYWORDS = [
        "неторговые операции", "non-trade operations", "неторговые",
        "операции с ценными бумагами", "non trade operations",
        "неторговая операция", "non-trade operation", "конвертация"
    ]

    def __init__(self):
        self.stats = {
            "total_rows": 0,
            "parsed": 0,
            "skipped_no_date": 0,
            "skipped_no_qty": 0,
            "skipped_not_conversion": 0,
            "skipped_invalid": 0,
            "detected_sheet": "",
            "column_mapping": {},
            "debug_info": []
        }

    def parse(self, file_path: str) -> Tuple[List[OperationDTO], Dict[str, Any]]:
        try:
            engine = self._detect_engine(file_path)
            excel_file = pd.ExcelFile(file_path, engine=engine)
            sheet_name = self._find_transfers_sheet(excel_file.sheet_names)
            self.stats["detected_sheet"] = sheet_name

            df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
            df = self._preprocess_dataframe(df)

            column_mapping = self._find_columns_by_structure(df)
            self.stats["column_mapping"] = column_mapping

            operations = self._process_rows(df, column_mapping)

            return operations, self.stats.copy()
        except Exception as e:
            logger.exception(f"Ошибка при парсинге неторговых операций: {e}")
            error_stats = {"error": str(e), **self.stats}
            return [], error_stats

    def _detect_engine(self, file_path: str) -> str:
        if file_path.lower().endswith('.xls'):
            return 'xlrd'
        elif file_path.lower().endswith('.xlsx'):
            return 'openpyxl'
        else:
            raise ValueError(f"Неподдерживаемый формат: {Path(file_path).suffix}")

    def _find_transfers_sheet(self, sheet_names: List[str]) -> str:
        for name in sheet_names:
            name_lower = name.lower()
            if any(keyword in name_lower for keyword in self.SHEET_KEYWORDS):
                return name

        exact_names = ["Неторговые операции", "Конвертации", "Non-trade operations"]
        for name in exact_names:
            if name in sheet_names:
                return name

        for name in sheet_names:
            name_lower = name.lower()
            if any(keyword in name_lower for keyword in ["неторг", "non-trade", "конверт", "transfer"]):
                return name

        if sheet_names:
            return sheet_names[0]

        raise ValueError("Не найдено ни одного листа")

    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(how='all').dropna(axis=1, how='all')
        df = df.reset_index(drop=True)
        return df

    def _find_columns_by_structure(self, df: pd.DataFrame) -> Dict[str, int]:
        column_mapping = {}
        header_row = -1
        operation_type_col = -1

        for i in range(min(10, len(df))):
            for col_idx in range(len(df.columns)):
                val = df.iloc[i, col_idx]
                if isinstance(val, str) and "наименование операции" in val.lower():
                    header_row = i
                    operation_type_col = col_idx
                    break
            if header_row != -1:
                break

        if header_row == -1:
            return self._find_columns_by_heuristics(df)

        header_row_data = df.iloc[header_row]

        for col_idx in range(operation_type_col - 1, -1, -1):
            val = header_row_data.iloc[col_idx] if col_idx < len(header_row_data) else None
            if isinstance(val, str) and "дата" in val.lower():
                column_mapping["date"] = col_idx
                break

        for col_idx in range(operation_type_col + 1, min(operation_type_col + 5, len(header_row_data))):
            val = header_row_data.iloc[col_idx] if col_idx < len(header_row_data) else None
            if isinstance(val, str) and any(keyword in val.lower() for keyword in ["актив", "инструмент", "наименование"]):
                column_mapping["asset_name"] = col_idx
                break

        for col_idx in range(operation_type_col + 2, min(operation_type_col + 6, len(header_row_data))):
            val = header_row_data.iloc[col_idx] if col_idx < len(header_row_data) else None
            if isinstance(val, str) and any(keyword in val.lower() for keyword in ["комментарий", "примечание", "основание"]):
                column_mapping["comment"] = col_idx
                break

        for col_idx in range(operation_type_col + 3, min(operation_type_col + 8, len(header_row_data))):
            val = header_row_data.iloc[col_idx] if col_idx < len(header_row_data) else None
            if isinstance(val, str) and any(keyword in val.lower() for keyword in ["зачисление", "списание", "количество", "кол-во"]):
                column_mapping["quantity"] = col_idx
                break

        column_mapping["operation_type"] = operation_type_col + 2

        required = ["date", "operation_type", "quantity"]
        missing = [field for field in required if field not in column_mapping]

        if missing:
            fallback = {
                "date": 1,
                "operation_type": 8,
                "asset_name": 9,
                "comment": 10,
                "quantity": 11
            }
            for field in missing:
                if field in fallback:
                    column_mapping[field] = fallback[field]

        return column_mapping

    def _find_columns_by_heuristics(self, df: pd.DataFrame) -> Dict[str, int]:
        column_mapping = {}

        for col_idx in range(len(df.columns)):
            date_count = 0
            for i in range(min(10, len(df))):
                val = df.iloc[i, col_idx]
                if self._looks_like_date(val):
                    date_count += 1
            if date_count >= 3:
                column_mapping["date"] = col_idx
                break

        for col_idx in range(len(df.columns)):
            transfer_count = 0
            for i in range(min(10, len(df))):
                val = df.iloc[i, col_idx]
                if isinstance(val, str) and "перевод" in val.lower():
                    transfer_count += 1
            if transfer_count >= 2:
                column_mapping["operation_type"] = col_idx
                break

        for col_idx in range(len(df.columns)):
            if col_idx in column_mapping.values():
                continue
            num_count = 0
            for i in range(min(10, len(df))):
                val = df.iloc[i, col_idx]
                if self._looks_like_number(val):
                    num_count += 1
            if num_count >= 3:
                column_mapping["quantity"] = col_idx
                break

        return column_mapping

    def _looks_like_date(self, value) -> bool:
        if isinstance(value, datetime):
            return True
        if isinstance(value, str):
            value = value.strip()
            patterns = [
                r'\d{1,2}[\.\/\-]\d{1,2}[\.\/\-]\d{2,4}',
                r'\d{4}[\.\/\-]\d{1,2}[\.\/\-]\d{1,2}'
            ]
            for pattern in patterns:
                if re.match(pattern, value):
                    return True
        return False

    def _looks_like_number(self, value) -> bool:
        if isinstance(value, (int, float)):
            return True
        if isinstance(value, str):
            value = value.strip()
            return re.match(r'^-?\d+[\.,]?\d*$', value) is not None
        return False

    def _parse_datetime(self, date_val) -> Optional[datetime]:
        if pd.isna(date_val):
            return None

        if isinstance(date_val, datetime):
            return date_val

        s = str(date_val).strip()
        if not s:
            return None

        s_clean = s.replace(",", ".").replace("/", ".")

        formats = [
            "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y",
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
            "%d.%m.%y %H:%M:%S", "%d.%m.%y %H:%M", "%d.%m.%y"
        ]

        for fmt in formats:
            try:
                return datetime.strptime(s_clean.split()[0], fmt)
            except (ValueError, TypeError):
                continue

        return None

    def _extract_field(self, row: pd.Series, column_mapping: Dict[str, int], field_name: str) -> str:
        if field_name not in column_mapping:
            return ""
        try:
            col_idx = column_mapping[field_name]
            if col_idx >= len(row):
                return ""
            value = row.iloc[col_idx]
            return str(value).strip() if not pd.isna(value) else ""
        except (IndexError, KeyError, Exception):
            return ""

    def _process_rows(self, df: pd.DataFrame, column_mapping: Dict[str, int]) -> List[OperationDTO]:
        operations = []
        start_row = self._find_data_start_row(df, column_mapping)

        if start_row == -1:
            return operations

        for i in range(start_row, len(df)):
            row = df.iloc[i]

            if row.count() == 0:
                continue

            self.stats["total_rows"] += 1

            operation = self._process_row(row, column_mapping, i)
            if operation:
                operations.append(operation)

        return operations

    def _find_data_start_row(self, df: pd.DataFrame, column_mapping: Dict[str, int]) -> int:
        if "date" not in column_mapping:
            return 0

        for i in range(min(10, len(df))):
            date_val = df.iloc[i, column_mapping["date"]]
            if self._looks_like_date(date_val):
                return i
        return 0

    def _process_row(self, row: pd.Series, column_mapping: Dict[str, int], row_num: int) -> Optional[OperationDTO]:
        try:
            date_val = row.iloc[column_mapping["date"]] if "date" in column_mapping else None
            trade_date = self._parse_datetime(date_val)

            if not trade_date:
                self.stats["skipped_no_date"] += 1
                return None

            qty_val = row.iloc[column_mapping["quantity"]] if "quantity" in column_mapping else None
            qty = to_float_safe(qty_val)

            if qty == 0:
                self.stats["skipped_no_qty"] += 1
                return None

            operation_type_raw = self._extract_field(row, column_mapping, "operation_type")
            comment = self._extract_field(row, column_mapping, "comment")
            asset_name = self._extract_field(row, column_mapping, "asset_name")
            isin = extract_isin_from_attr(asset_name)

            if not self._is_conversion_operation(operation_type_raw, comment):
                self.stats["skipped_not_conversion"] += 1
                return None

            operation_type = "asset_receive" if qty > 0 else "asset_withdrawal"

            operation = OperationDTO(
                date=trade_date,
                operation_type=operation_type,
                payment_sum=0.0,
                currency="",
                ticker="",
                isin=isin,
                reg_number="",
                price=0.0,
                quantity=abs(qty),
                aci=0.0,
                comment=comment,
                operation_id="",
                commission=0.0,
            )

            self.stats["parsed"] += 1
            return operation

        except Exception as e:
            logger.exception(f"Ошибка обработки строки {row_num}: {e}")
            self.stats["skipped_invalid"] += 1
            return None

    def _is_conversion_operation(self, operation_type: str, comment: str) -> bool:
        operation_type = (operation_type or "").strip().lower()
        comment = (comment or "").strip().lower()

        is_transfer = "перевод" in operation_type
        is_conversion = "конвертация" in comment or "конверсия" in comment

        return is_transfer and is_conversion


def parse_transfers_from_xls(file_path: str) -> Tuple[List[OperationDTO], Dict[str, Any]]:
    parser = XlsTransfersParser()
    return parser.parse(file_path)