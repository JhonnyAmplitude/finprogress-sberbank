import pandas as pd
from datetime import datetime
from decimal import Decimal
from typing import List, Tuple, Dict, Any, Optional
import re
from pathlib import Path

from src.OperationDTO import OperationDTO
from src.utils import extract_reg_number, extract_isin, parse_datetime_from_components, get_logger
from src.parsers.operation_classifier import OperationClassifier

logger = get_logger()


class XlsFinancialOperationsParser:
    """Парсер финансовых операций из Excel отчетов брокера (логика как в XML-парсере)"""

    SHEET_KEYWORDS = [
        "движение", "дс", "движение дс", "Движение ДС"
    ]

    HEADER_KEYWORDS = {
        "date": ["дата"],
        "time": ["время"],
        "operation_type": ["наименование операции", "операция", "тип операции", "вид операции", "содержание"],
        "comment": ["комментарий"],
        "amount": list(OperationClassifier.CURRENCY_DICT.keys()),
    }

    def __init__(self):
        self.stats = {
            "total_rows": 0,
            "parsed": 0,
            "skipped": 0,
            "example_comments": [],
            "amounts_by_mapped_type": {},
            "amounts_by_label": {},
            "total_income": Decimal("0"),
            "total_expense": Decimal("0"),
            "detected_sheet": "",
            "column_mapping": {},
            "currency_for_amount": "RUB"
        }

    def parse(self, file_path: str) -> Tuple[List[OperationDTO], Dict[str, Any]]:
        """Основной метод парсинга финансовых операций"""
        try:
            engine = self._detect_engine(file_path)
            excel_file = pd.ExcelFile(file_path, engine=engine)
            sheet_name = self._find_financial_sheet(excel_file.sheet_names)
            self.stats["detected_sheet"] = sheet_name

            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            df = self._preprocess_dataframe(df)
            column_mapping = self._detect_columns(df)
            self.stats["column_mapping"] = column_mapping

            operations = self._process_rows(df, column_mapping)

            return operations, self._finalize_stats()

        except Exception as e:
            logger.exception("Ошибка при парсинге Excel файла: %s", e)
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

    def _find_financial_sheet(self, sheet_names: List[str]) -> str:
        """Находит лист с финансовыми операциями"""
        for name in sheet_names:
            name_lower = name.lower()
            if any(keyword in name_lower for keyword in self.SHEET_KEYWORDS):
                return name

        fallback_names = ["Движение ДС", "Финансовые операции", "Движения денежных средств",
                          "Движение денежных средств"]
        for name in fallback_names:
            if name in sheet_names:
                return name

        if sheet_names:
            logger.warning("Не удалось определить лист с финансовыми операциями. Используется первый лист: %s",
                           sheet_names[0])
            return sheet_names[0]

        raise ValueError("Не найдено ни одного листа в файле Excel")

    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Предварительная обработка DataFrame"""
        df = df.dropna(how='all').dropna(axis=1, how='all')
        df = df.reset_index(drop=True)
        return df

    def _detect_columns(self, df: pd.DataFrame) -> Dict[str, int]:
        """Определяет назначение колонок по заголовкам (расширенные ключевые слова)"""
        column_mapping = {}
        header_row = self._find_header_row(df)
        if header_row == -1:
            raise ValueError("Не удалось найти строку с заголовками")

        headers = df.iloc[header_row].astype(str).str.lower().tolist()

        for col_idx, header in enumerate(headers):
            for field, keywords in self.HEADER_KEYWORDS.items():
                if field not in column_mapping and field != "amount":
                    for keyword in keywords:
                        if keyword in header:
                            column_mapping[field] = col_idx
                            logger.debug(
                                f"Найдена колонка '{field}' по ключевому слову '{keyword}' в позиции {col_idx}")
                            break

        amount_col, currency = self._find_amount_column(df, header_row, headers)
        column_mapping["amount"] = amount_col
        self.stats["currency_for_amount"] = currency

        required_fields = ["date", "operation_type", "comment", "amount"]
        for field in required_fields:
            if field not in column_mapping:
                logger.warning(f"Не найдена колонка '{field}'. Используется fallback-позиция.")
                if field == "date" and len(headers) > 0:
                    column_mapping[field] = 0
                elif field == "operation_type" and len(headers) > 2:
                    column_mapping[field] = 2
                elif field == "comment" and len(headers) > 3:
                    column_mapping[field] = 3
                elif field == "amount" and len(headers) > 4:
                    column_mapping[field] = 4

        return column_mapping

    def _find_amount_column(self, df: pd.DataFrame, header_row: int, headers: List[str]) -> Tuple[int, str]:
        """Находит колонку с суммой и определяет валюту (как в XML — ищет в подзаголовках)"""
        subheaders = []
        if header_row + 1 < len(df):
            subheaders = df.iloc[header_row + 1].astype(str).str.strip().str.upper().tolist()

        for col_idx in range(len(headers)):
            candidates = []
            if subheaders and col_idx < len(subheaders) and subheaders[col_idx].strip() and subheaders[
                col_idx] != "NAN":
                candidates.append(subheaders[col_idx].strip().upper())
            if col_idx < len(headers) and headers[col_idx].strip():
                candidates.append(headers[col_idx].strip().upper())

            for candidate in candidates:
                for currency_key, currency_code in OperationClassifier.CURRENCY_DICT.items():
                    if currency_key.upper() == candidate and self._column_has_numeric_data(df, header_row, col_idx):
                        logger.info(
                            f"Точное совпадение валюты: '{currency_key}' -> '{currency_code}' в колонке {col_idx}")
                        return col_idx, currency_code

                for currency_key, currency_code in OperationClassifier.CURRENCY_DICT.items():
                    if currency_key.upper() in candidate and self._column_has_numeric_data(df, header_row, col_idx):
                        logger.info(
                            f"Частичное совпадение валюты: '{candidate}' содержит '{currency_key}' -> '{currency_code}' в колонке {col_idx}")
                        return col_idx, currency_code

        logger.warning("Валюта не найдена. Используется RUB по умолчанию.")
        return 4, "RUB"

    def _column_has_numeric_data(self, df: pd.DataFrame, header_row: int, col_idx: int) -> bool:
        """Проверяет наличие числовых данных в колонке (как в XML — минимум 2 непустых значения)"""
        count = 0
        for i in range(header_row + 1, min(header_row + 6, len(df))):
            val = df.iloc[i, col_idx]
            if pd.notna(val) and self._is_numeric_value(val):
                count += 1
        return count >= 2

    def _is_numeric_value(self, value) -> bool:
        """Проверяет, является ли значение числовым (поддержка отрицательных и разделителей)"""
        if pd.isna(value):
            return False
        if isinstance(value, (int, float)):
            return True
        if isinstance(value, str):
            value = value.replace("–", "-").replace("−", "-").replace("\u2212", "-")
            cleaned = re.sub(r'[^\d\.\-,]', '', value)
            try:
                float(cleaned.replace(',', '.'))
                return True
            except (ValueError, TypeError):
                return False
        return False

    def _find_header_row(self, df: pd.DataFrame) -> int:
        """Ищет строку с заголовками (как в XML — ищет "дата" и "операция")"""
        for i in range(min(20, len(df))):
            row = df.iloc[i].astype(str).str.lower()
            has_date = any("Дата создания заявки" in str(val) for val in row)
            has_operation = any(
                ("номер заявки" in str(val) or "номер договора" in str(val) or "содержание" in str(val))
                for val in row
            )
            if has_date and has_operation:
                return i
        return -1

    def _process_rows(self, df: pd.DataFrame, column_mapping: Dict[str, int]) -> List[OperationDTO]:
        """Обрабатывает строки (как в XML — без ограничения на 500 строк)"""
        operations = []
        last_valid_date = None
        header_row = self._find_header_row(df)
        start_row = header_row + 1 if header_row != -1 else 0

        for i in range(start_row, len(df)):
            row = df.iloc[i]
            self.stats["total_rows"] += 1

            operation, current_date = self._process_row_with_date_inheritance(
                row, column_mapping, last_valid_date, row_index=i
            )

            if current_date:
                last_valid_date = current_date
            if operation:
                operations.append(operation)

        return operations

    def _process_row_with_date_inheritance(
            self,
            row: pd.Series,
            column_mapping: Dict[str, int],
            last_valid_date: Optional[datetime],
            row_index: int
    ) -> Tuple[Optional[OperationDTO], Optional[datetime]]:
        """Обрабатывает строку (полностью как в XML)"""
        current_date = self._extract_date_with_inheritance(row, column_mapping, last_valid_date)
        if not current_date:
            self.stats["skipped"] += 1
            logger.debug(f"ROW {row_index}: пропущена из-за отсутствия даты")
            return None, None

        oper_type = self._extract_field(row, column_mapping, "operation_type")
        comment = self._extract_field(row, column_mapping, "comment")
        raw_amount = self._extract_amount(row, column_mapping)
        amount = Decimal(str(raw_amount)) if raw_amount is not None else Decimal("0")

        label_source = (oper_type or "").strip() or (comment or "").strip()
        if self._should_skip(oper_type, comment, amount, label_source):
            self.stats["skipped"] += 1
            logger.debug(f"ROW {row_index}: ПРОПУЩЕНА. oper_type='{oper_type}', comment='{comment}', amount={amount}")
            return None, current_date

        payment_sum = float(amount)
        op_type = OperationClassifier.determine_operation_type(oper_type, comment, payment_sum)
        if op_type == "_skip_":
            self.stats["skipped"] += 1
            logger.debug(f"ROW {row_index}: ПРОПУЩЕНА (классификатор вернул '_skip_'). Тип: {op_type}")
            return None, current_date

        isin = extract_isin(comment)
        reg_number = extract_reg_number(comment)
        operation = self._create_operation_dto(
            current_date, op_type, self.stats["currency_for_amount"],
            payment_sum, comment, isin, reg_number
        )

        self._update_stats(oper_type, comment, amount, op_type)
        self._collect_example_comment(comment)

        self.stats["parsed"] += 1
        logger.debug(
            f"ROW {row_index}: УСПЕШНО. Тип={op_type}, Сумма={amount}, oper_type='{oper_type}', comment='{comment}'")
        return operation, current_date

    def _extract_date_with_inheritance(
            self, row: pd.Series, column_mapping: Dict[str, int], last_valid_date: Optional[datetime]
    ) -> Optional[datetime]:
        """Извлекает дату с наследованием от предыдущей строки (как в XML: settlement_date или last_update)"""
        if "date" not in column_mapping:
            return last_valid_date

        date_val = row.iloc[column_mapping["date"]]
        if pd.isna(date_val) or not str(date_val).strip():
            return last_valid_date

        if isinstance(date_val, datetime):
            return date_val
        return parse_datetime_from_components(str(date_val))

    def _should_skip(self, oper_type: str, comment: str, amount: Decimal, label_source: str) -> bool:
        """Логика пропуска как в XML"""
        if not label_source and (amount is None or float(amount or 0) == 0.0):
            return True

        if self._is_total_row(oper_type, comment):
            return True

        return OperationClassifier.should_skip_operation(oper_type, comment, label_source)

    def _is_total_row(self, oper_type: str, comment: str) -> bool:
        """Проверяет, является ли строка итоговой"""
        oper_lower = (oper_type or "").lower()
        comment_lower = (comment or "").lower()

        total_indicators = [
            "итого", "итог:", "итого:",  "баланс"
        ]

        if any(indicator in comment_lower for indicator in total_indicators):
            return True
        if any(indicator in oper_lower for indicator in total_indicators):
            return True

        return False

    def _create_operation_dto(
            self,
            date: datetime,
            op_type: str,
            currency: str,
            payment_sum: float,
            comment: str,
            isin: str,
            reg_number: str
    ) -> OperationDTO:
        """Создает DTO операции (знак суммы не важен — берем модуль)"""
        return OperationDTO(
            date=date,
            operation_type=op_type,
            payment_sum=abs(payment_sum),
            currency=currency or "RUB",
            ticker="",
            isin=isin,
            reg_number=reg_number,
            price=0.0,
            quantity=0,
            aci=0.0,
            comment=comment,
            operation_id="",
            commission=0.0
        )

    def _update_stats(self, oper_type: str, comment: str, amount: Decimal, mapped_type: str):
        """Статистика как в XML"""
        if amount is None:
            return

        self.stats["amounts_by_mapped_type"][mapped_type] = (
                self.stats["amounts_by_mapped_type"].get(mapped_type, Decimal("0")) + amount
        )

        label_key = (oper_type or "").strip() or (comment.splitlines()[0].strip() if comment else "")
        if label_key:
            self.stats["amounts_by_label"][label_key] = (
                    self.stats["amounts_by_label"].get(label_key, Decimal("0")) + amount
            )

        if amount > 0:
            self.stats["total_income"] += amount
        elif amount < 0:
            self.stats["total_expense"] += amount

    def _collect_example_comment(self, comment: str):
        """Собирает примеры комментариев как в XML"""
        if comment and len(self.stats["example_comments"]) < 5:
            self.stats["example_comments"].append(comment)

    def _extract_field(self, row: pd.Series, column_mapping: Dict[str, int], field_name: str) -> str:
        """Извлекает значение поля из строки"""
        if field_name not in column_mapping or column_mapping[field_name] >= len(row):
            return ""
        value = row.iloc[column_mapping[field_name]]
        return str(value).strip() if not pd.isna(value) else ""

    def _extract_amount(self, row: pd.Series, column_mapping: Dict[str, int]) -> Optional[float]:
        """Парсит сумму со знаком """
        if "amount" not in column_mapping or column_mapping["amount"] >= len(row):
            return None

        amount_val = row.iloc[column_mapping["amount"]]
        if pd.isna(amount_val) or not str(amount_val).strip():
            return None

        if isinstance(amount_val, (int, float)):
            return float(amount_val)

        amount_str = str(amount_val).strip()
        amount_str = amount_str.replace("–", "-").replace("−", "-").replace("\u2212", "-")
        amount_str = re.sub(r'[^\d\.\-,]', '', amount_str)

        if re.search(r'\d{1,3}(?:\.\d{3})+,\d+', amount_str):
            amount_str = amount_str.replace(".", "").replace(",", ".")
        elif re.search(r'\d{1,3}(?: \d{3})+,\d+', amount_str):
            amount_str = amount_str.replace(" ", "").replace(",", ".")
        elif "," in amount_str and amount_str.count(",") == 1:
            amount_str = amount_str.replace(",", ".")

        try:
            return float(amount_str)
        except (ValueError, TypeError):
            logger.debug(f"Не удалось распарсить сумму: '{amount_str}'")
            return None

    def _finalize_stats(self) -> Dict[str, Any]:
        """Форматирует статистику как в XML"""
        stats = self.stats.copy()

        stats["amounts_by_mapped_type"] = {
            k: self._format_decimal(v) for k, v in stats["amounts_by_mapped_type"].items()
        }

        stats["amounts_by_label"] = {
            k: self._format_decimal(v) for k, v in stats["amounts_by_label"].items()
        }

        stats["total_income"] = self._format_decimal(stats["total_income"])
        stats["total_expense"] = self._format_decimal(stats["total_expense"])

        return stats

    def _format_decimal(self, d: Decimal) -> str:
        """Форматирует Decimal в строку как в XML"""
        try:
            return format(d.quantize(Decimal("0.0001")), "f")
        except Exception:
            return str(d)


def parse_fin_operations_from_xls(file_path: str) -> Tuple[List[OperationDTO], Dict[str, Any]]:
    """Публичный интерфейс для парсинга финансовых операций из Excel"""
    parser = XlsFinancialOperationsParser()
    return parser.parse(file_path)