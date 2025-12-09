from __future__ import annotations
import re
import logging
import os
from datetime import datetime
from typing import Any, Optional, Dict, List, Tuple


def get_logger(name: str = "parser_alfa") -> logging.Logger:
    level_name = os.getenv("PARSER_LOGLEVEL", "DEBUG").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger

logger = get_logger()

DATE_RE = re.compile(r"\d{2}[,.]\d{2}[,.]\d{4}")
ISIN_RE = re.compile(r"\b[A-Z]{2}[A-Z0-9]{9}[0-9]\b", re.IGNORECASE)
REG_NUMBER_PATTERNS = [
    ("full", re.compile(r"\b[0-9][0-9A-ZА-Я]{0,7}[-/][0-9A-ZА-Я\-\/]*\d[0-9A-ZА-Я\-\/]*\b", re.IGNORECASE)),
    ("short", re.compile(r'\b\d{8}[A-Za-zА-Яа-я]\b', re.IGNORECASE)),
    ("rare", re.compile(r'\b[A-Z]{2}[0-9A-Z]{7,10}\b', re.IGNORECASE)),
]

def format_date_from_match(value: str) -> str:
    return value.replace(",", ".")


def extract_date(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y")

    s = str(value).strip() if value else ""
    s = re.sub(r"[\s\u00A0]", "", s)

    if re.match(r"\d{2}[,.]\d{2}[,.]\d{4}", s):
        return s.replace(",", ".")

    return None


def to_float_safe(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        s = str(v).strip()
        if s in ("", "-", "--"):
            return 0.0
        s = s.replace("\u00A0", " ").replace(" ", "").replace(",", ".")
        return float(s)
    except Exception:
        try:
            return float(str(v).replace(",", "."))
        except Exception:
            return 0.0


def to_int_safe(v: Any) -> int:
    """
    Аналогично, безопасно в int.
    """
    try:
        return int(round(float(str(v).replace("\u00A0", " ").replace(" ", "").replace(",", ".") or 0.0)))
    except Exception:
        return 0

def _local_name(tag: str) -> str:
    """Возвращает локальное имя тега без namespace."""
    if tag is None:
        return ""
    return tag.split("}")[-1] if "}" in tag else tag

def _normalize_attrib(attrib: Dict[str, str]) -> Dict[str, str]:
    """Нормализация атрибутов: приводим ключи к lowercase."""
    return {k.lower(): v for k, v in attrib.items()}

def extract_isin_from_attr(s: Optional[str]) -> str:
    if not s:
        return ""
    m = ISIN_RE.search(str(s))
    return m.group(0).upper() if m else str(s).strip()

def extract_reg_number(comment_text: Optional[str]) -> str:
    """Извлекает регистрационный номер"""
    if not comment_text:
        return ""

    for pattern_name, pattern in REG_NUMBER_PATTERNS:
        match = pattern.search(comment_text)
        if match:
            return match.group(0)
    return ""

def extract_isin(comment_text: Optional[str]) -> str:
    """Извлекает ISIN"""
    if not comment_text:
        return ""
    m = ISIN_RE.search(comment_text)
    return m.group(0) if m else ""


def extract_first_value(text: Optional[str], separator: str = r'[\s\r\n\t]+') -> str:
    """
    Извлекает первое значение из строки с разделителями.
    Пример: "14533071091\r\n1280737003" -> "14533071091"
    """
    if not text:
        return ""
    parts = re.split(separator, str(text).strip())
    return parts[0].strip() if parts and parts[0] else ""


def parse_datetime_from_components(date_str: Optional[str], time_str: Optional[str] = None) -> Optional[datetime]:
    """
    Парсит datetime из отдельных компонентов даты и времени.
    """
    if not date_str:
        return None

    try:
        if "T" in date_str:
            date_part = date_str.split("T")[0]
            if time_str:
                time_clean = time_str.split(".")[0]
                return datetime.strptime(f"{date_part} {time_clean}", "%Y-%m-%d %H:%M:%S")
            return datetime.fromisoformat(date_str)
        else:
            if time_str:
                return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
            return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        try:
            if "T" in date_str:
                return datetime.strptime(date_str.split("T")[0], "%Y-%m-%d")
            return datetime.strptime(date_str, "%Y-%m-%d")
        except Exception:
            return None

def extract_min_max_dates(ops) -> Tuple[str, str]:
    """
    Извлекает минимальную и максимальную дату из списка операций.
    """
    valid_dates = []
    for op in ops:
        d = op.date
        if isinstance(d, datetime):
            valid_dates.append(d)
        elif isinstance(d, str):
            try:
                valid_dates.append(datetime.fromisoformat(d))
            except Exception:
                try:
                    date_part = d.split()[0]
                    valid_dates.append(datetime.strptime(date_part, "%d.%m.%Y"))
                except Exception:
                    continue

    if not valid_dates:
        return "", ""

    min_date = min(valid_dates)
    max_date = max(valid_dates)
    return min_date.strftime("%d.%m.%Y"), max_date.strftime("%d.%m.%Y")

def extract_account_id_from_attributes(attrib: Dict[str, str]) -> str:
    """Извлекает account_id из атрибутов элемента"""
    acc_code = attrib.get("acc_code") or attrib.get("acc_code".lower())
    if acc_code and acc_code.strip():
        return acc_code.strip().split("-")[0]
    return ""