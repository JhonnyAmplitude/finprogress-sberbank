from typing import Dict, Callable


class OperationClassifier:
    """Классификатор финансовых операций"""

    DYNAMIC_TYPE_HANDLERS: Dict[str, Callable[[str, float, str], str]] = {
        "НДФЛ": lambda name, amount, comment: "refund" if amount > 0 else "withholding",
    }
    OPERATION_TYPE_MAP = {
        "Возмещение": "commission_refund",
        "Дивиденды": "dividend",
        "Купонный доход": "coupon",
        "Погашение купона": "coupon",
    }

    SKIP_OPERATIONS = {
        "Расчеты по сделке",
        "Комиссия по сделке",
        "НКД по сделке",
        "Покупка/Продажа",
        "Покупка/Продажа (репо)",
        "Переводы между площадками",
    }

    TRANSFER_COMMENT_PATTERNS = {
        "coupon": ["погашение купона", "погашением купона"],
        "amortization": ["частичное погашение номинала", "частичном погашении номинала"],
        "repayment": ["полное погашение номинала", "полном погашении номинала", "досрочное погашение номинала"],
        "deposit": ["из ао \"альфа-банк", "из ао альфа-банк", "card2catd", "card2bpk"],
        "dividend": ["дивиденд"],
        "withdrawal": ["списание по поручению клиента", "возврат средств по дог"],
        "other_income": ["выплата по поручению клиента в рамках", 'исполнение обязательств'],
    }

    CURRENCY_DICT = {
        "AED": "AED", "AMD": "AMD", "BYN": "BYN", "CHF": "CHF", "CNY": "CNY",
        "EUR": "EUR", "GBP": "GBP", "HKD": "HKD", "JPY": "JPY", "KGS": "KGS",
        "KZT": "KZT", "NOK": "NOK", "RUB": "RUB", "РУБЛЬ": "RUB", "Рубль": "RUB",
        "SEK": "SEK", "TJS": "TJS", "TRY": "TRY", "USD": "USD", "UZS": "UZS",
        "XAG": "XAG", "XAU": "XAU", "ZAR": "ZAR"
    }

    @classmethod
    def determine_operation_type(cls, oper_type_val: str, comment_text: str, payment_sum: float) -> str:
        """Определяет тип операции"""
        oper_lower = oper_type_val.lower()
        comment_lower = comment_text.lower()

        for pattern, handler in cls.DYNAMIC_TYPE_HANDLERS.items():
            if pattern.lower() in oper_lower:
                try:
                    return handler(oper_type_val, payment_sum, comment_text)
                except Exception:
                    continue

        if oper_type_val in cls.OPERATION_TYPE_MAP:
            return cls.OPERATION_TYPE_MAP[oper_type_val]

        for key, mapped_type in cls.OPERATION_TYPE_MAP.items():
            if key.lower() in oper_lower:
                return mapped_type

        if "перевод" in oper_lower:
            for op_type, patterns in cls.TRANSFER_COMMENT_PATTERNS.items():
                for pattern in patterns:
                    if pattern in comment_lower:
                        return op_type
            return "transfer"

        return oper_type_val.strip() if oper_type_val.strip() else "unknown"

    @classmethod
    def should_skip_operation(cls, oper_type: str, comment: str, label_source: str) -> bool:
        """Проверяет, нужно ли пропустить операцию"""
        if not label_source:
            return True

        low = label_source.lower()
        return any(skip_pattern.lower() in low for skip_pattern in cls.SKIP_OPERATIONS)