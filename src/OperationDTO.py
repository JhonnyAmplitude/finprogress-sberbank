from datetime import datetime
from dataclasses import dataclass, asdict, field
from typing import Optional, Union
from src.utils import to_float_safe, parse_datetime_from_components


@dataclass
class OperationDTO:
    date: Optional[datetime]
    operation_type: str
    payment_sum: Union[str, float]
    currency: str
    ticker: Optional[str] = ""
    isin: Optional[str] = ""
    reg_number: Optional[str] = ""
    price: Optional[float] = 0.0
    quantity: Union[int, float] = 0.0
    aci: Optional[Union[str, float]] = 0.0
    comment: Optional[str] = ""
    operation_id: Optional[str] = ""
    commission: float = 0.0
    _sort_key: Optional[str] = field(init=False, default=None)

    def __post_init__(self):
        if isinstance(self.date, str):
            self.date = parse_datetime_from_components(
                self.date.split(" ")[0], self.date.split(" ")[1] if len(self.date.split(" ")) > 1 else None
            )

        if self.date:
            self._sort_key = self.date.isoformat()
        else:
            self._sort_key = ""

        self.quantity = to_float_safe(self.quantity)
        self.aci = to_float_safe(self.aci)
        self.commission = to_float_safe(self.commission)

    def to_dict(self):
        result = asdict(self)
        if isinstance(self.date, datetime):
            result['date'] = self.date.isoformat()
        result.pop("_sort_key", None)
        return result