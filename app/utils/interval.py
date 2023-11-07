from typing import TypeVar
from datetime import date, datetime

T = TypeVar("T")

BOUNDS: dict = {
    int: [-float("inf"), float("inf")],
    float: [-float("inf"), float("inf")],
    date: [date.min, date.max],
    datetime: [datetime.min, datetime.max]
}

class Interval[T]:
    def __init__(self, start: T | None, end: T | None):
        self.start = start
        self.end = end
        if self.start is None and self.end is None:
            raise ValueError("Cannot define a [None, None] interval")
        self.__fill_nones()

    def __repr__(self) -> str:
        return f"[{self.start}, {self.end})"

    def __fill_nones(self):
        """
        Fill None values with type-specific information.
        """
        if self.start is None:
            self.start = BOUNDS[type(self.end)][0]
        elif self.end is None:
            self.end = BOUNDS[type(self.start)][1]

    def intersects(self, other: "Interval") -> bool:
        intervals: list[Interval] = [self, other]
        intervals.sort(key=lambda x: x.start)
        return intervals[0].end > intervals[1].start
