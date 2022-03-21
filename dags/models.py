from typing import Optional

from sqlmodel import SQLModel, Field


class Measurement(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    dt: int
    temp: float
    pressure: int
    humidity: int
    wind: float
    country: str
    city: str

    def csv(self, separator=','):
        data = [str(self.dt), str(self.temp), str(self.pressure), str(self.humidity), str(self.wind), self.country,
                self.city]
        return separator.join(data)
