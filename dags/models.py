from datetime import datetime

from sqlmodel import SQLModel, Field
from pydantic import validator


class Measurement(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    temperature: float
    humidity: int
    pressure: int
    city: str
    country: str
    dt: datetime

    @validator("humidity")
    def humidity_must_be_in_range_from_zero_to_hundred(cls, value):
        if 0 <= value <= 100:
            return value

        raise ValueError("must be in range [0, 100]")

    @validator("temperature")
    def temperature_must_be_greater_than_absolute_zero(cls, value):
        if value < -273:
            raise ValueError("must be greater than -273")

        return value

    @validator("country")
    def coutry_must_be_specific(cls, value):
        if len(value) != 2:
            raise ValueError("length of country code must be 2")

        elif value not in ["SK", "CZ", "HU", "PL", "UA", "AU"]:
            raise ValueError("country must be one of specific")

        else:
            return value

    def csv(self):
        return f"{self.dt};{self.country};{self.city};{self.temperature};{self.humidity};{self.pressure}"
