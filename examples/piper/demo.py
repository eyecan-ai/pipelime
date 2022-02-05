from typing import Any, Optional
from pydantic import BaseModel, validator


class M(BaseModel):
    a: Any

    @validator("a")
    def _validate(cls, v: Any, values: Optional[Any]):
        print("VALIDATE", v, values)
        return v


data = {"c": 3}

print(M(**data).dict())
