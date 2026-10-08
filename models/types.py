"""Strict field types shared by all models.

Pydantic's default (lax) mode converts "2437" -> 2437.0, true -> 1.0 and "yes" -> True.
API clients and the rules file must send real JSON/YAML types instead.
"""

from typing import Annotated

from pydantic import Field

# A JSON number (int or float), no strings, no booleans, no inf/NaN.
Number = Annotated[float, Field(strict=True, allow_inf_nan=False)]
StrictBool = Annotated[bool, Field(strict=True)]
StrictStr = Annotated[str, Field(strict=True)]
