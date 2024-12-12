from typing import Any
from pydantic import BaseModel


class InferenceRequest(BaseModel):
    image: str  # base64 encoded image


class InferenceResponse(BaseModel):
    predictions: Any
    # predictions: List[Dict[str, Any]]
