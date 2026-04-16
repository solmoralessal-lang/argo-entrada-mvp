from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class AprobarOperacionRequest(BaseModel):
    id_operacion: str
    aprobada_por: str = Field(default="sistema")


class HistorialQueryResponse(BaseModel):
    ok: bool
    total: int
    cliente_id: Optional[str] = None
    operaciones: List[Dict[str, Any]]
