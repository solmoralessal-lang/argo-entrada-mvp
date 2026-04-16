from __future__ import annotations
import os, json, secrets
from datetime import datetime, timezone
from typing import Any, Dict, Optional

def generar_id_operacion(prefix: str = "OP") -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    rnd = secrets.token_hex(4).upper()
    return f"{prefix}-{ts}-{rnd}"

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def escribir_log_operacion(
    id_operacion: str,
    payload: Dict[str, Any],
    logs_dir: str = "logs",
    filename: Optional[str] = None,
) -> str:
    ensure_dir(logs_dir)
    if not filename:
        filename = f"{id_operacion}.json"
    path = os.path.join(logs_dir, filename)

    data = {
        "id_operacion": id_operacion,
        "logged_at_utc": datetime.now(timezone.utc).isoformat(),
        **payload,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return path
