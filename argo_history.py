# argo_history.py
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List


HISTORY_DIR = "logs"
HISTORY_FILE = "operaciones_historial.jsonl"


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _history_path(logs_dir: str = HISTORY_DIR) -> str:
    _ensure_dir(logs_dir)
    return os.path.join(logs_dir, HISTORY_FILE)


def build_history_record(pipeline_result: Dict[str, Any]) -> Dict[str, Any]:
    pipeline_result = pipeline_result if isinstance(pipeline_result, dict) else {}

    master = pipeline_result.get("master", {}) if isinstance(pipeline_result.get("master"), dict) else {}
    resumen_global = master.get("resumen_global", {}) if isinstance(master.get("resumen_global"), dict) else {}
    descargas = master.get("descargas", {}) if isinstance(master.get("descargas"), dict) else {}

    record = {
        "id_operacion": pipeline_result.get("id_operacion", ""),
        "timestamp_local": datetime.now().isoformat(timespec="seconds"),
        "estatus_global": resumen_global.get("estatus_global", ""),
        "semaforo_operacion": resumen_global.get("semaforo_operacion", ""),
        "riesgo_global": resumen_global.get("riesgo_global", ""),
        "score_documental_global": resumen_global.get("score_documental_global", 0),
        "alertas_totales": resumen_global.get("alertas_totales", 0),
        "control_output_path": descargas.get("control_output_path", ""),
        "class_output_path": descargas.get("class_output_path", ""),
        "document_output_path": descargas.get("document_output_path", ""),
        "master": master,
    }
    return record


def append_history_record(record: Dict[str, Any], logs_dir: str = HISTORY_DIR) -> str:
    path = _history_path(logs_dir)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return path


def save_pipeline_to_history(pipeline_result: Dict[str, Any], logs_dir: str = HISTORY_DIR) -> str:
    record = build_history_record(pipeline_result)
    return append_history_record(record, logs_dir=logs_dir)


def read_history(limit: int = 50, logs_dir: str = HISTORY_DIR) -> List[Dict[str, Any]]:
    path = _history_path(logs_dir)

    if not os.path.exists(path):
        return []

    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue

    rows = list(reversed(rows))
    return rows[:limit]
