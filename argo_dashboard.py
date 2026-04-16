from typing import Any, Dict, List


def _safe_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    return []


def _safe_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def build_dashboard_output(history_items: List[Dict[str, Any]]) -> Dict[str, Any]:

    items = _safe_list(history_items)

    criticas = 0
    revision = 0
    operables = 0

    operaciones = []

    for item in items:

        item = _safe_dict(item)

        semaforo = item.get("semaforo_operacion", "")
        estatus_global = item.get("estatus_global", "")
        riesgo_global = item.get("riesgo_global", "")
        score_documental_global = item.get("score_documental_global", 0)
        alertas_totales = item.get("alertas_totales", 0)

        if semaforo == "🔴":
            criticas += 1
        elif semaforo == "🟡":
            revision += 1
        elif semaforo == "🟢":
            operables += 1

        operaciones.append({
            "id_operacion": item.get("id_operacion", ""),
            "timestamp_local": item.get("timestamp_local", ""),
            "semaforo_operacion": semaforo,
            "estatus_global": estatus_global,
            "riesgo_global": riesgo_global,
            "score_documental_global": score_documental_global,
            "alertas_totales": alertas_totales,
            "control_output_path": item.get("control_output_path", ""),
            "class_output_path": item.get("class_output_path", ""),
            "document_output_path": item.get("document_output_path", "")
        })

    resumen = {
        "operaciones_total": len(items),
        "criticas": criticas,
        "revision": revision,
        "operables": operables
    }

    return {
        "ok": True,
        "modulo": "ARGO_DASHBOARD",
        "resumen": resumen,
        "operaciones": operaciones
    }
