from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import requests


SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
SUPABASE_TABLE = "argo_operaciones"


def supabase_config_ok() -> bool:
    return bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)


def _headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def guardar_operacion_supabase(operacion: Dict[str, Any]) -> Dict[str, Any]:
    if not supabase_config_ok():
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")

    from datetime import datetime

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"

    entrada = operacion.get("entrada", {}) or {}
    ocr = operacion.get("ocr", {}) or {}
    decision = operacion.get("decision", {}) or {}
    generacion = operacion.get("generacion", {}) or {}

    cliente_nombre = (
        entrada.get("cliente")
        or ocr.get("consolidado", {}).get("cliente")
        or operacion.get("cliente_nombre")
        or "SIN_CLIENTE"
    )

    shipment_id = (
        entrada.get("shipment_id")
        or entrada.get("tracking")
        or ocr.get("consolidado", {}).get("tracking")
        or operacion.get("shipment_id")
    )

    estatus_global = (
        operacion.get("estado")
        or generacion.get("estado")
        or "OK"
    )

    payload = {
        "id_operacion": operacion.get("id_operacion"),
        "timestamp_local": datetime.now().replace(microsecond=0).isoformat(),
        "cliente_id": cliente_nombre,
        "cliente_nombre": cliente_nombre,
        "shipment_id": shipment_id,
        "estatus_global": estatus_global,
        "riesgo_global": decision.get("accion"),
        "score_documental_global": None,
        "semaforo_operacion": operacion.get("severidad_maxima"),
        "alertas_totales": (
            generacion.get("conteo", {}).get("alertas")
            or ocr.get("conteo", {}).get("alertas")
            or 0
        ),
        "control_output_path": generacion.get("ruta_archivo"),
        "class_output_path": None,
        "document_output_path": generacion.get("descarga"),
        "master_output_path": None,
        "aprobada": operacion.get("aprobada", False),
        "aprobada_por": operacion.get("aprobada_por"),
        "fecha_aprobacion": operacion.get("fecha_aprobacion"),
    }

    response = requests.post(
        url,
        headers=_headers(),
        json=payload,
        timeout=30,
    )

    if response.status_code not in (200, 201):
        raise RuntimeError(f"Error guardando en Supabase: {response.status_code} - {response.text}")

    data = response.json()
    return data[0] if isinstance(data, list) and data else payload


def obtener_historial_supabase(cliente_id: Optional[str] = None) -> List[Dict[str, Any]]:
    if not supabase_config_ok():
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")

    url = (
        f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
        f"?select=*"
        f"&order=id.desc"
    )

    if cliente_id:
        url += f"&cliente_id=eq.{cliente_id}"

    response = requests.get(url, headers=_headers(), timeout=30)

    if response.status_code != 200:
        raise RuntimeError(f"Error leyendo historial de Supabase: {response.status_code} - {response.text}")

    data = response.json()
    return data if isinstance(data, list) else []


def obtener_dashboard_supabase(cliente_id: Optional[str] = None) -> Dict[str, Any]:
    registros = obtener_historial_supabase(cliente_id)

    operaciones_total = len(registros)
    criticas = sum(1 for r in registros if str(r.get("estatus_global", "")).upper() in ["CRITICO", "ALTA", "REVISION"])
    revision = sum(
        1 for r in registros
        if str(r.get("estatus_global", "")).upper() in ["ADVERTENCIA", "CON_OBSERVACIONES"]
    )
    operables = sum(1 for r in registros if str(r.get("estatus_global", "")).upper() == "OK")

    return {
        "ok": True,
        "modulo": "ARGO_DASHBOARD",
        "resumen": {
            "operaciones_total": operaciones_total,
            "criticas": criticas,
            "revision": revision,
            "operables": operables,
        },
        "operaciones": registros,
    }


def aprobar_operacion_supabase(id_operacion: str, aprobada_por: str = "sistema") -> Dict[str, Any]:
    if not supabase_config_ok():
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")

    from datetime import datetime

    url = (
        f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
        f"?id_operacion=eq.{id_operacion}"
    )

    payload = {
        "aprobada": True,
        "aprobada_por": aprobada_por,
        "fecha_aprobacion": datetime.now().replace(microsecond=0).isoformat(),
    }

    headers = _headers()
    headers["Prefer"] = "return=representation"

    response = requests.patch(
        url,
        headers=headers,
        json=payload,
        timeout=30,
    )

    if response.status_code != 200:
        raise RuntimeError(f"Error aprobando en Supabase: {response.status_code} - {response.text}")

    data = response.json()
    return {
        "ok": True,
        "mensaje": "Operación aprobada correctamente",
        "operacion": data[0] if isinstance(data, list) and data else None,
    }


def obtener_clientes_supabase() -> Dict[str, Any]:
    registros = obtener_historial_supabase()

    mapa = {}
    for r in registros:
        cid = r.get("cliente_id")
        cname = r.get("cliente_nombre")

        if cid:
            mapa[cid] = {
                "cliente_id": cid,
                "cliente_nombre": cname or cid,
            }

    clientes = sorted(mapa.values(), key=lambda x: x["cliente_nombre"].lower())

    return {
        "ok": True,
        "total": len(clientes),
        "clientes": clientes,
    }
