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

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"

    payload = {
        "id_operacion": operacion.get("id_operacion"),
        "timestamp_local": operacion.get("timestamp_local"),
        "cliente_id": operacion.get("cliente_id"),
        "cliente_nombre": operacion.get("cliente_nombre"),
        "shipment_id": operacion.get("shipment_id"),
        "estatus_global": operacion.get("estatus_global"),
        "riesgo_global": operacion.get("riesgo_global"),
        "score_documental_global": operacion.get("score_documental_global"),
        "semaforo_operacion": operacion.get("semaforo_operacion"),
        "alertas_totales": operacion.get("alertas_totales"),
        "control_output_path": operacion.get("control_output_path"),
        "class_output_path": operacion.get("class_output_path"),
        "document_output_path": operacion.get("document_output_path"),
        "master_output_path": operacion.get("master_output_path"),
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
        f"&order=created_at.desc"
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
    criticas = sum(1 for r in registros if str(r.get("estatus_global", "")).upper() == "CRITICO")
    revision = sum(
        1 for r in registros
        if str(r.get("estatus_global", "")).upper() in ["REVISION", "CON_OBSERVACIONES"]
    )
    operables = sum(1 for r in registros if str(r.get("estatus_global", "")).upper() == "OPERABLE")

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
