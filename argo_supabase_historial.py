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


# =========================================================
# 🔥 GUARDAR OPERACIÓN (CORREGIDO PARA FLUJO OCR REAL)
# =========================================================
def guardar_operacion_supabase(operacion: Dict[str, Any]) -> Dict[str, Any]:
    if not supabase_config_ok():
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")

    from datetime import datetime

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"

    generacion = operacion.get("generacion", {}) or {}
    entrada = generacion.get("entrada", {}) or {}
    ocr = operacion.get("ocr", {}) or {}
    decision = operacion.get("decision", {}) or {}

    cliente_nombre = (
        entrada.get("cliente")
        or ocr.get("consolidado", {}).get("cliente")
        or "SIN_CLIENTE"
    )

    raw_tracking = (entrada.get("shipment_id") or ocr.get("consolidado", {}).get("tracking") or ""); shipment_id = raw_tracking.replace(" ", "").strip(); shipment_id = ("1Z" + shipment_id[2:]) if shipment_id.startswith("12") else shipment_id

    sem = operacion.get("semaforo_operacion") or ocr.get("severidad_maxima"); estatus_global = ("CRITICO" if sem == "ALTA" else ("ADVERTENCIA" if sem == "MEDIA" else "OK"))

    payload = {
        "id_operacion": operacion.get("id_operacion"),
        "timestamp_local": datetime.now().replace(microsecond=0).isoformat(),

        "cliente_id": cliente_nombre,
        "cliente_nombre": cliente_nombre,

        "shipment_id": shipment_id,
        "estatus_global": estatus_global,
        "riesgo_global": ("CRITICO" if (operacion.get("semaforo_operacion") or ocr.get("severidad_maxima")) == "ALTA" else ("CONTINUAR_CON_ALERTA" if (operacion.get("semaforo_operacion") or ocr.get("severidad_maxima")) == "MEDIA" else "CONTINUAR")),
        "semaforo_operacion": operacion.get("semaforo_operacion") or operacion.get("severidad_maxima") or ocr.get("severidad_maxima"),

        "alertas_totales": (
            generacion.get("conteo", {}).get("alertas")
            or ocr.get("conteo", {}).get("alertas")
            or 0
        ),

        "control_output_path": generacion.get("ruta_archivo"),
        "document_output_path": generacion.get("descarga"),

        "aprobada": False,
    }

    response = requests.post(
        url,
        headers=_headers(),
        json=payload,
        timeout=30,
    )

    if response.status_code not in (200, 201):
    print("🔥 SUPABASE ERROR:", response.status_code, response.text)
    return {
        "ok": False,
        "error": response.text
    }

    data = response.json()
    return data[0] if isinstance(data, list) and data else payload


# =========================================================
# 📊 HISTORIAL
# =========================================================
def obtener_historial_supabase(cliente_id: Optional[str] = None) -> List[Dict[str, Any]]:
    if not supabase_config_ok():
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?select=*&order=id.desc"

    if cliente_id:
        url += f"&cliente_id=eq.{cliente_id}"

    response = requests.get(url, headers=_headers(), timeout=30)

    if response.status_code != 200:
        raise RuntimeError(
            f"Error leyendo historial: {response.status_code} - {response.text}"
        )

    data = response.json()
    return data if isinstance(data, list) else []


# =========================================================
# 📈 DASHBOARD
# =========================================================
def obtener_dashboard_supabase(cliente_id: Optional[str] = None) -> Dict[str, Any]:
    registros = obtener_historial_supabase(cliente_id)

    total = len(registros)
    criticas = sum(1 for r in registros if r.get("semaforo_operacion") == "ALTA")
    advertencias = sum(1 for r in registros if r.get("semaforo_operacion") == "MEDIA")
    ok = sum(1 for r in registros if r.get("semaforo_operacion") == "NINGUNA")

    return {
        "ok": True,
        "modulo": "ARGO_DASHBOARD",
        "resumen": {
            "total": total,
            "criticas": criticas,
            "advertencias": advertencias,
            "ok": ok,
        },
        "operaciones": registros,
    }


# =========================================================
# ✅ APROBAR
# =========================================================
def aprobar_operacion_supabase(id_operacion: str, usuario: str = "sistema") -> Dict[str, Any]:
    if not supabase_config_ok():
        raise RuntimeError("Faltan variables de Supabase")

    from datetime import datetime

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?id_operacion=eq.{id_operacion}"

    payload = {
        "aprobada": True,
        "aprobada_por": usuario,
        "fecha_aprobacion": datetime.now().isoformat(),
    }

    headers = _headers()
    headers["Prefer"] = "return=representation"

    response = requests.patch(url, headers=headers, json=payload)

    if response.status_code != 200:
        raise RuntimeError(
            f"Error aprobando: {response.status_code} - {response.text}"
        )

    return {"ok": True, "mensaje": "Operación aprobada"}


# =========================================================
# 👥 CLIENTES
# =========================================================
def obtener_clientes_supabase() -> Dict[str, Any]:
    registros = obtener_historial_supabase()

    clientes = {}
    for r in registros:
        cid = r.get("cliente_id")
        cname = r.get("cliente_nombre")

        if cid:
            clientes[cid] = {
                "cliente_id": cid,
                "cliente_nombre": cname or cid,
            }

    return {
        "ok": True,
        "total": len(clientes),
        "clientes": list(clientes.values()),
    }
