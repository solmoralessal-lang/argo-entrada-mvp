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
        "timestamp_local": (
            operacion.get("timestamp_local")
            or operacion.get("fecha")
            or datetime.now().replace(microsecond=0).isoformat()
        ),

        "cliente_id": operacion.get("cliente_id") or cliente_nombre,
        "cliente_nombre": cliente_nombre,

        "shipment_id": shipment_id,

        "estatus_global": estatus_global,
        "riesgo_global": (
            "CRITICO"
            if (
                operacion.get("semaforo_operacion")
                or ocr.get("severidad_maxima")
            ) == "ALTA"
            else (
                "CONTINUAR_CON_ALERTA"
                if (
                    operacion.get("semaforo_operacion")
                    or ocr.get("severidad_maxima")
                ) == "MEDIA"
                else "CONTINUAR"
            )
        ),
        "score_documental_global": (
            operacion.get("score_documental_global")
            or operacion.get("score_documental")
        ),
        "semaforo_operacion": (
            operacion.get("semaforo_operacion")
            or operacion.get("severidad_maxima")
            or ocr.get("severidad_maxima")
        ),

        "alertas_totales": (
            generacion.get("conteo", {}).get("alertas")
            or ocr.get("conteo", {}).get("alertas")
            or 0
        ),

        "control_output_path": generacion.get("ruta_archivo"),
        "class_output_path": operacion.get("class_output_path"),
        "document_output_path": generacion.get("descarga"),
        "master_output_path": operacion.get("master_output_path"),

        "aprobada": False,

        "payload_operacion": operacion,
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

    if not isinstance(data, list):
        return []

    registros_normalizados = []

    for r in data:
        payload_operacion = r.get("payload_operacion") or {}

        if isinstance(payload_operacion, dict):
            combinado = {
                **payload_operacion,
                **r,
            }
        else:
            combinado = dict(r)

        combinado["aprobada"] = bool(combinado.get("aprobada"))
        combinado["aprobada_por"] = combinado.get("aprobada_por") or ""
        combinado["fecha_aprobacion"] = (
            combinado.get("fecha_aprobacion") or ""
        )

        registros_normalizados.append(combinado)

    return registros_normalizados


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
        "operaciones_total": total,
        "criticas": criticas,
        "revision": advertencias,
        "operables": ok,
        "aprobadas": sum(1 for r in registros if r.get("aprobada") is True),
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

    response = requests.patch(
        url,
        headers=headers,
        json=payload,
    )

    if response.status_code not in (200, 204):
        raise RuntimeError(
            f"Error aprobando: {response.status_code} - {response.text}"
        )

    try:
        data = response.json()
    except Exception:
        data = []

    if not data:
        raise RuntimeError(
            f"No se encontró operación para aprobar: {id_operacion}"
        )

    return {
        "ok": True,
        "mensaje": "Operación aprobada",
        "id_operacion": id_operacion,
        "actualizado": data[0],
    }



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
