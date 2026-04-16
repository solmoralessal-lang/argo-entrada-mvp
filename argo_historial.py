from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional


HISTORIAL_PATH = "logs/operaciones_historial.jsonl"


def asegurar_directorio_historial() -> None:
    os.makedirs(os.path.dirname(HISTORIAL_PATH), exist_ok=True)


def now_local_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def leer_historial_jsonl() -> List[Dict[str, Any]]:
    asegurar_directorio_historial()

    if not os.path.exists(HISTORIAL_PATH):
        return []

    registros: List[Dict[str, Any]] = []

    with open(HISTORIAL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                registros.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    return registros


def escribir_historial_jsonl(registros: List[Dict[str, Any]]) -> None:
    asegurar_directorio_historial()

    with open(HISTORIAL_PATH, "w", encoding="utf-8") as f:
        for r in registros:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def guardar_operacion_historial(operacion: Dict[str, Any]) -> None:
    asegurar_directorio_historial()

    with open(HISTORIAL_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(operacion, ensure_ascii=False) + "\n")


def buscar_operacion_por_id(id_operacion: str) -> Optional[Dict[str, Any]]:
    registros = leer_historial_jsonl()

    for r in reversed(registros):
        if r.get("id_operacion") == id_operacion:
            return r

    return None


def actualizar_operacion_historial(id_operacion: str, cambios: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    registros = leer_historial_jsonl()
    actualizada = None

    for i, r in enumerate(registros):
        if r.get("id_operacion") == id_operacion:
            registros[i] = {**r, **cambios}
            actualizada = registros[i]

    if actualizada is not None:
        escribir_historial_jsonl(registros)

    return actualizada

def extraer_operacion_desde_pipeline(pipeline_output: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrae la mejor versión posible de la operación.
    Prioridad:
    1. master
    2. operaciones[]
    3. raíz
    """

    # 🔥 PRIORIDAD 1: MASTER (lo más importante)
    if isinstance(pipeline_output.get("master"), dict):
        return pipeline_output["master"]

    # PRIORIDAD 2: lista de operaciones
    if isinstance(pipeline_output.get("operaciones"), list) and pipeline_output["operaciones"]:
        return pipeline_output["operaciones"][0]

    # PRIORIDAD 3: fallback
    return pipeline_output


def normalizar_operacion_para_historial(pipeline_output, cliente_id, cliente_nombre):
    master = pipeline_output.get("master", {})
    resumen = master.get("resumen_global", {})
    descargas = master.get("descargas", {})

    return {
        "id_operacion": pipeline_output.get("id_operacion"),
        "timestamp_local": master.get("meta", {}).get("timestamp_local"),
        "cliente_id": cliente_id,
        "cliente_nombre": cliente_nombre,

        "shipment_id": None,

        "estatus_global": resumen.get("estatus_global"),
        "riesgo_global": resumen.get("riesgo_global"),
        "score_documental_global": resumen.get("score_documental_global"),
        "semaforo_operacion": resumen.get("semaforo_operacion"),
        "alertas_totales": resumen.get("alertas_totales"),

        "control_output_path": descargas.get("control_output_path"),
        "class_output_path": descargas.get("class_output_path"),
        "document_output_path": descargas.get("document_output_path"),

        "master_output_path": "",

        "aprobada": False,
        "aprobada_por": None,
        "fecha_aprobacion": None,
    }

def obtener_dashboard_desde_historial(cliente_id: Optional[str] = None) -> Dict[str, Any]:
    registros = leer_historial_jsonl()

    if cliente_id:
        registros = [r for r in registros if r.get("cliente_id") == cliente_id]

    registros = sorted(
        registros,
        key=lambda x: x.get("timestamp_local", ""),
        reverse=True
    )

    operaciones_total = len(registros)
    criticas = sum(1 for r in registros if str(r.get("estatus_global", "")).upper() == "CRITICO")
    revision = sum(
        1 for r in registros
        if str(r.get("estatus_global", "")).upper() in ["REVISION", "CON_OBSERVACIONES"]
    )
    operables = sum(1 for r in registros if str(r.get("estatus_global", "")).upper() == "OPERABLE")
    aprobadas = sum(1 for r in registros if r.get("aprobada") is True)

    return {
        "ok": True,
        "modulo": "ARGO_DASHBOARD",
        "resumen": {
            "operaciones_total": operaciones_total,
            "criticas": criticas,
            "revision": revision,
            "operables": operables,
            "aprobadas": aprobadas
        },
        "operaciones": registros[:100]
    }


def obtener_clientes_desde_historial() -> Dict[str, Any]:
    registros = leer_historial_jsonl()

    mapa: Dict[str, Dict[str, str]] = {}

    for r in registros:
        cid = r.get("cliente_id")
        cname = r.get("cliente_nombre")

        if cid:
            mapa[cid] = {
                "cliente_id": cid,
                "cliente_nombre": cname or cid
            }

    clientes = sorted(mapa.values(), key=lambda x: x["cliente_nombre"].lower())

    return {
        "ok": True,
        "total": len(clientes),
        "clientes": clientes
    }


def obtener_historial(cliente_id: Optional[str] = None) -> Dict[str, Any]:
    registros = leer_historial_jsonl()

    registros = sorted(
        registros,
        key=lambda x: x.get("timestamp_local", ""),
        reverse=True
    )

    if cliente_id:
        registros = [r for r in registros if r.get("cliente_id") == cliente_id]

    return {
        "ok": True,
        "total": len(registros),
        "cliente_id": cliente_id,
        "operaciones": registros
    }


def aprobar_operacion(id_operacion: str, aprobada_por: str = "sistema") -> Dict[str, Any]:
    operacion = buscar_operacion_por_id(id_operacion)

    if not operacion:
        return {
            "ok": False,
            "mensaje": "Operación no encontrada",
            "operacion": None
        }

    if operacion.get("aprobada") is True:
        return {
            "ok": True,
            "mensaje": "La operación ya estaba aprobada",
            "operacion": operacion
        }

    operacion_actualizada = actualizar_operacion_historial(
        id_operacion,
        {
            "aprobada": True,
            "aprobada_por": aprobada_por,
            "fecha_aprobacion": now_local_iso()
        }
    )

    return {
        "ok": True,
        "mensaje": "Operación aprobada correctamente",
        "operacion": operacion_actualizada
    }
