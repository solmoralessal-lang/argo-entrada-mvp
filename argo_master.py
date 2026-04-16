# argo_master.py
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List


SCHEMA_NAME = "ARGO_MASTER_OUTPUT_V2026"
VERSION_MASTER = "2026.1"


# ========================================================
# HELPERS
# =========================================================

def _safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _rank_severidad(sev: str) -> int:
    order = {
        "BAJA": 1,
        "MEDIA": 2,
        "ALTA": 3,
        "CRITICA": 4,
    }
    return order.get(str(sev).upper().strip(), 0)


def _max_severidad(*values: str) -> str:
    vals = [v for v in values if str(v).strip()]
    if not vals:
        return "BAJA"
    return max(vals, key=_rank_severidad)


def _map_severidad_to_icon(sev: str) -> str:
    sev = str(sev).upper().strip()
    if sev == "CRITICA":
        return "🔴"
    if sev == "ALTA":
        return "🟠"
    if sev == "MEDIA":
        return "🟡"
    return "🟢"


def _riesgo_rank(riesgo: str) -> int:
    order = {
        "BAJO": 1,
        "MEDIO": 2,
        "ALTO": 3,
        "CRITICO": 4,
    }
    return order.get(str(riesgo).upper().strip(), 0)


def _max_riesgo(*values: str) -> str:
    vals = [v for v in values if str(v).strip()]
    if not vals:
        return "BAJO"
    return max(vals, key=_riesgo_rank)


def _dictamen_por_estatus_global(estatus_global: str) -> str:
    est = str(estatus_global).upper().strip()

    if est == "GENERADO_OK":
        return "Operación procesada sin hallazgos críticos."
    if est == "CON_OBSERVACIONES":
        return "Operación procesada con observaciones. Se recomienda validación previa."
    if est == "REQUIERE_REVISION":
        return "Operación con hallazgos relevantes. Requiere revisión antes de ejecutar cruce."
    if est == "CRITICO":
        return "Operación con riesgo crítico documental/técnico. No se recomienda proceder sin validación especializada."
    if est == "ERROR_TECNICO":
        return "Operación con error técnico. No fue posible completar el flujo."
    if est == "OPERABLE":
        return "Operación procesada y operable."
    return "Operación procesada."


def _estatus_global_from_inputs(
    riesgo_global: str,
    severidad_maxima_global: str,
    modulos_ok: int,
    modulos_ejecutados: int,
) -> str:
    if modulos_ok < modulos_ejecutados:
        return "ERROR_TECNICO"

    if str(riesgo_global).upper() == "CRITICO" or str(severidad_maxima_global).upper() == "CRITICA":
        return "CRITICO"

    if str(severidad_maxima_global).upper() == "ALTA":
        return "REQUIERE_REVISION"

    if str(severidad_maxima_global).upper() == "MEDIA":
        return "CON_OBSERVACIONES"

    return "GENERADO_OK"


# =========================================================
# DATACLASSES
# =========================================================

@dataclass
class MasterMeta:
    schema: str
    version_master: str
    timestamp_local: str
    id_operacion: str
    id_cliente: str
    usuario_ejecucion: str
    origen_ejecucion: str


@dataclass
class ResumenGlobal:
    estatus_global: str
    icono_global: str
    semaforo_operacion: str
    dictamen_operacion: str
    riesgo_global: str
    score_documental_global: int
    severidad_maxima_global: str
    modulos_ejecutados: int
    modulos_ok: int
    modulos_con_alertas: int
    alertas_totales: int


@dataclass
class Trazabilidad:
    pipeline: List[str]
    duracion_ms: int


# =========================================================
# NORMALIZADORES DE MÓDULOS
# =========================================================

def _normalize_control(control: Dict[str, Any]) -> Dict[str, Any]:
    control = _safe_dict(control)
    resumen = _safe_dict(control.get("resumen"))

    return {
        "ok": bool(control.get("ok", False)),
        "estatus": control.get("estatus", "NO_EJECUTADO"),
        "icono": control.get("icono", "⚪"),
        "output_path": control.get("output_path", ""),
        "resumen": resumen,
    }


def _normalize_class(class_data: Dict[str, Any]) -> Dict[str, Any]:
    class_data = _safe_dict(class_data)
    salida = _safe_dict(class_data.get("salida"))
    score_documental = _safe_dict(salida.get("score_documental"))
    certeza_y_riesgo = _safe_dict(salida.get("certeza_y_riesgo"))
    clasificacion = _safe_dict(salida.get("clasificacion"))

    riesgo_automatico = certeza_y_riesgo.get("riesgo_automatico", "CRITICO")
    icono = "🔴" if str(riesgo_automatico).upper() == "CRITICO" else "🟠"

    return {
        "ok": True if class_data else False,
        "estatus": riesgo_automatico,
        "icono": icono,
        "output_path": class_data.get("output_path", ""),
        "resumen": {
            "score_documental": score_documental.get("score_total_0_100", 0),
            "nivel_debida_diligencia": score_documental.get("nivel_debida_diligencia", "INTENSIVA"),
            "riesgo_automatico": riesgo_automatico,
            "fraccion_sugerida": clasificacion.get("fraccion_sugerida", "POR_DEFINIR"),
            "confianza_fraccion_pct": clasificacion.get("confianza_fraccion_pct", 0),
            "certeza_final_pct": certeza_y_riesgo.get("certeza_final_pct", 0),
        },
    }


def _normalize_document(document: Dict[str, Any]) -> Dict[str, Any]:
    document = _safe_dict(document)

    return {
        "ok": bool(document.get("ok", False)),
        "estatus": document.get("estatus", "NO_EJECUTADO"),
        "icono": document.get("icono", "⚪"),
        "output_path": document.get("output_path", ""),
        "resumen": _safe_dict(document.get("resumen")),
    }


# =========================================================
# ALERTAS CONSOLIDADAS
# =========================================================

def _build_alertas_consolidadas(
    control: Dict[str, Any],
    class_data: Dict[str, Any],
    document: Dict[str, Any],
) -> List[Dict[str, Any]]:
    alertas: List[Dict[str, Any]] = []

    control_resumen = _safe_dict(control.get("resumen"))
    observaciones_total = int(control_resumen.get("observaciones_total", 0) or 0)
    control_severidad = str(control_resumen.get("severidad_maxima", "BAJA")).upper()

    if observaciones_total > 0:
        alertas.append({
            "modulo": "ARGO_CONTROL",
            "codigo": "CONTROL_OBSERVACIONES",
            "nivel": control_severidad,
            "mensaje": f"ARGO CONTROL reporta {observaciones_total} observación(es).",
            "accion_sugerida": "Revisar inconsistencias detectadas por CONTROL.",
        })

    class_alertas = _safe_list(
        _safe_dict(_safe_dict(class_data.get("salida")).get("tablas")).get("alertas")
    )
    for a in class_alertas:
        a = _safe_dict(a)
        alertas.append({
            "modulo": "ARGO_CLASS",
            "codigo": a.get("codigo", "CLASS_ALERTA"),
            "nivel": a.get("nivel", "ADVERTENCIA"),
            "mensaje": a.get("mensaje", ""),
            "accion_sugerida": a.get("accion_sugerida", ""),
        })

    doc_alertas = _safe_list(document.get("alertas"))
    for a in doc_alertas:
        a = _safe_dict(a)
        alertas.append({
            "modulo": "ARGO_DOCUMENT",
            "codigo": a.get("codigo", "DOC_ALERTA"),
            "nivel": a.get("severidad", "MEDIA"),
            "mensaje": a.get("mensaje", ""),
            "accion_sugerida": a.get("accion_recomendada", ""),
        })

    return alertas


# =========================================================
# INDICADORES
# =========================================================

def _build_indicadores(
    control: Dict[str, Any],
    class_data: Dict[str, Any],
    document: Dict[str, Any],
) -> Dict[str, Any]:
    control_resumen = _safe_dict(control.get("resumen"))
    class_salida = _safe_dict(class_data.get("salida"))
    class_score = _safe_dict(class_salida.get("score_documental"))
    class_clasificacion = _safe_dict(class_salida.get("clasificacion"))
    class_riesgo = _safe_dict(class_salida.get("certeza_y_riesgo"))
    doc_resumen = _safe_dict(document.get("resumen"))
    doc_score = _safe_dict(document.get("score_documental_doc"))

    return {
        "control": {
            "observaciones_total": int(control_resumen.get("observaciones_total", 0) or 0),
            "severidad_maxima": control_resumen.get("severidad_maxima", "BAJA"),
        },
        "class": {
            "fraccion_sugerida": class_clasificacion.get("fraccion_sugerida", "POR_DEFINIR"),
            "confianza_fraccion_pct": class_clasificacion.get("confianza_fraccion_pct", 0),
            "certeza_final_pct": class_riesgo.get("certeza_final_pct", 0),
            "riesgo_automatico": class_riesgo.get("riesgo_automatico", "CRITICO"),
            "nivel_debida_diligencia": class_score.get("nivel_debida_diligencia", "INTENSIVA"),
            "score_documental": class_score.get("score_total_0_100", 0),
        },
        "document": {
            "documentos_detectados": doc_resumen.get("documentos_detectados", 0),
            "documentos_requeridos": doc_resumen.get("documentos_requeridos", 0),
            "faltantes": doc_resumen.get("faltantes", 0),
            "campos_no_verificables": doc_resumen.get("campos_no_verificables", 0),
            "score_documental_doc": doc_score.get("score_total", 0),
        },
    }


# =========================================================
# BUILDER PRINCIPAL
# =========================================================

def build_master_output(
    pipeline_result: Dict[str, Any],
    id_cliente: str = "CL-001",
    usuario_ejecucion: str = "system",
    origen_ejecucion: str = "WEB",
    duracion_ms: int = 0,
) -> Dict[str, Any]:
    pipeline_result = _safe_dict(pipeline_result)

    id_operacion = pipeline_result.get("id_operacion", "")
    control_raw = _safe_dict(pipeline_result.get("control"))
    class_raw = _safe_dict(pipeline_result.get("class"))
    document_raw = _safe_dict(pipeline_result.get("document"))

    control = _normalize_control(control_raw)
    class_mod = _normalize_class(class_raw)
    document = _normalize_document(document_raw)

    alertas_consolidadas = _build_alertas_consolidadas(control_raw, class_raw, document_raw)
    indicadores = _build_indicadores(control_raw, class_raw, document_raw)

    control_sev = str(_safe_dict(control_raw.get("resumen")).get("severidad_maxima", "BAJA")).upper()
    document_sev = str(_safe_dict(document_raw.get("resumen")).get("severidad_maxima", "BAJA")).upper()
    class_riesgo = str(
        _safe_dict(_safe_dict(class_raw.get("salida")).get("certeza_y_riesgo")).get("riesgo_automatico", "CRITICO")
    ).upper()

    class_severidad = (
        "CRITICA" if class_riesgo == "CRITICO"
        else "ALTA" if class_riesgo == "ALTO"
        else "MEDIA" if class_riesgo == "MEDIO"
        else "BAJA"
    )

    severidad_maxima_global = _max_severidad(control_sev, document_sev, class_severidad)
    riesgo_global = _max_riesgo(class_riesgo)

    modulos = {
        "control": control,
        "class": class_mod,
        "document": document,
    }

    modulos_ejecutados = len(modulos)
    modulos_ok = sum(1 for _, m in modulos.items() if m.get("ok") is True)

    modulos_con_alertas = 0
    if int(indicadores["control"]["observaciones_total"]) > 0:
        modulos_con_alertas += 1
    if len(_safe_list(_safe_dict(_safe_dict(class_raw.get("salida")).get("tablas")).get("alertas"))) > 0:
        modulos_con_alertas += 1
    if len(_safe_list(document_raw.get("alertas"))) > 0:
        modulos_con_alertas += 1

    alertas_totales = len(alertas_consolidadas)
    score_documental_global = int(indicadores["class"]["score_documental"])

    riesgo_operativo = 0
    if class_riesgo == "CRITICO":
        riesgo_operativo += 60
    elif class_riesgo == "ALTO":
        riesgo_operativo += 45
    elif class_riesgo == "MEDIO":
        riesgo_operativo += 25
    else:
        riesgo_operativo += 10

    riesgo_operativo += min(alertas_totales * 2, 20)
    riesgo_operativo += max(0, round((100 - score_documental_global) * 0.2))
    riesgo_operativo = min(riesgo_operativo, 100)

    # Lógica corregida de estatus global
    control_ok = control.get("ok", False)
    class_ok = class_mod.get("ok", False)
    document_ok = document.get("ok", False)

    if not control_ok and not class_ok and not document_ok:
        estatus_global = "ERROR_TECNICO"
    elif document.get("estatus") == "REQUIERE_REVISION":
        estatus_global = "CON_OBSERVACIONES"
    elif control.get("estatus") == "CON_OBSERVACIONES":
        estatus_global = "CON_OBSERVACIONES"
    elif riesgo_operativo >= 61:
        estatus_global = "CRITICO"
    elif riesgo_operativo >= 31:
        estatus_global = "REQUIERE_REVISION"
    else:
        estatus_global = "OPERABLE"

    if estatus_global == "ERROR_TECNICO":
        icono_global = "🔴"
    elif estatus_global == "CRITICO":
        icono_global = "🔴"
    elif estatus_global in ("CON_OBSERVACIONES", "REQUIERE_REVISION"):
        icono_global = "🟡"
    else:
        icono_global = "🟢"

    semaforo_operacion = icono_global

    resumen_global = ResumenGlobal(
        estatus_global=estatus_global,
        icono_global=icono_global,
        semaforo_operacion=semaforo_operacion,
        dictamen_operacion=_dictamen_por_estatus_global(estatus_global),
        riesgo_global=riesgo_global,
        score_documental_global=score_documental_global,
        severidad_maxima_global=severidad_maxima_global,
        modulos_ejecutados=modulos_ejecutados,
        modulos_ok=modulos_ok,
        modulos_con_alertas=modulos_con_alertas,
        alertas_totales=alertas_totales,
    )

    meta = MasterMeta(
        schema=SCHEMA_NAME,
        version_master=VERSION_MASTER,
        timestamp_local=_now_iso(),
        id_operacion=id_operacion,
        id_cliente=id_cliente,
        usuario_ejecucion=usuario_ejecucion,
        origen_ejecucion=origen_ejecucion,
    )

    trazabilidad = Trazabilidad(
        pipeline=["ARGO_CONTROL", "ARGO_CLASS", "ARGO_DOCUMENT"],
        duracion_ms=duracion_ms,
    )

    return {
        "ok": True,
        "modulo": "ARGO_MASTER",
        "meta": asdict(meta),
        "resumen_global": asdict(resumen_global),
        "modulos": modulos,
        "indicadores": indicadores,
        "alertas_consolidadas": alertas_consolidadas,
        "descargas": {
            "control_output_path": control.get("output_path", ""),
            "class_output_path": class_mod.get("output_path", ""),
            "document_output_path": document.get("output_path", ""),
            "control_url": pipeline_result.get("archivos_publicos", {}).get("control_url", ""),
            "document_url": pipeline_result.get("archivos_publicos", {}).get("document_url", ""),
        },
        "trazabilidad": asdict(trazabilidad),
        "errores": [],
    }
