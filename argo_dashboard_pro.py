from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import Any, Dict, List, Optional


def _parse_dt(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _get(op: Dict[str, Any], *keys, default=None):
    for k in keys:
        if k in op and op.get(k) not in [None, ""]:
            return op.get(k)
    return default


def _normalizar_operaciones(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ["operaciones", "historial", "items", "data"]:
            if isinstance(data.get(key), list):
                return data.get(key)
        if isinstance(data.get("dashboard"), dict):
            return _normalizar_operaciones(data.get("dashboard"))
    return []


def construir_dashboard_pro(data: Any, cliente_id: Optional[str] = None) -> Dict[str, Any]:
    operaciones = _normalizar_operaciones(data)
    ahora = datetime.now()
    hace_24h = ahora - timedelta(hours=24)
    hace_7d = ahora - timedelta(days=7)

    ops_24h = []
    ops_7d = []
    timeline = []
    heatmap = defaultdict(lambda: defaultdict(int))
    operadores = Counter()
    incidencias = []
    alertas = []

    total = len(operaciones)
    criticas = 0
    revision = 0
    operables = 0
    aprobadas = 0

    for op in operaciones:
        fecha = _parse_dt(_get(op, "fecha", "created_at", "fecha_operacion", "timestamp", "fecha_creacion"))
        estado = str(_get(op, "estatus_global", "estado", "status", default="SIN_ESTADO")).upper()
        riesgo = str(_get(op, "riesgo_global", "riesgo", "semaforo_operacion", default="SIN_RIESGO")).upper()
        operador = _get(op, "usuario", "operador", "creado_por", "email", "aprobada_por", default="SIN_OPERADOR")
        id_operacion = _get(op, "id_operacion", "id", "operacion_id", default="SIN_ID")
        cliente = _get(op, "cliente_nombre", "cliente_id", "cliente", default=cliente_id or "SIN_CLIENTE")
        score_doc = _get(op, "score_documental_global", "score_documental", default=None)
        alertas_totales = _get(op, "alertas_totales", "conteo_alertas", default=0)

        if estado in ["CRITICA", "CRITICO", "ERROR"] or riesgo in ["ALTO", "CRITICO", "ROJO"]:
            criticas += 1
        if estado in ["REVISION", "REQUIERE_REVISION", "ADVERTENCIA"]:
            revision += 1
        if estado in ["OK", "OPERABLE", "APROBADA"]:
            operables += 1
        if op.get("aprobada") is True:
            aprobadas += 1

        if fecha:
            if fecha >= hace_24h:
                ops_24h.append(op)
            if fecha >= hace_7d:
                ops_7d.append(op)

            dia = fecha.strftime("%Y-%m-%d")
            hora = fecha.strftime("%H:00")
            heatmap[dia][hora] += 1

            timeline.append({
                "fecha": fecha.isoformat(timespec="seconds"),
                "id_operacion": id_operacion,
                "cliente": cliente,
                "operador": operador,
                "estado": estado,
                "riesgo": riesgo,
                "evento": "Operación registrada",
            })

        operadores[str(operador)] += 1

        es_critica = estado in ["CRITICA", "CRITICO", "ERROR"] or riesgo in ["ALTO", "CRITICO", "ROJO"]
        if es_critica:
            incidencias.append({
                "id_operacion": id_operacion,
                "cliente": cliente,
                "operador": operador,
                "estado": estado,
                "riesgo": riesgo,
                "score_documental": score_doc,
                "alertas_totales": alertas_totales,
                "prioridad": "ALTA",
                "accion_sugerida": "Revisión inmediata por supervisor/admin",
            })

        try:
            score_num = float(score_doc)
            if score_num < 70:
                alertas.append({
                    "tipo": "SCORE_DOCUMENTAL_BAJO",
                    "id_operacion": id_operacion,
                    "mensaje": f"Score documental bajo: {score_num}",
                    "severidad": "MEDIA" if score_num >= 50 else "ALTA",
                })
        except Exception:
            pass

        try:
            alertas_num = int(alertas_totales or 0)
            if alertas_num >= 3:
                alertas.append({
                    "tipo": "ALERTAS_ACUMULADAS",
                    "id_operacion": id_operacion,
                    "mensaje": f"Operación con {alertas_num} alertas",
                    "severidad": "ALTA",
                })
        except Exception:
            pass

    timeline = sorted(timeline, key=lambda x: x.get("fecha", ""), reverse=True)[:50]
    incidencias = incidencias[:50]
    alertas = alertas[:50]

    ranking_operadores = [
        {"operador": operador, "operaciones": cantidad}
        for operador, cantidad in operadores.most_common(20)
    ]

    heatmap_operacional = [
        {"dia": dia, "horas": dict(sorted(horas.items()))}
        for dia, horas in sorted(heatmap.items(), reverse=True)
    ]

    return {
        "ok": True,
        "modulo": "ARGO_DASHBOARD_PRO",
        "cliente_id": cliente_id,
        "generado_en": ahora.isoformat(timespec="seconds"),
        "kpis": {
            "operaciones_total": total,
            "operaciones_24h": len(ops_24h),
            "operaciones_7d": len(ops_7d),
            "criticas": criticas,
            "revision": revision,
            "operables": operables,
            "aprobadas": aprobadas,
            "incidencias_criticas": len(incidencias),
            "alertas_inteligentes": len(alertas),
        },
        "tendencias": {
            "ultimas_24h": len(ops_24h),
            "ultimos_7_dias": len(ops_7d),
        },
        "heatmap_operacional": heatmap_operacional,
        "ranking_operadores": ranking_operadores,
        "timeline_vivo": timeline,
        "alertas_inteligentes": alertas,
        "incidencias_criticas": incidencias,
        "export_pdf_ready": True,
    }
