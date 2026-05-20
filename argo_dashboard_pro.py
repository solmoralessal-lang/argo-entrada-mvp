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



def _pdf_escape(value: str) -> str:
    value = str(value or "")
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _wrap_text(text: str, width: int = 92) -> List[str]:
    text = str(text or "")
    words = text.split()
    lines = []
    current = ""

    for word in words:
        candidate = f"{current} {word}".strip()
        if len(candidate) <= width:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = word

    if current:
        lines.append(current)

    return lines or [""]


def _build_simple_pdf(lines: List[str]) -> bytes:
    page_lines = []
    chunk = []

    for line in lines:
        chunk.append(line)
        if len(chunk) >= 44:
            page_lines.append(chunk)
            chunk = []

    if chunk:
        page_lines.append(chunk)

    objects = []
    pages_refs = []

    def add_obj(content: bytes) -> int:
        objects.append(content)
        return len(objects)

    font_id = add_obj(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    for page in page_lines:
        commands = ["BT", "/F1 10 Tf", "50 790 Td", "14 TL"]
        for line in page:
            commands.append(f"({_pdf_escape(line)}) Tj")
            commands.append("T*")
        commands.append("ET")

        stream = "\n".join(commands).encode("latin-1", errors="replace")
        content_id = add_obj(
            b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream"
        )

        page_id = add_obj(
            b"<< /Type /Page /Parent 0 0 R /MediaBox [0 0 612 792] "
            + b"/Resources << /Font << /F1 "
            + str(font_id).encode()
            + b" 0 R >> >> /Contents "
            + str(content_id).encode()
            + b" 0 R >>"
        )
        pages_refs.append(page_id)

    kids = b" ".join([f"{pid} 0 R".encode() for pid in pages_refs])
    pages_id = add_obj(
        b"<< /Type /Pages /Kids [" + kids + b"] /Count " + str(len(pages_refs)).encode() + b" >>"
    )

    catalog_id = add_obj(b"<< /Type /Catalog /Pages " + str(pages_id).encode() + b" 0 R >>")

    patched = []
    for obj in objects:
        patched.append(obj.replace(b"/Parent 0 0 R", f"/Parent {pages_id} 0 R".encode()))

    pdf = bytearray()
    pdf.extend(b"%PDF-1.4\n")
    offsets = [0]

    for idx, obj in enumerate(patched, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{idx} 0 obj\n".encode())
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")

    xref_pos = len(pdf)
    pdf.extend(f"xref\n0 {len(patched) + 1}\n".encode())
    pdf.extend(b"0000000000 65535 f \n")

    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode())

    pdf.extend(
        b"trailer\n<< /Size "
        + str(len(patched) + 1).encode()
        + b" /Root "
        + str(catalog_id).encode()
        + b" 0 R >>\nstartxref\n"
        + str(xref_pos).encode()
        + b"\n%%EOF"
    )

    return bytes(pdf)


def generar_pdf_dashboard_pro(pro: Dict[str, Any], output_path: str) -> str:
    kpis = pro.get("kpis", {})
    tendencias = pro.get("tendencias", {})
    ranking = pro.get("ranking_operadores", [])
    incidencias = pro.get("incidencias_criticas", [])
    alertas = pro.get("alertas_inteligentes", [])
    timeline = pro.get("timeline_vivo", [])
    heatmap = pro.get("heatmap_operacional", [])

    lines = []
    lines.append("ARGO - REPORTE EJECUTIVO DASHBOARD PRO")
    lines.append("=" * 72)
    lines.append(f"Cliente/Tenant: {pro.get('cliente_id') or 'N/A'}")
    lines.append(f"Generado en: {pro.get('generado_en') or 'N/A'}")
    lines.append("")

    lines.append("1. KPIs EJECUTIVOS")
    lines.append("-" * 72)
    for key, label in [
        ("operaciones_total", "Operaciones totales"),
        ("operaciones_24h", "Operaciones ultimas 24h"),
        ("operaciones_7d", "Operaciones ultimos 7 dias"),
        ("criticas", "Operaciones criticas"),
        ("revision", "Operaciones en revision"),
        ("operables", "Operaciones operables"),
        ("aprobadas", "Operaciones aprobadas"),
        ("incidencias_criticas", "Incidencias criticas"),
        ("alertas_inteligentes", "Alertas inteligentes"),
    ]:
        lines.append(f"{label}: {kpis.get(key, 0)}")

    lines.append("")
    lines.append("2. TENDENCIAS TEMPORALES")
    lines.append("-" * 72)
    lines.append(f"Ultimas 24h: {tendencias.get('ultimas_24h', 0)}")
    lines.append(f"Ultimos 7 dias: {tendencias.get('ultimos_7_dias', 0)}")

    lines.append("")
    lines.append("3. RANKING DE OPERADORES")
    lines.append("-" * 72)
    if ranking:
        for idx, item in enumerate(ranking[:15], start=1):
            lines.append(f"{idx}. {item.get('operador', 'SIN_OPERADOR')} - {item.get('operaciones', 0)} operaciones")
    else:
        lines.append("Sin datos de operadores.")

    lines.append("")
    lines.append("4. HEATMAP OPERACIONAL")
    lines.append("-" * 72)
    if heatmap:
        for dia in heatmap[:15]:
            horas = ", ".join([f"{h}: {v}" for h, v in (dia.get("horas") or {}).items()])
            lines.append(f"{dia.get('dia')}: {horas}")
    else:
        lines.append("Sin actividad temporal.")

    lines.append("")
    lines.append("5. ALERTAS INTELIGENTES")
    lines.append("-" * 72)
    if alertas:
        for a in alertas[:20]:
            msg = f"[{a.get('severidad', 'N/A')}] {a.get('tipo', 'ALERTA')}: {a.get('mensaje', '')}"
            lines.extend(_wrap_text(msg))
    else:
        lines.append("Sin alertas inteligentes.")

    lines.append("")
    lines.append("6. CENTRO DE INCIDENCIAS CRITICAS")
    lines.append("-" * 72)
    if incidencias:
        for i in incidencias[:20]:
            lines.append(f"Operacion: {i.get('id_operacion')}")
            lines.append(f"Cliente: {i.get('cliente')} | Operador: {i.get('operador')}")
            lines.append(f"Estado: {i.get('estado')} | Riesgo: {i.get('riesgo')} | Score: {i.get('score_documental')}")
            lines.extend(_wrap_text(f"Accion sugerida: {i.get('accion_sugerida')}"))
            lines.append("")
    else:
        lines.append("Sin incidencias criticas.")

    lines.append("")
    lines.append("7. TIMELINE VIVO")
    lines.append("-" * 72)
    if timeline:
        for ev in timeline[:25]:
            lines.append(f"{ev.get('fecha')} | {ev.get('id_operacion')} | {ev.get('estado')} | {ev.get('riesgo')}")
            lines.extend(_wrap_text(f"{ev.get('evento')} - {ev.get('operador')} - {ev.get('cliente')}"))
    else:
        lines.append("Sin eventos recientes.")

    lines.append("")
    lines.append("8. CONCLUSION EJECUTIVA")
    lines.append("-" * 72)

    if kpis.get("criticas", 0) > 0 or kpis.get("incidencias_criticas", 0) > 0:
        conclusion = "Salud operativa: CRITICA. Se recomienda intervencion inmediata en operaciones con riesgo critico, score documental bajo o alertas acumuladas."
    elif kpis.get("revision", 0) > 0:
        conclusion = "Salud operativa: EN REVISION. Existen operaciones que requieren seguimiento antes de liberacion."
    else:
        conclusion = "Salud operativa: ESTABLE. No se detectan incidencias criticas en el corte actual."

    lines.extend(_wrap_text(conclusion))
    lines.append("")
    lines.append("Documento generado automaticamente por ARGO Dashboard PRO.")

    pdf_bytes = _build_simple_pdf(lines)

    with open(output_path, "wb") as f:
        f.write(pdf_bytes)

    return output_path
