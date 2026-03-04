from typing import Dict, Any, List
import hashlib
import json


# =========================
# Utilidades
# =========================
def hash_payload(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()


def clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))


# =========================
# Config "embebida" (rápido y estable)
# Luego lo pasamos a /config/*.json
# =========================
SECTOR_KEYWORDS = {
    "QUIMICO": ["msds", "sds", "solvente", "acido", "ácido", "resina", "cas", "corrosive", "flammable", "peligroso"],
    "ELECTRONICO": ["volt", "watt", "usb", "sensor", "pcb", "chip", "bluetooth", "adapter", "cargador", "transformer"],
    "TEXTIL": ["fibra", "algodon", "algodón", "poliester", "poliéster", "tejido", "punto", "denier", "tela", "gramaje"],
    "MAQUINARIA": ["motor", "bomba", "rpm", "valvula", "válvula", "compressor", "compresor", "torque", "hp"],
    "ALIMENTARIO": ["ingredientes", "food", "edible", "congelado", "frozen", "beverage", "consumo humano", "animal feed"],
    "PLASTICOS": ["plastic", "polymer", "polyethylene", "polypropylene", "pvc", "abs", "pellet", "granule", "resin"],
    "METALMECANICO": ["steel", "aluminum", "aluminium", "alloy", "aleacion", "aleación", "stainless", "copper", "brass", "forged"],
}

# Faltantes críticos por sector (MVP PRO)
SECTOR_FALTANTES = {
    "QUIMICO": [
        {"campo": "MSDS", "criticidad": "CRITICA", "documento": "MSDS/SDS", "motivo": "Químico sin hoja de seguridad (SDS/MSDS).", "penal": 15},
        {"campo": "CAS", "criticidad": "CRITICA", "documento": "MSDS o ficha con CAS", "motivo": "Químico sin identificador CAS.", "penal": 10},
        {"campo": "COMPOSICION", "criticidad": "CRITICA", "documento": "MSDS / ficha técnica / laboratorio", "motivo": "Composición o concentración no especificada.", "penal": 10},
    ],
    "ELECTRONICO": [
        {"campo": "FUNCION_PRINCIPAL", "criticidad": "CRITICA", "documento": "Ficha técnica / manual", "motivo": "Falta función principal del equipo.", "penal": 12},
        {"campo": "PARTE_O_EQUIPO", "criticidad": "CRITICA", "documento": "Catálogo / BOM / ficha", "motivo": "No se define si es parte o equipo completo.", "penal": 10},
    ],
    "TEXTIL": [
        {"campo": "COMPOSICION_FIBRAS_%", "criticidad": "CRITICA", "documento": "Ficha / etiqueta", "motivo": "Textil sin composición de fibras (%).", "penal": 12},
        {"campo": "ESTRUCTURA_TEJIDO", "criticidad": "CRITICA", "documento": "Ficha técnica", "motivo": "Textil sin estructura (tejido/punto/no tejido).", "penal": 10},
    ],
    "MAQUINARIA": [
        {"campo": "FUNCION_PRINCIPAL", "criticidad": "CRITICA", "documento": "Manual / ficha", "motivo": "Maquinaria sin función principal clara.", "penal": 12},
    ],
}

# Alertas (MVP)
def make_alert(nivel: str, codigo: str, mensaje: str, accion: str, origen: str) -> Dict[str, Any]:
    return {"nivel": nivel, "codigo": codigo, "mensaje": mensaje, "accion_sugerida": accion, "modulo_origen": origen}


# =========================
# 1) Detección de sector (determinística)
# =========================
def detect_sector(descripcion: str) -> Dict[str, Any]:
    desc = (descripcion or "").lower()
    scores: Dict[str, int] = {k: 0 for k in SECTOR_KEYWORDS.keys()}

    for sector, terms in SECTOR_KEYWORDS.items():
        for t in terms:
            if t in desc:
                scores[sector] += 1

    # top-2
    ordered = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top_sector, top_score = ordered[0]
    second_sector, second_score = ordered[1]

    if top_score == 0:
        return {
            "sector_detectado": "OTRO",
            "confianza_sector_pct": 50,
            "posible_multisector": False,
            "justificacion": ["sin coincidencias fuertes"],
        }

    posible_multisector = second_score > 0 and (second_score / max(top_score, 1)) >= 0.75

    # confianza simple
    conf = 60 + min(35, top_score * 10)
    if posible_multisector:
        conf = int(conf * 0.8)

    conf = clamp(int(conf), 0, 100)
    return {
        "sector_detectado": top_sector,
        "confianza_sector_pct": conf,
        "posible_multisector": posible_multisector,
        "justificacion": [f"hits:{top_score}", f"2do:{second_sector}={second_score}"],
    }


# =========================
# 2) Score documental por bloques (MVP PRO)
# =========================
def score_documental(payload: Dict[str, Any], sector: str, descripcion: str) -> Dict[str, Any]:
    """
    Score total 0-100 construido por bloques:
    - Identificación técnica (0-40)
    - Soporte técnico (0-35)
    - Congruencia y control (0-25)  (si viene ARGO CONTROL)
    Penalizaciones por faltantes críticos por sector.
    """
    alerts: List[Dict[str, Any]] = []
    faltantes: List[Dict[str, Any]] = []

    desc = (descripcion or "").lower()
    meta = payload.get("meta", {}) or {}
    control = payload.get("control", {}) or {}          # opcional
    docs = payload.get("documentos", []) or []          # opcional: lista de strings o dicts

    def has_doc(name: str) -> bool:
        # docs puede ser ["MSDS","FACTURA"] o [{"tipo":"MSDS","presente":true}]
        for d in docs:
            if isinstance(d, str):
                if d.strip().lower() == name.lower():
                    return True
            if isinstance(d, dict):
                tipo = str(d.get("tipo", "")).lower()
                presente = bool(d.get("presente", False))
                if tipo == name.lower() and presente:
                    return True
        return False

    # ---- Bloque A: Identificación técnica (0-40)
    ident = 0
    if desc.strip():
        ident += 12
    # señales de composición/material
    if any(k in desc for k in ["%","porcentaje","composition","composición","aleacion","alloy","poly","fibra"]):
        ident += 10
    # señales de función/uso
    if any(k in desc for k in ["uso","funcion","función","aplicacion","application","for ","para "]):
        ident += 10
    # modelo/parte/código
    if any(k in desc for k in ["model","modelo","part","p/n","sku","serial"]):
        ident += 8
    ident = clamp(ident, 0, 40)

    # ---- Bloque B: Soporte técnico (0-35)
    soporte = 0
    if has_doc("FICHA_TECNICA") or ("datasheet" in desc) or ("ficha técnica" in desc):
        soporte += 15
    if has_doc("CATALOGO") or ("catalog" in desc) or ("catálogo" in desc):
        soporte += 10
    if has_doc("MANUAL") or ("manual" in desc):
        soporte += 8
    if has_doc("MSDS") or ("msds" in desc) or ("sds" in desc):
        soporte += 12
    soporte = clamp(soporte, 0, 35)

    # ---- Bloque C: Congruencia/control (0-25)
    congr = 15  # base
    consistencia = control.get("consistencia_pct")
    severidad = str(control.get("severidad_maxima", "BAJA")).upper()

    if isinstance(consistencia, (int, float)):
        if consistencia >= 90:
            congr += 8
        elif consistencia >= 75:
            congr += 2
        else:
            congr -= 8

    if severidad == "MEDIA":
        congr -= 4
    elif severidad == "ALTA":
        congr -= 8
    elif severidad == "CRITICA":
        congr -= 12

    congr = clamp(congr, 0, 25)

    # ---- Penalizaciones por faltantes críticos por sector
    reglas = SECTOR_FALTANTES.get(sector, [])
    for r in reglas:
        campo = r["campo"]
        penal = int(r["penal"])

        missing = False
        if campo == "MSDS":
            missing = not (has_doc("MSDS") or "msds" in desc or "sds" in desc)
        elif campo == "CAS":
            missing = "cas" not in desc and not has_doc("MSDS")
        elif campo == "COMPOSICION":
            missing = not any(k in desc for k in ["%","porcentaje","composition","composición","concentracion","concentración"])
        elif campo == "FUNCION_PRINCIPAL":
            missing = not any(k in desc for k in ["uso","funcion","función","aplicacion","application"])
        elif campo == "PARTE_O_EQUIPO":
            missing = not any(k in desc for k in ["parte","part","component","equipo","device","unit"])
        elif campo == "COMPOSICION_FIBRAS_%":
            missing = not any(k in desc for k in ["%","poliester","algodon","nylon","rayon","acrylic","lana"])
        elif campo == "ESTRUCTURA_TEJIDO":
            missing = not any(k in desc for k in ["tejido","punto","no tejido","woven","knit","nonwoven"])

        if missing:
            faltantes.append({
                "campo": campo,
                "criticidad": r["criticidad"],
                "documento_requerido": r["documento"],
                "motivo": r["motivo"],
                "impacto_estimado_en_certeza": f"-{penal}"
            })
            alerts.append(make_alert(
                "CRITICA",
                "CLASS_FALTANTE_CRITICO",
                f"Faltante crítico: {campo}. {r['motivo']}",
                f"Aportar: {r['documento']}",
                "SCORING"
            ))
            # penalizamos sobre total luego (para mantener explicable)
    penal_total = sum(int(x["impacto_estimado_en_certeza"].replace("-", "")) for x in faltantes)

    # ---- Score total
    score_raw = ident + soporte + congr
    score_final = clamp(score_raw - penal_total, 0, 100)
        # ---- Piso de score por sector (evita colapso extremo)
    piso = 0
    if sector == "QUIMICO":
        piso = 25
    elif sector in ["ELECTRONICO", "TEXTIL", "MAQUINARIA"]:
        piso = 20
    else:
        piso = 30 if desc.strip() else 0

    if score_final < piso:
        score_final = piso
    # Debida diligencia (mapeo)
    if score_final >= 80:
        dd = "BASICA"
    elif score_final >= 65:
        dd = "MEDIA"
    elif score_final >= 50:
        dd = "ALTA"
    else:
        dd = "INTENSIVA"

    # Alertas por score
    if score_final < 45:
        alerts.append(make_alert("CRITICA", "CLASS_SCORE_CRITICO", "Score documental crítico (<45).", "Aportar soporte técnico robusto.", "SCORING"))
    elif score_final < 60:
        alerts.append(make_alert("ADVERTENCIA", "CLASS_SCORE_MEDIO", "Score documental medio (45–59).", "Reforzar documentación técnica.", "SCORING"))

    return {
        "score_total_0_100": score_final,
        "nivel_debida_diligencia": dd,
        "bloques": {
            "identificacion_tecnica_0_40": ident,
            "soporte_tecnico_0_35": soporte,
            "congruencia_control_0_25": congr,
            "penalizaciones_por_faltantes": penal_total,
        },
        "alertas": alerts,
        "datos_faltantes": faltantes,
        "control_influencia": {
            "consistencia_pct": consistencia,
            "severidad_maxima": severidad,
        }
    }


# =========================
# 3) Riesgo automático (MVP PRO)
# =========================
def riesgo_automatico(score: int, sector_conf: int, posible_multisector: bool, alertas: List[Dict[str, Any]]) -> str:
    crit = sum(1 for a in alertas if str(a.get("nivel","")).upper() == "CRITICA")

    # gatillos duros
    if crit >= 2:
        return "CRITICO"
    if posible_multisector and score < 65:
        return "ALTO"

    # por score
    if score >= 80 and sector_conf >= 70:
        return "BAJO"
    if score >= 65:
        return "MODERADO"
    if score >= 50:
        return "ALTO"
    return "CRITICO"


# =========================
# 4) Motor principal
# =========================
def _argo_control_extraer_resumen(payload: dict) -> dict:
    """
    Busca el resumen de ARGO CONTROL en cualquiera de estas rutas:
    - payload["argo_control"]["resumen"]
    - payload["control"]["resumen"]
    - payload["argo_control_resumen"]
    """
    if not isinstance(payload, dict):
        return {}

    ac = payload.get("argo_control")
    if isinstance(ac, dict):
        r = ac.get("resumen")
        if isinstance(r, dict):
            return r

    c = payload.get("control")
    if isinstance(c, dict):
        r = c.get("resumen")
        if isinstance(r, dict):
            return r

    r = payload.get("argo_control_resumen")
    if isinstance(r, dict):
        return r

    return {}


def _argo_control_influencia_y_penalizacion(resumen: dict) -> tuple[dict, int]:
    """
    Convierte el resumen en:
    - control_influencia (para tu salida)
    - penalizacion (puntos a restar al score)
    """
    observaciones = resumen.get("observaciones_total")
    severidad = (resumen.get("severidad_maxima") or "").strip().upper()
    dictamen = (resumen.get("dictamen") or "").strip().upper()

    if severidad == "ALTA" or dictamen == "RECHAZADO":
        penal = 20
        sev_out = "ALTA"
    elif severidad == "MEDIA" or dictamen == "CON_OBSERVACIONES":
        penal = 10
        sev_out = "MEDIA"
    else:
        penal = 0
        sev_out = "BAJA"

    control_influencia = {
        "consistencia_pct": None,
        "severidad_maxima": sev_out,
        "dictamen": dictamen or None,
        "observaciones_total": int(observaciones) if isinstance(observaciones, (int, float)) else None,
        "penalizacion_aplicada": penal
    }

    return control_influencia, penal
def build_output(payload_master: Dict[str, Any]) -> Dict[str, Any]:
    meta = payload_master.get("meta", {}) or {}
    descripcion = str(payload_master.get("descripcion", "") or "")

    # 1) Sector IA
    sector_info = detect_sector(descripcion)
    sector = sector_info["sector_detectado"]
    conf_sector = int(sector_info["confianza_sector_pct"])
    posible_multisector = bool(sector_info["posible_multisector"])

    # alertas por baja confianza
    alerts: List[Dict[str, Any]] = []
    if conf_sector < 45:
        alerts.append(make_alert("CRITICA", "CLASS_SECTOR_CRITICO", "Confianza sectorial crítica (<45%).", "Aportar ficha técnica/catálogo/composición.", "SECTOR_IA"))
    elif conf_sector < 70:
        alerts.append(make_alert("ADVERTENCIA", "CLASS_SECTOR_BAJA", "Confianza sectorial baja (45–69%).", "Reforzar evidencia técnica de función/material.", "SECTOR_IA"))
    if posible_multisector:
        alerts.append(make_alert("ADVERTENCIA", "CLASS_MULTI_SECTOR", "Posible multisector detectado.", "Confirmar función principal y componente dominante.", "SECTOR_IA"))

    # 2) Score documental por bloques + faltantes
scoring = score_documental(payload_master, sector, descripcion)

# --- Integración ARGO CONTROL ---
resumen_control = _argo_control_extraer_resumen(payload_master)
control_influencia, penal_control = _argo_control_influencia_y_penalizacion(resumen_control)

score = int(scoring["score_total_0_100"])
dd = scoring["nivel_debida_diligencia"]

# aplicar penalización de control
score = max(0, score - penal_control)

alerts.extend(scoring["alertas"])
datos_faltantes = scoring["datos_faltantes"]

    # 3) Certeza (por ahora: base=score, final=score)
    certeza_base = score
    certeza_final = score

    # 4) Riesgo automático
    riesgo = riesgo_automatico(score, conf_sector, posible_multisector, alerts)

    # 5) Salida
    out = {
        "meta": {
            "schema": "ARGO_CLASS_OUTPUT_V2026",
            "id_operacion": meta.get("id_operacion"),
            "id_shipment": meta.get("id_shipment"),
            "id_item": meta.get("id_item"),
            "hash_input": hash_payload(payload_master),
        },
        "salida": {
            "sector_ia": {
                "sector_detectado": sector,
                "confianza_sector_pct": conf_sector,
                "posible_multisector": posible_multisector,
                "justificacion": sector_info.get("justificacion", []),
            },
            "score_documental": {
                "score_total_0_100": score,
                "nivel_debida_diligencia": dd,
                "bloques": scoring["bloques"],
                "control_influencia": control_influencia
            },
            "clasificacion": {
                "fraccion_sugerida": "POR_DEFINIR",
                "nota": "Motor de fracción (LIGIE/IGI/Notas) se integra en Bloque 2.",
            },
            "certeza_y_riesgo": {
                "certeza_base_pct": certeza_base,
                "certeza_final_pct": certeza_final,
                "riesgo_automatico": riesgo,
            },
            "tablas": {
                "alertas": alerts,
                "datos_faltantes": datos_faltantes,
            },
            "advertencia_juridica": {
                "nivel": "CRITICA" if riesgo == "CRITICO" else "PREVENTIVA",
                "texto": "Clasificación/certeza calculadas según documentación proporcionada; el criterio final corresponde al usuario/cliente.",
                "fundamento": ["ART_54_LA_REFORMADO", "CIRCULAR_T_0250_2025"],
            }
        }
    }
    return out
