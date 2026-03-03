from typing import Dict, Any
import hashlib
import json


def hash_payload(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()


def build_output(payload_master: Dict[str, Any]) -> Dict[str, Any]:

    meta = payload_master.get("meta", {})
        # -------- DETECCION BASICA DE SECTOR --------
    sector = "OTRO"
    confianza = 50

    descripcion = str(payload_master.get("descripcion", "")).lower()

    if any(p in descripcion for p in ["msds", "solvente", "acido", "resina"]):
        sector = "QUIMICO"
        confianza = 80
    elif any(p in descripcion for p in ["volt", "watt", "usb", "sensor"]):
        sector = "ELECTRONICO"
        confianza = 80
    elif any(p in descripcion for p in ["fibra", "algodon", "poliester", "tejido"]):
        sector = "TEXTIL"
        confianza = 80
    elif any(p in descripcion for p in ["motor", "bomba", "rpm", "valvula"]):
        sector = "MAQUINARIA"
        confianza = 75
            # -------- SCORE DOCUMENTAL BASICO (MVP) --------
    score = 50  # base

    # Si hay descripcion, sube
    if descripcion.strip():
        score += 10

    # Si detecta QUIMICO y menciona MSDS, sube más (porque ya trae soporte)
    if sector == "QUIMICO" and "msds" in descripcion:
        score += 10

    # límites 0-100
    if score > 100:
        score = 100
    if score < 0:
        score = 0
            # -------- RIESGO AUTOMATICO (MVP) --------
    if score >= 80:
        riesgo = "BAJO"
    elif score >= 65:
        riesgo = "MODERADO"
    elif score >= 50:
        riesgo = "ALTO"
    else:
        riesgo = "CRITICO"   
    return {
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
                "confianza_sector_pct": confianza
        },
            "score_documental": {
                "score_total_0_100": score,
                "nivel_debida_diligencia": "MEDIA" if score >= 60 else "ALTA"
        },
            "clasificacion": {
                "fraccion_sugerida": "POR_DEFINIR"
            },
            "certeza_y_riesgo": {
                "certeza_base_pct": score,
                "certeza_final_pct": score,
               "riesgo_automatico": riesgo
            }
        }
    }
