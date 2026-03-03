from typing import Dict, Any
import hashlib
import json


def hash_payload(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()


def build_output(payload_master: Dict[str, Any]) -> Dict[str, Any]:

    meta = payload_master.get("meta", {})

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
                "sector_detectado": "PENDIENTE",
                "confianza_sector_pct": 0
            },
            "score_documental": {
                "score_total_0_100": 0,
                "nivel_debida_diligencia": "PENDIENTE"
            },
            "clasificacion": {
                "fraccion_sugerida": "POR_DEFINIR"
            },
            "certeza_y_riesgo": {
                "certeza_base_pct": 0,
                "certeza_final_pct": 0,
                "riesgo_automatico": "PENDIENTE"
            }
        }
    }
