# argo_class_fraccion.py
from typing import Dict, Any, List, Tuple

def _add(cands: List[Tuple[str, str, int]], fraccion: str, motivo: str, score: int):
    cands.append((fraccion, motivo, score))

def sugerir_fraccion(descripcion: str, sector: str = "OTRO") -> Dict[str, Any]:
    """
    Motor inicial de fracción (Bloque 2).
    - NO detiene operación.
    - Devuelve candidatos con 'score' y una fracción sugerida.
    """
    desc = (descripcion or "").strip().lower()
    sec = (sector or "OTRO").strip().upper()

    candidatos: List[Tuple[str, str, int]] = []

    # ----------------------------
    # Reglas por palabras clave (base)
    # ----------------------------
    # Metales / herrajes
    if any(k in desc for k in ["tornillo", "perno", "tuerca", "rondana", "arandela", "birlo", "pija"]):
        _add(candidatos, "7318.15.99", "Coincidencia con tornillería/herrajes (palabras clave).", 85)

    if any(k in desc for k in ["herrajes", "bisagra", "cerradura", "manija", "jaladera"]):
        _add(candidatos, "8302.10.99", "Coincidencia con herrajes/bisagras/cerraduras (palabras clave).", 75)

    if any(k in desc for k in ["acero", "hierro"]):
        _add(candidatos, "7326.90.99", "Menciona hierro/acero; candidato genérico manufacturas.", 60)

    # Plásticos
    if any(k in desc for k in ["plástico", "plastico", "polietileno", "pp", "p/p", "pvc", "abs", "nylon"]):
        _add(candidatos, "3926.90.99", "Coincidencia con plásticos; candidato genérico manufacturas de plástico.", 65)

    # Textiles
    if any(k in desc for k in ["algodón", "algodon", "poliéster", "poliester", "textil", "tela", "prenda"]):
        _add(candidatos, "6307.90.99", "Coincidencia con textiles/prendas; candidato genérico confecciones.", 60)

    # Electrónica / eléctricos
    if any(k in desc for k in ["electrónico", "electronico", "circuito", "pcb", "sensor", "módulo", "modulo", "power supply"]):
        _add(candidatos, "8543.70.99", "Coincidencia con aparatos eléctricos con función propia (palabras clave).", 55)

    # ----------------------------
    # Ajustes por sector (si aplica)
    # ----------------------------
    # (Este ajuste es suave; no “decide” solo por sector.)
    if sec == "ACERO":
        candidatos = [(f, m + " (Sector=ACERO refuerza candidato)", s + 5) for (f, m, s) in candidatos]
    elif sec == "PLASTICOS":
        candidatos = [(f, m + " (Sector=PLASTICOS refuerza candidato)", s + 5) for (f, m, s) in candidatos]
    elif sec == "TEXTIL":
        candidatos = [(f, m + " (Sector=TEXTIL refuerza candidato)", s + 5) for (f, m, s) in candidatos]
    elif sec == "ELECTRONICA":
        candidatos = [(f, m + " (Sector=ELECTRONICA refuerza candidato)", s + 5) for (f, m, s) in candidatos]

    # Ordenar por score desc
    candidatos.sort(key=lambda x: x[2], reverse=True)

    if not candidatos:
        return {
            "fraccion_sugerida": "POR_DEFINIR",
            "descripcion_fraccion": "Sin coincidencias suficientes para sugerir fracción.",
            "metodo_clasificacion": "REGLAS_BLOQUE2",
            "candidatos": [],
            "confianza_fraccion_pct": 0
        }

    top = candidatos[0]
    # Confianza simple: top score cap 100
    confianza = max(0, min(100, int(top[2])))

    return {
        "fraccion_sugerida": top[0],
        "descripcion_fraccion": top[1],
        "metodo_clasificacion": "REGLAS_BLOQUE2",
        "candidatos": [
            {"fraccion": f, "motivo": m, "score": s} for (f, m, s) in candidatos[:5]
        ],
        "confianza_fraccion_pct": confianza
    }
