# argo_class_fraccion.py
from typing import Dict, Any, List, Tuple


def _add(cands: List[Tuple[str, str, int]], fraccion: str, motivo: str, score: int):
    cands.append((fraccion, motivo, score))


def sugerir_fraccion(descripcion: str, sector: str = "OTRO") -> Dict[str, Any]:
    """
    Motor de fracción ARGO CLASS v2026.
    - Nunca detiene operación.
    - Siempre devuelve una fracción sugerida.
    - Si la información es insuficiente, devuelve mejor candidato genérico con baja certeza.
    - La certeza refleja calidad de información y fuerza de coincidencia.
    """
    desc = (descripcion or "").strip().lower()
    sec = (sector or "OTRO").strip().upper()

    candidatos: List[Tuple[str, str, int]] = []
    evidencia_detectada = []
    reglas_activadas = []
    factores_positivos = []
    factores_negativos = []

    def marcar(regla: str, palabras: list, impacto: str):
        hits = [k for k in palabras if k in desc]
        if hits:
            reglas_activadas.append(regla)
            evidencia_detectada.extend(hits[:6])
            factores_positivos.append(impacto)
        return bool(hits)

    # ----------------------------
    # Reglas por palabras clave
    # ----------------------------

    # Tornillería / sujetadores
    if any(k in desc for k in [
        "tornillo", "screw", "bolt", "perno", "tuerca", "nut",
        "rondana", "washer", "arandela", "birlo", "pija",
        "fastener", "threaded"
    ]):
        _add(
            candidatos,
            "7318.15.99",
            "Coincidencia con tornillería/sujetadores metálicos.",
            85
        )

    # Resortes / plungers / componentes mecánicos con resorte
    if any(k in desc for k in [
        "spring plunger", "plunger", "ball-nose", "ball nose",
        "spring-loaded", "resorte", "émbolo", "embolo"
    ]):
        _add(
            candidatos,
            "7320.90.99",
            "Coincidencia con artículo mecánico asociado a resorte/plunger; candidato por manufactura/resorte metálico.",
            68
        )
        _add(
            candidatos,
            "7326.90.99",
            "Candidato alterno: manufactura de hierro/acero no expresada en otra partida.",
            62
        )

    # Herrajes
    if any(k in desc for k in [
        "herrajes", "hardware", "bisagra", "hinge",
        "cerradura", "lock", "manija", "handle", "jaladera"
    ]):
        _add(
            candidatos,
            "8302.10.99",
            "Coincidencia con herrajes/bisagras/cerraduras.",
            75
        )

    # Manufacturas de acero/hierro genéricas
    if any(k in desc for k in [
        "acero", "steel", "stainless", "hierro", "iron",
        "metal", "metálico", "metalico"
    ]):
        _add(
            candidatos,
            "7326.90.99",
            "Menciona acero/hierro/metal; candidato genérico para manufacturas de hierro o acero.",
            60
        )

    # Plásticos
    if any(k in desc for k in [
        "plástico", "plastico", "plastic", "polyethylene", "polietileno",
        "polypropylene", "polipropileno", "pp", "p/p", "pvc", "abs",
        "nylon", "polymer", "resin", "pellet"
    ]):
        _add(
            candidatos,
            "3926.90.99",
            "Coincidencia con manufacturas de plástico.",
            65
        )

    # Textiles
    if any(k in desc for k in [
        "algodón", "algodon", "cotton", "poliéster", "poliester",
        "polyester", "textil", "textile", "tela", "fabric",
        "prenda", "garment"
    ]):
        _add(
            candidatos,
            "6307.90.99",
            "Coincidencia con artículos textiles confeccionados genéricos.",
            60
        )

    # Electrónica / eléctricos
    if any(k in desc for k in [
        "electrónico", "electronico", "electronic", "circuito",
        "pcb", "sensor", "módulo", "modulo", "module",
        "power supply", "adapter", "charger", "transformer",
        "control board", "display", "hmi", "plc", "relay", "relevador"
    ]):
        _add(
            candidatos,
            "8543.70.99",
            "Coincidencia con aparato eléctrico/electrónico con función propia.",
            58
        )

    # Partes automotrices
    if any(k in desc for k in [
        "automotive", "auto part", "car part", "vehicle", "vehículo", "vehiculo",
        "bracket automotive", "sensor automotriz", "arnes", "arnés", "wiring harness",
        "bumper", "fascia", "trim", "automotriz"
    ]):
        _add(
            candidatos,
            "8708.99.99",
            "Coincidencia con parte/accesorio para vehículo automotor; requiere validar función y vehículo.",
            62
        )

    # Empaques / juntas
    if any(k in desc for k in [
        "gasket", "seal", "o-ring", "oring", "empaque", "junta", "sello",
        "rubber seal", "silicone gasket"
    ]):
        _add(
            candidatos,
            "4016.93.99",
            "Coincidencia con juntas/empaques/sellos de caucho o material flexible; validar material.",
            64
        )

    # Herramientas
    if any(k in desc for k in [
        "tool", "herramienta", "drill bit", "broca", "cutter", "blade",
        "wrench", "llave", "pliers", "pinza", "die", "molde"
    ]):
        _add(
            candidatos,
            "8207.90.99",
            "Coincidencia con herramienta/intercambiable o útil; validar uso específico y material.",
            60
        )

    # Instrumentos de medición/control
    if any(k in desc for k in [
        "gauge", "meter", "caliper", "micrometer", "sensor de presión",
        "pressure sensor", "thermometer", "termómetro", "instrumento de medición",
        "measuring instrument", "probe", "transducer"
    ]):
        _add(
            candidatos,
            "9026.20.99",
            "Coincidencia con instrumento/aparato de medición o control; validar variable medida.",
            63
        )

    # Maquinaria / bombas / motores
    if any(k in desc for k in [
        "motor", "pump", "bomba", "valve", "válvula", "valvula",
        "compressor", "compresor", "machine", "machinery",
        "actuator", "bearing", "rodamiento"
    ]):
        _add(
            candidatos,
            "8487.90.99",
            "Coincidencia con parte/componente de maquinaria sin función eléctrica específica clara.",
            55
        )

    # Químicos
    if any(k in desc for k in [
        "chemical", "químico", "quimico", "solvent", "solvente",
        "acid", "ácido", "acido", "compound", "cas", "sds", "msds"
    ]):
        _add(
            candidatos,
            "3824.99.99",
            "Coincidencia con preparación/producto químico no especificado.",
            55
        )

    # Residuos / scrap
    if any(k in desc for k in [
        "scrap", "waste", "residuo", "residuos", "desperdicio",
        "chatarra", "recycled", "reciclado"
    ]):
        _add(
            candidatos,
            "7204.49.99",
            "Coincidencia con desperdicios/chatarra metálica; requiere validar material.",
            50
        )

    # ----------------------------
    # Refuerzo por sector detectado
    # ----------------------------
    if sec == "METALMECANICO":
        candidatos = [(f, m + " Sector METALMECANICO refuerza candidato.", s + 5) for (f, m, s) in candidatos]
        if not candidatos:
            _add(
                candidatos,
                "7326.90.99",
                "Sector METALMECANICO sin coincidencia específica; candidato genérico de manufactura metálica.",
                42
            )

    elif sec == "MAQUINARIA":
        candidatos = [(f, m + " Sector MAQUINARIA refuerza candidato.", s + 5) for (f, m, s) in candidatos]
        if not candidatos:
            _add(
                candidatos,
                "8487.90.99",
                "Sector MAQUINARIA sin función específica suficiente; candidato genérico de partes de maquinaria.",
                40
            )

    elif sec == "PLASTICOS":
        candidatos = [(f, m + " Sector PLASTICOS refuerza candidato.", s + 5) for (f, m, s) in candidatos]
        if not candidatos:
            _add(
                candidatos,
                "3926.90.99",
                "Sector PLASTICOS sin descripción técnica suficiente; candidato genérico.",
                42
            )

    elif sec == "TEXTIL":
        candidatos = [(f, m + " Sector TEXTIL refuerza candidato.", s + 5) for (f, m, s) in candidatos]
        if not candidatos:
            _add(
                candidatos,
                "6307.90.99",
                "Sector TEXTIL sin composición/uso suficiente; candidato genérico.",
                40
            )

    elif sec == "ELECTRONICO":
        candidatos = [(f, m + " Sector ELECTRONICO refuerza candidato.", s + 5) for (f, m, s) in candidatos]
        if not candidatos:
            _add(
                candidatos,
                "8543.70.99",
                "Sector ELECTRONICO sin función específica suficiente; candidato genérico.",
                38
            )

    elif sec == "QUIMICO":
        candidatos = [(f, m + " Sector QUIMICO refuerza candidato.", s + 5) for (f, m, s) in candidatos]
        if not candidatos:
            _add(
                candidatos,
                "3824.99.99",
                "Sector QUIMICO sin composición suficiente; candidato genérico.",
                35
            )

    # ----------------------------
    # Fallback obligatorio
    # ----------------------------
    if not candidatos:
        _add(
            candidatos,
            "7326.90.99",
            "Fallback ARGO CLASS: información insuficiente; se sugiere fracción genérica de manufacturas metálicas solo como mejor esfuerzo operativo.",
            25
        )

    candidatos.sort(key=lambda x: x[2], reverse=True)

    top = candidatos[0]
    confianza = max(5, min(100, int(top[2])))

    descripcion_fraccion = top[1]

    if confianza < 50:
        descripcion_fraccion += (
            " Advertencia: certeza baja por información documental insuficiente; "
            "requiere validación técnica/legal antes de uso definitivo."
        )

    nom_preliminar = []
    rrna_preliminar = []

    if top[0].startswith("85"):
        nom_preliminar.append("Validar NOM eléctrica/electrónica aplicable según tensión, función y uso.")
        rrna_preliminar.append("Validar cumplimiento de regulaciones eléctricas/electrónicas y etiquetado aplicable.")
    if top[0].startswith("87"):
        nom_preliminar.append("Validar NOM/seguridad aplicable a partes automotrices según uso y vehículo.")
        rrna_preliminar.append("Validar restricciones o permisos aplicables a autopartes según fracción final.")
    if top[0].startswith("39") or top[0].startswith("40"):
        nom_preliminar.append("Validar NOM de información comercial/etiquetado si aplica al producto terminado.")
    if top[0].startswith("38"):
        nom_preliminar.append("Validar SDS/MSDS y NOM aplicable a químicos si corresponde.")
        rrna_preliminar.append("Validar restricciones sanitarias, ambientales o permisos por composición química.")
    if top[0].startswith("63"):
        nom_preliminar.append("Validar NOM de etiquetado textil/comercial según presentación y uso.")

    if not nom_preliminar:
        nom_preliminar.append("Sin NOM preliminar automática; validar contra fracción final y uso declarado.")
    if not rrna_preliminar:
        rrna_preliminar.append("Sin RRNA preliminar automática; validar contra fracción final y régimen aduanero.")

    # Evidencia explicable ARGO CLASS
    palabras_clave = [
        "tornillo", "screw", "bolt", "spring", "plunger", "steel", "metal",
        "plastic", "nylon", "pcb", "sensor", "module", "automotive", "vehicle",
        "wiring harness", "gasket", "seal", "o-ring", "gauge", "meter",
        "pump", "valve", "motor", "chemical", "sds", "msds"
    ]

    for palabra in palabras_clave:
        if palabra in desc and palabra not in evidencia_detectada:
            evidencia_detectada.append(palabra)

    if sec and sec != "OTRO":
        reglas_activadas.append(f"SECTOR_{sec}")
        factores_positivos.append(f"Sector detectado/reforzado: {sec}")

    if confianza < 50:
        factores_negativos.append("Confianza baja por información documental insuficiente.")
    if top[2] < 60:
        factores_negativos.append("Coincidencia débil: requiere ficha técnica, material y función principal.")
    if len(candidatos) > 1:
        factores_positivos.append(f"Se evaluaron {len(candidatos)} candidatos alternos.")

    evidencia_detectada = list(dict.fromkeys(evidencia_detectada))[:12]
    reglas_activadas = list(dict.fromkeys(reglas_activadas))[:12]
    factores_positivos = list(dict.fromkeys(factores_positivos))[:12]
    factores_negativos = list(dict.fromkeys(factores_negativos))[:12]

    explicacion_clasificacion = (
        f"ARGO CLASS sugiere {top[0]} con {confianza}% de confianza. "
        f"La decisión se basa en coincidencias detectadas en la descripción, sector informado y reglas internas v2026. "
        f"Esta sugerencia debe validarse con documentación técnica y criterio del clasificador."
    )

    return {
        "fraccion_sugerida": top[0],
        "descripcion_fraccion": descripcion_fraccion,
        "metodo_clasificacion": "REGLAS_ARGO_CLASS_V2026_MEJOR_ESFUERZO",
        "candidatos": [
            {"fraccion": f, "motivo": m, "score": s}
            for (f, m, s) in candidatos[:5]
        ],
        "confianza_fraccion_pct": confianza,
        "nom_preliminar": nom_preliminar,
        "rrna_preliminar": rrna_preliminar,
        "evidencia_detectada": evidencia_detectada,
        "reglas_activadas": reglas_activadas,
        "factores_positivos": factores_positivos,
        "factores_negativos": factores_negativos,
        "explicacion_clasificacion": explicacion_clasificacion,
    }
