"""
ARGO Technologies
Motor de Comparacion de Mercancia
P003 - Pilot

Compara:
    esperado  = informacion documental
    observado = lectura fisica de Camera PRO

La decision es deterministica.
El modelo de IA NO decide COINCIDE / DIFERENCIA / DUDA.
"""

import re
import unicodedata
from typing import Any, Dict, Optional


CAMPOS = {
    "purchase_order": {
        "etiqueta": "Purchase Order",
        "criticidad": "CRITICA",
        "tipo": "identificador",
    },
    "partida": {
        "etiqueta": "Partida",
        "criticidad": "CRITICA",
        "tipo": "identificador",
    },
    "numero_parte": {
        "etiqueta": "Numero de parte",
        "criticidad": "CRITICA",
        "tipo": "identificador",
    },
    "cantidad": {
        "etiqueta": "Cantidad",
        "criticidad": "CRITICA",
        "tipo": "cantidad",
    },
    "unidad": {
        "etiqueta": "Unidad",
        "criticidad": "IMPORTANTE",
        "tipo": "unidad",
    },
    "marca": {
        "etiqueta": "Marca",
        "criticidad": "IMPORTANTE",
        "tipo": "texto",
    },
    "modelo": {
        "etiqueta": "Modelo",
        "criticidad": "IMPORTANTE",
        "tipo": "identificador",
    },
    "lote": {
        "etiqueta": "Lote",
        "criticidad": "COMPLEMENTARIA",
        "tipo": "identificador",
    },
    "serie": {
        "etiqueta": "Serie",
        "criticidad": "COMPLEMENTARIA",
        "tipo": "identificador",
    },
    "pais_origen": {
        "etiqueta": "Pais de origen",
        "criticidad": "COMPLEMENTARIA",
        "tipo": "texto",
    },
    "descripcion": {
        "etiqueta": "Descripcion",
        "criticidad": "COMPLEMENTARIA",
        "tipo": "descripcion",
    },
}


VALORES_AUSENTES = {
    "",
    "N/A",
    "NA",
    "N/D",
    "ND",
    "NONE",
    "NULL",
    "NO LEGIBLE",
    "NO DETECTADO",
    "NO VISIBLE",
    "SIN DATO",
}


UNIDADES = {
    "EACH": "EA",
    "EA": "EA",
    "PIEZA": "EA",
    "PIEZAS": "EA",
    "PZA": "EA",
    "PZAS": "EA",
    "PCS": "EA",
    "PC": "EA",
    "UNIT": "EA",
    "UNITS": "EA",
    "UNIDAD": "EA",
    "UNIDADES": "EA",

    "BOX": "BOX",
    "BOXES": "BOX",
    "CAJA": "BOX",
    "CAJAS": "BOX",

    "PACK": "PACK",
    "PACKAGE": "PACK",
    "PAQUETE": "PACK",
    "PAQUETES": "PACK",
}


def _texto(valor: Any) -> str:
    if valor is None:
        return ""

    texto = str(valor).strip()

    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(
        c for c in texto
        if not unicodedata.combining(c)
    )

    return texto.upper().strip()


def _ausente(valor: Any) -> bool:
    return _texto(valor) in VALORES_AUSENTES


def _normalizar_identificador(valor: Any) -> Optional[str]:
    if _ausente(valor):
        return None

    return re.sub(r"[\s\-_./]+", "", _texto(valor))


def _normalizar_texto(valor: Any) -> Optional[str]:
    if _ausente(valor):
        return None

    texto = _texto(valor)
    texto = re.sub(r"\s+", " ", texto)

    return texto.strip()


def _normalizar_cantidad(valor: Any) -> Optional[float]:
    if _ausente(valor):
        return None

    if isinstance(valor, (int, float)):
        return float(valor)

    texto = _texto(valor).replace(",", "")

    match = re.search(r"-?\d+(?:\.\d+)?", texto)

    if not match:
        return None

    try:
        return float(match.group(0))
    except ValueError:
        return None


def _normalizar_unidad(valor: Any) -> Optional[str]:
    if _ausente(valor):
        return None

    texto = _texto(valor)
    texto = re.sub(r"[^A-Z]", "", texto)

    return UNIDADES.get(texto, texto or None)


def _normalizar_descripcion(valor: Any) -> Optional[str]:
    if _ausente(valor):
        return None

    texto = _texto(valor)
    texto = re.sub(r"[^A-Z0-9]+", " ", texto)
    texto = re.sub(r"\s+", " ", texto)

    return texto.strip()


def _normalizar(valor: Any, tipo: str):
    if tipo == "cantidad":
        return _normalizar_cantidad(valor)

    if tipo == "unidad":
        return _normalizar_unidad(valor)

    if tipo == "identificador":
        return _normalizar_identificador(valor)

    if tipo == "descripcion":
        return _normalizar_descripcion(valor)

    return _normalizar_texto(valor)


def _confianza_observada(
    observado: Dict[str, Any],
    campo: str,
) -> Optional[float]:

    confianza = observado.get("confianza") or {}

    valor = confianza.get(campo)

    if valor is None and campo == "cantidad":
        valor = confianza.get("cantidad_visible")

    try:
        return float(valor) if valor is not None else None
    except (TypeError, ValueError):
        return None


def _requiere_confirmacion(
    observado: Dict[str, Any],
    campo: str,
) -> bool:

    requeridos = observado.get("requiere_confirmacion") or []

    aliases = {campo}

    if campo == "cantidad":
        aliases.add("cantidad_visible")

    requeridos_norm = {
        _texto(item)
        for item in requeridos
    }

    return any(
        _texto(alias) in requeridos_norm
        for alias in aliases
    )


def _valor_observado(
    observado: Dict[str, Any],
    campo: str,
):

    if campo == "cantidad":
        return observado.get(
            "cantidad",
            observado.get("cantidad_visible"),
        )

    return observado.get(campo)


def comparar_campo(
    campo: str,
    esperado: Any,
    observado: Any,
    confianza: Optional[float],
    marcado_confirmacion: bool,
) -> Dict[str, Any]:

    config = CAMPOS[campo]
    tipo = config["tipo"]

    esperado_norm = _normalizar(esperado, tipo)
    observado_norm = _normalizar(observado, tipo)

    resultado = "COINCIDE"
    razon = "Valores equivalentes despues de normalizacion"

    # Documento no exige este dato.
    if esperado_norm is None:
        resultado = "NO_APLICA"
        razon = "El documento no proporciona un valor esperado"

    # Documento espera valor, pero Camera PRO no pudo observarlo.
    elif observado_norm is None:
        resultado = "DUDA"
        razon = "Existe valor esperado pero no fue observado en la mercancia"

    # Camera PRO marco explicitamente el campo para confirmacion.
    elif marcado_confirmacion:
        resultado = "DUDA"
        razon = "Camera PRO requiere confirmacion humana para este campo"

    # Baja confianza OCR: no declarar diferencia automaticamente.
    elif confianza is not None and confianza < 0.90:
        resultado = "DUDA"
        razon = (
            f"Confianza OCR {confianza:.2f} inferior al umbral 0.90"
        )

    elif esperado_norm != observado_norm:
        resultado = "DIFERENCIA"
        razon = "El valor observado no coincide con el esperado"

    return {
        "campo": campo,
        "etiqueta": config["etiqueta"],
        "criticidad": config["criticidad"],
        "esperado": esperado,
        "observado": observado,
        "esperado_normalizado": esperado_norm,
        "observado_normalizado": observado_norm,
        "confianza_observada": confianza,
        "resultado": resultado,
        "razon": razon,
    }


def comparar_mercancia(
    esperado: Dict[str, Any],
    observado: Dict[str, Any],
) -> Dict[str, Any]:

    comparaciones = []

    for campo in CAMPOS:

        valor_esperado = esperado.get(campo)
        valor_observado = _valor_observado(observado, campo)

        comparaciones.append(
            comparar_campo(
                campo=campo,
                esperado=valor_esperado,
                observado=valor_observado,
                confianza=_confianza_observada(
                    observado,
                    campo,
                ),
                marcado_confirmacion=_requiere_confirmacion(
                    observado,
                    campo,
                ),
            )
        )

    diferencias = [
        c for c in comparaciones
        if c["resultado"] == "DIFERENCIA"
    ]

    dudas = [
        c for c in comparaciones
        if c["resultado"] == "DUDA"
    ]

    diferencias_criticas = [
        c for c in diferencias
        if c["criticidad"] == "CRITICA"
    ]

    dudas_criticas = [
        c for c in dudas
        if c["criticidad"] == "CRITICA"
    ]

    if diferencias_criticas:
        resultado_general = "DIFERENCIA"

    elif dudas_criticas:
        resultado_general = "DUDA"

    elif diferencias:
        resultado_general = "DIFERENCIA"

    elif dudas:
        resultado_general = "DUDA"

    else:
        resultado_general = "COINCIDE"

    return {
        "ok": True,
        "modulo": "ARGO_COMPARADOR_MERCANCIA",
        "version": "1.0-pilot",
        "resultado_general": resultado_general,
        "requiere_revision_humana": (
            resultado_general in {"DIFERENCIA", "DUDA"}
        ),
        "resumen": {
            "campos_evaluados": len(comparaciones),
            "coinciden": sum(
                c["resultado"] == "COINCIDE"
                for c in comparaciones
            ),
            "diferencias": len(diferencias),
            "dudas": len(dudas),
            "no_aplica": sum(
                c["resultado"] == "NO_APLICA"
                for c in comparaciones
            ),
        },
        "comparaciones": comparaciones,
    }
