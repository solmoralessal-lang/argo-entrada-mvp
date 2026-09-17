from __future__ import annotations

from typing import Any, Dict, List, Optional


CAMPOS_PARTIDA = (
    "purchase_order",
    "partida",
    "numero_parte",
    "descripcion",
    "cantidad",
    "unidad",
    "marca",
    "modelo",
    "lote",
    "serie",
    "pais_origen",
)


def nueva_partida(indice: int) -> Dict[str, Any]:
    return {
        "indice": indice,
        "referencia_documental": {
            campo: None for campo in CAMPOS_PARTIDA
        },
        "dato_fisico": {
            campo: None for campo in CAMPOS_PARTIDA
        },
        "dato_operativo": {
            campo: None for campo in CAMPOS_PARTIDA
        },
        "proveniencia": {
            campo: [] for campo in CAMPOS_PARTIDA
        },
        "evidencias": [],
        "comparacion": None,
        "clasificacion_argo_class": None,
        "control": None,
        "excepciones": [],
        "estado": "PENDIENTE",
    }


def nueva_operacion(id_operacion: Optional[str] = None) -> Dict[str, Any]:
    return {
        "modulo": "ARGO_ORQUESTADOR",
        "version": "0.1-pilot",
        "id_operacion": id_operacion,
        "documentos": [],
        "partidas_esperadas": [],
        "evidencias_fisicas": [],
        "partidas": [],
        "excepciones_humanas": [],
        "estado": "NUEVA",
    }


def agregar_partida(
    operacion: Dict[str, Any],
    partida: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not isinstance(operacion, dict):
        raise ValueError("operacion debe ser un diccionario")

    partidas: List[Dict[str, Any]] = operacion.setdefault("partidas", [])
    indice = len(partidas) + 1

    nueva = nueva_partida(indice)

    if isinstance(partida, dict):
        for bloque in (
            "referencia_documental",
            "dato_fisico",
            "dato_operativo",
        ):
            origen = partida.get(bloque)
            if isinstance(origen, dict):
                for campo in CAMPOS_PARTIDA:
                    if campo in origen:
                        nueva[bloque][campo] = origen.get(campo)

        if isinstance(partida.get("evidencias"), list):
            nueva["evidencias"] = list(partida["evidencias"])

    partidas.append(nueva)
    return nueva


def resumen_operacion(operacion: Dict[str, Any]) -> Dict[str, Any]:
    partidas = operacion.get("partidas", []) or []

    return {
        "id_operacion": operacion.get("id_operacion"),
        "total_partidas": len(partidas),
        "total_documentos": len(operacion.get("documentos", []) or []),
        "total_evidencias_fisicas": len(
            operacion.get("evidencias_fisicas", []) or []
        ),
        "total_excepciones": sum(
            len(p.get("excepciones", []) or [])
            for p in partidas
            if isinstance(p, dict)
        ),
        "estado": operacion.get("estado"),
    }


# === P004: RESOLUCION DE DATO OPERATIVO ===

def _valor_presente(valor: Any) -> bool:
    if valor is None:
        return False

    if isinstance(valor, str):
        return bool(valor.strip())

    return True


def _clave_valor(valor: Any) -> str:
    """
    Normalización básica para detectar si dos evidencias físicas
    expresan el mismo valor sin depender de mayúsculas o espacios.
    """
    if valor is None:
        return ""

    return " ".join(str(valor).strip().upper().split())


def establecer_referencia_documental(
    partida: Dict[str, Any],
    datos: Dict[str, Any],
    fuente: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Registra lo que dicen packing/invoice/documentos.
    No convierte esos valores automáticamente en dato físico.
    """
    if not isinstance(partida, dict):
        raise ValueError("partida debe ser un diccionario")

    if not isinstance(datos, dict):
        raise ValueError("datos debe ser un diccionario")

    referencia = partida.setdefault(
        "referencia_documental",
        {campo: None for campo in CAMPOS_PARTIDA},
    )

    proveniencia = partida.setdefault(
        "proveniencia",
        {campo: [] for campo in CAMPOS_PARTIDA},
    )

    for campo in CAMPOS_PARTIDA:
        valor = datos.get(campo)

        if not _valor_presente(valor):
            continue

        referencia[campo] = valor

        proveniencia[campo].append({
            "tipo_fuente": "DOCUMENTAL",
            "fuente": fuente,
            "valor": valor,
        })

    return partida


def registrar_evidencia_fisica(
    partida: Dict[str, Any],
    evidencia_id: str,
    datos: Dict[str, Any],
    archivo: Optional[str] = None,
    confianza: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Registra una fotografía/evidencia física y los campos visibles
    detectados en ella.

    Varias fotografías pueden pertenecer a la misma partida.
    """
    if not isinstance(partida, dict):
        raise ValueError("partida debe ser un diccionario")

    if not isinstance(datos, dict):
        raise ValueError("datos debe ser un diccionario")

    evidencias = partida.setdefault("evidencias", [])

    evidencia = {
        "evidencia_id": evidencia_id,
        "archivo": archivo,
        "datos_detectados": {},
        "confianza": confianza or {},
    }

    proveniencia = partida.setdefault(
        "proveniencia",
        {campo: [] for campo in CAMPOS_PARTIDA},
    )

    for campo in CAMPOS_PARTIDA:
        valor = datos.get(campo)

        if not _valor_presente(valor):
            continue

        evidencia["datos_detectados"][campo] = valor

        confianza_campo = None
        if isinstance(confianza, dict):
            confianza_campo = confianza.get(campo)

        proveniencia[campo].append({
            "tipo_fuente": "FISICA",
            "evidencia_id": evidencia_id,
            "archivo": archivo,
            "valor": valor,
            "confianza": confianza_campo,
        })

    evidencias.append(evidencia)
    return evidencia


def resolver_dato_operativo(partida: Dict[str, Any]) -> Dict[str, Any]:
    """
    Regla central P004:

    1. La evidencia física manda para el dato operativo.
    2. La referencia documental se conserva por separado.
    3. Si varias evidencias físicas coinciden, se consolida el valor.
    4. Si varias evidencias físicas se contradicen, NO se elige
       arbitrariamente: el campo queda pendiente de revisión humana.
    5. Un valor exclusivamente documental NO se presenta como
       físicamente verificado.
    """
    if not isinstance(partida, dict):
        raise ValueError("partida debe ser un diccionario")

    fisico = partida.setdefault(
        "dato_fisico",
        {campo: None for campo in CAMPOS_PARTIDA},
    )

    operativo = partida.setdefault(
        "dato_operativo",
        {campo: None for campo in CAMPOS_PARTIDA},
    )

    proveniencia = partida.setdefault(
        "proveniencia",
        {campo: [] for campo in CAMPOS_PARTIDA},
    )

    excepciones_previas = partida.get("excepciones", []) or []

    excepciones = [
        e for e in excepciones_previas
        if not (
            isinstance(e, dict)
            and e.get("codigo") == "CONFLICTO_EVIDENCIA_FISICA"
        )
    ]

    for campo in CAMPOS_PARTIDA:
        fuentes_fisicas = [
            p
            for p in proveniencia.get(campo, [])
            if isinstance(p, dict)
            and p.get("tipo_fuente") == "FISICA"
            and _valor_presente(p.get("valor"))
        ]

        valores_unicos = {}

        for fuente in fuentes_fisicas:
            valor = fuente.get("valor")
            clave = _clave_valor(valor)

            if clave not in valores_unicos:
                valores_unicos[clave] = {
                    "valor": valor,
                    "fuentes": [],
                }

            valores_unicos[clave]["fuentes"].append(fuente)

        if len(valores_unicos) == 1:
            unico = next(iter(valores_unicos.values()))
            fisico[campo] = unico["valor"]
            operativo[campo] = unico["valor"]

        elif len(valores_unicos) > 1:
            fisico[campo] = None
            operativo[campo] = None

            excepciones.append({
                "codigo": "CONFLICTO_EVIDENCIA_FISICA",
                "campo": campo,
                "valores": [
                    item["valor"]
                    for item in valores_unicos.values()
                ],
                "mensaje": (
                    "Dos o más evidencias físicas muestran valores "
                    "incompatibles para el mismo campo."
                ),
                "requiere_revision_humana": True,
            })

        else:
            fisico[campo] = None
            operativo[campo] = None

    partida["excepciones"] = excepciones

    if any(
        isinstance(e, dict)
        and e.get("requiere_revision_humana") is True
        for e in excepciones
    ):
        partida["estado"] = "REQUIERE_REVISION"
    else:
        partida["estado"] = "DATOS_FISICOS_RESUELTOS"

    return partida


# === P004: COMPARACION DOCUMENTAL VS REALIDAD FISICA ===

def comparar_partida(partida: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compara lo esperado documentalmente contra lo observado físicamente.

    IMPORTANTE:
    - La comparación NO modifica el dato_operativo.
    - Una diferencia documental queda registrada.
    - La evidencia física sigue siendo la base del dato operativo de cruce.
    """

    if not isinstance(partida, dict):
        raise ValueError("partida debe ser un diccionario")

    from argo_comparador import comparar_mercancia

    esperado = partida.get("referencia_documental", {}) or {}
    fisico = partida.get("dato_fisico", {}) or {}

    observado = {
        "purchase_order": fisico.get("purchase_order"),
        "partida": fisico.get("partida"),
        "numero_parte": fisico.get("numero_parte"),
        "cantidad_visible": fisico.get("cantidad"),
        "unidad": fisico.get("unidad"),
        "descripcion": fisico.get("descripcion"),
        "marca": fisico.get("marca"),
        "modelo": fisico.get("modelo"),
        "lote": fisico.get("lote"),
        "serie": fisico.get("serie"),
        "pais_origen": fisico.get("pais_origen"),
    }

    resultado = comparar_mercancia(
        esperado=esperado,
        observado=observado,
    )

    partida["comparacion"] = resultado

    resultado_general = resultado.get("resultado_general")

    excepciones_previas = partida.get("excepciones", []) or []

    excepciones = [
        e for e in excepciones_previas
        if not (
            isinstance(e, dict)
            and e.get("codigo") == "DIFERENCIA_DOCUMENTAL"
        )
    ]

    if resultado_general in ("DIFERENCIA", "DUDA"):
        excepciones.append({
            "codigo": "DIFERENCIA_DOCUMENTAL",
            "resultado": resultado_general,
            "requiere_revision_humana":
                resultado.get("requiere_revision_humana", False),
            "comparaciones": [
                c
                for c in resultado.get("comparaciones", [])
                if c.get("resultado") in ("DIFERENCIA", "DUDA")
            ],
        })

    partida["excepciones"] = excepciones

    if resultado_general == "DIFERENCIA":
        partida["estado"] = "DIFERENCIA_DOCUMENTAL"
    elif resultado_general == "DUDA":
        partida["estado"] = "REQUIERE_REVISION"
    elif any(
        isinstance(e, dict)
        and e.get("requiere_revision_humana") is True
        for e in excepciones
    ):
        partida["estado"] = "REQUIERE_REVISION"
    else:
        partida["estado"] = "VERIFICADA"

    return resultado


# === P004: ASOCIACION AUTOMATICA EVIDENCIA ↔ PARTIDA ===

PESOS_ASOCIACION = {
    "purchase_order": 30,
    "partida": 35,
    "numero_parte": 40,
    "cantidad": 12,
    "unidad": 8,
    "marca": 6,
    "modelo": 10,
}


def _valor_equivalente(a: Any, b: Any) -> bool:
    if not _valor_presente(a) or not _valor_presente(b):
        return False

    return _clave_valor(a) == _clave_valor(b)


def puntuar_asociacion(
    referencia: Dict[str, Any],
    observado: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Calcula score determinístico para asociar una evidencia física
    con una partida documental.
    """

    score = 0
    coincidencias = []
    conflictos = []

    for campo, peso in PESOS_ASOCIACION.items():
        esperado = referencia.get(campo)
        visto = observado.get(campo)

        if not _valor_presente(esperado) or not _valor_presente(visto):
            continue

        if _valor_equivalente(esperado, visto):
            score += peso
            coincidencias.append(campo)
        else:
            conflictos.append({
                "campo": campo,
                "esperado": esperado,
                "observado": visto,
            })

    return {
        "score": score,
        "coincidencias": coincidencias,
        "conflictos": conflictos,
    }


def asociar_evidencia_a_partida(
    operacion: Dict[str, Any],
    evidencia: Dict[str, Any],
    *,
    score_minimo: int = 40,
) -> Dict[str, Any]:
    """
    Intenta asociar una evidencia física a una sola partida.

    Reglas:
    - Si una única partida supera el umbral y no hay empate, se asocia.
    - Si ninguna supera el umbral, queda SIN_ASOCIAR.
    - Si dos o más empatan con el mejor score, queda AMBIGUA.
    - ARGO no elige arbitrariamente.
    """

    if not isinstance(operacion, dict):
        raise ValueError("operacion debe ser un diccionario")

    if not isinstance(evidencia, dict):
        raise ValueError("evidencia debe ser un diccionario")

    observado = evidencia.get("datos_detectados") or {}

    candidatos = []

    for partida in operacion.get("partidas", []) or []:
        if not isinstance(partida, dict):
            continue

        referencia = partida.get("referencia_documental") or {}

        resultado = puntuar_asociacion(
            referencia=referencia,
            observado=observado,
        )

        candidatos.append({
            "indice_partida": partida.get("indice"),
            "score": resultado["score"],
            "coincidencias": resultado["coincidencias"],
            "conflictos": resultado["conflictos"],
        })

    candidatos_ordenados = sorted(
        candidatos,
        key=lambda x: x["score"],
        reverse=True,
    )

    if not candidatos_ordenados:
        return {
            "estado": "SIN_PARTIDAS",
            "partida_indice": None,
            "score": 0,
            "candidatos": [],
        }

    mejor_score = candidatos_ordenados[0]["score"]

    if mejor_score < score_minimo:
        return {
            "estado": "SIN_ASOCIAR",
            "partida_indice": None,
            "score": mejor_score,
            "candidatos": candidatos_ordenados,
        }

    mejores = [
        c
        for c in candidatos_ordenados
        if c["score"] == mejor_score
    ]

    if len(mejores) > 1:
        return {
            "estado": "AMBIGUA",
            "partida_indice": None,
            "score": mejor_score,
            "candidatos": candidatos_ordenados,
        }

    ganador = mejores[0]

    return {
        "estado": "ASOCIADA",
        "partida_indice": ganador["indice_partida"],
        "score": ganador["score"],
        "coincidencias": ganador["coincidencias"],
        "conflictos": ganador["conflictos"],
        "candidatos": candidatos_ordenados,
    }


# === P004: ASIGNACION DE EVIDENCIA A LA OPERACION ===

def asignar_evidencia_a_operacion(
    operacion: Dict[str, Any],
    evidencia: Dict[str, Any],
    *,
    score_minimo: int = 40,
) -> Dict[str, Any]:
    """
    Asocia una evidencia física con una partida y, si la asociación
    es segura, la registra dentro del expediente de esa partida.

    Si es ambigua o no alcanza el score mínimo, se conserva como
    excepción de operación para revisión humana.
    """

    if not isinstance(operacion, dict):
        raise ValueError("operacion debe ser un diccionario")

    if not isinstance(evidencia, dict):
        raise ValueError("evidencia debe ser un diccionario")

    resultado = asociar_evidencia_a_partida(
        operacion,
        evidencia,
        score_minimo=score_minimo,
    )

    estado = resultado.get("estado")

    if estado == "ASOCIADA":
        indice = resultado.get("partida_indice")

        partida_objetivo = None

        for partida in operacion.get("partidas", []) or []:
            if (
                isinstance(partida, dict)
                and partida.get("indice") == indice
            ):
                partida_objetivo = partida
                break

        if partida_objetivo is None:
            resultado["estado"] = "ERROR_PARTIDA_NO_ENCONTRADA"
            return resultado

        evidencia_id = (
            evidencia.get("evidencia_id")
            or evidencia.get("archivo")
            or f"EVIDENCIA-{len(partida_objetivo.get('evidencias', [])) + 1}"
        )

        registrar_evidencia_fisica(
            partida_objetivo,
            evidencia_id=str(evidencia_id),
            datos=evidencia.get("datos_detectados") or {},
            archivo=evidencia.get("archivo"),
            confianza=evidencia.get("confianza") or {},
        )

        resolver_dato_operativo(partida_objetivo)

        resultado["registrada"] = True
        resultado["estado_partida"] = partida_objetivo.get("estado")

        return resultado

    excepcion = {
        "codigo": (
            "ASOCIACION_AMBIGUA"
            if estado == "AMBIGUA"
            else "EVIDENCIA_SIN_ASOCIAR"
        ),
        "estado_asociacion": estado,
        "evidencia_id": evidencia.get("evidencia_id"),
        "archivo": evidencia.get("archivo"),
        "score": resultado.get("score"),
        "candidatos": resultado.get("candidatos", []),
        "requiere_revision_humana": True,
    }

    operacion.setdefault(
        "excepciones_humanas",
        [],
    ).append(excepcion)

    operacion.setdefault(
        "evidencias_fisicas",
        [],
    ).append({
        **evidencia,
        "estado_asociacion": estado,
    })

    resultado["registrada"] = False
    resultado["excepcion"] = excepcion

    return resultado


# === P004: PROCESAMIENTO MASIVO DE EVIDENCIAS ===

def procesar_evidencias_masivas(
    operacion: Dict[str, Any],
    evidencias: List[Dict[str, Any]],
    *,
    score_minimo: int = 40,
) -> Dict[str, Any]:
    """
    Procesa una colección completa de evidencias físicas.

    Cada evidencia:
    - se intenta asociar automáticamente a una partida;
    - si es clara, se registra en esa partida;
    - si es ambigua o insuficiente, pasa a excepciones humanas.

    Al final devuelve un resumen de la operación.
    """

    if not isinstance(operacion, dict):
        raise ValueError("operacion debe ser un diccionario")

    if not isinstance(evidencias, list):
        raise ValueError("evidencias debe ser una lista")

    resultados = []

    for evidencia in evidencias:
        if not isinstance(evidencia, dict):
            continue

        resultado = asignar_evidencia_a_operacion(
            operacion,
            evidencia,
            score_minimo=score_minimo,
        )

        resultados.append({
            "evidencia_id": evidencia.get("evidencia_id"),
            "archivo": evidencia.get("archivo"),
            **resultado,
        })

    asociadas = sum(
        1
        for r in resultados
        if r.get("estado") == "ASOCIADA"
    )

    ambiguas = sum(
        1
        for r in resultados
        if r.get("estado") == "AMBIGUA"
    )

    sin_asociar = sum(
        1
        for r in resultados
        if r.get("estado") == "SIN_ASOCIAR"
    )

    partidas_con_evidencia = sum(
        1
        for p in operacion.get("partidas", []) or []
        if isinstance(p, dict)
        and len(p.get("evidencias", []) or []) > 0
    )

    operacion["estado"] = (
        "REQUIERE_REVISION"
        if operacion.get("excepciones_humanas")
        else "EVIDENCIAS_ASOCIADAS"
    )

    return {
        "ok": True,
        "total_evidencias": len(resultados),
        "asociadas": asociadas,
        "ambiguas": ambiguas,
        "sin_asociar": sin_asociar,
        "partidas_totales": len(
            operacion.get("partidas", []) or []
        ),
        "partidas_con_evidencia": partidas_con_evidencia,
        "excepciones_humanas": len(
            operacion.get("excepciones_humanas", []) or []
        ),
        "estado_operacion": operacion.get("estado"),
        "resultados": resultados,
    }


# === P004: ARGO CLASS POR PARTIDA ===

def construir_descripcion_class(partida: Dict[str, Any]) -> str:
    """
    Construye una descripción enriquecida usando SOLO el dato operativo
    de la partida.

    No mezcla datos de otras partidas.
    """

    if not isinstance(partida, dict):
        raise ValueError("partida debe ser un diccionario")

    datos = partida.get("dato_operativo", {}) or {}

    etiquetas = [
        ("marca", "Marca"),
        ("modelo", "Modelo"),
        ("numero_parte", "Número de parte"),
        ("descripcion", "Descripción"),
        ("cantidad", "Cantidad"),
        ("unidad", "Unidad"),
        ("lote", "Lote"),
        ("serie", "Serie"),
        ("pais_origen", "País de origen"),
    ]

    partes = []

    for campo, etiqueta in etiquetas:
        valor = datos.get(campo)

        if not _valor_presente(valor):
            continue

        partes.append(f"{etiqueta}: {valor}")

    return ". ".join(partes)


def clasificar_partida(
    partida: Dict[str, Any],
    *,
    id_operacion: Optional[str] = None,
    id_shipment: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ejecuta ARGO CLASS exclusivamente para una partida.

    Cada partida recibe su propio id_item y su propia descripción
    enriquecida basada en la realidad física.
    """

    if not isinstance(partida, dict):
        raise ValueError("partida debe ser un diccionario")

    from argo_class_engine import build_output

    descripcion = construir_descripcion_class(partida)

    payload_master = {
        "meta": {
            "id_operacion": id_operacion,
            "id_shipment": id_shipment,
            "id_item": partida.get("indice"),
        },
        "descripcion": descripcion,
        "documentos": [],
        "control": {
            "resumen": {},
        },
        "datos_operativos": partida.get("dato_operativo", {}) or {},
    }

    salida = build_output(payload_master) or {}

    partida["clasificacion_argo_class"] = salida
    partida["descripcion_class"] = descripcion

    return salida


# === P004: FINALIZACION MULTIPARTE ===

def _partida_tiene_dato_fisico(partida: Dict[str, Any]) -> bool:
    """
    Indica si existe al menos un dato realmente observado
    en evidencia física de la partida.
    """
    if not isinstance(partida, dict):
        return False

    fisico = partida.get("dato_fisico", {}) or {}

    return any(
        _valor_presente(fisico.get(campo))
        for campo in CAMPOS_PARTIDA
    )


def _agregar_excepcion_unica(
    contenedor: Dict[str, Any],
    excepcion: Dict[str, Any],
) -> None:
    """
    Agrega una excepción evitando duplicados por codigo + campo.
    """
    excepciones = contenedor.setdefault("excepciones", [])

    codigo = excepcion.get("codigo")
    campo = excepcion.get("campo")

    for actual in excepciones:
        if not isinstance(actual, dict):
            continue

        if (
            actual.get("codigo") == codigo
            and actual.get("campo") == campo
        ):
            return

    excepciones.append(excepcion)


def _resumen_class_partida(
    class_result: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Extrae un resumen estable de ARGO CLASS para reportes P004.
    """
    if not isinstance(class_result, dict):
        return {
            "ejecutado": False,
            "producto_detectado": None,
            "familia_detectada": None,
            "sector_detectado": None,
            "confianza_sector_pct": None,
            "fraccion_sugerida": None,
            "confianza_fraccion_pct": None,
            "estado_clasificacion": "PENDIENTE_EVIDENCIA",
            "requiere_validacion_tecnica": True,
            "informacion_faltante": [
                "No existe evidencia física suficiente para clasificar."
            ],
        }

    salida = class_result.get("salida", {}) or {}

    sector = salida.get("sector_ia", {}) or {}
    clas = salida.get("clasificacion", {}) or {}

    return {
        "ejecutado": True,
        "producto_detectado":
            clas.get("producto_detectado"),
        "familia_detectada":
            clas.get("familia_detectada"),
        "sector_detectado":
            sector.get("sector_detectado"),
        "confianza_sector_pct":
            sector.get("confianza_sector_pct"),
        "fraccion_sugerida":
            clas.get("fraccion_sugerida"),
        "confianza_fraccion_pct":
            clas.get("confianza_fraccion_pct"),
        "estado_clasificacion":
            clas.get("estado_clasificacion"),
        "requiere_validacion_tecnica":
            clas.get("requiere_validacion_tecnica"),
        "informacion_faltante":
            clas.get("informacion_faltante", []),
        "evidencia_detectada":
            clas.get("evidencia_detectada", []),
        "reglas_activadas":
            clas.get("reglas_activadas", []),
    }


def construir_reporte_multiparte(
    operacion: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Construye una representación estructurada del reporte P004.

    Regla:
    - referencia_documental = lo esperado;
    - dato_fisico = lo observado;
    - dato_operativo = realidad física que alimenta el cruce;
    - comparación conserva diferencias sin sobrescribir lo físico;
    - CLASS se conserva individualmente por partida.
    """
    if not isinstance(operacion, dict):
        raise ValueError("operacion debe ser un diccionario")

    partidas_reporte = []

    for partida in operacion.get("partidas", []) or []:
        if not isinstance(partida, dict):
            continue

        comparacion = partida.get("comparacion", {}) or {}

        class_resumen = _resumen_class_partida(
            partida.get("clasificacion_argo_class")
        )

        partidas_reporte.append({
            "indice": partida.get("indice"),
            "estado": partida.get("estado"),

            "referencia_documental":
                dict(partida.get("referencia_documental", {}) or {}),

            "dato_fisico":
                dict(partida.get("dato_fisico", {}) or {}),

            "dato_operativo":
                dict(partida.get("dato_operativo", {}) or {}),

            "proveniencia":
                dict(partida.get("proveniencia", {}) or {}),

            "evidencias":
                list(partida.get("evidencias", []) or []),

            "comparacion": {
                "resultado_general":
                    comparacion.get("resultado_general"),
                "resumen":
                    comparacion.get("resumen", {}),
                "comparaciones":
                    comparacion.get("comparaciones", []),
            },

            "argo_class": class_resumen,

            "excepciones":
                list(partida.get("excepciones", []) or []),
        })

    return {
        "modulo": "ARGO_P004_REPORTE_MULTIPARTE",
        "version": "0.1-pilot",
        "id_operacion": operacion.get("id_operacion"),
        "estado_operacion": operacion.get("estado"),
        "total_partidas": len(partidas_reporte),
        "partidas": partidas_reporte,
        "excepciones_humanas":
            list(operacion.get("excepciones_humanas", []) or []),
    }


def finalizar_partidas_operacion(
    operacion: Dict[str, Any],
    *,
    id_shipment: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Finaliza todas las partidas de una operación P004.

    Para cada partida:
    1. consolida evidencia física;
    2. compara documento vs realidad física;
    3. ejecuta ARGO CLASS exclusivamente con esa partida;
    4. conserva datos y resultados independientemente;
    5. genera resumen general de la operación.

    CLASS no se ejecuta si no existe evidencia física suficiente.
    """

    if not isinstance(operacion, dict):
        raise ValueError("operacion debe ser un diccionario")

    id_operacion = operacion.get("id_operacion")

    resultados = []

    for partida in operacion.get("partidas", []) or []:
        if not isinstance(partida, dict):
            continue

        # -----------------------------------------
        # 1) Consolidar realidad física
        # -----------------------------------------
        resolver_dato_operativo(partida)

        tiene_fisico = _partida_tiene_dato_fisico(partida)

        if not tiene_fisico:
            _agregar_excepcion_unica(
                partida,
                {
                    "codigo": "PARTIDA_SIN_EVIDENCIA_FISICA",
                    "campo": None,
                    "mensaje": (
                        "La partida documental no tiene evidencia "
                        "física suficiente asociada."
                    ),
                    "requiere_revision_humana": True,
                },
            )

        # -----------------------------------------
        # 2) Comparación documental
        # -----------------------------------------
        comparacion = comparar_partida(partida)

        # -----------------------------------------
        # 3) CLASS exclusivamente por partida
        # -----------------------------------------
        class_result = None

        if tiene_fisico:
            class_result = clasificar_partida(
                partida,
                id_operacion=id_operacion,
                id_shipment=id_shipment,
            )
        else:
            partida["clasificacion_argo_class"] = None
            partida["descripcion_class"] = ""

        class_resumen = _resumen_class_partida(
            class_result
        )

        resultado_comparacion = (
            comparacion.get("resultado_general")
        )

        # -----------------------------------------
        # 4) Estado operativo de la partida
        # -----------------------------------------
        hay_excepcion_humana = any(
            isinstance(e, dict)
            and e.get("requiere_revision_humana") is True
            for e in partida.get("excepciones", []) or []
        )

        if resultado_comparacion == "DIFERENCIA":
            estado_partida = "DIFERENCIA_DOCUMENTAL"

        elif (
            resultado_comparacion == "DUDA"
            or hay_excepcion_humana
        ):
            estado_partida = "REQUIERE_REVISION"

        else:
            estado_partida = "VERIFICADA"

        partida["estado"] = estado_partida

        resultados.append({
            "indice": partida.get("indice"),
            "estado": estado_partida,

            "resultado_comparacion":
                resultado_comparacion,

            "producto_detectado":
                class_resumen.get("producto_detectado"),

            "familia_detectada":
                class_resumen.get("familia_detectada"),

            "sector_detectado":
                class_resumen.get("sector_detectado"),

            "fraccion_sugerida":
                class_resumen.get("fraccion_sugerida"),

            "confianza_fraccion_pct":
                class_resumen.get("confianza_fraccion_pct"),

            "estado_clasificacion":
                class_resumen.get("estado_clasificacion"),

            "requiere_validacion_tecnica":
                class_resumen.get(
                    "requiere_validacion_tecnica"
                ),

            "evidencias":
                len(partida.get("evidencias", []) or []),
        })

    # ---------------------------------------------
    # 5) Resumen de la operación
    # ---------------------------------------------
    coinciden = sum(
        1 for r in resultados
        if r.get("resultado_comparacion") == "COINCIDE"
    )

    diferencias = sum(
        1 for r in resultados
        if r.get("resultado_comparacion") == "DIFERENCIA"
    )

    dudas = sum(
        1 for r in resultados
        if r.get("resultado_comparacion") == "DUDA"
    )

    verificadas = sum(
        1 for r in resultados
        if r.get("estado") == "VERIFICADA"
    )

    requieren_revision = sum(
        1 for r in resultados
        if r.get("estado") == "REQUIERE_REVISION"
    )

    requieren_validacion_tecnica = sum(
        1 for r in resultados
        if r.get("requiere_validacion_tecnica") is True
    )

    partidas_sin_evidencia = sum(
        1 for r in resultados
        if int(r.get("evidencias") or 0) == 0
    )

    # P004-PATCH-P:
    # El resumen final debe conservar tambien el contador positivo
    # que consume el tablero del operador.
    partidas_con_evidencia = sum(
        1 for r in resultados
        if int(r.get("evidencias") or 0) > 0
    )

    excepciones_operacion = len(
        operacion.get("excepciones_humanas", []) or []
    )

    # La validación técnica de CLASS se reporta aparte.
    # No convierte por sí sola una recepción correcta en diferencia.
    if diferencias > 0:
        estado_operacion = "DIFERENCIAS_DETECTADAS"

    elif (
        dudas > 0
        or requieren_revision > 0
        or excepciones_operacion > 0
        or partidas_sin_evidencia > 0
    ):
        estado_operacion = "REQUIERE_REVISION"

    else:
        estado_operacion = "VERIFICADA"

    operacion["estado"] = estado_operacion
    operacion["resumen_partidas"] = resultados

    reporte = construir_reporte_multiparte(
        operacion
    )

    operacion["reporte_multiparte"] = reporte

    resumen = {
        "ok": True,
        "id_operacion": id_operacion,

        "partidas_totales": len(resultados),
        "partidas_verificadas": verificadas,

        "coinciden": coinciden,
        "diferencias": diferencias,
        "dudas": dudas,

        "requieren_revision": requieren_revision,

        "requieren_validacion_tecnica":
            requieren_validacion_tecnica,

        "partidas_sin_evidencia":
            partidas_sin_evidencia,

        "partidas_con_evidencia":
            partidas_con_evidencia,

        "excepciones_humanas":
            excepciones_operacion,

        "estado_operacion":
            estado_operacion,

        "partidas":
            resultados,

        "reporte_multiparte":
            reporte,
    }

    operacion["resumen_final"] = resumen

    return resumen
