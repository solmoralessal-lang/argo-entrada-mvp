import os
from datetime import datetime

from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.drawing.image import Image



# === P004-PATCH-F: REPORTE EXCEL MULTIPARTE ===

def _p004_valor(valor, default="N/D"):
    if valor is None:
        return default

    if isinstance(valor, str):
        valor = valor.strip()
        if not valor:
            return default

    return valor


def _p004_dict(valor):
    return valor if isinstance(valor, dict) else {}


def _p004_lista(valor):
    return valor if isinstance(valor, list) else []


def _p004_ajustar_hoja(ws, anchos=None):
    ws.sheet_view.showGridLines = False
    ws.freeze_panes = "A4"

    if isinstance(anchos, dict):
        for columna, ancho in anchos.items():
            ws.column_dimensions[columna].width = ancho


def _p004_estilo_titulo(
    ws,
    rango,
    texto,
    *,
    color_fondo="08162B",
    color_texto="FFFFFF",
):
    ws.merge_cells(rango)

    celda = ws[rango.split(":")[0]]
    celda.value = texto
    celda.font = Font(
        size=15,
        bold=True,
        color=color_texto,
    )
    celda.fill = PatternFill(
        start_color=color_fondo,
        end_color=color_fondo,
        fill_type="solid",
    )
    celda.alignment = Alignment(
        horizontal="center",
        vertical="center",
    )


def _p004_estilo_encabezado(celda, borde):
    celda.font = Font(
        size=10,
        bold=True,
        color="FFFFFF",
    )
    celda.fill = PatternFill(
        start_color="244062",
        end_color="244062",
        fill_type="solid",
    )
    celda.alignment = Alignment(
        horizontal="center",
        vertical="center",
        wrap_text=True,
    )
    celda.border = borde


def _p004_estilo_dato(celda, borde):
    celda.font = Font(size=9)
    celda.alignment = Alignment(
        vertical="top",
        wrap_text=True,
    )
    celda.border = borde


def _p004_buscar_clasificacion(partida):
    if not isinstance(partida, dict):
        return {}

    for clave in (
        "clasificacion_argo_class",
        "class",
        "clasificacion",
    ):
        valor = partida.get(clave)

        if isinstance(valor, dict) and valor:
            if isinstance(valor.get("salida"), dict):
                salida = valor["salida"]

                if isinstance(
                    salida.get("clasificacion"),
                    dict,
                ):
                    return salida["clasificacion"]

            if isinstance(valor.get("clasificacion"), dict):
                return valor["clasificacion"]

            return valor

    return {}


def _p004_buscar_comparacion(partida):
    if not isinstance(partida, dict):
        return {}

    for clave in (
        "comparacion",
        "comparacion_documental",
        "resultado_comparacion",
    ):
        valor = partida.get(clave)
        if isinstance(valor, dict):
            return valor

    return {}


def _p004_extraer_partidas(datos_operacion):
    operacion = _p004_dict(
        datos_operacion.get("operacion_multiparte")
    )

    partidas = _p004_lista(operacion.get("partidas"))

    if partidas:
        return partidas

    reporte = _p004_dict(
        datos_operacion.get("reporte_multiparte")
    )

    return _p004_lista(reporte.get("partidas"))


def _p004_extraer_excepciones(datos_operacion, partidas):
    operacion = _p004_dict(
        datos_operacion.get("operacion_multiparte")
    )

    excepciones = []

    for item in _p004_lista(
        operacion.get("excepciones_humanas")
    ):
        if isinstance(item, dict):
            excepciones.append(
                {
                    "nivel": "OPERACION",
                    "partida": None,
                    **item,
                }
            )

    for partida in partidas:
        if not isinstance(partida, dict):
            continue

        indice = (
            partida.get("indice")
            or partida.get("indice_partida")
            or partida.get("partida")
        )

        for item in _p004_lista(
            partida.get("excepciones")
        ):
            if isinstance(item, dict):
                excepciones.append(
                    {
                        "nivel": "PARTIDA",
                        "partida": indice,
                        **item,
                    }
                )

    return excepciones


def _p004_crear_hoja_partidas(
    wb,
    datos_operacion,
    borde,
):
    nombre = "Partidas verificadas"

    if nombre in wb.sheetnames:
        del wb[nombre]

    ws = wb.create_sheet(nombre)

    _p004_ajustar_hoja(
        ws,
        {
            "A": 10,
            "B": 16,
            "C": 18,
            "D": 20,
            "E": 20,
            "F": 34,
            "G": 12,
            "H": 12,
            "I": 20,
            "J": 18,
            "K": 16,
            "L": 18,
            "M": 18,
            "N": 18,
            "O": 20,
            "P": 22,
            "Q": 22,
            "R": 18,
            "S": 18,
            "T": 18,
            "U": 24,
            "V": 18,
        },
    )

    _p004_estilo_titulo(
        ws,
        "A1:V2",
        "ARGO - VERIFICACION MULTIPARTE",
    )

    encabezados = [
        "Partida",
        "Purchase Order",
        "Parte documental",
        "Parte físico-operativa",
        "Descripción física",
        "Cantidad",
        "Unidad",
        "Marca",
        "Modelo",
        "Lote",
        "Serie",
        "País de origen",
        "Resultado documento vs físico",
        "Estado partida",
        "Producto detectado",
        "Familia",
        "Fracción sugerida",
        "Confianza CLASS",
        "Estado CLASS",
        "Revisión técnica",
        "Dato operativo usado",
        "Evidencias físicas",
    ]

    for col, titulo in enumerate(encabezados, 1):
        celda = ws.cell(row=3, column=col, value=titulo)
        _p004_estilo_encabezado(celda, borde)

    partidas = _p004_extraer_partidas(datos_operacion)

    fila = 4

    for partida in partidas:
        if not isinstance(partida, dict):
            continue

        documental = _p004_dict(
            partida.get("referencia_documental")
        )
        fisico = _p004_dict(
            partida.get("dato_fisico")
        )
        operativo = _p004_dict(
            partida.get("dato_operativo")
        )

        comparacion = _p004_buscar_comparacion(partida)
        clasificacion = _p004_buscar_clasificacion(partida)

        indice = (
            partida.get("indice")
            or partida.get("indice_partida")
            or documental.get("partida")
        )

        resultado_comparacion = (
            comparacion.get("resultado_general")
            or comparacion.get("resultado")
            or partida.get("resultado_comparacion")
            or "N/D"
        )

        numero_parte_documental = (
            documental.get("numero_parte")
        )

        numero_parte_operativo = (
            operativo.get("numero_parte")
            or fisico.get("numero_parte")
        )

        descripcion_fisica = (
            operativo.get("descripcion")
            or fisico.get("descripcion")
        )

        cantidad = (
            operativo.get("cantidad")
            if operativo.get("cantidad") is not None
            else fisico.get("cantidad")
        )

        unidad = (
            operativo.get("unidad")
            or fisico.get("unidad")
        )

        marca = (
            operativo.get("marca")
            or fisico.get("marca")
        )

        modelo = (
            operativo.get("modelo")
            or fisico.get("modelo")
        )

        lote = (
            operativo.get("lote")
            or fisico.get("lote")
        )

        serie = (
            operativo.get("serie")
            or fisico.get("serie")
        )

        pais = (
            operativo.get("pais_origen")
            or fisico.get("pais_origen")
        )

        producto = (
            clasificacion.get("producto_detectado")
            or clasificacion.get("producto")
        )

        familia = (
            clasificacion.get("familia_detectada")
            or clasificacion.get("familia")
        )

        fraccion = (
            clasificacion.get("fraccion_sugerida")
            or clasificacion.get("fraccion")
        )

        confianza = (
            clasificacion.get("confianza_fraccion_pct")
        )

        estado_class = (
            clasificacion.get("estado_clasificacion")
            or clasificacion.get("estado")
        )

        requiere_validacion = (
            clasificacion.get(
                "requiere_validacion_tecnica"
            )
        )

        if requiere_validacion is True:
            revision_txt = "SI"
        elif requiere_validacion is False:
            revision_txt = "NO"
        else:
            revision_txt = "N/D"

        evidencias = _p004_lista(
            partida.get("evidencias_fisicas")
        )

        if not evidencias:
            evidencias = _p004_lista(
                partida.get("evidencias")
            )

        dato_operativo_usado = []

        for campo in (
            "numero_parte",
            "marca",
            "modelo",
            "lote",
            "serie",
            "pais_origen",
        ):
            valor = operativo.get(campo)

            if valor not in [None, ""]:
                dato_operativo_usado.append(
                    f"{campo}={valor}"
                )

        valores = [
            indice,
            documental.get("purchase_order"),
            numero_parte_documental,
            numero_parte_operativo,
            descripcion_fisica,
            cantidad,
            unidad,
            marca,
            modelo,
            lote,
            serie,
            pais,
            resultado_comparacion,
            partida.get("estado"),
            producto,
            familia,
            fraccion,
            (
                f"{confianza}%"
                if confianza not in [None, ""]
                else "N/D"
            ),
            estado_class,
            revision_txt,
            "; ".join(dato_operativo_usado)
            if dato_operativo_usado
            else "N/D",
            len(evidencias),
        ]

        for col, valor in enumerate(valores, 1):
            celda = ws.cell(
                row=fila,
                column=col,
                value=_p004_valor(valor),
            )
            _p004_estilo_dato(celda, borde)

        resultado_upper = str(
            resultado_comparacion
        ).upper()

        if "DIFERENCIA" in resultado_upper:
            for col in range(1, len(encabezados) + 1):
                ws.cell(
                    row=fila,
                    column=col,
                ).fill = PatternFill(
                    start_color="FCE4D6",
                    end_color="FCE4D6",
                    fill_type="solid",
                )

        elif "COINCIDE" in resultado_upper:
            for col in range(1, len(encabezados) + 1):
                ws.cell(
                    row=fila,
                    column=col,
                ).fill = PatternFill(
                    start_color="E2F0D9",
                    end_color="E2F0D9",
                    fill_type="solid",
                )

        fila += 1

    ws.auto_filter.ref = (
        f"A3:V{max(3, fila - 1)}"
    )

    return ws


def _p004_crear_hoja_diferencias(
    wb,
    datos_operacion,
    borde,
):
    nombre = "Diferencias"

    if nombre in wb.sheetnames:
        del wb[nombre]

    ws = wb.create_sheet(nombre)

    _p004_ajustar_hoja(
        ws,
        {
            "A": 10,
            "B": 18,
            "C": 24,
            "D": 24,
            "E": 24,
            "F": 18,
            "G": 44,
        },
    )

    _p004_estilo_titulo(
        ws,
        "A1:G2",
        "ARGO - DIFERENCIAS Y EXCEPCIONES",
    )

    encabezados = [
        "Partida",
        "Campo / código",
        "Esperado documental",
        "Observado físico",
        "Dato operativo",
        "Resultado",
        "Detalle / acción requerida",
    ]

    for col, titulo in enumerate(encabezados, 1):
        celda = ws.cell(row=3, column=col, value=titulo)
        _p004_estilo_encabezado(celda, borde)

    partidas = _p004_extraer_partidas(datos_operacion)

    fila = 4
    filas_escritas = 0

    for partida in partidas:
        if not isinstance(partida, dict):
            continue

        indice = (
            partida.get("indice")
            or partida.get("indice_partida")
        )

        documental = _p004_dict(
            partida.get("referencia_documental")
        )
        fisico = _p004_dict(
            partida.get("dato_fisico")
        )
        operativo = _p004_dict(
            partida.get("dato_operativo")
        )
        comparacion = _p004_buscar_comparacion(partida)

        comparaciones = _p004_lista(
            comparacion.get("comparaciones")
        )

        for item in comparaciones:
            if not isinstance(item, dict):
                continue

            resultado = str(
                item.get("resultado") or ""
            ).upper()

            if resultado not in (
                "DIFERENCIA",
                "DUDA",
                "NO_VERIFICABLE",
            ):
                continue

            campo = item.get("campo")

            valores = [
                indice,
                item.get("etiqueta") or campo,
                (
                    item.get("esperado")
                    if item.get("esperado") is not None
                    else documental.get(campo)
                ),
                (
                    item.get("observado")
                    if item.get("observado") is not None
                    else fisico.get(campo)
                ),
                operativo.get(campo),
                resultado,
                (
                    item.get("razon")
                    or item.get("mensaje")
                    or "Requiere revisión."
                ),
            ]

            for col, valor in enumerate(valores, 1):
                celda = ws.cell(
                    row=fila,
                    column=col,
                    value=_p004_valor(valor),
                )
                _p004_estilo_dato(celda, borde)

            fill_color = (
                "FCE4D6"
                if resultado == "DIFERENCIA"
                else "FFF2CC"
            )

            for col in range(1, 8):
                ws.cell(
                    row=fila,
                    column=col,
                ).fill = PatternFill(
                    start_color=fill_color,
                    end_color=fill_color,
                    fill_type="solid",
                )

            fila += 1
            filas_escritas += 1

    excepciones = _p004_extraer_excepciones(
        datos_operacion,
        partidas,
    )

    for item in excepciones:
        codigo = (
            item.get("codigo")
            or "EXCEPCION"
        )

        campo = item.get("campo")

        valores = [
            item.get("partida"),
            campo or codigo,
            None,
            None,
            None,
            codigo,
            (
                item.get("mensaje")
                or item.get("razon")
                or (
                    "Requiere revisión humana."
                    if item.get(
                        "requiere_revision_humana"
                    )
                    else "Excepción registrada."
                )
            ),
        ]

        for col, valor in enumerate(valores, 1):
            celda = ws.cell(
                row=fila,
                column=col,
                value=_p004_valor(valor),
            )
            _p004_estilo_dato(celda, borde)

        for col in range(1, 8):
            ws.cell(
                row=fila,
                column=col,
            ).fill = PatternFill(
                start_color="FFF2CC",
                end_color="FFF2CC",
                fill_type="solid",
            )

        fila += 1
        filas_escritas += 1

    if filas_escritas == 0:
        ws.merge_cells("A4:G5")
        ws["A4"] = (
            "No se detectaron diferencias ni excepciones "
            "en las partidas verificadas."
        )
        ws["A4"].font = Font(
            size=11,
            bold=True,
            color="2E7D32",
        )
        ws["A4"].alignment = Alignment(
            horizontal="center",
            vertical="center",
            wrap_text=True,
        )
        ws["A4"].border = borde

    ws.auto_filter.ref = (
        f"A3:G{max(3, fila - 1)}"
    )

    return ws


def _p004_agregar_resumen_multiparte(
    ws,
    datos_operacion,
    borde,
):
    resumen = _p004_dict(
        datos_operacion.get("resumen_multiparte")
    )

    if not resumen:
        reporte = _p004_dict(
            datos_operacion.get("reporte_multiparte")
        )
        resumen = _p004_dict(
            reporte.get("resumen")
        )

    partidas = _p004_extraer_partidas(
        datos_operacion
    )

    total_partidas = (
        resumen.get("partidas_totales")
        or resumen.get("partidas")
        or len(partidas)
    )

    coinciden = (
        resumen.get("coinciden")
        or resumen.get("partidas_coinciden")
        or 0
    )

    diferencias = (
        resumen.get("diferencias")
        or resumen.get("partidas_con_diferencia")
        or 0
    )

    dudas = (
        resumen.get("dudas")
        or resumen.get("partidas_con_duda")
        or 0
    )

    estado = (
        resumen.get("estado_operacion")
        or resumen.get("estado")
        or datos_operacion.get("estado_multiparte")
        or "MULTIPARTE"
    )

    # Área lateral disponible del reporte ejecutivo.
    ws["E14"] = "Modo mercancía"
    ws["F14"] = "MULTIPARTE"

    ws["E15"] = "Estado multiparte"
    ws["F15"] = estado

    ws["E16"] = "Partidas"
    ws["F16"] = total_partidas

    ws["E17"] = "Coinciden"
    ws["F17"] = coinciden

    ws["E18"] = "Diferencias"
    ws["F18"] = diferencias

    ws["E19"] = "Dudas"
    ws["F19"] = dudas

    for fila in range(14, 20):
        ws[f"E{fila}"].font = Font(
            size=10,
            bold=True,
        )
        ws[f"E{fila}"].fill = PatternFill(
            start_color="EAEAEA",
            end_color="EAEAEA",
            fill_type="solid",
        )
        ws[f"E{fila}"].border = borde

        ws[f"F{fila}"].font = Font(
            size=10,
            bold=True,
        )
        ws[f"F{fila}"].border = borde
        ws[f"F{fila}"].alignment = Alignment(
            horizontal="center",
            vertical="center",
            wrap_text=True,
        )


def _p004_aplicar_reporte_multiparte(
    wb,
    ws_ejecutivo,
    datos_operacion,
    borde,
):
    modo = str(
        datos_operacion.get("modo_mercancia")
        or ""
    ).upper()

    operacion = _p004_dict(
        datos_operacion.get("operacion_multiparte")
    )

    partidas = _p004_extraer_partidas(
        datos_operacion
    )

    es_multiparte = (
        modo == "MULTIPARTE"
        or bool(operacion)
        or len(partidas) > 1
    )

    if not es_multiparte:
        return False

    _p004_agregar_resumen_multiparte(
        ws_ejecutivo,
        datos_operacion,
        borde,
    )

    _p004_crear_hoja_partidas(
        wb,
        datos_operacion,
        borde,
    )

    _p004_crear_hoja_diferencias(
        wb,
        datos_operacion,
        borde,
    )

    # Evitar que una clasificación global aparente representar
    # todas las partidas.
    ws_ejecutivo["B47"] = (
        "ANÁLISIS DOCUMENTAL Y CLASIFICACIÓN MULTIPARTE"
    )

    ws_ejecutivo["B48"] = (
        "Esta operación contiene múltiples partidas. "
        "Cada partida fue evaluada de forma independiente. "
        "Las fracciones, niveles de confianza, diferencias "
        "documentales y datos físicos utilizados para el cruce "
        "se muestran en las hojas 'Partidas verificadas' y "
        "'Diferencias'. No debe interpretarse una fracción "
        "global como clasificación de toda la operación."
    )

    return True

def generar_reporte_ejecutivo(plantilla, datos_operacion, carpeta_salida):

    os.makedirs(carpeta_salida, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    ruta_salida = os.path.join(
        carpeta_salida,
        f"reporte_ejecutivo_argo_{timestamp}.xlsx"
    )

    wb = load_workbook(plantilla)

    if "Reporte Ejecutivo" in wb.sheetnames:
        del wb["Reporte Ejecutivo"]

    ws = wb.create_sheet("Reporte Ejecutivo", 0)

    azul_oscuro = "08162B"
    gris_claro = "EAEAEA"
    blanco = "FFFFFF"

    borde = Border(
        left=Side(style="thin", color="999999"),
        right=Side(style="thin", color="999999"),
        top=Side(style="thin", color="999999"),
        bottom=Side(style="thin", color="999999"),
    )

    ws.sheet_view.showGridLines = False

    ws.column_dimensions["A"].width = 4
    ws.column_dimensions["B"].width = 25
    ws.column_dimensions["C"].width = 60
    ws.column_dimensions["D"].width = 4
    ws.column_dimensions["E"].width = 22
    ws.column_dimensions["F"].width = 32

    for row in range(1, 70):
        ws.row_dimensions[row].height = 24

    # =========================
    # LOGO
    # =========================
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        logo_path = os.path.join(base_dir, "assets", "logo_argo_excel.jpg")

        print(f"[ARGO] Buscando logo en: {logo_path}")

        if os.path.exists(logo_path):
            img = Image(logo_path)
            img.width = 210
            img.height = 85
            ws.add_image(img, "B2")
            print("[ARGO] Logo cargado correctamente")
        else:
            print(f"[ARGO] Logo no encontrado: {logo_path}")

    except Exception as e:
        print(f"[ARGO] Error cargando logo: {e}")

    # =========================
    # ENCABEZADO
    # =========================
    ws.merge_cells("B6:F8")
    ws["B6"] = "ARGO - REPORTE EJECUTIVO PREMIUM"
    ws["B6"].font = Font(size=22, bold=True, color=blanco)
    ws["B6"].fill = PatternFill(
        start_color=azul_oscuro,
        end_color=azul_oscuro,
        fill_type="solid"
    )
    ws["B6"].alignment = Alignment(horizontal="center", vertical="center")

    ws.merge_cells("B9:F9")
    ws["B9"] = "Automatización inteligente de procesos aduaneros"
    ws["B9"].font = Font(size=11, italic=True, color="666666")
    ws["B9"].alignment = Alignment(horizontal="center")

    # =========================
    # DATOS GENERALES
    # =========================
    datos = [
        ("Cliente", datos_operacion.get("cliente")),
        ("Proveedor", datos_operacion.get("proveedor")),
        ("Paquetería", datos_operacion.get("paqueteria")),
        ("Tracking", datos_operacion.get("tracking")),
        ("Descripción", datos_operacion.get("descripcion")),
        ("Cantidad Bultos", datos_operacion.get("cantidad_bultos")),
        ("Peso Total", datos_operacion.get("peso_total")),
        ("Unidad Peso", datos_operacion.get("peso_unidad")),
    ]

    fila = 12

    for campo, valor in datos:
        ws[f"B{fila}"] = campo
        ws[f"B{fila}"].font = Font(size=11, bold=True)
        ws[f"B{fila}"].fill = PatternFill(
            start_color=gris_claro,
            end_color=gris_claro,
            fill_type="solid"
        )
        ws[f"B{fila}"].border = borde
        ws[f"B{fila}"].alignment = Alignment(vertical="center")

        ws[f"C{fila}"] = str(valor) if valor not in [None, ""] else "N/D"
        ws[f"C{fila}"].font = Font(size=11)
        ws[f"C{fila}"].border = borde
        ws[f"C{fila}"].alignment = Alignment(wrap_text=True, vertical="center")

        fila += 1

    ws["E12"] = "Fecha"
    ws["E12"].font = Font(size=11, bold=True)
    ws["E12"].fill = PatternFill(
        start_color=gris_claro,
        end_color=gris_claro,
        fill_type="solid"
    )
    ws["E12"].border = borde

    ws["F12"] = datetime.now().strftime("%d/%m/%Y %H:%M")
    ws["F12"].font = Font(size=11)
    ws["F12"].border = borde

    # =========================
    # VALIDACIÓN OPERACIONAL
    # Prioridad ejecutiva y comercial del reporte
    # =========================
    semaforo_operativo = (
        datos_operacion.get("semaforo_operativo") or "SIN CONTROL"
    )
    icono_operativo = datos_operacion.get("icono_operativo") or ""
    cobertura = datos_operacion.get("cobertura_validacion_pct") or 0
    dictamen = (
        datos_operacion.get("dictamen_operativo")
        or "Sin dictamen operativo."
    )
    campos_totales = datos_operacion.get("campos_totales") or 0
    campos_disponibles = datos_operacion.get("campos_disponibles") or 0
    campos_no_verificables = (
        datos_operacion.get("campos_no_verificables") or 0
    )
    validaciones = (
        datos_operacion.get("validaciones_operativas") or []
    )

    semaforo_texto = str(semaforo_operativo).upper()

    color_semaforo = {
        "VERDE": "2E7D32",
        "AMARILLO": "F9A825",
        "ROJO": "C62828",
    }.get(semaforo_texto, "666666")

    color_texto_semaforo = (
        "000000" if semaforo_texto == "AMARILLO" else "FFFFFF"
    )

    borde_estado = Border(
        left=Side(style="medium", color="333333"),
        right=Side(style="medium", color="333333"),
        top=Side(style="medium", color="333333"),
        bottom=Side(style="medium", color="333333"),
    )

    ws.merge_cells("B22:F22")
    ws["B22"] = "VALIDACIÓN OPERACIONAL"
    ws["B22"].font = Font(size=15, bold=True, color=blanco)
    ws["B22"].fill = PatternFill(
        start_color=azul_oscuro,
        end_color=azul_oscuro,
        fill_type="solid"
    )
    ws["B22"].alignment = Alignment(
        horizontal="center",
        vertical="center"
    )

    ws.merge_cells("B23:C26")
    ws["B23"] = (
        f"ESTADO OPERATIVO\n"
        f"{icono_operativo} {semaforo_texto}"
    )
    ws["B23"].font = Font(
        size=18,
        bold=True,
        color=color_texto_semaforo
    )
    ws["B23"].fill = PatternFill(
        start_color=color_semaforo,
        end_color=color_semaforo,
        fill_type="solid"
    )
    ws["B23"].alignment = Alignment(
        horizontal="center",
        vertical="center",
        wrap_text=True
    )

    for fila_estado in range(23, 27):
        for columna_estado in ["B", "C"]:
            ws[f"{columna_estado}{fila_estado}"].border = borde_estado

    indicadores_operativos = [
        ("Cobertura de validación", f"{cobertura}%"),
        ("Campos disponibles", campos_disponibles),
        ("Campos no verificables", campos_no_verificables),
        ("Campos evaluados", campos_totales),
    ]

    fila_indicador = 23

    for campo, valor in indicadores_operativos:
        ws[f"E{fila_indicador}"] = campo
        ws[f"E{fila_indicador}"].font = Font(size=10, bold=True)
        ws[f"E{fila_indicador}"].fill = PatternFill(
            start_color=gris_claro,
            end_color=gris_claro,
            fill_type="solid"
        )
        ws[f"E{fila_indicador}"].border = borde
        ws[f"E{fila_indicador}"].alignment = Alignment(
            vertical="center",
            wrap_text=True
        )

        ws[f"F{fila_indicador}"] = str(valor)
        ws[f"F{fila_indicador}"].font = Font(size=11, bold=True)
        ws[f"F{fila_indicador}"].border = borde
        ws[f"F{fila_indicador}"].alignment = Alignment(
            horizontal="center",
            vertical="center",
            wrap_text=True
        )

        fila_indicador += 1

    ws.merge_cells("B28:F30")
    ws["B28"] = f"DICTAMEN OPERATIVO\n\n{dictamen}"
    ws["B28"].font = Font(size=12, bold=True, color="000000")
    ws["B28"].alignment = Alignment(
        wrap_text=True,
        vertical="center"
    )
    ws["B28"].border = borde

    # =========================
    # TABLA DE VALIDACIONES
    # =========================
    ws.merge_cells("B32:F32")
    ws["B32"] = "DETALLE DE VALIDACIONES OPERATIVAS"
    ws["B32"].font = Font(size=13, bold=True, color=blanco)
    ws["B32"].fill = PatternFill(
        start_color=azul_oscuro,
        end_color=azul_oscuro,
        fill_type="solid"
    )
    ws["B32"].alignment = Alignment(
        horizontal="center",
        vertical="center"
    )

    encabezados = [
        ("B33", "Campo"),
        ("C33", "Valor documental"),
        ("E33", "Estado"),
        ("F33", "Severidad"),
    ]

    for celda, titulo in encabezados:
        ws[celda] = titulo
        ws[celda].font = Font(size=10, bold=True)
        ws[celda].fill = PatternFill(
            start_color=gris_claro,
            end_color=gris_claro,
            fill_type="solid"
        )
        ws[celda].border = borde
        ws[celda].alignment = Alignment(
            horizontal="center",
            vertical="center"
        )

    fila_validacion = 34

    for item in validaciones[:12]:
        etiqueta = (
            item.get("etiqueta")
            or item.get("campo")
            or "N/D"
        )
        valor_documental = item.get("valor_documental")
        estado = item.get("estado") or ""
        resultado = (
            item.get("resultado")
            or estado
            or "N/D"
        )
        severidad = item.get("severidad") or "N/D"

        if estado == "DISPONIBLE":
            estado_impresion = "OK - DISPONIBLE"
        elif estado == "NO_VERIFICABLE":
            estado_impresion = "REVISAR - NO VERIFICABLE"
        else:
            estado_impresion = resultado

        ws[f"B{fila_validacion}"] = etiqueta
        ws[f"C{fila_validacion}"] = (
            str(valor_documental)
            if valor_documental not in [None, ""]
            else "N/D"
        )
        ws[f"E{fila_validacion}"] = estado_impresion
        ws[f"F{fila_validacion}"] = severidad

        if estado == "NO_VERIFICABLE":
            ws[f"E{fila_validacion}"].font = Font(
                size=10,
                bold=True
            )
        else:
            ws[f"E{fila_validacion}"].font = Font(size=10)

        for columna in ["B", "C", "E", "F"]:
            ws[f"{columna}{fila_validacion}"].border = borde
            ws[f"{columna}{fila_validacion}"].alignment = Alignment(
                wrap_text=True,
                vertical="center"
            )

            if columna != "E":
                ws[f"{columna}{fila_validacion}"].font = Font(
                    size=10
                )

        fila_validacion += 1

    # =========================
    # ANÁLISIS DOCUMENTAL
    # Información complementaria, no protagonista
    # =========================
    riesgo = datos_operacion.get("riesgo_automatico") or "MEDIA"
    score = datos_operacion.get("score_documental") or 0
    fraccion = (
        datos_operacion.get("fraccion_sugerida")
        or "7318.15.99"
    )
    confianza = (
        datos_operacion.get("confianza_fraccion_pct") or 0
    )
    certeza = datos_operacion.get("certeza_final_pct") or 0
    diligencia = (
        datos_operacion.get("nivel_debida_diligencia")
        or "BASICA"
    )

    ws.merge_cells("B47:F47")
    ws["B47"] = "ANÁLISIS DOCUMENTAL Y CLASIFICACIÓN"
    ws["B47"].font = Font(size=13, bold=True, color=blanco)
    ws["B47"].fill = PatternFill(
        start_color=azul_oscuro,
        end_color=azul_oscuro,
        fill_type="solid"
    )
    ws["B47"].alignment = Alignment(
        horizontal="center",
        vertical="center"
    )

    resumen_documental = (
        f"ARGO identificó riesgo automático {riesgo}, "
        f"score documental {score}, fracción sugerida {fraccion}, "
        f"confianza de clasificación {confianza}% y certeza final "
        f"{certeza}%. Nivel de debida diligencia recomendado: "
        f"{diligencia}."
    )

    ws.merge_cells("B48:F50")
    ws["B48"] = resumen_documental
    ws["B48"].font = Font(size=10, color="000000")
    ws["B48"].alignment = Alignment(
        wrap_text=True,
        vertical="center"
    )
    ws["B48"].border = borde

    matriz_documental = [
        ("Riesgo automático", riesgo),
        ("Score documental", score),
        ("Fracción sugerida", fraccion),
        ("Confianza fracción", f"{confianza}%"),
        ("Certeza final", f"{certeza}%"),
        ("Debida diligencia", diligencia),
    ]

    fila_matriz = 52

    for campo, valor in matriz_documental:
        ws[f"B{fila_matriz}"] = campo
        ws[f"B{fila_matriz}"].font = Font(size=10, bold=True)
        ws[f"B{fila_matriz}"].fill = PatternFill(
            start_color=gris_claro,
            end_color=gris_claro,
            fill_type="solid"
        )
        ws[f"B{fila_matriz}"].border = borde

        ws[f"C{fila_matriz}"] = str(valor)
        ws[f"C{fila_matriz}"].font = Font(size=10)
        ws[f"C{fila_matriz}"].border = borde
        ws[f"C{fila_matriz}"].alignment = Alignment(
            wrap_text=True
        )

        fila_matriz += 1

    # =========================
    # ADVERTENCIA OPERATIVA
    # =========================
    ws.merge_cells("B60:F63")
    ws["B60"] = (
        "Advertencia: ARGO procesa información con base en los "
        "documentos e imágenes proporcionados por el operador. "
        "La captura, legibilidad y validez documental son "
        "responsabilidad del usuario operativo. La clasificación "
        "sugerida debe ser validada por personal autorizado antes "
        "de su uso definitivo."
    )
    ws["B60"].font = Font(
        size=9,
        italic=True,
        color="555555"
    )
    ws["B60"].alignment = Alignment(
        wrap_text=True,
        vertical="center"
    )
    ws["B60"].border = borde

    # =========================
    # PIE
    # =========================
    ws.merge_cells("B65:F66")
    ws["B65"] = (
        "Reporte generado automáticamente por ARGO v2026"
    )
    ws["B65"].font = Font(
        size=10,
        italic=True,
        color="666666"
    )
    ws["B65"].alignment = Alignment(
        horizontal="center",
        vertical="center"
    )

    # P004-PATCH-F
    # Extensión multiparte aislada del reporte legacy.
    _p004_aplicar_reporte_multiparte(
        wb,
        ws,
        datos_operacion,
        borde,
    )

    wb.save(ruta_salida)

    print(f"[ARGO] Reporte ejecutivo generado: {ruta_salida}")

    return ruta_salida
