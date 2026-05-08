import os
from datetime import datetime

from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.drawing.image import Image


def generar_reporte_ejecutivo(datos_operacion, ruta_salida):
    """
    Genera reporte ejecutivo premium ARGO
    """

    plantilla = "PLANTILLA_OFICIAL_ARGO_DOCUMENT_MEJORADA_v2026.xlsx"

    wb = load_workbook(plantilla)
    ws = wb.active

    # =========================
    # ESTILOS
    # =========================

    azul_argo = "102B4E"
    gris_claro = "EAEAEA"
    blanco = "FFFFFF"

    borde = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    titulo_font = Font(
        name="Calibri",
        size=20,
        bold=True,
        color=blanco
    )

    subtitulo_font = Font(
        name="Calibri",
        size=11,
        bold=True,
        color="000000"
    )

    texto_font = Font(
        name="Calibri",
        size=10,
        color="000000"
    )

    # =========================
    # ENCABEZADO
    # =========================

    ws.merge_cells("B2:H4")

    encabezado = ws["B2"]
    encabezado.value = "ARGO - REPORTE EJECUTIVO PREMIUM"
    encabezado.font = titulo_font
    encabezado.fill = PatternFill(
        start_color=azul_argo,
        end_color=azul_argo,
        fill_type="solid"
    )
    encabezado.alignment = Alignment(
        horizontal="center",
        vertical="center"
    )

    # =========================
    # LOGO ARGO PREMIUM
    # =========================

    try:

        base_dir = os.path.dirname(os.path.abspath(__file__))

        logo_path = os.path.join(
            base_dir,
            "assets",
            "logo_argo.png"
        )

        print(f"[ARGO] Buscando logo en: {logo_path}")

        if os.path.exists(logo_path):

            img = Image(logo_path)

            # Tamaño premium
            img.width = 240
            img.height = 95

            # Posición
            ws.add_image(img, "B2")

            print("[ARGO] Logo cargado correctamente")

        else:
            print(f"[ARGO] Logo no encontrado: {logo_path}")

    except Exception as e:
        print(f"[ARGO] Error cargando logo: {e}")

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
        ("Estado", datos_operacion.get("estado")),
        ("Riesgo", datos_operacion.get("riesgo_global")),
    ]

    fila = 7

    for titulo, valor in datos:

        ws[f"B{fila}"] = titulo
        ws[f"B{fila}"].font = subtitulo_font
        ws[f"B{fila}"].fill = PatternFill(
            start_color=gris_claro,
            end_color=gris_claro,
            fill_type="solid"
        )
        ws[f"B{fila}"].border = borde

        ws[f"C{fila}"] = str(valor) if valor else "N/D"
        ws[f"C{fila}"].font = texto_font
        ws[f"C{fila}"].border = borde

        fila += 1

    # =========================
    # FECHA
    # =========================

    ws["F7"] = "Fecha"
    ws["F7"].font = subtitulo_font
    ws["F7"].fill = PatternFill(
        start_color=gris_claro,
        end_color=gris_claro,
        fill_type="solid"
    )
    ws["F7"].border = borde

    ws["G7"] = datetime.now().strftime("%d/%m/%Y %H:%M")
    ws["G7"].font = texto_font
    ws["G7"].border = borde

    # =========================
    # AJUSTE COLUMNAS
    # =========================

    columnas = {
        "B": 25,
        "C": 45,
        "F": 18,
        "G": 25
    }

    for col, ancho in columnas.items():
        ws.column_dimensions[col].width = ancho

    # =========================
    # PIE DE REPORTE
    # =========================

    fila_footer = fila + 3

    ws.merge_cells(f"B{fila_footer}:G{fila_footer}")

    footer = ws[f"B{fila_footer}"]

    footer.value = (
        "Reporte generado automáticamente por ARGO v2026 "
        "- Plataforma Inteligente de Automatización Aduanal"
    )

    footer.font = Font(
        name="Calibri",
        size=9,
        italic=True,
        color="666666"
    )

    footer.alignment = Alignment(
        horizontal="center"
    )

    # =========================
    # GUARDAR
    # =========================

    wb.save(ruta_salida)

    print(f"[ARGO] Reporte ejecutivo generado: {ruta_salida}")

    return ruta_salida
