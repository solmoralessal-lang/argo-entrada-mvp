import os
from datetime import datetime

from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.drawing.image import Image


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
    ws.column_dimensions["E"].width = 18
    ws.column_dimensions["F"].width = 28

    for row in range(1, 35):
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

    ws.merge_cells("B28:F29")
    ws["B28"] = "Reporte generado automáticamente por ARGO v2026"
    ws["B28"].font = Font(size=10, italic=True, color="666666")
    ws["B28"].alignment = Alignment(horizontal="center", vertical="center")

    wb.save(ruta_salida)

    print(f"[ARGO] Reporte ejecutivo generado: {ruta_salida}")

    return ruta_salida
