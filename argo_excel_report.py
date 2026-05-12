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
    ws.column_dimensions["E"].width = 22
    ws.column_dimensions["F"].width = 32

    for row in range(1, 45):
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
    # RESUMEN EJECUTIVO
    # =========================
    ws.merge_cells("B22:F22")
    ws["B22"] = "RESUMEN EJECUTIVO"
    ws["B22"].font = Font(size=14, bold=True, color=blanco)
    ws["B22"].fill = PatternFill(
        start_color=azul_oscuro,
        end_color=azul_oscuro,
        fill_type="solid"
    )
    ws["B22"].alignment = Alignment(horizontal="center", vertical="center")

    riesgo = datos_operacion.get("riesgo_automatico") or "MEDIA"
    score = datos_operacion.get("score_documental") or 0
    fraccion = datos_operacion.get("fraccion_sugerida") or "PENDIENTE ARGO CLASS"
    confianza = datos_operacion.get("confianza_fraccion_pct") or 0
    certeza = datos_operacion.get("certeza_final_pct") or 0
    diligencia = datos_operacion.get("nivel_debida_diligencia") or "PENDIENTE ARGO CLASS"

    resumen = (
        f"La operación fue procesada por ARGO con riesgo automático {riesgo}, "
        f"score documental {score}, fracción sugerida {fraccion}, "
        f"confianza de clasificación {confianza}% y certeza final {certeza}%. "
        f"Nivel de debida diligencia recomendado: {diligencia}."
    )

    ws.merge_cells("B23:F25")
    ws["B23"] = resumen
    ws["B23"].font = Font(size=11, color="000000")
    ws["B23"].alignment = Alignment(wrap_text=True, vertical="top")
    ws["B23"].border = borde

    # =========================
    # MATRIZ DOCUMENTAL Y CLASS
    # =========================
    ws.merge_cells("B27:F27")
    ws["B27"] = "MATRIZ DOCUMENTAL Y CLASIFICACIÓN"
    ws["B27"].font = Font(size=14, bold=True, color=blanco)
    ws["B27"].fill = PatternFill(
        start_color=azul_oscuro,
        end_color=azul_oscuro,
        fill_type="solid"
    )
    ws["B27"].alignment = Alignment(horizontal="center", vertical="center")

    matriz = [
        ("Riesgo automático", riesgo),
        ("Score documental", score),
        ("Fracción sugerida", fraccion),
        ("Confianza fracción", f"{confianza}%"),
        ("Certeza final", f"{certeza}%"),
        ("Debida diligencia", diligencia),
    ]

    fila_matriz = 29

    for campo, valor in matriz:
        ws[f"B{fila_matriz}"] = campo
        ws[f"B{fila_matriz}"].font = Font(size=11, bold=True)
        ws[f"B{fila_matriz}"].fill = PatternFill(
            start_color=gris_claro,
            end_color=gris_claro,
            fill_type="solid"
        )
        ws[f"B{fila_matriz}"].border = borde

        ws[f"C{fila_matriz}"] = str(valor)
        ws[f"C{fila_matriz}"].font = Font(size=11)
        ws[f"C{fila_matriz}"].border = borde
        ws[f"C{fila_matriz}"].alignment = Alignment(wrap_text=True)

        fila_matriz += 1

    # =========================
    # ADVERTENCIA OPERATIVA
    # =========================
    ws.merge_cells("B37:F40")
    ws["B37"] = (
        "Advertencia: ARGO procesa información con base en los documentos e imágenes "
        "proporcionados por el operador. La captura, legibilidad y validez documental "
        "son responsabilidad del usuario operativo. La clasificación sugerida debe ser "
        "validada por personal autorizado antes de su uso definitivo."
    )
    ws["B37"].font = Font(size=10, italic=True, color="666666")
    ws["B37"].alignment = Alignment(wrap_text=True, vertical="top")
    ws["B37"].border = borde

    # =========================
    # PIE
    # =========================
    ws.merge_cells("B42:F43")
    ws["B42"] = "Reporte generado automáticamente por ARGO v2026"
    ws["B42"].font = Font(size=10, italic=True, color="666666")
    ws["B42"].alignment = Alignment(horizontal="center", vertical="center")

    wb.save(ruta_salida)

    print(f"[ARGO] Reporte ejecutivo generado: {ruta_salida}")

    return ruta_salida
