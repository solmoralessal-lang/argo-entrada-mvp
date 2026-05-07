from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from datetime import datetime
import os


ARGO_BLUE = "0F172A"
ARGO_GOLD = "F59E0B"
ARGO_LIGHT = "F8FAFC"
ARGO_GREEN = "22C55E"
ARGO_YELLOW = "FACC15"
ARGO_RED = "EF4444"
ARGO_GRAY = "64748B"
WHITE = "FFFFFF"


def _safe(value, default=""):
    if value is None:
        return default
    return value


def _risk_color(risk):
    risk = str(risk or "").upper()
    if risk in ["CRITICO", "CRÍTICO", "ALTO", "ALTA"]:
        return ARGO_RED
    if risk in ["MEDIO", "MEDIA", "ADVERTENCIA"]:
        return ARGO_YELLOW
    return ARGO_GREEN


def _set_cell(ws, cell, value, bold=False, size=11, color="111827", fill=None, align="left"):
    ws[cell] = value
    ws[cell].font = Font(bold=bold, size=size, color=color)
    ws[cell].alignment = Alignment(horizontal=align, vertical="center", wrap_text=True)
    if fill:
        ws[cell].fill = PatternFill(start_color=fill, end_color=fill, fill_type="solid")


def _merge_title(ws, cell_range, text, size=18, fill=ARGO_BLUE):
    ws.merge_cells(cell_range)
    cell = ws[cell_range.split(":")[0]]
    cell.value = text
    cell.font = Font(bold=True, size=size, color=WHITE)
    cell.fill = PatternFill(start_color=fill, end_color=fill, fill_type="solid")
    cell.alignment = Alignment(horizontal="center", vertical="center")


def _section(ws, row, title):
    ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=7)
    cell = ws.cell(row=row, column=2)
    cell.value = title
    cell.font = Font(bold=True, size=13, color=WHITE)
    cell.fill = PatternFill(start_color=ARGO_BLUE, end_color=ARGO_BLUE, fill_type="solid")
    cell.alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[row].height = 24


def _label_value(ws, row, label, value, col_label=2, col_value=4):
    label_cell = ws.cell(row=row, column=col_label)
    value_cell = ws.cell(row=row, column=col_value)

    label_cell.value = label
    label_cell.font = Font(bold=True, size=11, color=ARGO_GRAY)
    label_cell.alignment = Alignment(horizontal="left", vertical="center")

    value_cell.value = _safe(value)
    value_cell.font = Font(size=11, color="111827")
    value_cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)


def _style_range(ws, start_row, end_row, start_col=2, end_col=7):
    thin = Side(style="thin", color="CBD5E1")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for row in ws.iter_rows(min_row=start_row, max_row=end_row, min_col=start_col, max_col=end_col):
        for cell in row:
            cell.border = border
            cell.alignment = Alignment(vertical="center", wrap_text=True)


def _kpi_card(ws, row, col, title, value, fill):
    ws.merge_cells(start_row=row, start_column=col, end_row=row, end_column=col + 1)
    ws.merge_cells(start_row=row + 1, start_column=col, end_row=row + 2, end_column=col + 1)

    title_cell = ws.cell(row=row, column=col)
    value_cell = ws.cell(row=row + 1, column=col)

    title_cell.value = title
    title_cell.font = Font(bold=True, size=10, color=WHITE)
    title_cell.fill = PatternFill(start_color=ARGO_BLUE, end_color=ARGO_BLUE, fill_type="solid")
    title_cell.alignment = Alignment(horizontal="center", vertical="center")

    value_cell.value = value
    value_cell.font = Font(bold=True, size=18, color=WHITE)
    value_cell.fill = PatternFill(start_color=fill, end_color=fill, fill_type="solid")
    value_cell.alignment = Alignment(horizontal="center", vertical="center")

    _style_range(ws, row, row + 2, col, col + 1)


def generar_reporte_ejecutivo(ruta_plantilla, data, output_path):
    os.makedirs(output_path, exist_ok=True)

    wb = load_workbook(ruta_plantilla)

    if "RESUMEN_EJECUTIVO_ARGO" in wb.sheetnames:
        del wb["RESUMEN_EJECUTIVO_ARGO"]

    ws = wb.create_sheet("RESUMEN_EJECUTIVO_ARGO", 0)
    ws.sheet_view.showGridLines = False

    for col in range(1, 9):
        ws.column_dimensions[get_column_letter(col)].width = 18

    ws.column_dimensions["A"].width = 4
    ws.column_dimensions["B"].width = 22
    ws.column_dimensions["C"].width = 16
    ws.column_dimensions["D"].width = 26
    ws.column_dimensions["E"].width = 18
    ws.column_dimensions["F"].width = 18
    ws.column_dimensions["G"].width = 24
    ws.column_dimensions["H"].width = 4

    for row in range(1, 55):
        ws.row_dimensions[row].height = 22

    # ===== PORTADA / ENCABEZADO =====
    _merge_title(ws, "B2:G3", "ARGO EXECUTIVE REPORT v2026", size=20, fill=ARGO_BLUE)

    ws.merge_cells("B4:G4")
    ws["B4"] = "Reporte ejecutivo generado por ARGO Entrada + ARGO Class"
    ws["B4"].font = Font(italic=True, size=11, color=ARGO_GRAY)
    ws["B4"].alignment = Alignment(horizontal="center")

    cliente = _safe(data.get("cliente"), "SIN CLIENTE")
    tracking = _safe(data.get("tracking") or data.get("shipment_id"), "SIN TRACKING")
    riesgo = _safe(data.get("riesgo_automatico"), "POR_DEFINIR")
    score = _safe(data.get("score_documental"), 0)
    fraccion = _safe(data.get("fraccion_sugerida"), "POR_DEFINIR")
    confianza = _safe(data.get("confianza_fraccion_pct"), 0)
    certeza = _safe(data.get("certeza_final_pct"), 0)
    dd = _safe(data.get("nivel_debida_diligencia"), "POR_DEFINIR")

    _label_value(ws, 6, "Cliente", cliente)
    _label_value(ws, 7, "Fecha de generación", datetime.now().strftime("%Y-%m-%d %H:%M"))
    _label_value(ws, 8, "Shipment / Tracking", tracking)
    _label_value(ws, 9, "Operador / Sistema", "ARGO")

    # ===== KPIs =====
    _kpi_card(ws, 11, 2, "RIESGO GLOBAL", riesgo, _risk_color(riesgo))
    _kpi_card(ws, 11, 4, "SCORE DOCUMENTAL", score, ARGO_BLUE)
    _kpi_card(ws, 11, 6, "FRACCIÓN SUGERIDA", fraccion, ARGO_GOLD)

    # ===== RESUMEN EJECUTIVO =====
    _section(ws, 16, "1. RESUMEN EJECUTIVO DE OPERACIÓN")

    resumen = (
        "ARGO procesó la evidencia documental disponible para consolidar datos operativos, "
        "evaluar consistencia documental y generar una clasificación arancelaria sugerida "
        "mediante ARGO Class. Este reporte concentra los hallazgos principales para revisión "
        "operativa, documental y legal."
    )
    ws.merge_cells("B18:G21")
    ws["B18"] = resumen
    ws["B18"].font = Font(size=11, color="111827")
    ws["B18"].alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
    ws["B18"].fill = PatternFill(start_color=ARGO_LIGHT, end_color=ARGO_LIGHT, fill_type="solid")
    _style_range(ws, 18, 21, 2, 7)

    # ===== DATOS OPERATIVOS =====
    _section(ws, 23, "2. DATOS DE ENTRADA LOGÍSTICA")

    _label_value(ws, 25, "Cliente", cliente)
    _label_value(ws, 26, "Proveedor", data.get("proveedor"))
    _label_value(ws, 27, "Paquetería", data.get("paqueteria"))
    _label_value(ws, 28, "Tracking", tracking)
    _label_value(ws, 29, "Descripción", data.get("descripcion"))
    _label_value(ws, 30, "Peso total", data.get("peso_total"))
    _label_value(ws, 31, "Cantidad de bultos", data.get("cantidad_bultos"))
    _style_range(ws, 25, 31, 2, 7)

    # ===== ARGO CLASS =====
    _section(ws, 34, "3. ANÁLISIS ARGO CLASS — CLASIFICACIÓN SUGERIDA")

    _label_value(ws, 36, "Fracción TIGIE sugerida", fraccion)
    _label_value(ws, 37, "Confianza de fracción", f"{confianza}%")
    _label_value(ws, 38, "Certeza final", f"{certeza}%")
    _label_value(ws, 39, "Score documental", score)
    _label_value(ws, 40, "Nivel de debida diligencia", dd)
    _label_value(ws, 41, "Riesgo automático", riesgo)
    _label_value(ws, 42, "Método", "OCR + consolidación documental + ARGO Class v2026")
    _style_range(ws, 36, 42, 2, 7)

    # ===== VALIDACIÓN / SOPORTE =====
    _section(ws, 45, "4. SOPORTE DOCUMENTAL Y TRAZABILIDAD")

    ws.merge_cells("B47:G50")
    ws["B47"] = (
        "La evaluación considera la evidencia cargada al sistema: fotografías del producto, "
        "packing list, etiquetas de paquetería, factura, hojas de seguridad, certificados, "
        "manuales u otros documentos disponibles. La certeza final depende directamente de la "
        "calidad, cantidad y consistencia de la documentación aportada."
    )
    ws["B47"].font = Font(size=11, color="111827")
    ws["B47"].alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
    ws["B47"].fill = PatternFill(start_color=ARGO_LIGHT, end_color=ARGO_LIGHT, fill_type="solid")
    _style_range(ws, 47, 50, 2, 7)

    # ===== ADVERTENCIA LEGAL =====
    _section(ws, 52, "5. ADVERTENCIA LEGAL Y USO DEL REPORTE")

    ws.merge_cells("B54:G58")
    ws["B54"] = (
        "La clasificación arancelaria sugerida por ARGO Class se genera con base en la información "
        "disponible al momento del análisis y debe ser validada por el responsable legal, aduanal "
        "o especialista autorizado antes de su uso definitivo en operaciones de comercio exterior. "
        "El presente reporte no sustituye el criterio profesional ni las obligaciones de debida "
        "diligencia aplicables."
    )
    ws["B54"].font = Font(size=10, color="7F1D1D")
    ws["B54"].alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
    ws["B54"].fill = PatternFill(start_color="FEE2E2", end_color="FEE2E2", fill_type="solid")
    _style_range(ws, 54, 58, 2, 7)

    # ===== PIE =====
    ws.merge_cells("B61:G61")
    ws["B61"] = "Generado automáticamente por ARGO v2026 · Entrada + Control + Document + Class"
    ws["B61"].font = Font(italic=True, size=10, color=ARGO_GRAY)
    ws["B61"].alignment = Alignment(horizontal="center")

    # Congelar visualmente inicio
    ws.freeze_panes = "A1"

    nombre = f"REPORTE_ARGO_{int(datetime.now().timestamp())}.xlsx"
    ruta_final = os.path.join(output_path, nombre)

    wb.save(ruta_final)

    return ruta_final
