from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.drawing.image import Image
from datetime import datetime
import os
import hashlib


ARGO_BLUE = "0B132B"
ARGO_NAVY = "1C2541"
ARGO_GOLD = "F59E0B"
ARGO_GREEN = "22C55E"
ARGO_YELLOW = "EAB308"
ARGO_RED = "EF4444"
ARGO_LIGHT = "F8FAFC"
ARGO_GRAY = "64748B"
WHITE = "FFFFFF"


def _safe(v, default=""):
    if v is None:
        return default
    return v


def _risk_color(risk):
    risk = str(risk or "").upper()

    if risk in ["CRITICO", "CRÍTICO", "ALTO", "ALTA"]:
        return ARGO_RED

    if risk in ["MEDIA", "MEDIO", "ADVERTENCIA"]:
        return ARGO_YELLOW

    return ARGO_GREEN


def _set_fill(cell, color):
    cell.fill = PatternFill(
        start_color=color,
        end_color=color,
        fill_type="solid"
    )


def _style_cell(
    cell,
    bold=False,
    size=11,
    color="111827",
    fill=None,
    align="left"
):
    cell.font = Font(
        bold=bold,
        size=size,
        color=color
    )

    cell.alignment = Alignment(
        horizontal=align,
        vertical="center",
        wrap_text=True
    )

    if fill:
        _set_fill(cell, fill)


def _merge_title(ws, rango, texto, fill=ARGO_BLUE, size=22):
    ws.merge_cells(rango)

    cell = ws[rango.split(":")[0]]

    cell.value = texto

    _style_cell(
        cell,
        bold=True,
        size=size,
        color=WHITE,
        fill=fill,
        align="center"
    )


def _section(ws, row, title):
    ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=8)

    cell = ws.cell(row=row, column=2)

    cell.value = title

    _style_cell(
        cell,
        bold=True,
        size=13,
        color=WHITE,
        fill=ARGO_NAVY
    )

    ws.row_dimensions[row].height = 24


def _label_value(ws, row, label, value):
    label_cell = ws.cell(row=row, column=2)
    value_cell = ws.cell(row=row, column=4)

    label_cell.value = label

    _style_cell(
        label_cell,
        bold=True,
        size=11,
        color=ARGO_GRAY
    )

    value_cell.value = _safe(value)

    _style_cell(
        value_cell,
        size=11,
        color="111827"
    )


def _border_range(ws, r1, r2, c1=2, c2=8):
    thin = Side(style="thin", color="CBD5E1")

    border = Border(
        left=thin,
        right=thin,
        top=thin,
        bottom=thin
    )

    for row in ws.iter_rows(
        min_row=r1,
        max_row=r2,
        min_col=c1,
        max_col=c2
    ):
        for cell in row:
            cell.border = border
            cell.alignment = Alignment(
                vertical="center",
                wrap_text=True
            )


def _kpi_card(ws, row, col, title, value, color):
    ws.merge_cells(
        start_row=row,
        start_column=col,
        end_row=row,
        end_column=col + 1
    )

    ws.merge_cells(
        start_row=row + 1,
        start_column=col,
        end_row=row + 3,
        end_column=col + 1
    )

    title_cell = ws.cell(row=row, column=col)
    value_cell = ws.cell(row=row + 1, column=col)

    title_cell.value = title

    _style_cell(
        title_cell,
        bold=True,
        size=10,
        color=WHITE,
        fill=ARGO_NAVY,
        align="center"
    )

    value_cell.value = value

    _style_cell(
        value_cell,
        bold=True,
        size=20,
        color=WHITE,
        fill=color,
        align="center"
    )

    _border_range(ws, row, row + 3, col, col + 1)


def _calc_score(data):
    score = 0

    if data.get("proveedor"):
        score += 10

    if data.get("paqueteria"):
        score += 10

    if data.get("tracking"):
        score += 15

    if data.get("descripcion"):
        score += 20

    if data.get("cantidad_bultos"):
        score += 15

    if data.get("peso_total"):
        score += 15

    if data.get("fraccion_sugerida"):
        score += 15

    return min(score, 100)


def _ai_analysis(data):
    descripcion = _safe(data.get("descripcion"), "mercancía")
    proveedor = _safe(data.get("proveedor"), "proveedor no identificado")
    paqueteria = _safe(data.get("paqueteria"), "paquetería no identificada")
    riesgo = _safe(data.get("riesgo_automatico"), "BAJO")

    return (
        f"ARGO detectó consistencia documental parcial para la mercancía "
        f"'{descripcion}'. La operación presenta relación documental con "
        f"proveedor '{proveedor}' y paquetería '{paqueteria}'. "
        f"El nivel de riesgo automático identificado por ARGO es '{riesgo}'. "
        f"La clasificación sugerida fue determinada mediante OCR, "
        f"consolidación documental y análisis contextual ARGO CLASS v2026."
    )


def generar_reporte_ejecutivo(
    ruta_plantilla,
    data,
    output_path
):
    os.makedirs(output_path, exist_ok=True)

    wb = load_workbook(ruta_plantilla)

    if "RESUMEN_EJECUTIVO_ARGO" in wb.sheetnames:
        del wb["RESUMEN_EJECUTIVO_ARGO"]

    ws = wb.create_sheet("RESUMEN_EJECUTIVO_ARGO", 0)

    ws.sheet_view.showGridLines = False

    for col in range(1, 10):
        ws.column_dimensions[get_column_letter(col)].width = 18

    ws.column_dimensions["A"].width = 4
    ws.column_dimensions["B"].width = 22
    ws.column_dimensions["C"].width = 14
    ws.column_dimensions["D"].width = 26
    ws.column_dimensions["E"].width = 18
    ws.column_dimensions["F"].width = 18
    ws.column_dimensions["G"].width = 18
    ws.column_dimensions["H"].width = 18
    ws.column_dimensions["I"].width = 4

    for row in range(1, 90):
        ws.row_dimensions[row].height = 22

    # =====================================================
    # LOGO
    # =====================================================

    logo_path = "assets/logo_argo.png"

    if os.path.exists(logo_path):
        try:
            img = Image(logo_path)
            img.width = 340
            img.height = 170
            ws.add_image(img, "B2")
        except Exception as e:
            print("ERROR LOGO:", str(e))

    # =====================================================
    # TITULO
    # =====================================================

    _merge_title(
        ws,
        "B10:H11",
        "ARGO EXECUTIVE REPORT v2026",
        fill=ARGO_BLUE,
        size=20
    )

    ws.merge_cells("B12:H12")

    ws["B12"] = (
        "Automatización inteligente de procesos aduaneros · "
        "Intelligent Customs Process Automation"
    )

    _style_cell(
        ws["B12"],
        size=11,
        color=ARGO_GRAY,
        align="center"
    )

    cliente = _safe(data.get("cliente"), "SIN CLIENTE")
    tracking = _safe(data.get("tracking"), "SIN TRACKING")
    proveedor = _safe(data.get("proveedor"))
    paqueteria = _safe(data.get("paqueteria"))
    descripcion = _safe(data.get("descripcion"))
    peso = _safe(data.get("peso_total"))
    bultos = _safe(data.get("cantidad_bultos"))

    fraccion = _safe(
        data.get("fraccion_sugerida"),
        "POR DEFINIR"
    )

    confianza = _safe(
        data.get("confianza_fraccion_pct"),
        0
    )

    certeza = _safe(
        data.get("certeza_final_pct"),
        0
    )

    riesgo = _safe(
        data.get("riesgo_automatico"),
        "NINGUNO"
    )

    score = _calc_score(data)

    # =====================================================
    # DATOS GENERALES
    # =====================================================

    _label_value(ws, 15, "Cliente", cliente)
    _label_value(
        ws,
        16,
        "Fecha generación",
        datetime.now().strftime("%Y-%m-%d %H:%M")
    )

    _label_value(ws, 17, "Tracking", tracking)

    op_id = hashlib.md5(
        f"{cliente}{tracking}{datetime.now()}".encode()
    ).hexdigest()[:16].upper()

    _label_value(ws, 18, "Operation ID", op_id)

    # =====================================================
    # KPI
    # =====================================================

    riesgo_texto = "🟢 OPERACIÓN SALUDABLE"

    if str(riesgo).upper() in ["MEDIA", "MEDIO"]:
        riesgo_texto = "🟡 REQUIERE REVISIÓN"

    if str(riesgo).upper() in ["CRITICO", "CRÍTICO", "ALTA"]:
        riesgo_texto = "🔴 RIESGO CRÍTICO"

    _kpi_card(
        ws,
        21,
        2,
        "RIESGO GLOBAL",
        riesgo_texto,
        _risk_color(riesgo)
    )

    _kpi_card(
        ws,
        21,
        5,
        "SCORE DOCUMENTAL",
        f"{score}/100",
        ARGO_BLUE
    )

    # =====================================================
    # RESUMEN
    # =====================================================

    _section(
        ws,
        27,
        "1. RESUMEN EJECUTIVO"
    )

    resumen = (
        "ARGO procesó y consolidó automáticamente la evidencia documental "
        "disponible para validar datos logísticos, evaluar consistencia "
        "documental y generar una clasificación arancelaria sugerida mediante "
        "ARGO CLASS v2026."
    )

    ws.merge_cells("B29:H32")

    ws["B29"] = resumen

    _style_cell(
        ws["B29"],
        size=11,
        color="111827"
    )

    _set_fill(ws["B29"], ARGO_LIGHT)

    _border_range(ws, 29, 32)

    # =====================================================
    # DATOS LOGISTICOS
    # =====================================================

    _section(
        ws,
        35,
        "2. DATOS DE ENTRADA LOGÍSTICA"
    )

    _label_value(ws, 37, "Proveedor", proveedor)
    _label_value(ws, 38, "Paquetería", paqueteria)
    _label_value(ws, 39, "Tracking", tracking)
    _label_value(ws, 40, "Descripción", descripcion)
    _label_value(ws, 41, "Peso total", peso)
    _label_value(ws, 42, "Cantidad bultos", bultos)

    _border_range(ws, 37, 42)

    # =====================================================
    # ARGO CLASS
    # =====================================================

    _section(
        ws,
        45,
        "3. ANÁLISIS ARGO CLASS"
    )

    _label_value(
        ws,
        47,
        "Fracción TIGIE sugerida",
        fraccion
    )

    _label_value(
        ws,
        48,
        "Confianza fracción",
        f"{confianza}%"
    )

    _label_value(
        ws,
        49,
        "Certeza final",
        f"{certeza}%"
    )

    _label_value(
        ws,
        50,
        "Score documental",
        f"{score}/100"
    )

    _label_value(
        ws,
        51,
        "Método inferencia",
        "OCR + consolidación + ARGO CLASS"
    )

    _label_value(
        ws,
        52,
        "Riesgo automático",
        riesgo
    )

    _border_range(ws, 47, 52)

    # =====================================================
    # AI ANALYSIS
    # =====================================================

    _section(
        ws,
        55,
        "4. ARGO AI ANALYSIS"
    )

    ws.merge_cells("B57:H62")

    ws["B57"] = _ai_analysis(data)

    _style_cell(
        ws["B57"],
        size=11,
        color="111827"
    )

    _set_fill(ws["B57"], ARGO_LIGHT)

    _border_range(ws, 57, 62)

    # =====================================================
    # SOPORTE DOCUMENTAL
    # =====================================================

    _section(
        ws,
        65,
        "5. SOPORTE DOCUMENTAL Y TRAZABILIDAD"
    )

    soporte = (
        "La evaluación considera fotografías del producto, packing list, "
        "etiquetas logísticas, invoice, hojas de seguridad, certificados, "
        "manuales y cualquier evidencia documental cargada al sistema."
    )

    ws.merge_cells("B67:H70")

    ws["B67"] = soporte

    _style_cell(
        ws["B67"],
        size=11,
        color="111827"
    )

    _set_fill(ws["B67"], ARGO_LIGHT)

    _border_range(ws, 67, 70)

    # =====================================================
    # LEGAL
    # =====================================================

    _section(
        ws,
        73,
        "6. ADVERTENCIA LEGAL"
    )

    legal = (
        "La clasificación sugerida por ARGO CLASS se genera con base en "
        "información disponible al momento del análisis y debe ser validada "
        "por el responsable legal o especialista autorizado antes de su uso "
        "definitivo en operaciones de comercio exterior."
    )

    ws.merge_cells("B75:H79")

    ws["B75"] = legal

    _style_cell(
        ws["B75"],
        size=10,
        color="7F1D1D"
    )

    _set_fill(ws["B75"], "FEE2E2")

    _border_range(ws, 75, 79)

    # =====================================================
    # FOOTER
    # =====================================================

    ws.merge_cells("B83:H83")

    ws["B83"] = (
        "Generated by ARGO AI Platform v2026 · "
        "Entrada + Control + Document + Class"
    )

    _style_cell(
        ws["B83"],
        size=10,
        color=ARGO_GRAY,
        align="center"
    )

    # =====================================================
    # SAVE
    # =====================================================

    nombre = (
        f"REPORTE_ARGO_{int(datetime.now().timestamp())}.xlsx"
    )

    ruta_final = os.path.join(
        output_path,
        nombre
    )

    wb.save(ruta_final)

    return ruta_final
