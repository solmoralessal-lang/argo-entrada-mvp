from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment
from datetime import datetime
import os

def generar_reporte_ejecutivo(ruta_plantilla, data, output_path):
    
    wb = load_workbook(ruta_plantilla)

    # Crear nueva hoja
    ws = wb.create_sheet("RESUMEN_EJECUTIVO_ARGO")

    # ===== ESTILOS =====
    titulo_font = Font(size=18, bold=True)
    header_font = Font(size=12, bold=True)
    normal_font = Font(size=11)

    fill_header = PatternFill(start_color="0F172A", end_color="0F172A", fill_type="solid")
    fill_ok = PatternFill(start_color="22C55E", fill_type="solid")
    fill_warn = PatternFill(start_color="FACC15", fill_type="solid")
    fill_bad = PatternFill(start_color="EF4444", fill_type="solid")

    center = Alignment(horizontal="center")
    
    # ===== ENCABEZADO =====
    ws["B2"] = "ARGO - REPORTE EJECUTIVO"
    ws["B2"].font = titulo_font

    ws["B4"] = "Cliente:"
    ws["C4"] = data.get("cliente", "")

    ws["B5"] = "Fecha:"
    ws["C5"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    ws["B6"] = "Operación ID:"
    ws["C6"] = data.get("shipment_id", "")

    # ===== RESUMEN =====
    ws["B8"] = "RESUMEN OPERATIVO"
    ws["B8"].font = header_font

    riesgo = data.get("riesgo_automatico", "CRITICO")

    ws["B10"] = "Estado"
    ws["C10"] = riesgo

    if riesgo == "CRITICO":
        ws["C10"].fill = fill_bad
    elif riesgo == "ALTO":
        ws["C10"].fill = fill_warn
    else:
        ws["C10"].fill = fill_ok

    ws["B11"] = "Score Documental"
    ws["C11"] = data.get("score_documental", 0)

    # ===== DATOS =====
    ws["B13"] = "DATOS DE ENTRADA"
    ws["B13"].font = header_font

    ws["B15"] = "Proveedor"
    ws["C15"] = data.get("proveedor", "")

    ws["B16"] = "Paquetería"
    ws["C16"] = data.get("paqueteria", "")

    ws["B17"] = "Tracking"
    ws["C17"] = data.get("tracking", "")

    ws["B18"] = "Peso"
    ws["C18"] = data.get("peso_total", "")

    ws["B19"] = "Bultos"
    ws["C19"] = data.get("cantidad_bultos", "")

    # ===== ARGO CLASS =====
    ws["B21"] = "ANÁLISIS ARGO CLASS"
    ws["B21"].font = header_font

    ws["B23"] = "Fracción sugerida"
    ws["C23"] = data.get("fraccion_sugerida", "")

    ws["B24"] = "Confianza %"
    ws["C24"] = data.get("confianza_fraccion_pct", "")

    ws["B25"] = "Certeza final %"
    ws["C25"] = data.get("certeza_final_pct", "")

    ws["B26"] = "Nivel Debida Diligencia"
    ws["C26"] = data.get("nivel_debida_diligencia", "")

    # ===== LEGAL =====
    ws["B28"] = "ADVERTENCIA LEGAL"
    ws["B28"].font = header_font

    ws["B30"] = "Clasificación sugerida por ARGO Class con base en información disponible. Debe ser validada por el responsable legal."

    # ===== AJUSTES =====
    ws.column_dimensions["B"].width = 25
    ws.column_dimensions["C"].width = 40

    # Guardar
    nombre = f"REPORTE_ARGO_{int(datetime.now().timestamp())}.xlsx"
    ruta_final = os.path.join(output_path, nombre)

    wb.save(ruta_final)

    return ruta_final
