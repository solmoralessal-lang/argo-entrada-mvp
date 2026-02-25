from fastapi import FastAPI, Form
from fastapi.responses import FileResponse
from openpyxl import load_workbook
import os
import re
from datetime import datetime

app = FastAPI()

TEMPLATE_FILE = "PLANTILLA_OFICIAL_ARGO_ENTRADA.xlsx"
OUTPUT_FOLDER = "salidas"

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)


@app.get("/")
def home():
    return {"mensaje": "ARGO ENTRADA MVP funcionando"}


def _safe_filename(text: str) -> str:
    text = (text or "").strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^A-Za-z0-9 _-]", "_", text)
    text = text.replace(" ", "_")
    return text or "SIN_CLIENTE"


def _clear_sheet(ws, max_rows=200, max_cols=10):
    for r in range(1, max_rows + 1):
        for c in range(1, max_cols + 1):
            ws.cell(row=r, column=c).value = None


@app.post("/entrada/validar")
def entrada_validar(
    shipment_id: str = Form(...),
    cliente: str = Form(...),
    tracking: str = Form(...),
    peso_total: str = Form(...),
    unidad: str = Form(...),
    pais_origen: str = Form(...),
    proveedor: str = Form("No legible"),
    paqueteria: str = Form("No legible"),
    descripcion: str = Form("No legible"),
    marca: str = Form("No legible"),
    modelo: str = Form("No legible"),
    no_parte: str = Form("No legible"),
    no_lote: str = Form("No legible"),
    no_serie: str = Form("No legible"),
    cantidad: str = Form("No legible"),
):
        # --- Validación anti-placeholder de Swagger ("string") y vacíos ---
    def _clean_required(name: str, v: str) -> str:
        s = (v or "").strip()
        if s == "" or s.lower() == "string":
            raise ValueError(f"Campo requerido inválido: {name}")
        return s

    # Campos críticos (los que NO deben aceptar "string" / vacío)
    shipment_id = _clean_required("shipment_id", shipment_id)
    cliente     = _clean_required("cliente", cliente)
    tracking    = _clean_required("tracking", tracking)
    peso_total  = _clean_required("peso_total", peso_total)
    unidad      = _clean_required("unidad", unidad)
    pais_origen = _clean_required("pais_origen", pais_origen)
    fecha_recepcion = datetime.now().strftime("%m/%d/%Y")

    tracking_original = (tracking or "").strip()
    tracking_texto = tracking_original
    if (
        tracking_texto.lower().endswith("e")
        or "e+" in tracking_texto.lower()
        or "e-" in tracking_texto.lower()
    ):
        tracking_texto = "No legible"

    cliente_n = (cliente or "").strip() or "No legible"
    proveedor_n = (proveedor or "").strip() or "No legible"
    paqueteria_n = (paqueteria or "").strip() or "No legible"

    entrada = {
        "shipment_id": (shipment_id or "").strip() or "No legible",
        "fecha_recepcion": fecha_recepcion,
        "cliente": cliente_n,
        "proveedor": proveedor_n,
        "paqueteria": paqueteria_n,
        "tracking": tracking_texto,
        "descripcion": (descripcion or "").strip() or "No legible",
        "marca": (marca or "").strip() or "No legible",
        "modelo": (modelo or "").strip() or "No legible",
        "no_parte": (no_parte or "").strip() or "No legible",
        "no_lote": (no_lote or "").strip() or "No legible",
        "no_serie": (no_serie or "").strip() or "No legible",
        "cantidad": (cantidad or "").strip() or "No legible",
        "unidad": (unidad or "").strip() or "No legible",
        "peso_total": (peso_total or "").strip() or "No legible",
        "pais_origen": (pais_origen or "").strip() or "No legible",
    }

    faltantes = []
    for k, v in entrada.items():
        if str(v).strip() == "No legible":
            faltantes.append({"campo": k, "valor": "No legible"})

    alertas = []
    if entrada["tracking"] == "No legible":
        alertas.append(
            {"alerta": "Tracking inválido", "detalle": "Formato incorrecto", "severidad": "ALTA"}
        )
    if entrada["peso_total"] == "No legible":
        alertas.append(
            {"alerta": "Peso no legible", "detalle": "No se detectó peso válido", "severidad": "MEDIA"}
        )
    if entrada["cliente"] == "No legible":
        alertas.append(
            {"alerta": "Cliente faltante", "detalle": "No se detectó cliente", "severidad": "ALTA"}
        )
    if entrada["proveedor"] == "No legible":
        alertas.append(
            {"alerta": "Proveedor faltante", "detalle": "No se detectó proveedor", "severidad": "MEDIA"}
        )

    hay_alta = any(a["severidad"] == "ALTA" for a in alertas)
    if hay_alta:
        estado = "REVISIÓN"
    elif len(faltantes) > 0 or len(alertas) > 0:
        estado = "ADVERTENCIA"
    else:
        estado = "OK"

    severidad_maxima = "NINGUNA"
    if any(a["severidad"] == "ALTA" for a in alertas):
        severidad_maxima = "ALTA"
    elif any(a["severidad"] == "MEDIA" for a in alertas):
        severidad_maxima = "MEDIA"
    elif any(a["severidad"] == "BAJA" for a in alertas):
        severidad_maxima = "BAJA"

    return {
        "modulo": "ARGO_ENTRADA",
        "estado": estado,
        "severidad_maxima": severidad_maxima,
        "conteo": {"faltantes": len(faltantes), "alertas": len(alertas)},
        "faltantes": faltantes,
        "alertas": alertas,
        "entrada": entrada,
    }


@app.post("/generar")
def generar_excel(
    shipment_id: str = Form(...),
    cliente: str = Form(...),
    tracking: str = Form(...),
    peso_total: str = Form(...),
    unidad: str = Form(...),
    pais_origen: str = Form(...),
    proveedor: str = Form("No legible"),
    paqueteria: str = Form("No legible"),
    descripcion: str = Form("No legible"),
    marca: str = Form("No legible"),
    modelo: str = Form("No legible"),
    no_parte: str = Form("No legible"),
    no_lote: str = Form("No legible"),
    no_serie: str = Form("No legible"),
    cantidad: str = Form("No legible"),
):
    wb = load_workbook(TEMPLATE_FILE)
    ws = wb["Entrada"]

    fecha_recepcion = datetime.now().strftime("%m/%d/%Y")

    tracking_original = (tracking or "").strip()
    tracking_texto = tracking_original

    if (
        tracking_texto.lower().endswith("e")
        or "e+" in tracking_texto.lower()
        or "e-" in tracking_texto.lower()
    ):
        tracking_texto = "No legible"

    cliente = (cliente or "").strip() or "No legible"
    proveedor = (proveedor or "").strip() or "No legible"

    # =========================
    # HOJA ENTRADA
    # =========================
    ws["B2"] = (shipment_id or "").strip() or "No legible"
    ws["B3"] = fecha_recepcion
    ws["B4"] = cliente
    ws["B5"] = proveedor
    ws["B6"] = (paqueteria or "").strip() or "No legible"

    ws["B7"].number_format = "@"
    ws["B7"] = tracking_texto

    ws["B8"] = (descripcion or "").strip() or "No legible"
    ws["B9"] = (marca or "").strip() or "No legible"
    ws["B10"] = (modelo or "").strip() or "No legible"
    ws["B11"] = (no_parte or "").strip() or "No legible"
    ws["B12"] = (no_lote or "").strip() or "No legible"
    ws["B13"] = (no_serie or "").strip() or "No legible"
    ws["B14"] = (cantidad or "").strip() or "No legible"
    ws["B15"] = (unidad or "").strip() or "No legible"
    ws["B16"] = (peso_total or "").strip() or "No legible"
    ws["B17"] = (pais_origen or "").strip() or "No legible"

    # =========================
    # DATOS FALTANTES
    # =========================
    faltantes = []

    campos = [
        ("Shipment ID", ws["B2"].value),
        ("Fecha recepción", ws["B3"].value),
        ("Cliente", ws["B4"].value),
        ("Proveedor", ws["B5"].value),
        ("Paquetería", ws["B6"].value),
        ("Tracking", ws["B7"].value),
        ("Descripción", ws["B8"].value),
        ("Marca", ws["B9"].value),
        ("Modelo", ws["B10"].value),
        ("No. parte", ws["B11"].value),
        ("No. lote", ws["B12"].value),
        ("No. serie", ws["B13"].value),
        ("Cantidad", ws["B14"].value),
        ("Unidad", ws["B15"].value),
        ("Peso total", ws["B16"].value),
        ("País origen", ws["B17"].value),
    ]

    for campo, valor in campos:
        if str(valor).strip() == "No legible":
            faltantes.append((campo, "No legible"))

    ws_df = wb["Datos faltantes"]
    _clear_sheet(ws_df)

    ws_df["A1"] = "Campo"
    ws_df["B1"] = "Valor"

    fila = 2
    for campo, valor in faltantes:
        ws_df[f"A{fila}"] = campo
        ws_df[f"B{fila}"] = valor
        fila += 1

    # =========================
    # ALERTAS
    # =========================
    alertas = []

    if tracking_texto == "No legible":
        alertas.append(("Tracking inválido", "Formato incorrecto", "ALTA"))

    if ws["B16"].value == "No legible":
        alertas.append(("Peso no legible", "No se detectó peso válido", "MEDIA"))

    if ws["B4"].value == "No legible":
        alertas.append(("Cliente faltante", "No se detectó cliente", "ALTA"))

    if ws["B5"].value == "No legible":
        alertas.append(("Proveedor faltante", "No se detectó proveedor", "MEDIA"))

    ws_al = wb["Alertas"]
    _clear_sheet(ws_al)

    ws_al["A1"] = "Alerta"
    ws_al["B1"] = "Detalle"
    ws_al["C1"] = "Severidad"

    fila = 2
    for a, d, s in alertas:
        ws_al[f"A{fila}"] = a
        ws_al[f"B{fila}"] = d
        ws_al[f"C{fila}"] = s
        fila += 1

    # =========================
    # RESUMEN OPERATIVO
    # =========================
    ws_res = wb["Resumen operativo"]
    _clear_sheet(ws_res)

    ws_res["A1"] = "Item"
    ws_res["B1"] = "Valor"

    resumen = [
        ("Fecha recepción", ws["B3"].value),
        ("Cliente", ws["B4"].value),
        ("Proveedor", ws["B5"].value),
        ("Paquetería", ws["B6"].value),
        ("Tracking", ws["B7"].value),
        ("Shipment ID", ws["B2"].value),
        ("País origen", ws["B17"].value),
        ("Faltantes (#)", len(faltantes)),
        ("Alertas (#)", len(alertas)),
    ]

    hay_alta = any(a[2] == "ALTA" for a in alertas)
    if hay_alta:
        estado = "REVISIÓN"
    elif len(faltantes) > 0 or len(alertas) > 0:
        estado = "ADVERTENCIA"
    else:
        estado = "OK"

    resumen.append(("Estado", estado))

    severidad_max = "NINGUNA"
    if any(a[2] == "ALTA" for a in alertas):
        severidad_max = "ALTA"
    elif any(a[2] == "MEDIA" for a in alertas):
        severidad_max = "MEDIA"
    elif any(a[2] == "BAJA" for a in alertas):
        severidad_max = "BAJA"

    resumen.append(("Severidad máxima", severidad_max))

    fila = 2
    for item, valor in resumen:
        ws_res[f"A{fila}"] = item
        ws_res[f"B{fila}"] = valor
        fila += 1

    # =========================
    # GUARDADO
    # =========================
    cliente_archivo = _safe_filename(cliente)
    ult4 = (tracking_original[-4:] if len(tracking_original) >= 4 else tracking_original) or "XXXX"

    output_name = f"ENTRADA_{cliente_archivo}_{ult4}.xlsx"
    output_path = os.path.join(OUTPUT_FOLDER, output_name)

    wb.save(output_path)

    return FileResponse(output_path, filename=output_name)

   
       
   

   
       

  
   
   
    
  

   
