from fastapi import FastAPI, Form, HTTPException, UploadFile, File, Query
from fastapi.responses import FileResponse

from utils_operacion import generar_id_operacion, escribir_log_operacion
from openpyxl import load_workbook
import os
import re
from datetime import datetime
from argo_control import argo_control_validar_v2

app = FastAPI()

TEMPLATE_FILE = "PLANTILLA_OFICIAL_ARGO_ENTRADA.xlsx"
OUTPUT_FOLDER = "salidas"

ROUTING_V1 = {
    "routing_version": "1.0",
    "rules": [
        {
            "when_estado": "OK",
            "action": "CONTINUAR",
            "next": ["ARGO_CONTROL", "ARGO_CLASS", "ARGO_DOCUMENT"]
        },
        {
            "when_estado": "ADVERTENCIA",
            "action": "CONTINUAR_CON_BANDERA",
            "flag": "REQUIERE_CONFIRMACION",
            "next": ["ARGO_CONTROL", "ARGO_CLASS", "ARGO_DOCUMENT"]
        },
        {
            "when_estado": "REVISION",
            "action": "DETENER",
            "next": [],
            "reason": "DATOS_INSUFICIENTES_O_RIESGO_ALTO"
        }
    ]
}
def argo_control_validar(payload_entrada: dict) -> dict:
    return {
        "version": "1.0",
        "modulo": "ARGO_CONTROL",
        "estado": "OK",
        "severidad_maxima": "NINGUNA",
        "conteo": {"alertas": 0},
        "alertas": [],
        "entrada_ref": {
            "version": payload_entrada.get("version"),
            "modulo": payload_entrada.get("modulo"),
            "shipment_id": payload_entrada.get("entrada", {}).get("shipment_id"),
            "tracking": payload_entrada.get("entrada", {}).get("tracking")
        }
    }
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
            raise HTTPException(status_code=422, detail=f"Campo requerido inválido: {name}")
        return s

    shipment_id = _clean_required("shipment_id", shipment_id)
    cliente = _clean_required("cliente", cliente)
    tracking = _clean_required("tracking", tracking)
    peso_total = _clean_required("peso_total", peso_total)
    unidad = _clean_required("unidad", unidad)
    pais_origen = _clean_required("pais_origen", pais_origen)

    fecha_recepcion = datetime.now().strftime("%m/%d/%Y")

    entrada = {
        "shipment_id": shipment_id,
        "fecha_recepcion": fecha_recepcion,
        "cliente": cliente,
        "proveedor": proveedor,
        "paqueteria": paqueteria,
        "tracking": tracking,
        "descripcion": descripcion,
        "marca": marca,
        "modelo": modelo,
        "no_parte": no_parte,
        "no_lote": no_lote,
        "no_serie": no_serie,
        "cantidad": cantidad,
        "unidad": unidad,
        "peso_total": peso_total,
        "pais_origen": pais_origen
    }

    faltantes = []
    alertas = []

    for campo, valor in entrada.items():
        if valor == "No legible":
            faltantes.append({"campo": campo, "valor": valor})

    # Estado base
    estado = "OK"
    severidad_maxima = "NINGUNA"

    if len(faltantes) > 0:
        estado = "ADVERTENCIA"
        severidad_maxima = "MEDIA"

    control_stub = argo_control_validar({
        "version": "1.0",
        "modulo": "ARGO_ENTRADA",
        "entrada": entrada
    })

    return {
        "version": "1.0",
        "modulo": "ARGO_ENTRADA",
        "estado": estado,
        "severidad_maxima": severidad_maxima,
        "conteo": {"faltantes": len(faltantes), "alertas": len(alertas)},
        "faltantes": faltantes,
        "alertas": alertas,
        "entrada": entrada,
        "control": control_stub
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

@app.post("/argo-control")
async def ejecutar_argo_control(
    archivo_entrada: UploadFile = File(...),
    plantilla_control: UploadFile = File(...),
    modo: str = Form("excel")
):
    # Guardar archivos temporalmente
    entrada_path = f"temp_{archivo_entrada.filename}"
    control_path = f"temp_{plantilla_control.filename}"

    with open(entrada_path, "wb") as f:
        f.write(await archivo_entrada.read())

    with open(control_path, "wb") as f:
        f.write(await plantilla_control.read())

    # Ejecutar validación
    output_path, icono, estatus = argo_control_validar_v2(
        entrada_path,
        control_path
    )

    # Modo JSON (para ARGO CLASS)
    if modo.lower() == "json":

            from argo_control import extraer_resumen_control_desde_excel

            resumen = extraer_resumen_control_desde_excel(output_path)

            return {
            "ok": True,
            "modulo": "ARGO_CONTROL",
            "estatus": estatus,
            "icono": icono,
            "output_path": output_path,
            "resumen": resumen
        }

    # Mantener Excel
    return FileResponse(
        path=output_path,
        filename=output_path.split("/")[-1],
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
from pydantic import BaseModel
from typing import Any, Dict
from argo_class_engine import build_output

class ArgoClassRequest(BaseModel):
    payload: Dict[str, Any]

@app.post("/argo/class/v2026/clasificar")
def argo_class_clasificar(req: ArgoClassRequest):

    payload = req.payload

    # Si viene información de ARGO CONTROL, agregarla al payload
    if "control" in payload:
        payload["argo_control"] = payload["control"]

    return build_output(payload)

from fastapi import UploadFile, File, Form, HTTPException
import os

# IMPORTS necesarios (ajusta si tus rutas/nombres son distintos)
from argo_control import argo_control_validar_v2, extraer_resumen_control_desde_excel
from argo_class_engine import build_output


@app.post("/argo/pipeline/clasificar")
async def argo_pipeline_clasificar(
    archivo_entrada: UploadFile = File(...),
    plantilla_control: UploadFile = File(...),
    descripcion: str = Form(""),
):
    id_operacion = generar_id_operacion()
    print(f"ID_OPERACION_PIPELINE: {id_operacion}")

    entrada_path = None
    control_path = None

    try:
        # 1) Guardar archivos temporales
        entrada_path = f"temp_{archivo_entrada.filename}"
        control_path = f"temp_{plantilla_control.filename}"

        with open(entrada_path, "wb") as f:
            f.write(await archivo_entrada.read())

        with open(control_path, "wb") as f:
            f.write(await plantilla_control.read())

        # 2) Ejecutar ARGO CONTROL (con trazabilidad)
        output_path, icono, estatus = argo_control_validar_v2(
            entrada_path,
            control_path,
            id_operacion=id_operacion
        )

        resumen = extraer_resumen_control_desde_excel(output_path)

        control_json = {
            "ok": True,
            "modulo": "ARGO_CONTROL",
            "id_operacion": id_operacion,
            "estatus": estatus,
            "icono": icono,
            "output_path": output_path,
            "resumen": resumen
        }

        # 3) Payload para ARGO CLASS (con id_operacion)
        payload_master = {
            "meta": {
                "id_operacion": id_operacion,
                "id_shipment": None,
                "id_item": None,
            },
            "descripcion": descripcion or "",
            "control": {"resumen": resumen},
            "argo_control": control_json
        }

        # 4) Ejecutar ARGO CLASS
        salida_class = build_output(payload_master)

        # 5) Log JSON por operación (no rompe si falla)
        try:
            escribir_log_operacion(
                id_operacion=id_operacion,
                payload={
                    "modulo": "ARGO_PIPELINE",
                    "inputs": {
                        "entrada_path": entrada_path,
                        "plantilla_control_path": control_path,
                        "descripcion": descripcion or "",
                    },
                    "outputs": {
                        "control_output_path": output_path,
                        "class_output_path": (salida_class.get("output_path") if isinstance(salida_class, dict) else None),
                    },
                    "control": {
                        "estatus": estatus,
                        "icono": icono,
                        "resumen": resumen,
                    },
                    "class": salida_class,
                },
                logs_dir="logs",
            )
        except Exception as log_err:
            print(f"WARNING LOG {id_operacion}: {log_err}")

        # 6) Respuesta unificada
        return {
            "ok": True,
            "modulo": "ARGO_PIPELINE",
            "id_operacion": id_operacion,
            "control": control_json,
            "class": salida_class
        }

    except Exception as e:
        # AHORA sí: el error real se ve en /docs
        raise HTTPException(status_code=500, detail=f"ARGO_PIPELINE error: {str(e)}")

    finally:
        # Limpieza segura (no revienta aunque algo falle arriba)
        try:
            if entrada_path and os.path.exists(entrada_path):
                os.remove(entrada_path)
            if control_path and os.path.exists(control_path):
                os.remove(control_path)
        except Exception as cleanup_err:
            print(f"WARNING CLEANUP {id_operacion}: {cleanup_err}")

   
       

  
   
   
    
  

   
