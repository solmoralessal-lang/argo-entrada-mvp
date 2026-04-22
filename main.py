from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI, Form, HTTPException, UploadFile, File, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import hmac
import os
import base64
import hashlib
from typing import Optional, List
from fastapi.responses import JSONResponse
import requests

from argo_document import argo_document_bloque1, salida_to_dict
from argo_master import build_master_output
from argo_history import save_pipeline_to_history, read_history
from argo_dashboard import build_dashboard_output
from utils_operacion import generar_id_operacion, escribir_log_operacion
from openpyxl import load_workbook
import re
from datetime import datetime
from argo_control import argo_control_validar_v2
from argo_historial import (
    normalizar_operacion_para_historial,
    guardar_operacion_historial,
    obtener_dashboard_desde_historial,
    obtener_clientes_desde_historial,
    obtener_historial,
    aprobar_operacion as aprobar_operacion_hist
)

from argo_models import AprobarOperacionRequest
from argo_supabase_historial import aprobar_operacion_supabase

app = FastAPI()

# 🔷 Primero crear carpeta
if not os.path.exists("outputs"):
    os.makedirs("outputs")

# 🔷 Luego exponerla
app.mount("/outputs", StaticFiles(directory="outputs"), name="outputs")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def supabase_config_ok() -> bool:
    print("DEBUG_SUPABASE_URL_PRESENTE:", bool(SUPABASE_URL))
    print("DEBUG_SUPABASE_SERVICE_KEY_PRESENTE:", bool(SUPABASE_SERVICE_KEY))
    return bool(SUPABASE_URL and SUPABASE_SERVICE_KEY)

def _headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json"
    }

SUPABASE_BUCKET = "argo-files"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi import Request
from fastapi.responses import JSONResponse

from argo_supabase_historial import (
    guardar_operacion_supabase,
    obtener_dashboard_supabase,
    aprobar_operacion_supabase,
    obtener_clientes_supabase,
)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "error_type": type(exc).__name__,
            "detail": str(exc),
            "path": str(request.url.path),
        },
    )

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


@app.get("/health")
def health():
    return {"mensaje": "ARGO ENTRADA MVP funcionando"}

@app.get("/descargar/{nombre_archivo}")
def descargar_archivo(nombre_archivo: str):
    ruta_salidas = os.path.join("salidas", nombre_archivo)
    ruta_outputs = os.path.join("outputs", nombre_archivo)

    if os.path.exists(ruta_salidas):
        ruta = ruta_salidas
    elif os.path.exists(ruta_outputs):
        ruta = ruta_outputs
    else:
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    return FileResponse(
        path=ruta,
        filename=nombre_archivo,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    
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

def subir_archivo_a_supabase(file_path: str, bucket: str = SUPABASE_BUCKET) -> str:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise Exception("Faltan variables de entorno de Supabase")

    if not os.path.exists(file_path):
        raise Exception(f"Archivo no existe: {file_path}")

    file_name = os.path.basename(file_path)
    upload_url = f"{SUPABASE_URL}/storage/v1/object/{bucket}/{file_name}"

    with open(file_path, "rb") as f:
        response = requests.post(
            upload_url,
            headers={
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "apikey": SUPABASE_SERVICE_KEY,
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "x-upsert": "true",
            },
            data=f.read(),
        )

    if response.status_code not in [200, 201]:
        raise Exception(f"Error subiendo a Supabase: {response.text}")

    public_url = f"{SUPABASE_URL}/storage/v1/object/public/{bucket}/{file_name}"
    return public_url

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
    id_operacion = None
    entrada_path = None
    control_path = None

    try:
        # ID único end-to-end (AHORA sí dentro del try)
        id_operacion = generar_id_operacion()
        print(f"ID_OPERACION_PIPELINE: {id_operacion}")

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

        # Paths estándar + compatibilidad (evita NameError)
        "entrada_path": entrada_path,
        "control_path": control_path,
        "archivo_entrada_path": entrada_path,
        "plantilla_control_path": control_path,

        # Resumen de CONTROL para influir certeza
        "control": {"resumen": resumen},

        # Objeto completo de CONTROL por si se ocupa
        "argo_control": control_json
    }

        # 4) Ejecutar ARGO CLASS
        salida_class = build_output(payload_master)

        # 5) Log JSON por operación (si falla NO rompe)
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

  
                       # 6) ARGO DOCUMENT
        salida_document = argo_document_bloque1(
            input_xlsx_path=output_path,
            plantilla_path="PLANTILLA_OFICIAL_ARGO_DOCUMENT_MEJORADA_v2026.xlsx",
            outputs_dir="outputs",
            id_operacion=id_operacion,
        )
        document_json = salida_to_dict(salida_document)

        master_json = build_master_output({
            "id_operacion": id_operacion,
            "control": control_json,
            "class": salida_class,
            "document": document_json
        })

        control_public_url = ""
        document_public_url = ""

        try:
            if output_path:
                control_public_url = subir_archivo_a_supabase(output_path)

            if document_json.get("output_path"):
                document_public_url = subir_archivo_a_supabase(document_json["output_path"])

        except Exception as e:
            print("ERROR SUBIDA:", str(e))


# 🔥 ahora sí ya existen las URLs
        master_json = build_master_output(...)

        if "descargas" not in master_json:
            master_json["descargas"] = {}

        master_json["descargas"]["control_url"] = control_public_url
        master_json["descargas"]["document_url"] = document_public_url

        try:
            if output_path:
                control_public_url = subir_archivo_a_supabase(output_path)
        except Exception as supa_err:
            print(f"WARNING SUPABASE CONTROL [{id_operacion}]: {supa_err}")

        try:
            if document_json.get("output_path"):
                document_public_url = subir_archivo_a_supabase(document_json["output_path"])
        except Exception as supa_err:
            print(f"WARNING SUPABASE DOCUMENT [{id_operacion}]: {supa_err}")

        pipeline_result = {
            "ok": True,
            "modulo": "ARGO_PIPELINE",
            "id_operacion": id_operacion,
            "control": control_json,
            "class": salida_class,
            "document": document_json,
            "archivos_publicos": {
                "control_url": control_public_url,
                "document_url": document_public_url,
            }
        }

        master_json = build_master_output(pipeline_result)
        pipeline_result["master"] = master_json

        history_save_path = ""
        history_save_error = ""

        # asegurar que archivos_publicos exista en pipeline_result
        if "archivos_publicos" not in pipeline_result:
            pipeline_result["archivos_publicos"] = {}

        # ===== HISTORIAL ANTIGUO DESACTIVADO =====
        # try:
        #     history_save_path = save_pipeline_to_history(pipeline_result, logs_dir="logs")
        #     print(f"HISTORY OK [{id_operacion}] -> {history_save_path}")
        # except Exception as history_err:
        #     history_save_error = str(history_err)
        #     print(f"WARNING HISTORY [{id_operacion}]: {history_save_error}")
        #
        # pipeline_result["history_debug"] = {
        #     "saved": history_save_error == "",
        #     "path": history_save_path,
        #     "error": history_save_error,
        # }

        print("DEBUG PIPELINE_RESULT KEYS:", list(pipeline_result.keys()) if isinstance(pipeline_result, dict) else type(pipeline_result))
        print("DEBUG MASTER:", pipeline_result.get("master") if isinstance(pipeline_result, dict) else None)
        print("DEBUG OPERACIONES:", pipeline_result.get("operaciones") if isinstance(pipeline_result, dict) else None)
        
        # ===== GUARDAR EN HISTORIAL =====
        try:
            print("DEBUG: entrando a guardado de historial")
            
            cliente_id = "cliente_bodega_2"
            cliente_nombre = "Bodega El Güero"

            registro_historial = normalizar_operacion_para_historial(
                pipeline_output=pipeline_result,
                cliente_id=cliente_id,
                cliente_nombre=cliente_nombre
            )

            print("DEBUG REGISTRO:", registro_historial)

            if registro_historial.get("id_operacion"):
                guardar_operacion_supabase(registro_historial)

        except Exception as e:
            print("ERROR guardando historial:", str(e))

        return pipeline_result        

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ARGO_PIPELINE error: {str(e)}")

    finally:
        try:
            if entrada_path and os.path.exists(entrada_path):
                os.remove(entrada_path)
            if control_path and os.path.exists(control_path):
                os.remove(control_path)
        except Exception as cleanup_err:
            print(f"WARNING CLEANUP {id_operacion}: {cleanup_err}")

@app.post("/argo-document")
async def ejecutar_argo_document(
    archivo_xlsx: UploadFile = File(...),
    id_operacion: Optional[str] = Form(None),
):
    temp_input_path = f"temp_{archivo_xlsx.filename}"

    with open(temp_input_path, "wb") as f:
        f.write(await archivo_xlsx.read())

    plantilla_path = "PLANTILLA_OFICIAL_ARGO_DOCUMENT_MEJORADA_v2026.xlsx"

    salida = argo_document_bloque1(
        input_xlsx_path=temp_input_path,
        plantilla_path=plantilla_path,
        outputs_dir="outputs",
        id_operacion=id_operacion,
    )

    try:
        os.remove(temp_input_path)
    except Exception:
        pass

    return JSONResponse(content=salida_to_dict(salida))
       
@app.get("/argo/history")
async def consultar_historial_argo(limit: int = 50):
    data = read_history(limit=limit, logs_dir="logs")
    return {
        "ok": True,
        "modulo": "ARGO_HISTORY",
        "total": len(data),
        "items": data
    }

@app.get("/argo/dashboard")
async def consultar_dashboard_argo(cliente_id: str | None = None):
    dashboard = obtener_dashboard_supabase(cliente_id)
    return dashboard

# =========================================================
# ENDPOINTS HISTORIAL / DASHBOARD / APROBACION
# =========================================================

@app.post("/argo/operacion/aprobar")
async def aprobar_operacion(payload: AprobarOperacionRequest):
    try:
        resultado = aprobar_operacion_supabase(
            id_operacion=payload.id_operacion,
            aprobada_por=payload.aprobada_por
        )

        return {
            "ok": True,
            "mensaje": "Operación aprobada correctamente",
            "data": resultado
        }

    except Exception as e:
        return {
            "ok": False,
            "error": str(e)
        }
# =========================================
# LOGIN USUARIO
# =========================================
# =========================================
# PASSWORD SEGURA
# =========================================

PBKDF2_ITERATIONS = 100_000


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS
    )
    salt_b64 = base64.b64encode(salt).decode("utf-8")
    hash_b64 = base64.b64encode(dk).decode("utf-8")
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt_b64}${hash_b64}"


def verify_password(password_plano: str, password_guardado: str) -> bool:
    if not password_guardado:
        return False

    # Compatibilidad temporal con passwords viejos en texto plano
    if not password_guardado.startswith("pbkdf2_sha256$"):
        return hmac.compare_digest(password_plano, password_guardado)

    try:
        algoritmo, iteraciones_str, salt_b64, hash_b64 = password_guardado.split("$", 3)
        iteraciones = int(iteraciones_str)
        salt = base64.b64decode(salt_b64.encode("utf-8"))
        hash_real = base64.b64decode(hash_b64.encode("utf-8"))

        hash_test = hashlib.pbkdf2_hmac(
            "sha256",
            password_plano.encode("utf-8"),
            salt,
            iteraciones
        )

        return hmac.compare_digest(hash_test, hash_real)

    except Exception:
        return False

from fastapi import Body

@app.post("/argo/login")
async def login_usuario(payload: dict = Body(...)):
    
    email = payload.get("email")
    password = payload.get("password")

    if not email or not password:
        return {
            "ok": False,
            "error": "Faltan credenciales"
        }

    if not supabase_config_ok():
        return {
            "ok": False,
            "error": "Supabase no configurado"
        }

    url = f"{SUPABASE_URL}/rest/v1/argo_usuarios?email=eq.{email}&select=*"

    response = requests.get(url, headers=_headers())

    if response.status_code != 200:
        return {
            "ok": False,
            "error": "Error en consulta"
        }

    data = response.json()

    if not data:
        return {
            "ok": False,
            "error": "Credenciales inválidas"
        }

    user = data[0]

    if not verify_password(password, user.get("password", "")):
        return {
            "ok": False,
            "error": "Credenciales inválidas"
        }

    return {
        "ok": True,
        "usuario": {
            "email": user["email"],
            "nombre": user["nombre"],
            "id_cliente": user["id_cliente"],
            "rol": user["rol"]
        }
    }
@app.get("/argo/historial")
async def endpoint_historial(cliente_id: str = Query(default=None)):
    return obtener_historial(cliente_id)


@app.get("/argo/clientes")
async def endpoint_clientes():
    return obtener_clientes_supabase()


@app.get("/argo/dashboard")
async def endpoint_dashboard(cliente_id: str = Query(default=None)):
    return obtener_dashboard_desde_historial(cliente_id)
    
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def convertir_a_base64(file_bytes):
    return base64.b64encode(file_bytes).decode("utf-8")


@app.get("/argo/historial")
async def endpoint_historial(cliente_id: str = Query(default=None)):
    return obtener_historial(cliente_id)


@app.get("/argo/clientes")
async def endpoint_clientes():
    return obtener_clientes_supabase()


@app.get("/argo/dashboard")
async def endpoint_dashboard(cliente_id: str = Query(default=None)):
    return obtener_dashboard_desde_historial(cliente_id)

@app.post("/argo/ocr")
async def argo_ocr(
    archivo1: UploadFile = File(None),
    archivo2: UploadFile = File(None),
    archivo3: UploadFile = File(None),
    archivo4: UploadFile = File(None),
    archivo5: UploadFile = File(None),
):
    import json

    archivos = [archivo1, archivo2, archivo3, archivo4, archivo5]
    archivos_validos = [a for a in archivos if a is not None]

    if not archivos_validos:
        return {
            "ok": False,
            "error": "No se recibieron archivos"
        }

    resultados = []
    errores = []

    # =========================
    # OCR POR ARCHIVO
    # =========================
    for file in archivos_validos:
        try:
            contenido = await file.read()
            imagen_base64 = convertir_a_base64(contenido)

            response = client.responses.create(
                model="gpt-5.4",
                input=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "input_text",
                                "text": """
Eres un sistema OCR experto en logística.

Extrae SOLO en formato JSON:

{
  "cliente": null,
  "proveedor": null,
  "paqueteria": null,
  "tracking": null,
  "descripcion": null,
  "cantidad_bultos": null,
  "peso_total": null,
  "peso_unidad": null,
  "direccion_origen": null,
  "direccion_destino": null
}

Reglas:
- No inventar datos
- Si no se ve -> null
- tracking = número principal
- paqueteria = FedEx, UPS, DHL, etc
- responde solo JSON válido
"""
                            },
                            {
                                "type": "input_image",
                                "image_url": f"data:image/jpeg;base64,{imagen_base64}"
                            }
                        ]
                    }
                ]
            )

            texto = response.output_text.strip()

            try:
                ocr_json = json.loads(texto)
            except Exception:
                ocr_json = {
                    "cliente": None,
                    "proveedor": None,
                    "paqueteria": None,
                    "tracking": None,
                    "descripcion": None,
                    "cantidad_bultos": None,
                    "peso_total": None,
                    "peso_unidad": None,
                    "direccion_origen": None,
                    "direccion_destino": None
                }

            resultados.append({
                "archivo": file.filename,
                "ocr_raw": texto,
                "ocr_json": ocr_json
            })

        except Exception as e:
            errores.append({
                "archivo": file.filename if file else None,
                "error": str(e)
            })

    # =========================
    # CONSOLIDADO INTELIGENTE
    # =========================
    consolidado = {
        "cliente": None,
        "proveedor": None,
        "paqueteria": None,
        "tracking": None,
        "descripcion": None,
        "cantidad_bultos": None,
        "peso_total": None,
        "peso_unidad": None,
        "direccion_origen": None,
        "direccion_destino": None
    }

    for item in resultados:
        data = item.get("ocr_json", {})
        nombre_archivo = item.get("archivo", "").lower()

        prioridad_cliente_proveedor = 1
        prioridad_tracking_paqueteria = 1

        if "invoice" in nombre_archivo or "packing" in nombre_archivo:
            prioridad_cliente_proveedor = 3

        if "paqueteria" in nombre_archivo or "label" in nombre_archivo:
            prioridad_tracking_paqueteria = 3

        for campo in consolidado.keys():
            valor_actual = consolidado[campo]
            valor_nuevo = data.get(campo)

            if valor_nuevo in [None, "", "null"]:
                continue

            if campo in ["cliente", "proveedor"]:
                if prioridad_cliente_proveedor >= 3:
                    consolidado[campo] = valor_nuevo
                elif consolidado[campo] in [None, "", "null"]:
                    consolidado[campo] = valor_nuevo

            elif campo in ["tracking", "paqueteria"]:
                if prioridad_tracking_paqueteria >= 3:
                    consolidado[campo] = valor_nuevo
                elif consolidado[campo] in [None, "", "null"]:
                    consolidado[campo] = valor_nuevo

            elif campo == "cantidad_bultos":
                if isinstance(valor_nuevo, str) and "OF" in valor_nuevo.upper():
                    try:
                        total = int(valor_nuevo.upper().split("OF")[1].strip())
                        consolidado[campo] = total
                    except Exception:
                        pass
                elif consolidado[campo] is None:
                    consolidado[campo] = valor_nuevo

            elif campo == "peso_total":
                if consolidado["peso_total"] in [None, "", "null"]:
                    if isinstance(valor_nuevo, str):
                        numeros = "".join(filter(str.isdigit, valor_nuevo))
                        consolidado["peso_total"] = int(numeros) if numeros else valor_nuevo
                    else:
                        consolidado["peso_total"] = valor_nuevo

            elif campo == "peso_unidad":
                texto_peso = str(valor_nuevo).upper()
                if "LBS" in texto_peso or "LB" in texto_peso:
                    consolidado["peso_unidad"] = "LBS"
                elif "KGS" in texto_peso or "KG" in texto_peso:
                    consolidado["peso_unidad"] = "KGS"

            elif campo == "descripcion":
                if not valor_actual or len(str(valor_nuevo)) > len(str(valor_actual)):
                    consolidado[campo] = valor_nuevo

            elif consolidado[campo] in [None, "", "null"]:
                consolidado[campo] = valor_nuevo

    # =========================
    # REFUERZO FINAL PESO
    # =========================
    if not consolidado.get("peso_unidad") or not consolidado.get("peso_total"):
        for item in resultados:
            data = item.get("ocr_json", {})
            for val in data.values():
                texto = str(val).upper()

                if "LBS" in texto or "LB" in texto:
                    numeros = "".join(filter(str.isdigit, texto))
                    if numeros and not consolidado.get("peso_total"):
                        consolidado["peso_total"] = int(numeros)
                    if not consolidado.get("peso_unidad"):
                        consolidado["peso_unidad"] = "LBS"

                elif "KGS" in texto or "KG" in texto:
                    numeros = "".join(filter(str.isdigit, texto))
                    if numeros and not consolidado.get("peso_total"):
                        consolidado["peso_total"] = int(numeros)
                    if not consolidado.get("peso_unidad"):
                        consolidado["peso_unidad"] = "KGS"

    # =========================
    # ESTADO / FALTANTES / ALERTAS
    # =========================
    faltantes = []
    alertas = []

    campos_requeridos = [
        "cliente",
        "proveedor",
        "paqueteria",
        "tracking",
        "descripcion",
        "cantidad_bultos",
        "peso_total",
        "peso_unidad",
    ]

    for campo in campos_requeridos:
        valor = consolidado.get(campo)
        if valor in [None, "", "null"]:
            faltantes.append({
                "campo": campo,
                "valor": "No legible"
            })

    estado = "OK"
    severidad_maxima = "NINGUNA"

    if len(faltantes) > 0:
        estado = "ADVERTENCIA"
        severidad_maxima = "MEDIA"

    return {
        "ok": len(resultados) > 0,
        "modulo": "ARGO_OCR",
        "estado": estado,
        "severidad_maxima": severidad_maxima,
        "conteo": {
            "faltantes": len(faltantes),
            "alertas": len(alertas)
        },
        "faltantes": faltantes,
        "alertas": alertas,
        "total_archivos": len(archivos_validos),
        "procesados": len(resultados),
        "errores": errores,
        "consolidado": consolidado,
        "resultados": resultados
    }

@app.post("/argo/generar_desde_ocr")
async def argo_generar_desde_ocr(payload: dict = Body(...)):
    ocr = payload.get("ocr") or payload

    cliente = ocr.get("cliente") or "No legible"
    proveedor = ocr.get("proveedor") or "No legible"
    paqueteria = ocr.get("paqueteria") or "No legible"
    tracking = ocr.get("tracking") or "No legible"
    descripcion = ocr.get("descripcion") or "No legible"
    cantidad_bultos = ocr.get("cantidad_bultos")
    peso_total = ocr.get("peso_total")
    peso_unidad = ocr.get("peso_unidad") or "No legible"
    direccion_origen = ocr.get("direccion_origen") or "No legible"
    direccion_destino = ocr.get("direccion_destino") or "No legible"

    # =========================
    # NORMALIZACIÓN
    # =========================
    peso_unidad_txt = str(peso_unidad).strip().upper() if peso_unidad not in [None, "", "null"] else "NO LEGIBLE"
    if "LBS" in peso_unidad_txt or peso_unidad_txt == "LB":
        peso_unidad_norm = "LBS"
    elif "KGS" in peso_unidad_txt or peso_unidad_txt == "KG":
        peso_unidad_norm = "KGS"
    else:
        peso_unidad_norm = peso_unidad if peso_unidad not in [None, "", "null"] else "No legible"

    shipment_id = tracking if tracking != "No legible" else f"OCR-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    fecha_recepcion = datetime.now().strftime("%m/%d/%Y")

    cantidad = str(cantidad_bultos) if cantidad_bultos not in [None, "", "null"] else "No legible"
    peso_total_str = str(peso_total) if peso_total not in [None, "", "null"] else "No legible"

    entrada = {
        "shipment_id": shipment_id,
        "fecha_recepcion": fecha_recepcion,
        "cliente": cliente,
        "proveedor": proveedor,
        "paqueteria": paqueteria,
        "tracking": tracking,
        "descripcion": descripcion,
        "marca": "No legible",
        "modelo": "No legible",
        "no_parte": "No legible",
        "no_lote": "No legible",
        "no_serie": "No legible",
        "cantidad": cantidad,
        "unidad": peso_unidad_norm,
        "peso_total": peso_total_str,
        "pais_origen": "No legible",
        "direccion_origen": direccion_origen,
        "direccion_destino": direccion_destino
    }

    faltantes = []
    alertas = []

    for campo, valor in entrada.items():
        if valor == "No legible":
            faltantes.append({
                "campo": campo,
                "valor": valor
            })

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

    wb = load_workbook(TEMPLATE_FILE)
    ws = wb["Entrada"]

    tracking_original = (tracking or "").strip()
    tracking_texto = tracking_original

    if (
        tracking_texto.lower().endswith("e")
        or "e+" in tracking_texto.lower()
        or "e-" in tracking_texto.lower()
    ):
        tracking_texto = "No legible"

    ws["B2"] = shipment_id
    ws["B3"] = fecha_recepcion
    ws["B4"] = cliente
    ws["B5"] = proveedor
    ws["B6"] = paqueteria

    ws["B7"].number_format = "@"
    ws["B7"] = tracking_texto

    ws["B8"] = descripcion
    ws["B9"] = entrada["marca"]
    ws["B10"] = entrada["modelo"]
    ws["B11"] = entrada["no_parte"]
    ws["B12"] = entrada["no_lote"]
    ws["B13"] = entrada["no_serie"]
    ws["B14"] = cantidad
    ws["B15"] = peso_unidad_norm
    ws["B16"] = peso_total_str
    ws["B17"] = entrada["pais_origen"]

    ws_df = wb["Datos faltantes"]
    _clear_sheet(ws_df)
    ws_df["A1"] = "Campo"
    ws_df["B1"] = "Valor"

    fila = 2
    for item in faltantes:
        ws_df[f"A{fila}"] = item["campo"]
        ws_df[f"B{fila}"] = item["valor"]
        fila += 1

    ws_al = wb["Alertas"]
    _clear_sheet(ws_al)
    ws_al["A1"] = "Alerta"
    ws_al["B1"] = "Detalle"
    ws_al["C1"] = "Severidad"

    fila = 2
    for alerta in alertas:
        ws_al[f"A{fila}"] = alerta.get("alerta", "")
        ws_al[f"B{fila}"] = alerta.get("detalle", "")
        ws_al[f"C{fila}"] = alerta.get("severidad", "")
        fila += 1

    ws_res = wb["Resumen operativo"]
    _clear_sheet(ws_res)
    ws_res["A1"] = "Item"
    ws_res["B1"] = "Valor"

    resumen = [
        ("Fecha recepción", fecha_recepcion),
        ("Cliente", cliente),
        ("Proveedor", proveedor),
        ("Paquetería", paqueteria),
        ("Tracking", tracking_texto),
        ("Shipment ID", shipment_id),
        ("País origen", entrada["pais_origen"]),
        ("Faltantes (#)", len(faltantes)),
        ("Alertas (#)", len(alertas)),
        ("Estado", estado),
        ("Severidad máxima", severidad_maxima),
    ]

    fila = 2
    for item, valor in resumen:
        ws_res[f"A{fila}"] = item
        ws_res[f"B{fila}"] = valor
        fila += 1

    cliente_archivo = _safe_filename(cliente)
    ult4 = (tracking_original[-4:] if len(tracking_original) >= 4 else tracking_original) or "XXXX"

    output_name = f"ENTRADA_OCR_{cliente_archivo}_{ult4}.xlsx"
    output_path = os.path.join(OUTPUT_FOLDER, output_name)

    wb.save(output_path)

    return {
        "ok": True,
        "version": "1.0",
        "modulo": "ARGO_GENERAR_DESDE_OCR",
        "estado": estado,
        "severidad_maxima": severidad_maxima,
        "conteo": {
            "faltantes": len(faltantes),
            "alertas": len(alertas)
        },
        "faltantes": faltantes,
        "alertas": alertas,
        "ocr_consolidado": ocr,
        "entrada": entrada,
        "control": control_stub,
        "archivo_generado": output_name,
        "ruta_archivo": output_path,
        "descarga": f"/descargar/{output_name}"
    }

app.mount("/", StaticFiles(directory="dist", html=True), name="frontend")