from fastapi import FastAPI, Form
from fastapi.responses import FileResponse
from openpyxl import load_workbook
import os

app = FastAPI()

TEMPLATE_FILE = "PLANTILLA_OFICIAL_ARGO_ENTRADA.xlsx"
OUTPUT_FOLDER = "salidas"

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

@app.get("/")
def home():
    return {"mensaje": "ARGO ENTRADA MVP funcionando"}

@app.post("/generar")
def generar_excel(
    shipment_id: str = Form(...),
    cliente: str = Form(...),
    tracking: str = Form(...),
    peso_total: str = Form(...),
    unidad: str = Form(...),
    pais_origen: str = Form(...)
):
    wb = load_workbook(TEMPLATE_FILE)
    ws = wb.active

    ws["B2"] = shipment_id
    ws["B4"] = cliente
    ws["B7"] = str(tracking)
    ws["B7"].number_format = "@"
    ws["B16"] = peso_total
    ws["B15"] = unidad
    ws["B17"] = pais_origen

    output_path = os.path.join(OUTPUT_FOLDER, f"ENTRADA_{tracking}.xlsx")
    wb.save(output_path)

    return FileResponse(output_path, filename=f"ENTRADA_{tracking}.xlsx")
