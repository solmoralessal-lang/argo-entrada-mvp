from openpyxl import load_workbook
from datetime import datetime
import os


def argo_control_validar_v2(archivo_entrada_path, plantilla_control_path):
    if len(args) != 2:
        raise ValueError("argo_control_validar requiere 2 argumentos: archivo_entrada_path, plantilla_control_path")

    archivo_entrada_path, plantilla_control_path = args

    # Cargar archivos
    wb_entrada = load_workbook(archivo_entrada_path)
    wb_control = load_workbook(plantilla_control_path)

    ws_entrada = wb_entrada.active
    ws_control = wb_control["Control"]

    errores = 0
    no_verificables = 0

    fila = 2  # Asumimos que fila 1 es encabezado

    while ws_control[f"A{fila}"].value:

        campo = ws_control[f"A{fila}"].value

        # Buscar valor en archivo entrada
        valor_detectado = None

        for row in ws_entrada.iter_rows(values_only=True):
            if campo in str(row):
                valor_detectado = campo

        if valor_detectado:
            ws_control[f"B{fila}"] = valor_detectado
            ws_control[f"C{fila}"] = "OK"
        else:
            ws_control[f"B{fila}"] = "No verificable"
            ws_control[f"C{fila}"] = "ERROR"
            ws_control[f"D{fila}"] = "Campo no encontrado en ARGO ENTRADA"
            errores += 1

        fila += 1

    # Determinar estatus general
    if errores > 0:
        estatus = "RECHAZADO"
        icono = "🔴"
    elif no_verificables > 0:
        estatus = "CON_OBSERVACIONES"
        icono = "🟡"
    else:
        estatus = "APROBADO"
        icono = "🟢"

    fecha = datetime.now().strftime("%m%d%Y")
    nombre_salida = f"ARGO_CONTROL_{estatus}_{fecha}.xlsx"

    output_path = os.path.join("outputs", nombre_salida)
    os.makedirs("outputs", exist_ok=True)

    wb_control.save(output_path)

    return output_path, icono, estatus
