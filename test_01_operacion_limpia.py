from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict

import requests


PIPELINE_URL = "http://127.0.0.1:8000/argo/pipeline/clasificar"
ARCHIVO_ENTRADA = "tests_data/entrada_limpia.xlsx"
PLANTILLA_CONTROL = "tests_data/PLANTILLA_CONTROL.xlsx"
DESCRIPCION = "Refacciones para carros de golf (kit de mantenimiento), con soporte documental completo."


def fail(msg: str, payload: Dict[str, Any] | None = None) -> None:
    print(f"\n❌ PRUEBA FALLIDA: {msg}")
    if payload is not None:
        print("\n--- RESPUESTA RECIBIDA ---")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"✅ {msg}")


def assert_true(condition: bool, msg: str, payload: Dict[str, Any] | None = None) -> None:
    if not condition:
        fail(msg, payload)


def main() -> None:
    print("=====================================================")
    print("PRUEBA 01 - OPERACIÓN LIMPIA / PIPELINE END-TO-END")
    print("=====================================================")

    assert_true(os.path.exists(ARCHIVO_ENTRADA), f"No existe archivo de entrada: {ARCHIVO_ENTRADA}")
    assert_true(os.path.exists(PLANTILLA_CONTROL), f"No existe plantilla control: {PLANTILLA_CONTROL}")

    with open(ARCHIVO_ENTRADA, "rb") as f_entrada, open(PLANTILLA_CONTROL, "rb") as f_control:
        files = {
            "archivo_entrada": (
                os.path.basename(ARCHIVO_ENTRADA),
                f_entrada,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
            "plantilla_control": (
                os.path.basename(PLANTILLA_CONTROL),
                f_control,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        }

        data = {
            "descripcion": DESCRIPCION,
        }

        try:
            response = requests.post(PIPELINE_URL, files=files, data=data, timeout=120)
        except requests.RequestException as e:
            fail(f"No se pudo conectar al pipeline: {e}")

    print(f"HTTP STATUS: {response.status_code}")

    try:
        payload = response.json()
    except Exception:
        print(response.text)
        fail("La respuesta no fue JSON válido.")

    print("\n--- RESPUESTA JSON ---")
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    assert_true(response.status_code == 200, "El pipeline no respondió con HTTP 200.", payload)
    assert_true(payload.get("ok") is True, "El campo 'ok' no vino en true.", payload)
    assert_true(payload.get("modulo") == "ARGO_PIPELINE", "El módulo no es ARGO_PIPELINE.", payload)

    id_operacion = payload.get("id_operacion")
    assert_true(
        isinstance(id_operacion, str) and id_operacion.strip() != "",
        "No se recibió id_operacion válido.",
        payload,
    )
    ok(f"id_operacion detectado: {id_operacion}")

    control = payload.get("control")
    assert_true(isinstance(control, dict), "No existe bloque 'control'.", payload)
    assert_true(control.get("ok") is True, "El bloque control no viene en ok=true.", payload)
    assert_true(control.get("modulo") == "ARGO_CONTROL", "El bloque control no tiene modulo ARGO_CONTROL.", payload)

    estatus_control = control.get("estatus")
    assert_true(
        isinstance(estatus_control, str) and estatus_control.strip() != "",
        "Control no devolvió estatus.",
        payload,
    )
    ok(f"CONTROL estatus: {estatus_control}")

    resumen_control = control.get("resumen", {})
    assert_true(isinstance(resumen_control, dict), "Control no devolvió resumen.", payload)

    master = payload.get("master")
    assert_true(isinstance(master, dict), "No existe bloque 'master'.", payload)

    indicadores = master.get("indicadores", {})
    assert_true(isinstance(indicadores, dict), "MASTER no tiene indicadores.", payload)

    class_ind = indicadores.get("class", {})
    assert_true(isinstance(class_ind, dict), "MASTER no tiene indicadores.class.", payload)

    certeza_final = class_ind.get("certeza_final_pct")
    print("DEBUG certeza_final_pct (MASTER):", certeza_final, type(certeza_final))

    assert_true(
        isinstance(certeza_final, (int, float)),
        "MASTER no devolvió certeza_final_pct numérica.",
        payload,
    )

    ok(f"CLASS certeza_final_pct (MASTER): {certeza_final}")

    print("\n🎉 PRUEBA 01 EXITOSA")


if __name__ == "__main__":
    main()
