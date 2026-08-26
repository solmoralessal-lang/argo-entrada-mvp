# ARGO Enterprise SaaS

Sistema empresarial SaaS para procesamiento, clasificación, control documental, analítica y administración de operaciones de bodega.

## Release certificado

- Versión: v1.0.0
- Fecha de certificación E2E: 2026-08-26
- Commit certificado: 6e7eb10e78b2fbc158cc955426882ddb74714953
- Estado: CERTIFICADO E2E
- Tag Git: v1.0.0

## Recuperación de la versión certificada

Clonar el repositorio y cambiar al release certificado:

git clone <URL_DEL_REPOSITORIO>
cd argo-entrada-mvp
git checkout v1.0.0

Crear un entorno virtual limpio:

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt

## Arranque del backend

uvicorn main:app --host 127.0.0.1 --port 8766

Verificación:

curl http://127.0.0.1:8766/health
curl http://127.0.0.1:8766/openapi.json

## Dependencias

Las dependencias directas validadas se encuentran fijadas en requirements.txt.

## Seguridad

No almacenar en Git:

- archivos .env
- API keys
- tokens
- certificados privados
- archivos *.key
- entornos virtuales
- respaldos locales
- salidas generadas

Los secretos necesarios para ejecutar ARGO deben configurarse externamente.

## Política de release

El tag v1.0.0 representa la versión exacta que superó la Certificación Funcional End-to-End.

No modificar ni mover este tag.

Todo desarrollo posterior debe realizarse en commits o ramas posteriores sin alterar la línea base certificada.

## Evidencia de recuperación

El 26 de agosto de 2026 se realizó una reconstrucción independiente de ARGO Enterprise SaaS v1.0.0:

- extracción independiente del release
- creación de entorno Python limpio
- instalación desde requirements.txt
- carga correcta de FastAPI
- arranque correcto mediante Uvicorn
- Swagger /docs: HTTP 200
- OpenAPI disponible: HTTP 200
- endpoints ARGO cargados correctamente

Resultado: RECUPERACIÓN TÉCNICA APROBADA.
