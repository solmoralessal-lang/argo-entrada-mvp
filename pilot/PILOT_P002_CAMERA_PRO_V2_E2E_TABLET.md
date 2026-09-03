# PILOT P002 — Camera PRO v2 E2E Tablet

Fecha: 2026-09-03  
Rama: `pilot/v1.0`

## Resultado

**PASS E2E CONTROLADO EN TABLET**

La validación corresponde a una sola etiqueta física McMASTER-CARR utilizada durante el piloto interno. Este resultado no implica validación general para todos los tipos de mercancía.

## Flujo validado

1. Captura desde Camera PRO en tablet.
2. Evaluación de calidad de imagen.
3. Autenticación mediante sesión firmada.
4. Envío a `/argo/ocr_mercancia`.
5. Lectura estructurada de mercancía.
6. Visualización de campos OCR en Camera PRO.
7. Modo Corregir con campos editables.
8. Confirmación por el operador.
9. Función Repetir foto con limpieza de lectura previa.

## Datos observados

- Marca: McMASTER-CARR
- Número de parte: 6659K239
- Cantidad visible: 24
- Unidad: Each
- Purchase Order: 1221877
- Partida / línea: 1
- Descripción: Oil-Embedded 841 Bronze Flanged Sleeve Bearing for 10 mm Shaft Diameter and 12 mm Housing ID, 7 mm Long
- Modelo: no visible
- Lote: no visible
- Serie: no visible
- País de origen: no visible

## Confianza mostrada por el modelo

- Marca: 99%
- Número de parte: 98%
- Cantidad visible: 98%
- Descripción: 97%

Los valores de confianza son autoevaluaciones del modelo y no deben interpretarse como probabilidades calibradas.

## Autenticación

Se confirmó mediante prueba aislada:

- `/argo/login`: sesión válida recibida.
- `/argo/me` con Bearer: HTTP 200.
- Usuario: operador del tenant `PILOT_ARGO_001`.
- Módulos autorizados: `camara_pro`, `entrada_documental`.

El frontend conserva el `ARGO_AUTH_BRIDGE`, que captura `session_token`, lo almacena en `sessionStorage` y agrega `Authorization: Bearer` a solicitudes posteriores.

El Auth Bridge fue restaurado al source reproducible y configurado para cargar antes del bundle React.

## RBAC

La petición del operador a `/argo/dashboard` devuelve:

- HTTP 403
- `MODULO_DENEGADO`
- módulo: `dashboard`

Esto corresponde al control de permisos del rol operador y no a una falla de autenticación.

## Alcance de P002

P002 valida de forma controlada:

`captura → calidad → autenticación → OCR mercancía → lectura estructurada → corrección → confirmación → repetir foto`

## Pendientes fuera de P002

Esta prueba todavía NO valida:

- persistencia definitiva de la inspección confirmada;
- comparación automática esperado vs. observado;
- robustez OCR con múltiples tipos de mercancía;
- calibración estadística de niveles de confianza;
- comportamiento de Camera PRO en condiciones físicas diversas;
- permisos de dashboard para otros roles;
- eliminación futura del mecanismo transitorio Auth Bridge.

