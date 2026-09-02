# ARGO Enterprise SaaS
## Piloto - P002 Cámara PRO Inspección Física v2

Tenant: PILOT_ARGO_001
Rama: pilot/v1.0

---

## 1. Objetivo

Validar un nuevo flujo OCR especializado en inspección física de
mercancía, independiente del OCR documental certificado de ARGO v1.0.

El objetivo de esta prueba fue determinar si el hallazgo detectado
durante P001 se debía a una limitación visual del modelo o al esquema
OCR documental utilizado por Cámara PRO.

---

## 2. Arquitectura implementada

Se agregó un endpoint independiente:

POST /argo/ocr_mercancia

El endpoint documental original permanece disponible:

POST /argo/ocr

No se modificó ni reemplazó el OCR documental existente.

El nuevo flujo está diseñado para detectar información física visible
en etiquetas y mercancía, incluyendo:

- marca
- modelo
- numero_parte
- lote
- serie
- pais_origen
- cantidad_visible
- descripcion
- purchase_order
- partida
- unidad

También incorpora:

- confianza por campo
- requiere_confirmacion
- observaciones
- política de no invención de datos

Los campos críticos ambiguos deben requerir confirmación humana.

La ausencia de lote, serie u origen no debe ser considerada
automáticamente como error.

---

## 3. Evidencia utilizada

Se utilizó la misma fotografía real empleada durante P001.

Archivo:

IMG_20260902_173818.jpg

La etiqueta contiene visualmente:

- Marca / proveedor: McMASTER-CARR
- Purchase Order: 1221877
- Line: 1
- Part Number: 6659K239
- Ordered: 24 Each
- Shipped: 24 Each
- Unidad: Each

Descripción visible:

Oil-Embedded 841 Bronze Flanged Sleeve Bearing for 10 mm Shaft
Diameter and 12 mm Housing ID, 7 mm Long

No se observa claramente en la etiqueta:

- lote
- serie
- pais de origen

---

## 4. Resultado Cámara PRO v2

Resultado obtenido:

- marca: McMASTER-CARR
- modelo: null
- numero_parte: 6659K239
- cantidad_visible: 24
- unidad: Each
- purchase_order: 1221877
- partida: 1
- descripcion: detectada correctamente
- lote: null
- serie: null
- pais_origen: null

Confianza reportada:

- marca: 0.99
- numero_parte: 0.97
- cantidad_visible: 0.98
- descripcion: 0.95

Campos que requieren confirmación:

Ninguno.

Observación generada:

No se observa lote, serie ni pais de origen en la etiqueta visible.

---

## 5. Comparación contra evidencia conocida

| Campo | Esperado | Detectado | Resultado |
|---|---|---|---|
| numero_parte | 6659K239 | 6659K239 | PASS |
| cantidad_visible | 24 | 24 | PASS |
| purchase_order | 1221877 | 1221877 | PASS |
| partida | 1 | 1 | PASS |
| lote | null | null | PASS |
| serie | null | null | PASS |
| pais_origen | null | null | PASS |

Resultado:

7 / 7 coincidencias.

PASS COMPLETO.

---

## 6. Comparación P001 vs P002

### P001 - flujo OCR anterior

La misma fotografía produjo extracción parcial.

Detectó correctamente:

- McMASTER-CARR
- descripción del producto

No estructuró:

- numero_parte 6659K239
- cantidad 24
- purchase_order
- partida

La investigación del código demostró que Cámara PRO reutilizaba un
esquema diseñado para documentos logísticos.

### P002 - OCR Mercancía v2

La misma fotografía produjo:

- numero_parte correcto
- cantidad correcta
- purchase_order correcto
- partida correcta
- descripción correcta
- ausencia correcta de lote
- ausencia correcta de serie
- ausencia correcta de país de origen

Resultado comparativo:

P001: PARCIAL
P002: PASS COMPLETO 7/7

---

## 7. Conclusión técnica

La prueba demuestra que el problema detectado durante P001 no era,
para esta evidencia, una incapacidad visual para interpretar la
etiqueta.

El problema estaba principalmente en la arquitectura y en el contrato
de extracción del OCR documental utilizado por Cámara PRO.

La separación entre:

OCR DOCUMENTAL

y

OCR DE MERCANCIA

mejora de forma significativa la extracción estructurada de datos
relevantes para inspección física.

Este resultado corresponde a una prueba controlada con una fotografía.
No constituye todavía validación general de exactitud sobre diferentes
tipos de mercancía, etiquetas, iluminación o condiciones operativas.

---

## 8. Estado

Backend Cámara PRO Inspección Física v2:

FASE 1 - PASS

Próxima fase:

Integrar /argo/ocr_mercancia con la interfaz Cámara PRO del operador y
mostrar los campos detectados inmediatamente después de cada captura
para permitir:

- confirmar
- corregir
- volver a capturar

Posteriormente se realizará validación con múltiples evidencias reales
durante el piloto.

