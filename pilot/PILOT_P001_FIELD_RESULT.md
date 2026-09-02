# ARGO Enterprise SaaS
## Piloto Operativo Interno Controlado - P001

Fecha: 2026-09-02
Tenant: PILOT_ARGO_001
Rama: pilot/v1.0

---

## 1. Baseline operacional CTL

Referencia CTL: T-98896

- Inicio revisión física: 16:10
- Fin revisión física: 16:26
- Tiempo revisión física: 16 minutos
- Número de partidas: 3
- Tipo de carga: cartón / caja
- Fotografías CTL aproximadas: 18
- Promedio aproximado: 6 fotografías por partida
- Números de serie: No
- Diferentes lotes: No
- Discrepancias físicas/documentales: No
- Dificultad especial: No

P001 representa una operación normal, sin excepciones operativas
significativas, adecuada como línea base.

---

## 2. Entrada documental ARGO

- Documentos seleccionados: 3
- Documentos procesados: 3
- Errores técnicos: 0
- Volumen procesado: 6.82 MB
- Reporte Ejecutivo: generado
- Cobertura reportada: 33.33 %

Datos detectados, entre otros:

- Cliente: FIVES AUTOMATIZACION MEXICO SA DE CV
- Proveedor: MCMASTER-CARR
- Paquetería: FedEx
- Tracking: 536538947200

Hallazgo:

INC-P001-001
Los documentos fueron procesados correctamente, pero el inventario
documental del Reporte Ejecutivo no identificó correctamente los tipos
de documentos cargados (Commercial Invoice / Packing List).

---

## 3. Cámara PRO - prueba física FOTO-P001-001

La cámara funcionó desde tablet y la captura fue aceptada por control
de calidad.

Calidad registrada:

- Brightness: 128
- Contrast: 12
- Detail: 7.5 %
- Resultado de calidad: Aceptada

Información visible en la etiqueta de prueba:

- Proveedor / marca visible: McMASTER-CARR
- Purchase Order: 1221877
- Line: 1
- Part Number: 6659K239
- Ordered: 24 Each
- Shipped: 24 Each
- Descripción:
  Oil-Embedded 841 Bronze Flanged Sleeve Bearing for 10 mm Shaft
  Diameter and 12 mm Housing ID, 7 mm Long

Resultado OCR ARGO:

- proveedor: McMASTER-CARR
- descripción: detectada correctamente
- cantidad: no detectada
- no_parte: no detectado
- marca: no estructurada
- modelo: no detectado
- lote: no detectado
- serie: no detectado
- país de origen: no detectado

Clasificación FOTO-P001-001: PARCIAL

---

## 4. Diagnóstico técnico

El endpoint /argo/ocr utiliza actualmente un esquema orientado a
documentos logísticos:

- cliente
- proveedor
- paqueteria
- tracking
- descripcion
- cantidad_bultos
- peso_total
- peso_unidad
- direccion_origen
- direccion_destino

El esquema no solicita campos críticos de inspección física como:

- numero_parte
- marca
- modelo
- lote
- serie
- pais_origen
- cantidad_visible

La lógica posterior de consolidación, faltantes y severidad también
está basada en el mismo esquema documental.

Adicionalmente, /argo/generar_desde_ocr establece actualmente como
"No legible" los campos marca, modelo, no_parte, no_lote y no_serie.

---

## 5. Hallazgos oficiales

INC-P001-002
La Cámara PRO procesa y guarda correctamente la imagen, pero la interfaz
del operador no muestra al usuario los campos OCR extraídos para
confirmación o corrección.

INC-P001-003 - ALTA
La Cámara PRO reutiliza el esquema OCR documental y no está diseñada
todavía para extraer los campos requeridos durante una inspección física
de mercancía.

MEJ-P001-001
Después de cada captura, la interfaz debe mostrar inmediatamente los
datos físicos detectados y permitir:

- Confirmar
- Corregir
- Volver a capturar

REQ-P001-001
Crear un flujo OCR especializado para mercancía física, separado del
OCR documental.

Campos mínimos propuestos:

- partida
- marca
- modelo
- numero_parte
- lote
- serie
- pais_origen
- cantidad_visible
- descripcion

Los campos críticos no legibles o ambiguos deben requerir confirmación
humana y nunca ser inventados.

---

## 6. Conclusión P001

P001 confirma que ARGO v1.0 funciona técnicamente en campo para:

- acceso desde tablet
- autenticación
- carga documental
- procesamiento OCR
- Cámara PRO
- captura de imagen
- creación de operación
- persistencia en Supabase
- generación de reporte

Sin embargo, P001 también demuestra que Cámara PRO requiere un flujo
especializado de inspección física antes de poder reducir de forma
significativa la recaptura manual de datos del revisador.

P001 queda establecido como baseline "ANTES".

La siguiente iteración será Cámara PRO Inspección Física v2 y será
evaluada mediante P002 como comparación "DESPUÉS".

