# ARGO Enterprise SaaS v1.0
## Checklist Operativo de Piloto

Tenant piloto: PILOT_ARGO_001

### 1. Acceso y usuarios
- [ ] admin_cliente puede iniciar sesión
- [ ] supervisor puede iniciar sesión
- [ ] operador puede iniciar sesión
- [ ] usuario inactivo no puede acceder
- [ ] usuario de otro tenant no puede acceder a datos de PILOT_ARGO_001

### 2. Entrada documental
- [ ] cargar archivo de entrada válido
- [ ] validar estructura documental
- [ ] detectar errores de formato
- [ ] generar operación correctamente
- [ ] conservar id_operacion único

### 3. ARGO Control
- [ ] ejecutar validación
- [ ] identificar incidencias correctamente
- [ ] generar resultado consistente
- [ ] vincular resultado al tenant correcto

### 4. ARGO Class
- [ ] clasificar correctamente operación limpia
- [ ] clasificar operación crítica
- [ ] mantener trazabilidad
- [ ] no mezclar información de otros tenants

### 5. ARGO Document
- [ ] generar documento de salida
- [ ] archivo generado descargable
- [ ] información consistente con entrada
- [ ] vínculo correcto con id_operacion

### 6. OCR
- [ ] procesar documento legible
- [ ] identificar campos principales
- [ ] manejar documento parcialmente ilegible
- [ ] no detener operación completa ante campo faltante

### 7. Dashboard
- [ ] mostrar operaciones del tenant
- [ ] no mostrar operaciones de otro tenant
- [ ] métricas consistentes
- [ ] incidencias visibles
- [ ] timeline correcto

### 8. Roles y permisos
- [ ] operador limitado a funciones operativas
- [ ] supervisor puede aprobar
- [ ] admin_cliente puede administrar usuarios propios
- [ ] admin_cliente no puede administrar otro tenant
- [ ] master_admin conserva acceso global

### 9. Reportes
- [ ] listar reportes
- [ ] descargar reporte
- [ ] reporte corresponde al tenant
- [ ] datos coinciden con operación

### 10. Seguridad
- [ ] sesión requerida en rutas protegidas
- [ ] token inválido rechazado
- [ ] rate limiting activo
- [ ] headers de seguridad presentes
- [ ] secretos fuera de Git
- [ ] aislamiento multi-tenant confirmado

### 11. Cierre del piloto
- [ ] incidencias documentadas
- [ ] errores críticos = 0
- [ ] resultados comparados contra operación manual
- [ ] feedback del usuario piloto registrado
- [ ] decisión GO / NO-GO documentada
