# ARGO Enterprise SaaS v1.0
## Runbook Operativo — Piloto Real

**Tenant técnico de preparación:** PILOT_ARGO_001  
**Versión base certificada:** v1.0.0  
**Rama piloto:** pilot/v1.0  
**Estado técnico previo al piloto:** 10/10 pruebas automatizadas aprobadas

---

## 1. Objetivo

Validar ARGO Enterprise SaaS en una operación real de bodega sin comprometer datos, continuidad operativa ni aislamiento multi-tenant.

El piloto debe demostrar:

- reducción de trabajo manual;
- reducción de errores;
- trazabilidad de operaciones;
- correcto aislamiento de información;
- facilidad de uso;
- estabilidad del flujo E2E.

---

## 2. Regla principal

ARGO funcionará inicialmente como sistema paralelo de validación.

Durante las primeras operaciones del piloto:

1. La bodega mantiene su procedimiento habitual.
2. ARGO procesa la misma operación.
3. Se comparan ambos resultados.
4. Ninguna decisión crítica depende exclusivamente de ARGO hasta completar la validación.

---

## 3. Roles

### Administrador del piloto
Responsable de:

- alta y baja de usuarios;
- revisión de accesos;
- revisión de incidencias;
- coordinación del piloto.

### Supervisor
Responsable de:

- revisar resultados;
- aprobar operaciones cuando corresponda;
- registrar diferencias contra el proceso manual.

### Operador
Responsable de:

- cargar documentos;
- ejecutar operaciones;
- reportar problemas de uso.

---

## 4. Inicio de cada sesión

Verificar:

- ARGO disponible;
- usuario correcto;
- tenant correcto;
- conexión disponible;
- archivos de prueba o reales identificados;
- sistema manual de respaldo disponible.

---

## 5. Flujo por operación

Para cada operación:

1. Registrar hora de inicio.
2. Registrar identificador interno de la bodega.
3. Procesar documentos mediante ARGO.
4. Registrar id_operacion de ARGO.
5. Registrar hora de finalización.
6. Comparar resultado contra proceso manual.
7. Registrar correcciones manuales requeridas.
8. Registrar incidencia si existe.
9. Clasificar resultado como:
   - CORRECTO
   - CORRECTO_CON_AJUSTES
   - INCORRECTO
   - BLOQUEADO

---

## 6. Incidencias

### CRÍTICA
- fuga entre tenants;
- pérdida de datos;
- acceso no autorizado;
- resultado incorrecto que pueda provocar una decisión operativa grave.

Acción: detener piloto.

### ALTA
- operación no puede completarse;
- módulo principal falla repetidamente;
- documento generado incorrectamente.

Acción: evaluar suspensión temporal.

### MEDIA
- requiere intervención manual;
- resultado parcial;
- error recuperable.

### BAJA
- problema visual;
- texto confuso;
- mejora de experiencia.

### OBSERVACIÓN
- sugerencia del usuario;
- oportunidad de mejora;
- comportamiento no bloqueante.

---

## 7. Seguridad

Nunca registrar en documentos del piloto:

- contraseñas;
- API keys;
- tokens;
- service role keys;
- secretos de sesión.

No compartir credenciales entre usuarios.

---

## 8. Cierre diario

Al terminar cada jornada:

- contar operaciones procesadas;
- revisar incidencias;
- verificar operaciones incorrectas;
- revisar diferencias contra proceso manual;
- confirmar que no existió mezcla entre tenants;
- registrar tiempo estimado ahorrado.

---

## 9. Condición de éxito

El piloto se considerará técnicamente satisfactorio cuando:

- no existan incidencias críticas;
- el aislamiento multi-tenant permanezca intacto;
- >= 98% de operaciones terminen sin error crítico;
- los resultados coincidan razonablemente con la operación manual;
- exista ahorro de tiempo medible;
- los usuarios puedan operar ARGO sin asistencia técnica constante.
