# ARGO Enterprise SaaS v1.0
## Matriz Inicial de Pruebas del Piloto

| ID | Escenario | Archivo | Resultado esperado |
|---|---|---|---|
| P-001 | Entrada limpia | tests_data/entrada_limpia.xlsx | Operación aceptada |
| P-002 | Entrada crítica | tests_data/entrada_critica.xlsx | Incidencias detectadas |
| P-003 | Entrada sucia | tests_data/entrada_sucia.xlsx | Validación controlada |
| P-004 | Control | tests_data/PLANTILLA_CONTROL.xlsx | ARGO Control ejecutado |
| P-005 | Usuario sin sesión | N/A | HTTP 401 |
| P-006 | Token inválido | N/A | HTTP 401 |
| P-007 | Tenant incorrecto | N/A | Acceso rechazado |
| P-008 | Admin crea usuario externo | N/A | Tenant forzado al propio |
| P-009 | Supervisor aprueba | N/A | Aprobación permitida |
| P-010 | Operador intenta aprobar | N/A | Permiso rechazado |

## Resultado requerido
Todos los casos P-001 a P-010 deben quedar documentados como:

PASS
FAIL
BLOCKED
NOT RUN
