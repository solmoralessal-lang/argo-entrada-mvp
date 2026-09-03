import { useEffect, useMemo, useRef, useState } from "react";
import "./App.css";

const API_BASE = window.location.origin;

const PLANES = {
  BASIC: {
    nombre: "BASIC",
    modulos: ["entrada_documental", "camara_pro", "dashboard"],
  },
  PRO: {
    nombre: "PRO",
    modulos: [
      "entrada_documental",
      "camara_pro",
      "dashboard",
      "analytics_pro",
      "aprobaciones",
      "reportes",
    ],
  },
  ENTERPRISE: {
    nombre: "ENTERPRISE",
    modulos: [
      "entrada_documental",
      "camara_pro",
      "dashboard",
      "analytics_pro",
      "admin_saas",
      "usuarios",
      "auditoria",
      "aprobaciones",
      "incidencias",
      "reportes",
      "argo_connect",
    ],
  },
};

const ROLES = {
  operador: ["entrada_documental", "camara_pro", "dashboard"],
  supervisor: [
    "entrada_documental",
    "camara_pro",
    "dashboard",
    "analytics_pro",
    "aprobaciones",
    "reportes",
  ],
  admin: [
    "entrada_documental",
    "camara_pro",
    "dashboard",
    "analytics_pro",
    "admin_saas",
    "usuarios",
    "auditoria",
    "argo_connect",
    "aprobaciones",
    "incidencias",
    "reportes",
  ],
  admin_cliente: [
    "entrada_documental",
    "camara_pro",
    "dashboard",
    "analytics_pro",
    "admin_saas",
    "usuarios",
    "auditoria",
    "argo_connect",
    "aprobaciones",
    "incidencias",
    "reportes",
  ],
  master_admin: [
    "entrada_documental",
    "camara_pro",
    "dashboard",
    "analytics_pro",
    "admin_saas",
    "usuarios",
    "auditoria",
    "argo_connect",
    "aprobaciones",
    "incidencias",
    "reportes",
  ],
};

function normalizarPlan(plan) {
  return String(plan || "BASIC").toUpperCase();
}

function normalizarRol(rol) {
  return String(rol || "operador").toLowerCase();
}

function interseccion(a, b) {
  return a.filter((x) => b.includes(x));
}

function App() {
  const videoRef = useRef(null);
  const canvasRef = useRef(null);
  const streamRef = useRef(null);

  const [usuario, setUsuario] = useState(null);
  const [sessionToken, setSessionToken] = useState("");
const [restaurandoSesion, setRestaurandoSesion] = useState(true);
  const [mostrarPassword, setMostrarPassword] = useState(false);
  const [login, setLogin] = useState({
    email: "guero@argo.com",
    password: "123456",
  });

  const [dashboard, setDashboard] = useState(null);
  const [dashboardPro, setDashboardPro] = useState(null);
  const [dashboardProCargando, setDashboardProCargando] = useState(false);
  const [masterDashboard, setMasterDashboard] = useState(null);

  const [connectCatalogo, setConnectCatalogo] = useState([]);
  const [connectPlantillas, setConnectPlantillas] = useState([]);
  const [connectNombre, setConnectNombre] = useState("SLAM CTL");
  const [connectDescripcion, setConnectDescripcion] = useState("Layout operativo para importación en sistema externo.");
  const [connectFormato, setConnectFormato] = useState("xlsx");
  const [connectSeparador, setConnectSeparador] = useState(",");
  const [connectOrientacion, setConnectOrientacion] = useState("horizontal");
  const [connectBusqueda, setConnectBusqueda] = useState("");
  const [connectPlantillaEditando, setConnectPlantillaEditando] = useState(null);
  const [connectJsonImport, setConnectJsonImport] = useState("");
  const [connectColumnas, setConnectColumnas] = useState([]);

  const [error, setError] = useState("");
  const [camaraActiva, setCamaraActiva] = useState(false);
  const [procesando, setProcesando] = useState(false);
  const [scanStatus, setScanStatus] = useState("Cámara apagada");
  const [preview, setPreview] = useState(null);
  const [connectPreview, setConnectPreview] = useState(null);
  const [adminUsuarios, setAdminUsuarios] = useState([]);
  const [adminAuditoria, setAdminAuditoria] = useState([]);

  const [reportes, setReportes] = useState([]);
  const [reportesCargando, setReportesCargando] = useState(false);
  const [reporteDescargando, setReporteDescargando] = useState(null);

  const [incidencias, setIncidencias] = useState([]);
  const [incidenciasCargando, setIncidenciasCargando] = useState(false);
  const [incidenciaEditando, setIncidenciaEditando] = useState(null);
  const [incidenciaForm, setIncidenciaForm] = useState({
    estado_incidencia: "EN_REVISION",
    severidad: "ALTA",
    asignado_a: "",
    comentario: "",
  });

  const [adminNuevoUsuario, setAdminNuevoUsuario] = useState({
    nombre: "",
    email: "",
    password: "",
    rol: "operador",
    activo: true,
  });

  const [adminCreandoUsuario, setAdminCreandoUsuario] = useState(false);
  const [auditoriaFiltroTexto, setAuditoriaFiltroTexto] = useState("");
  const [auditoriaFiltroAccion, setAuditoriaFiltroAccion] = useState("");
  const [auditoriaFiltroActor, setAuditoriaFiltroActor] = useState("");
  const [calidad, setCalidad] = useState(null);

  // Cámara PRO v2 - inspección física de mercancía
  const [lecturaMercancia, setLecturaMercancia] = useState(null);
  const [editandoMercancia, setEditandoMercancia] = useState(false);
  const [lecturaConfirmada, setLecturaConfirmada] = useState(false);
  const [reporteEjecutivo, setReporteEjecutivo] = useState(null);
  const [resultadoCarga, setResultadoCarga] = useState({
    archivosRecibidos: 0,
    archivosProcesados: 0,
    archivosConError: 0,
    bytesRecibidos: 0,
    estado: "Sin operación",
    errores: [],
  });

  const planUsuario = normalizarPlan(
    usuario?.plan_saas ||
      usuario?.plan?.codigo ||
      usuario?.plan?.nombre ||
      usuario?.plan ||
      usuario?.licencia ||
      "BASIC"
  );
  const rolUsuario = normalizarRol(usuario?.rol);

  const modulosPermitidos = useMemo(() => {
    if (!usuario) return [];

    const porPlan = PLANES[planUsuario]?.modulos || PLANES.BASIC.modulos;
    const porRol = ROLES[rolUsuario] || ROLES.operador;

    const desdeBackend = Array.isArray(usuario?.modulos_permitidos)
      ? usuario.modulos_permitidos
      : Array.isArray(usuario?.modulos)
      ? usuario.modulos
      : null;

    const base = interseccion(porPlan, porRol);

    if (!desdeBackend) return base;

    return interseccion(base, desdeBackend);
  }, [usuario, planUsuario, rolUsuario]);

  const puede = (modulo) => modulosPermitidos.includes(modulo);

  const esAdmin = rolUsuario === "admin" || rolUsuario === "master_admin";
  const esMaster = rolUsuario === "master_admin";

  useEffect(() => {
    let cancelado = false;

    async function restaurarSesion() {
      try {
        const res = await fetch(`${API_BASE}/argo/me`);
        const data = await res.json();

        if (!cancelado && res.ok && data.ok && data.usuario) {
          const usuarioSesion = data.usuario;

          setUsuario({
            ...usuarioSesion,
            plan_saas: normalizarPlan(
              usuarioSesion.plan_saas ||
                usuarioSesion.plan?.codigo ||
                usuarioSesion.plan?.nombre ||
                usuarioSesion.plan ||
                "BASIC"
            ),
            rol: normalizarRol(usuarioSesion.rol || "operador"),
          });
        }
      } catch (err) {
        console.error("No se pudo restaurar la sesión ARGO", err);
      } finally {
        if (!cancelado) {
          setRestaurandoSesion(false);
        }
      }
    }

    restaurarSesion();

    return () => {
      cancelado = true;
    };
  }, []);

  useEffect(() => {
    if (camaraActiva && videoRef.current && streamRef.current) {
      videoRef.current.srcObject = streamRef.current;
      videoRef.current.play().catch(() => {});
    }
  }, [camaraActiva]);

  useEffect(() => {

    if (usuario) {

      cargarDashboard();

      if (modulosPermitidos.includes("analytics_pro")) {
        cargarDashboardPro();
      }

      if (modulosPermitidos.includes("argo_connect")) {
        cargarConnect();
      }

      if (usuario?.rol === "master_admin") {
        cargarMasterDashboard();
      }
    }

    return () => detenerCamara();

  }, [usuario, modulosPermitidos]);

  async function iniciarSesion(e) {
    e.preventDefault();
    setError("");

    try {
      const res = await fetch(`${API_BASE}/argo/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(login),
      });

      const data = await res.json();

      if (!data.ok) {
        setError(data.error || "No se pudo iniciar sesión");
        return;
      }

      const usuarioLogin = data.usuario || data.data || data;

      const tokenLogin = data.session_token || "";

      if (!tokenLogin) {
        setError("ARGO no devolvió una sesión válida");
        return;
      }

      setSessionToken(tokenLogin);

      setUsuario({
        ...usuarioLogin,
        plan_saas: normalizarPlan(
          usuarioLogin.plan_saas ||
            usuarioLogin.plan?.codigo ||
            usuarioLogin.plan?.nombre ||
            usuarioLogin.plan ||
            "BASIC"
        ),
        rol: normalizarRol(usuarioLogin.rol || "operador"),
      });
    } catch (err) {
      console.error(err);
      setError("Error conectando con ARGO");
    }
  }

  async function cargarReportes() {
    setReportesCargando(true);

    try {
      const res = await fetch(
        `${API_BASE}/argo/reportes`,
        {
          headers: {
            "x-usuario-email": usuario?.email || "",
            "x-cliente-id": usuario?.id_cliente || "",
          },
        }
      );
      const data = await res.json();

      if (!res.ok || !data.ok) {
        alert(data.error || "No se pudieron cargar los reportes");
        return;
      }

      setReportes(Array.isArray(data.reportes) ? data.reportes : []);
    } catch (err) {
      console.error(err);
      alert("Error cargando reportes");
    } finally {
      setReportesCargando(false);
    }
  }

  async function descargarReporteProtegido(reporte) {
    const idOperacion = reporte?.id_operacion;

    if (!idOperacion) {
      alert("Reporte sin operación asociada");
      return;
    }

    setReporteDescargando(idOperacion);

    try {
      const res = await fetch(
        `${API_BASE}/argo/reportes/descargar/${encodeURIComponent(idOperacion)}`,
        {
          headers: {
            "x-usuario-email": usuario?.email || "",
            "x-cliente-id": usuario?.id_cliente || "",
          },
        }
      );

      if (!res.ok) {
        let mensaje = "No se pudo descargar el reporte";

        try {
          const data = await res.json();
          mensaje = data.error || mensaje;
        } catch (_) {
          // La respuesta puede no ser JSON.
        }

        alert(mensaje);
        return;
      }

      const blob = await res.blob();

      const contentDisposition =
        res.headers.get("content-disposition") || "";

      const match = contentDisposition.match(
        /filename="?([^";]+)"?/i
      );

      const nombreArchivo =
        match?.[1] ||
        reporte?.archivo ||
        `reporte_${idOperacion}.xlsx`;

      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");

      link.href = url;
      link.download = nombreArchivo;

      document.body.appendChild(link);
      link.click();
      link.remove();

      URL.revokeObjectURL(url);
    } catch (err) {
      console.error(err);
      alert("Error descargando reporte");
    } finally {
      setReporteDescargando(null);
    }
  }

  async function cargarIncidencias() {
    setIncidenciasCargando(true);

    try {
      const params = new URLSearchParams();

      if (usuario?.id_cliente) {
        params.set("cliente_id", usuario.id_cliente);
      }

      const query = params.toString();

      const res = await fetch(
        `${API_BASE}/argo/dashboard/pro/incidencias${query ? `?${query}` : ""}`,
        {
          headers: {
            "x-usuario-email": usuario?.email || "",
            "x-cliente-id": usuario?.id_cliente || "",
          },
        }
      );
      const data = await res.json();

      if (!res.ok || !data.ok) {
        alert(data.error || "No se pudieron cargar incidencias");
        return;
      }

      setIncidencias(data.incidencias_criticas || []);
    } catch (err) {
      console.error(err);
      alert("Error cargando incidencias");
    } finally {
      setIncidenciasCargando(false);
    }
  }

  async function guardarIncidencia(id_operacion) {
    try {
      const res = await fetch(`${API_BASE}/argo/dashboard/pro/incidencia`, {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          "x-usuario-email": usuario?.email || "",
          "x-cliente-id": usuario?.id_cliente || "",
        },
        body: JSON.stringify({
          id_operacion,
          estado_incidencia: incidenciaForm.estado_incidencia,
          severidad: incidenciaForm.severidad,
          asignado_a: incidenciaForm.asignado_a,
          comentario: incidenciaForm.comentario,
        }),
      });

      const data = await res.json();

      if (!res.ok || !data.ok) {
        alert(data.error || "No se pudo actualizar la incidencia");
        return;
      }

      alert("Incidencia actualizada correctamente");
      setIncidenciaEditando(null);
      setIncidenciaForm({
        estado_incidencia: "EN_REVISION",
        severidad: "ALTA",
        asignado_a: "",
        comentario: "",
      });

      await cargarIncidencias();
    } catch (err) {
      console.error(err);
      alert("Error actualizando incidencia");
    }
  }

  async function aprobarOperacion(id_operacion) {
    try {
      const res = await fetch(`${API_BASE}/argo/operacion/aprobar`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-cliente-id": usuario?.id_cliente || "",
          "x-usuario-email": usuario?.email || "",
          "x-usuario-rol": usuario?.rol || "operador",
        },
        body: JSON.stringify({
          id_operacion,
          aprobada_por: usuario?.nombre || usuario?.email || "sistema",
        }),
      });

      const data = await res.json();

      if (!data.ok) {
        alert(data.error || "No se pudo aprobar");
        return;
      }

      await cargarDashboard();
      alert("Operación aprobada correctamente");
    } catch {
      alert("Error aprobando operación");
    }
  }



  async function cargarDashboardPro() {
    if (!usuario) return;

    setDashboardProCargando(true);

    try {
      const params = new URLSearchParams();

      if (usuario?.id_cliente) {
        params.set("cliente_id", usuario.id_cliente);
      }

      const query = params.toString();

      const res = await fetch(
        `${API_BASE}/argo/dashboard/pro${query ? `?${query}` : ""}`,
        {
          headers: {
            "x-usuario-email": usuario?.email || "",
            "x-cliente-id": usuario?.id_cliente || "",
          },
        }
      );

      const data = await res.json();

      if (!res.ok || !data.ok) {
        console.error("Dashboard PRO:", data);
        setDashboardPro(null);
        alert(data.error || "No se pudo actualizar Analytics PRO");
        return;
      }

      setDashboardPro(data);

    } catch (err) {
      console.error("Error cargando Dashboard PRO:", err);
      setDashboardPro(null);
      alert("Error cargando Analytics PRO");
    } finally {
      setDashboardProCargando(false);
    }
  }


  async function cargarMasterDashboard() {

    try {

      const res = await fetch(
        `${API_BASE}/argo/master/dashboard`,
        {
          headers: {
            "x-usuario-email": usuario?.email || "",
            "x-cliente-id": usuario?.id_cliente || "",
          },
        }
      );

      const data = await res.json();

      if (!data.ok) {
        console.error(data);
        return;
      }

      setMasterDashboard(data);

    } catch (err) {
      console.error(err);
    }
  }



  async function cargarConnect() {
    if (!usuario?.email) return;
    try {
      const headers = {
        "x-usuario-email": usuario.email || "",
        "x-cliente-id": usuario.id_cliente || "",
      };
      const [catRes, tplRes] = await Promise.all([
        fetch(`${API_BASE}/argo/connect/catalogo`, { headers }),
        fetch(`${API_BASE}/argo/connect/plantillas`, { headers }),
      ]);
      const cat = await catRes.json();
      const tpl = await tplRes.json();
      if (cat.ok) setConnectCatalogo(cat.catalogo || []);
      if (tpl.ok) setConnectPlantillas(tpl.plantillas || []);
      setConnectColumnas((prev) =>
        prev && prev.length ? prev : [nuevaFilaConnect(), nuevaFilaConnect(), nuevaFilaConnect(), nuevaFilaConnect(), nuevaFilaConnect()]
      );
    } catch (err) {
      console.error(err);
    }
  }

  function etiquetaCampoConnect(campoId) {
    const item = (connectCatalogo || []).find((c) => c.campo === campoId);
    return item?.etiqueta || campoId || "";
  }

  function nuevaFilaConnect(base = {}) {
    return {
      tipo: base.tipo || "campo",
      campo: base.campo || "",
      titulo: base.titulo || "",
      valor_fijo: base.valor_fijo || "",
      inicio: base.inicio || 1,
    };
  }

  function agregarCampoConnect(campo) {
    setConnectColumnas([
      ...connectColumnas,
      nuevaFilaConnect({
        tipo: "campo",
        campo: campo?.campo || "",
        titulo: campo?.etiqueta || campo?.campo || "",
      }),
    ]);
  }

  function agregarFilaConnect() {
    setConnectColumnas([...connectColumnas, nuevaFilaConnect()]);
  }

  function cambiarFilaConnect(idx, cambios) {
    const nuevo = [...connectColumnas];
    const actual = nuevo[idx] || nuevaFilaConnect();

    if (Object.prototype.hasOwnProperty.call(cambios, "tipo")) {
      const tipoNuevo = cambios.tipo || "campo";
      nuevo[idx] = { ...actual, tipo: tipoNuevo, campo: tipoNuevo === "campo" ? (actual.campo || "") : "" };

      if (tipoNuevo === "campo") {
        nuevo[idx].titulo = actual.titulo || "";
        nuevo[idx].valor_fijo = "";
      }
      if (tipoNuevo === "vacio") {
        nuevo[idx].titulo = actual.titulo || "Vacío";
        nuevo[idx].valor_fijo = "";
      }
      if (tipoNuevo === "texto_fijo") {
        nuevo[idx].titulo = actual.titulo || "Texto fijo";
        nuevo[idx].valor_fijo = actual.valor_fijo || "";
      }
      if (tipoNuevo === "fecha_actual") {
        nuevo[idx].titulo = actual.titulo || "Fecha actual";
        nuevo[idx].valor_fijo = "";
      }
      if (tipoNuevo === "usuario_actual") {
        nuevo[idx].titulo = actual.titulo || "Usuario actual";
        nuevo[idx].valor_fijo = "";
      }
      if (tipoNuevo === "secuencia") {
        nuevo[idx].titulo = actual.titulo || "Secuencia";
        nuevo[idx].valor_fijo = "";
        nuevo[idx].inicio = actual.inicio || 1;
      }
      if (tipoNuevo === "formula" || tipoNuevo === "concatenacion") {
        nuevo[idx].titulo = actual.titulo || "Fórmula";
        nuevo[idx].valor_fijo = actual.valor_fijo || "{tracking}-{shipment_id}";
      }
    } else {
      nuevo[idx] = { ...actual, ...cambios };
    }

    if (Object.prototype.hasOwnProperty.call(cambios, "campo")) {
      const item = (connectCatalogo || []).find((c) => c.campo === cambios.campo);
      nuevo[idx].tipo = "campo";
      nuevo[idx].campo = cambios.campo || "";
      nuevo[idx].valor_fijo = "";
      if (item) nuevo[idx].titulo = item.etiqueta || item.campo;
    }

    if (Object.prototype.hasOwnProperty.call(cambios, "titulo")) nuevo[idx].titulo = cambios.titulo;
    if (Object.prototype.hasOwnProperty.call(cambios, "valor_fijo")) nuevo[idx].valor_fijo = cambios.valor_fijo;
    if (Object.prototype.hasOwnProperty.call(cambios, "inicio")) nuevo[idx].inicio = cambios.inicio;

    setConnectColumnas(nuevo);
  }

  function quitarCampoConnect(idx) {
    setConnectColumnas(connectColumnas.filter((_, i) => i !== idx));
  }

  function duplicarFilaConnect(idx) {
    const nuevo = [...connectColumnas];
    nuevo.splice(idx + 1, 0, { ...nuevo[idx], titulo: `${nuevo[idx]?.titulo || "Copia"} copia` });
    setConnectColumnas(nuevo);
  }

  function moverFilaConnect(idx, dir) {
    const nuevo = [...connectColumnas];
    const j = idx + dir;
    if (j < 0 || j >= nuevo.length) return;
    const tmp = nuevo[idx];
    nuevo[idx] = nuevo[j];
    nuevo[j] = tmp;
    setConnectColumnas(nuevo);
  }

  function limpiarConnect() {
    setConnectPlantillaEditando(null);
    setConnectNombre("SLAM CTL");
    setConnectDescripcion("Layout operativo para importación en sistema externo.");
    setConnectFormato("xlsx");
    setConnectSeparador(",");
    setConnectOrientacion("horizontal");
    setConnectColumnas([nuevaFilaConnect(), nuevaFilaConnect(), nuevaFilaConnect(), nuevaFilaConnect(), nuevaFilaConnect()]);
  }

  function editarPlantillaConnect(p) {
    setConnectPlantillaEditando(p);
    setConnectNombre(p.nombre || "");
    setConnectDescripcion(p.descripcion || "");
    setConnectFormato(p.formato || "xlsx");
    setConnectSeparador(p.separador || ",");
    setConnectOrientacion(p.orientacion || "horizontal");
    setConnectColumnas(Array.isArray(p.columnas) ? p.columnas.map((c) => nuevaFilaConnect(c)) : []);
    irA("mod-connect");
  }

  async function eliminarPlantillaConnect(p) {
    if (!window.confirm(`¿Eliminar plantilla ${p.nombre}?`)) return;
    try {
      const res = await fetch(`${API_BASE}/argo/connect/plantillas/${p.id}`, {
        method: "DELETE",
        headers: {
          "x-usuario-email": usuario?.email || "",
          "x-cliente-id": usuario?.id_cliente || "",
        },
      });
      const data = await res.json();
      if (!data.ok) {
        alert(data.error || "No se pudo eliminar plantilla");
        return;
      }
      await cargarConnect();
      alert("Plantilla eliminada");
    } catch (err) {
      console.error(err);
      alert("Error eliminando plantilla");
    }
  }

  function exportarJsonPlantillaConnect(p = null) {
    const plantilla = p || {
      nombre: connectNombre,
      descripcion: connectDescripcion,
      formato: connectFormato,
      separador: connectSeparador,
      orientacion: connectOrientacion,
      columnas: connectColumnas,
    };
    const blob = new Blob([JSON.stringify(plantilla, null, 2)], { type: "application/json" });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${plantilla.nombre || "ARGO_CONNECT_TEMPLATE"}.json`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(url);
  }

  function importarJsonPlantillaConnect() {
    try {
      const data = JSON.parse(connectJsonImport || "{}");
      setConnectNombre(data.nombre || "Plantilla importada");
      setConnectDescripcion(data.descripcion || "");
      setConnectFormato(data.formato || "xlsx");
      setConnectSeparador(data.separador || ",");
      setConnectOrientacion(data.orientacion || "horizontal");
      setConnectColumnas(Array.isArray(data.columnas) ? data.columnas.map((c) => nuevaFilaConnect(c)) : []);
      setConnectPlantillaEditando(null);
      setConnectJsonImport("");
      alert("Plantilla JSON cargada en el constructor");
    } catch {
      alert("JSON inválido");
    }
  }

  async function guardarPlantillaConnect() {
    if (!connectNombre.trim()) {
      alert("Nombre de plantilla requerido");
      return;
    }

    const columnasNormalizadas = connectColumnas.map((c) => {
      const tipo = c.tipo || "campo";
      if (tipo !== "campo" || c.campo) return c;
      const titulo = String(c.titulo || "").toLowerCase().trim();
      const item = (connectCatalogo || []).find((x) =>
        String(x.etiqueta || "").toLowerCase().trim() === titulo ||
        String(x.campo || "").toLowerCase().trim() === titulo
      );
      return { ...c, campo: item?.campo || "", titulo: c.titulo || item?.etiqueta || "" };
    });

    const columnasValidas = columnasNormalizadas.filter((c) => {
      const tipo = c.tipo || "campo";
      if (tipo === "vacio") return true;
      if (tipo === "texto_fijo") return true;
      if (tipo === "fecha_actual") return true;
      if (tipo === "usuario_actual") return true;
      if (tipo === "secuencia") return true;
      if (tipo === "formula" || tipo === "concatenacion") return !!c.valor_fijo;
      return !!c.campo;
    });

    if (!columnasValidas.length) {
      alert("Agrega al menos una posición válida");
      return;
    }

    try {
      const url = connectPlantillaEditando?.id
        ? `${API_BASE}/argo/connect/plantillas/${connectPlantillaEditando.id}`
        : `${API_BASE}/argo/connect/plantillas`;
      const res = await fetch(url, {
        method: connectPlantillaEditando?.id ? "PATCH" : "POST",
        headers: {
          "Content-Type": "application/json",
          "x-usuario-email": usuario?.email || "",
          "x-cliente-id": usuario?.id_cliente || "",
        },
        body: JSON.stringify({
          nombre: connectNombre,
          descripcion: connectDescripcion,
          formato: connectFormato,
          separador: connectSeparador,
          orientacion: connectOrientacion,
          columnas: columnasValidas,
        }),
      });
      const data = await res.json();
      if (!data.ok) {
        alert(data.error || "No se pudo guardar plantilla");
        return;
      }
      await cargarConnect();
      setConnectPlantillaEditando(data.plantilla || null);
      setConnectColumnas(columnasValidas);
      alert(connectPlantillaEditando?.id ? "Plantilla ARGO Connect actualizada" : "Plantilla ARGO Connect guardada");
    } catch (err) {
      console.error(err);
      alert("Error guardando plantilla");
    }
  }

  async function exportarConnect(plantilla, formato = null) {
    try {
      const res = await fetch(`${API_BASE}/argo/connect/exportar`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-usuario-email": usuario?.email || "",
          "x-cliente-id": usuario?.id_cliente || "",
        },
        body: JSON.stringify({
          plantilla_id: plantilla.id,
          formato: formato || plantilla.formato,
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        alert(data.error || "No se pudo exportar");
        return;
      }
      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      const ext = formato || plantilla.formato || "xlsx";
      a.href = url;
      a.download = `${plantilla.nombre || "ARGO_CONNECT"}.${ext}`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      console.error(err);
      alert("Error exportando ARGO Connect");
    }
  }

  async function vistaPreviaConnect(plantilla) {
    try {
      const res = await fetch(`${API_BASE}/argo/connect/exportar`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-usuario-email": usuario?.email || "",
          "x-cliente-id": usuario?.id_cliente || "",
        },
        body: JSON.stringify({
          plantilla_id: plantilla.id,
          formato: "csv",
        }),
      });

      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        alert(data.error || "No se pudo generar vista previa");
        return;
      }

      const texto = await res.text();
      const lineas = texto.split(/\r?\n/).filter(Boolean).slice(0, 6);
      const filas = lineas.map((l) => l.split(","));

      setConnectPreview({
        nombre: plantilla.nombre || "Layout ARGO Connect",
        filas,
      });
    } catch (err) {
      console.error(err);
      alert("Error generando vista previa ARGO Connect");
    }
  }

  async function actualizarTenantGlobal(tenant, cambios = {}) {
    try {
      if (!tenant) {
        alert("Tenant requerido");
        return;
      }

      if (cambios.plan) {
        const resPlan = await fetch(`${API_BASE}/argo/admin/tenant/plan`, {
          method: "PATCH",
          headers: {
            "Content-Type": "application/json",
            "x-usuario-email": usuario?.email || "",
          },
          body: JSON.stringify({
            tenant,
            plan_saas: cambios.plan,
          }),
        });

        const dataPlan = await resPlan.json().catch(() => ({}));
        if (!resPlan.ok || !dataPlan.ok) {
          alert(dataPlan.error || "No se pudo actualizar plan tenant");
          return;
        }
      }

      if (cambios.estado_licencia || cambios.fecha_vencimiento) {
        const resLic = await fetch(`${API_BASE}/argo/admin/tenant/licencia`, {
          method: "PATCH",
          headers: {
            "Content-Type": "application/json",
            "x-usuario-email": usuario?.email || "",
          },
          body: JSON.stringify({
            tenant,
            estado_licencia: cambios.estado_licencia || "ACTIVA",
            fecha_vencimiento: cambios.fecha_vencimiento || "",
          }),
        });

        const dataLic = await resLic.json().catch(() => ({}));
        if (!resLic.ok || !dataLic.ok) {
          alert(dataLic.error || "No se pudo actualizar licencia tenant");
          return;
        }
      }

      await cargarMasterDashboard();
      alert("Tenant actualizado correctamente");
    } catch (err) {
      console.error(err);
      alert("Error actualizando tenant");
    }
  }

  async function crearUsuarioAdmin(e) {
    e.preventDefault();

    const nombre = String(adminNuevoUsuario.nombre || "").trim();
    const email = String(adminNuevoUsuario.email || "").trim().toLowerCase();
    const password = String(adminNuevoUsuario.password || "");
    const rol = String(adminNuevoUsuario.rol || "operador");

    if (!nombre || !email || !password) {
      alert("Nombre, email y password son obligatorios");
      return;
    }

    if (!email.includes("@")) {
      alert("Captura un email válido");
      return;
    }

    if (password.length < 6) {
      alert("El password debe tener al menos 6 caracteres");
      return;
    }

    setAdminCreandoUsuario(true);

    try {
      const res = await fetch(`${API_BASE}/argo/admin/crear_usuario`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-usuario-email": usuario?.email || "",
        },
        body: JSON.stringify({
          nombre,
          email,
          password,
          rol,
          activo: adminNuevoUsuario.activo !== false,
          cliente_id: usuario?.id_cliente || "",
        }),
      });

      const data = await res.json().catch(() => ({}));

      if (!res.ok || !data.ok) {
        alert(data.error || "No se pudo crear el usuario");
        return;
      }

      setAdminNuevoUsuario({
        nombre: "",
        email: "",
        password: "",
        rol: "operador",
        activo: true,
      });

      await cargarUsuariosAdmin();
      await cargarAuditoriaAdmin();

      if (esMaster) {
        await cargarMasterDashboard();
      }

      alert("Usuario creado correctamente");
    } catch (err) {
      console.error(err);
      alert("Error creando usuario");
    } finally {
      setAdminCreandoUsuario(false);
    }
  }

  async function cargarUsuariosAdmin() {
    try {
      const res = await fetch(`${API_BASE}/argo/admin/usuarios?cliente_id=${encodeURIComponent(usuario?.id_cliente || "")}`, {
        headers: {
          "x-usuario-email": usuario?.email || "",
        },
      });
      const data = await res.json();
      if (!data.ok) {
        alert(data.error || "No se pudieron cargar usuarios");
        return;
      }
      setAdminUsuarios(data.usuarios || []);
    } catch (err) {
      console.error(err);
      alert("Error cargando usuarios");
    }
  }

  async function cargarAuditoriaAdmin() {
    try {
      const res = await fetch(`${API_BASE}/argo/admin/activity_feed?limit=25`, {
        headers: {
          "x-usuario-email": usuario?.email || "",
        },
      });
      const data = await res.json();
      if (!data.ok) {
        alert(data.error || "No se pudo cargar auditoría");
        return;
      }
      setAdminAuditoria(data.logs || []);
    } catch (err) {
      console.error(err);
      alert("Error cargando auditoría");
    }
  }

  async function cambiarRolAdmin(email, rol) {
    try {
      const res = await fetch(`${API_BASE}/argo/admin/usuario/rol`, {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          "x-usuario-email": usuario?.email || "",
        },
        body: JSON.stringify({ email, rol }),
      });
      const data = await res.json();
      if (!data.ok) {
        alert(data.error || "No se pudo cambiar rol");
        return;
      }
      await cargarUsuariosAdmin();
      await cargarAuditoriaAdmin();
      alert("Rol actualizado");
    } catch (err) {
      console.error(err);
      alert("Error cambiando rol");
    }
  }

  async function cambiarEstadoUsuarioAdmin(email, activo) {
    try {
      const res = await fetch(`${API_BASE}/argo/admin/usuario/activo`, {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          "x-usuario-email": usuario?.email || "",
        },
        body: JSON.stringify({ email, activo }),
      });
      const data = await res.json();
      if (!data.ok) {
        alert(data.error || "No se pudo cambiar estado");
        return;
      }
      await cargarUsuariosAdmin();
      await cargarAuditoriaAdmin();
      alert("Estado actualizado");
    } catch (err) {
      console.error(err);
      alert("Error cambiando estado");
    }
  }

  async function eliminarAccesoAdmin(email) {
    if (!confirm(`Eliminar acceso enterprise a ${email}?`)) return;
    try {
      const res = await fetch(`${API_BASE}/argo/admin/usuario/eliminar_acceso`, {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          "x-usuario-email": usuario?.email || "",
        },
        body: JSON.stringify({ email }),
      });
      const data = await res.json();
      if (!data.ok) {
        alert(data.error || "No se pudo eliminar acceso");
        return;
      }
      await cargarUsuariosAdmin();
      await cargarAuditoriaAdmin();
      alert("Acceso eliminado");
    } catch (err) {
      console.error(err);
      alert("Error eliminando acceso");
    }
  }

  async function resetPasswordAdmin(email) {
    const password = prompt(`Nuevo password para ${email}`);
    if (!password) return;
    try {
      const res = await fetch(`${API_BASE}/argo/admin/usuario/reset_password`, {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          "x-usuario-email": usuario?.email || "",
        },
        body: JSON.stringify({ email, password }),
      });
      const data = await res.json();
      if (!data.ok) {
        alert(data.error || "No se pudo resetear password");
        return;
      }
      await cargarAuditoriaAdmin();
      alert("Password actualizado");
    } catch (err) {
      console.error(err);
      alert("Error reseteando password");
    }
  }

  function auditoriaFiltrada() {
    const texto = String(auditoriaFiltroTexto || "").toLowerCase().trim();

    return (adminAuditoria || []).filter((a) => {
      const accion = String(a.accion || "");
      const actor = String(a.actor_email || "");
      const objetivo = String(a.objetivo_email || "");
      const tenant = String(a.tenant || "");
      const detalle = JSON.stringify(a.detalle || {});

      if (auditoriaFiltroAccion && accion !== auditoriaFiltroAccion) return false;
      if (auditoriaFiltroActor && actor !== auditoriaFiltroActor) return false;

      if (texto) {
        const bolsa = `${accion} ${actor} ${objetivo} ${tenant} ${detalle}`.toLowerCase();
        if (!bolsa.includes(texto)) return false;
      }

      return true;
    });
  }

  function exportarAuditoriaCSV() {
    const filas = auditoriaFiltrada();
    if (!filas.length) {
      alert("No hay eventos para exportar");
      return;
    }

    const headers = ["fecha", "accion", "actor_email", "actor_rol", "tenant", "objetivo_email", "modulo"];
    const csv = [
      headers.join(","),
      ...filas.map((a) =>
        headers.map((h) => `"${String(a[h] ?? "").replaceAll('"', '""')}"`).join(",")
      ),
    ].join("\n");

    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "ARGO_AUDITORIA_ENTERPRISE.csv";
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  }

  async function cargarDashboard() {
    if (!usuario?.id_cliente) return;

    try {
      const res = await fetch(
        `${API_BASE}/argo/dashboard?cliente_id=${encodeURIComponent(usuario.id_cliente)}`,
        {
          headers: {
            "x-cliente-id": usuario.id_cliente,
            "x-usuario-email": usuario.email || "",
          },
        }
      );

      const data = await res.json();

      if (!data.ok) {
        setError(data.error || "No se pudo cargar dashboard");
        return;
      }

      setDashboard(data.dashboard || data);
    } catch {
      setError("Error cargando dashboard");
    }
  }

  function evaluarCalidadCaptura(canvas) {
    const ctx = canvas.getContext("2d");
    const width = canvas.width;
    const height = canvas.height;
    const frame = ctx.getImageData(0, 0, width, height);
    const data = frame.data;

    let brilloTotal = 0;
    let contrasteTotal = 0;
    let bordes = 0;
    let muestras = 0;

    for (let y = 0; y < height - 2; y += 6) {
      for (let x = 0; x < width - 2; x += 6) {
        const i = (y * width + x) * 4;
        const j = (y * width + (x + 2)) * 4;

        const gris = (data[i] + data[i + 1] + data[i + 2]) / 3;
        const gris2 = (data[j] + data[j + 1] + data[j + 2]) / 3;

        brilloTotal += gris;
        contrasteTotal += Math.abs(gris - gris2);

        if (Math.abs(gris - gris2) > 28) bordes++;
        muestras++;
      }
    }

    const brilloPromedio = brilloTotal / muestras;
    const contrastePromedio = contrasteTotal / muestras;
    const porcentajeBordes = bordes / muestras;

    const problemas = [];

    if (brilloPromedio < 55) problemas.push("La foto está muy oscura");
    if (brilloPromedio > 235) problemas.push("La foto está sobreexpuesta");
    if (contrastePromedio < 8) problemas.push("La foto parece borrosa o con poco contraste");
    if (porcentajeBordes < 0.015) problemas.push("No se detecta suficiente texto o detalle");

    return {
      ok: problemas.length === 0,
      brillo: Math.round(brilloPromedio),
      contraste: Math.round(contrastePromedio),
      detalle: Math.round(porcentajeBordes * 1000) / 10,
      problemas,
    };
  }

  async function procesarArchivo(file) {
    if (!file || !puede("camara_pro")) return;

    setProcesando(true);
    setLecturaMercancia(null);
    setLecturaConfirmada(false);
    setEditandoMercancia(false);
    setScanStatus("Analizando etiqueta de mercancía...");

    const formData = new FormData();
    formData.append("archivo1", file);

    try {
      const res = await fetch(`${API_BASE}/argo/ocr_mercancia`, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${sessionToken}`,
          "x-cliente-id": usuario?.id_cliente || "",
          "x-usuario-email": usuario?.email || "",
          "x-usuario-rol": usuario?.rol || "operador",
        },
        body: formData,
      });

      const data = await res.json().catch(() => ({}));

      if (!res.ok || !data.ok) {
        alert(
          "ERROR OCR MERCANCÍA: " +
          JSON.stringify(data)
        );
        setScanStatus("Error procesando etiqueta.");
        return;
      }

      const lectura = data.lectura || {};

      setLecturaMercancia(lectura);
      setLecturaConfirmada(false);
      setEditandoMercancia(false);

      if (data.requiere_revision_humana) {
        setScanStatus(
          "Lectura terminada. Revisa los campos marcados antes de confirmar."
        );
      } else {
        setScanStatus(
          "Lectura terminada. Verifica y confirma los datos."
        );
      }

    } catch (err) {
      console.error(err);
      alert("Error procesando fotografía de mercancía");
      setScanStatus("Error de conexión con Cámara PRO v2.");
    } finally {
      setProcesando(false);
    }
  }

  async function procesarMultiplesArchivos(files) {
    const MAX_ARCHIVOS = 100;
    const MAX_BYTES_ARCHIVO = 20 * 1024 * 1024;
    const MAX_BYTES_TOTALES = 500 * 1024 * 1024;

    const lista = Array.from(files || []);

    if (!lista.length || !puede("entrada_documental")) return;

    if (lista.length > MAX_ARCHIVOS) {
      alert(
        `Seleccionaste ${lista.length} archivos. ` +
        `El máximo por operación es ${MAX_ARCHIVOS}.`
      );
      return;
    }

    const archivosGrandes = lista.filter(
      (file) => Number(file?.size || 0) > MAX_BYTES_ARCHIVO
    );

    if (archivosGrandes.length) {
      alert(
        "Estos archivos superan 20 MB:\n" +
        archivosGrandes
          .slice(0, 10)
          .map((file) => `• ${file.name}`)
          .join("\n")
      );
      return;
    }

    const bytesTotales = lista.reduce(
      (total, file) => total + Number(file?.size || 0),
      0
    );

    if (bytesTotales > MAX_BYTES_TOTALES) {
      alert(
        "La selección completa supera el límite de 500 MB. " +
        "Divide la operación en dos cargas."
      );
      return;
    }

    setProcesando(true);
    setReporteEjecutivo(null);
    setResultadoCarga({
      archivosRecibidos: lista.length,
      archivosProcesados: 0,
      archivosConError: 0,
      bytesRecibidos: bytesTotales,
      estado: "Procesando OCR",
      errores: [],
    });
    setScanStatus(
      `Preparando ${lista.length} archivo` +
      `${lista.length === 1 ? "" : "s"}...`
    );

    const formData = new FormData();

    lista.forEach((file) => {
      formData.append("archivos", file, file.name);
    });

    try {
      setScanStatus(
        `Enviando y procesando ${lista.length} archivo` +
        `${lista.length === 1 ? "" : "s"}...`
      );

      const res = await fetch(`${API_BASE}/argo/ocr`, {
        method: "POST",
        headers: {
          "x-cliente-id": usuario?.id_cliente || "",
          "x-usuario-email": usuario?.email || "",
          "x-usuario-rol": usuario?.rol || "operador",
        },
        body: formData,
      });

      const data = await res.json().catch(() => ({}));

      if (!res.ok || !data.ok) {
        const mensajeOcr = data.error || "La operación OCR fue rechazada";

        setResultadoCarga({
          archivosRecibidos:
            Number(data.recibidos || data.total_archivos || lista.length),
          archivosProcesados: Number(data.procesados || 0),
          archivosConError:
            Number(
              data.fallidos ??
                (Array.isArray(data.errores) ? data.errores.length : 0)
            ) || lista.length,
          bytesRecibidos:
            Number(data.bytes_recibidos || bytesTotales),
          estado: "Error en OCR",
          errores:
            Array.isArray(data.errores) && data.errores.length
              ? data.errores
              : [
                  {
                    archivo: "Operación",
                    error: mensajeOcr,
                    codigo: data.codigo || "ERROR_OCR",
                  },
                ],
        });

        alert("ERROR OCR: " + mensajeOcr);
        return;
      }

      setScanStatus(
        `OCR terminado: ${data.procesados || 0} de ` +
        `${data.total_archivos || lista.length} procesados`
      );

      data.cliente_id = usuario?.id_cliente;
      data.cliente_nombre = usuario?.nombre;

      setResultadoCarga({
        archivosRecibidos: Number(
          data.total_archivos || lista.length
        ),
        archivosProcesados: Number(data.procesados || 0),
        archivosConError: Number(
          data.fallidos ??
            (Array.isArray(data.errores) ? data.errores.length : 0)
        ),
        bytesRecibidos: Number(
          data.bytes_recibidos || bytesTotales
        ),
        estado: "Consolidando operación",
        errores: Array.isArray(data.errores) ? data.errores : [],
      });

      const res2 = await fetch(
        `${API_BASE}/argo/procesar_desde_ocr`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-cliente-id": usuario?.id_cliente || "",
            "x-usuario-email": usuario?.email || "",
            "x-usuario-rol": usuario?.rol || "operador",
          },
          body: JSON.stringify(data),
        }
      );

      const data2 = await res2.json().catch(() => ({}));

      if (!res2.ok || !data2.ok) {
        const mensajeConsolidacion =
          data2.error || "No se pudo consolidar la operación";

        setResultadoCarga({
          archivosRecibidos: Number(
            data.total_archivos || lista.length
          ),
          archivosProcesados: Number(data.procesados || 0),
          archivosConError: Number(
            data.fallidos ??
              (Array.isArray(data.errores) ? data.errores.length : 0)
          ),
          bytesRecibidos: Number(
            data.bytes_recibidos || bytesTotales
          ),
          estado: "Error al consolidar",
          errores: [
            ...(Array.isArray(data.errores) ? data.errores : []),
            {
              archivo: "Operación",
              error: mensajeConsolidacion,
              codigo: data2.codigo || "ERROR_CONSOLIDACION",
            },
          ],
        });

        alert("ERROR: " + mensajeConsolidacion);
        return;
      }

      const procesados = Number(data.procesados || 0);
      const fallidos = Number(
        data.fallidos ??
        (Array.isArray(data.errores)
          ? data.errores.length
          : 0)
      );

      setResultadoCarga({
        archivosRecibidos: Number(
          data.total_archivos || lista.length
        ),
        archivosProcesados: procesados,
        archivosConError: fallidos,
        bytesRecibidos: Number(
          data.bytes_recibidos || bytesTotales
        ),
        estado:
          fallidos > 0
            ? "Finalizada con errores"
            : "Finalizada correctamente",
        errores: Array.isArray(data.errores) ? data.errores : [],
      });

      if (
        data2.reporte_ejecutivo?.storage?.signed_url ||
        data2.reporte_ejecutivo?.descarga
      ) {
        setReporteEjecutivo(data2.reporte_ejecutivo);
      }

      alert(
        "Operación creada correctamente\n\n" +
        `Archivos recibidos: ${data.total_archivos || lista.length}\n` +
        `Procesados: ${procesados}\n` +
        `Con error: ${fallidos}`
      );

      cargarDashboard();

      if (esMaster) {
        cargarMasterDashboard();
      }
    } catch (err) {
      console.error(err);

      setResultadoCarga({
        archivosRecibidos: lista.length,
        archivosProcesados: 0,
        archivosConError: lista.length,
        bytesRecibidos: bytesTotales,
        estado: "Error de conexión",
        errores: [
          {
            archivo: "Operación",
            error:
              err?.message ||
              "No fue posible completar la carga masiva",
            codigo: "ERROR_CONEXION",
          },
        ],
      });

      alert(
        "Error procesando la carga masiva. " +
        "Verifica la conexión e inténtalo nuevamente."
      );
    } finally {
      setProcesando(false);
      setScanStatus(
        camaraActiva
          ? "Listo para capturar"
          : "Listo para nueva operación"
      );
    }
  }

  async function iniciarCamara() {
    if (!puede("camara_pro")) {
      alert("Cámara PRO no está disponible para este plan o rol.");
      return;
    }

    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        video: {
          facingMode: "environment",
          width: { ideal: 1920 },
          height: { ideal: 1080 },
        },
        audio: false,
      });

      streamRef.current = stream;
      setCamaraActiva(true);
      setScanStatus("Cámara lista. Acomoda bien la etiqueta y captura.");
    } catch (err) {
      console.error(err);
      alert("No se pudo abrir la cámara");
    }
  }

  function detenerCamara() {
    if (streamRef.current) {
      streamRef.current.getTracks().forEach((track) => track.stop());
      streamRef.current = null;
    }

    setCamaraActiva(false);
    setScanStatus("Cámara apagada");
  }

  async function capturarFoto() {
    if (!videoRef.current || !canvasRef.current || procesando || !puede("camara_pro")) return;

    setLecturaMercancia(null);
    setLecturaConfirmada(false);
    setEditandoMercancia(false);

    const video = videoRef.current;
    const canvas = canvasRef.current;

    if (!video.videoWidth || !video.videoHeight) {
      alert("La cámara aún no está lista");
      return;
    }

    canvas.width = video.videoWidth;
    canvas.height = video.videoHeight;

    const ctx = canvas.getContext("2d");
    ctx.drawImage(video, 0, 0, canvas.width, canvas.height);

    const resultadoCalidad = evaluarCalidadCaptura(canvas);
    setCalidad(resultadoCalidad);

    canvas.toBlob(
      async (blob) => {
        if (!blob) {
          alert("No se pudo capturar la foto");
          return;
        }

        const url = URL.createObjectURL(blob);
        setPreview(url);

        if (!resultadoCalidad.ok) {
          setScanStatus("Foto rechazada por calidad. Repite la captura.");
          alert(
            "Foto no apta para OCR:\n\n" +
              resultadoCalidad.problemas.join("\n") +
              "\n\nRecomendación: acerca más la etiqueta, mejora la luz y mantén la cámara fija."
          );
          return;
        }

        const file = new File([blob], `argo-captura-${Date.now()}.jpg`, {
          type: "image/jpeg",
        });

        await procesarArchivo(file);
      },
      "image/jpeg",
      0.95
    );
  }


  function irA(id) {
    const el = document.getElementById(id);
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }

  function cerrarSesion() {
    detenerCamara();
    setUsuario(null);
    setDashboard(null);
    setReporteEjecutivo(null);
    setPreview(null);
    setCalidad(null);
    setError("");
  }

  function ModuloBloqueado({ titulo, descripcion }) {
    return (
      <section
        className="panel"
        style={{
          opacity: 0.68,
          border: "1px dashed #94a3b8",
          background: "#f8fafc",
        }}
      >
        <h2>🔒 {titulo}</h2>
        <p>{descripcion}</p>
        <p style={{ fontSize: "13px", color: "#64748b" }}>
          No disponible para plan {planUsuario} con rol {rolUsuario}.
        </p>
      </section>
    );
  }

  if (restaurandoSesion) {
    return (
      <div className="app">
        <div className="login-card">
          <div className="login-brand">
            <img src="/logo_argo.png" alt="ARGO" />
            <div>
              <h1>ARGO</h1>
              <p>Restaurando sesión...</p>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (!usuario) {
    return (
      <div className="app">
        <div className="login-card">
          <div className="login-brand">
            <img src="/logo_argo.png" alt="ARGO" />
            <div>
              <h1>ARGO</h1>
              <p>Acceso operativo enterprise</p>
            </div>
          </div>

          <form onSubmit={iniciarSesion}>
            <input
              type="email"
              value={login.email}
              onChange={(e) => setLogin({ ...login, email: e.target.value })}
              placeholder="Correo"
            />

            <div style={{ position: "relative" }}>
              <input
                type={mostrarPassword ? "text" : "password"}
                value={login.password}
                onChange={(e) => setLogin({ ...login, password: e.target.value })}
                placeholder="Password"
                style={{ width: "100%", paddingRight: "48px" }}
              />

              <button
                type="button"
                onClick={() => setMostrarPassword(!mostrarPassword)}
                aria-label={mostrarPassword ? "Ocultar contraseña" : "Mostrar contraseña"}
                title={mostrarPassword ? "Ocultar contraseña" : "Mostrar contraseña"}
                style={{
                  position: "absolute",
                  right: "10px",
                  top: "50%",
                  transform: "translateY(-50%)",
                  border: "none",
                  background: "transparent",
                  padding: "4px",
                  cursor: "pointer",
                  boxShadow: "none",
                  color: "#475569",
                  zIndex: 2,
                }}
              >
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z" /><circle cx="12" cy="12" r="3" /></svg>
              </button>
            </div>

            <button type="submit">Entrar a ARGO</button>
            <button
              type="button"
              onClick={() => alert("Solicita al administrador de tu empresa el restablecimiento de contraseña.")}
              style={{
                background: "transparent",
                color: "#2563eb",
                border: "none",
                boxShadow: "none",
                padding: "4px 0",
                cursor: "pointer",
                fontSize: "13px",
                width: "100%",
                flexBasis: "100%",
                textAlign: "center",
                marginTop: "4px",
              }}
            >
              Olvidé mi contraseña
            </button>
          </form>

          {error && <div className="error">{error}</div>}
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <header className="topbar">
        <div>
          <div className="brand-title">
            <img src="/logo_argo.png" alt="ARGO" />
            <div>
              <h1>ARGO Control Operativo</h1>
              <p>
                {usuario.nombre || usuario.email} · {usuario.id_cliente}
              </p>
            </div>
          </div>

          <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", marginTop: "8px" }}>
            <span
              style={{
                background: "#e0f2fe",
                color: "#075985",
                padding: "5px 10px",
                borderRadius: "999px",
                fontSize: "12px",
                fontWeight: 700,
              }}
            >
              PLAN {planUsuario}
            </span>

            <span
              style={{
                background: "#dcfce7",
                color: "#166534",
                padding: "5px 10px",
                borderRadius: "999px",
                fontSize: "12px",
                fontWeight: 700,
              }}
            >
              ROL {rolUsuario}
            </span>

            <span
              style={{
                background: "#f1f5f9",
                color: "#334155",
                padding: "5px 10px",
                borderRadius: "999px",
                fontSize: "12px",
                fontWeight: 700,
              }}
            >
              MÓDULOS {modulosPermitidos.length}
            </span>
          </div>
        </div>

        <button onClick={cerrarSesion}>Salir</button>
      </header>

      <div className="argo-layout-shell">
        <aside className="argo-sidebar">
          <div className="argo-sidebar-brand">
            <img src="/logo_argo.png" alt="ARGO" />
            <div>
              <h2>ARGO</h2>
              <p>Enterprise SaaS</p>
            </div>
          </div>

          <div className="argo-sidebar-group">
            <span>Operación</span>
            {puede("entrada_documental") && <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-entrada")}>📦 Entrada documental</button>}
            {puede("camara_pro") && <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-camara")}>📷 Cámara manual</button>}
            {puede("dashboard") && <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-dashboard")}>📊 Dashboard</button>}
          </div>

          {puede("analytics_pro") && (
            <div className="argo-sidebar-group">
              <span>Analytics PRO</span>
              <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-analytics-kpis")}>📈 KPIs ejecutivos</button>
              <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-analytics-riesgos")}>⚠️ Riesgos</button>
            </div>
          )}

            {(puede("admin_saas") || puede("auditoria")) && (
              <div className="argo-sidebar-group">
                <span>Administracion</span>
                {puede("admin_saas") && <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-admin-usuarios")}>Usuarios</button>}
                {puede("admin_saas") && <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-admin-permisos")}>Permisos</button>}
                {puede("auditoria") && <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-admin-auditoria")}>Auditoria</button>}
              </div>
            )}

            {(puede("aprobaciones") || puede("incidencias") || puede("reportes")) && (
              <div className="argo-sidebar-group">
                <span>Control operativo</span>
                {puede("aprobaciones") && <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-aprobaciones")}>Aprobaciones</button>}
                {puede("incidencias") && <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-incidencias")}>Incidencias</button>}
                {puede("reportes") && <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-reportes")}>Reportes</button>}
              </div>
            )}


            {puede("argo_connect") && (
              <div className="argo-sidebar-group">
                <span>ARGO Connect</span>
                <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-connect")}>Connect Layouts</button>
              </div>
            )}

          {usuario?.rol === "master_admin" && (
            <div className="argo-sidebar-group">
              <span>SaaS Global</span>
              <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-master")}>🌎 Multi-tenant</button>
              <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-master")}>💰 Revenue</button>
              <button type="button" className="argo-sidebar-item" onClick={() => irA("mod-master")}>🚀 Upgrades</button>
            </div>
          )}
        </aside>

        <main className="argo-main-content">

      {error && <div className="error">{error}</div>}

      <section className="panel">
        <h2>Módulos disponibles</h2>
        <p style={{ color: "#475569" }}>
          Acceso calculado por plan SaaS + rol operativo.
        </p>

        <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
          {modulosPermitidos.map((modulo) => (
            <span
              key={modulo}
              style={{
                background: "#f8fafc",
                border: "1px solid #cbd5e1",
                borderRadius: "999px",
                padding: "6px 10px",
                fontSize: "13px",
                width: "100%",
                flexBasis: "100%",
                textAlign: "center",
                marginTop: "4px",
              }}
            >
              {modulo}
            </span>
          ))}
        </div>
      </section>

      {puede("entrada_documental") ? (
        <section id="mod-entrada" className="panel">
          <h2>Entrada documental</h2>

          <input
            type="file"
            accept="image/*,.pdf"
            multiple
            onChange={(e) => procesarMultiplesArchivos(e.target.files)}
            disabled={procesando}
          />

          <p>
            Hasta 100 fotografías o documentos por operación.
            Máximo 20 MB por archivo y 500 MB por carga.
          </p>

          <div
            style={{
              marginTop: "18px",
              padding: "18px",
              border: "1px solid #dbeafe",
              borderRadius: "18px",
              background: "#f8fafc",
            }}
          >
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                gap: "12px",
                flexWrap: "wrap",
                marginBottom: "14px",
              }}
            >
              <div>
                <h3 style={{ margin: 0 }}>
                  Estado de la última operación
                </h3>
                <p
                  style={{
                    margin: "4px 0 0",
                    color: "#64748b",
                    fontSize: "13px",
                width: "100%",
                flexBasis: "100%",
                textAlign: "center",
                marginTop: "4px",
                  }}
                >
                  El panel permanece visible hasta la siguiente carga.
                </p>
              </div>

              <span
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  minHeight: "32px",
                  padding: "6px 12px",
                  borderRadius: "999px",
                  fontSize: "13px",
                width: "100%",
                flexBasis: "100%",
                textAlign: "center",
                marginTop: "4px",
                  fontWeight: 700,
                  background:
                    resultadoCarga.archivosConError > 0
                      ? "#fee2e2"
                      : resultadoCarga.estado.includes("Finalizada")
                      ? "#dcfce7"
                      : "#e0f2fe",
                  color:
                    resultadoCarga.archivosConError > 0
                      ? "#991b1b"
                      : resultadoCarga.estado.includes("Finalizada")
                      ? "#166534"
                      : "#075985",
                }}
              >
                {resultadoCarga.estado}
              </span>
            </div>

            <div
              style={{
                display: "grid",
                gridTemplateColumns:
                  "repeat(auto-fit, minmax(150px, 1fr))",
                gap: "12px",
              }}
            >
              {[
                {
                  etiqueta: "Archivos recibidos",
                  valor: resultadoCarga.archivosRecibidos,
                },
                {
                  etiqueta: "Procesados",
                  valor: resultadoCarga.archivosProcesados,
                },
                {
                  etiqueta: "Con error",
                  valor: resultadoCarga.archivosConError,
                },
                {
                  etiqueta: "Tamaño recibido",
                  valor:
                    resultadoCarga.bytesRecibidos > 0
                      ? `${(
                          resultadoCarga.bytesRecibidos /
                          (1024 * 1024)
                        ).toFixed(2)} MB`
                      : "0 MB",
                },
              ].map((indicador) => (
                <div
                  key={indicador.etiqueta}
                  style={{
                    padding: "14px",
                    border: "1px solid #e2e8f0",
                    borderRadius: "14px",
                    background: "#ffffff",
                  }}
                >
                  <div
                    style={{
                      color: "#64748b",
                      fontSize: "12px",
                      fontWeight: 700,
                      textTransform: "uppercase",
                      letterSpacing: "0.04em",
                    }}
                  >
                    {indicador.etiqueta}
                  </div>
                  <div
                    style={{
                      marginTop: "6px",
                      color: "#0f172a",
                      fontSize: "24px",
                      fontWeight: 800,
                    }}
                  >
                    {indicador.valor}
                  </div>
                </div>
              ))}
            </div>

            <div style={{ marginTop: "16px" }}>
              <h4 style={{ margin: "0 0 10px" }}>
                Detalle de errores
              </h4>

              {resultadoCarga.errores.length === 0 ? (
                <div
                  style={{
                    padding: "12px 14px",
                    borderRadius: "12px",
                    background: "#ecfdf5",
                    color: "#166534",
                  }}
                >
                  No hay errores registrados en la operación.
                </div>
              ) : (
                <div
                  style={{
                    display: "grid",
                    gap: "10px",
                    maxHeight: "260px",
                    overflowY: "auto",
                  }}
                >
                  {resultadoCarga.errores.map((item, index) => (
                    <div
                      key={`${item.archivo || "error"}-${index}`}
                      style={{
                        padding: "12px 14px",
                        border: "1px solid #fecaca",
                        borderRadius: "12px",
                        background: "#fff1f2",
                      }}
                    >
                      <strong>
                        {item.archivo || "Archivo sin nombre"}
                      </strong>
                      <div
                        style={{
                          marginTop: "4px",
                          color: "#9f1239",
                        }}
                      >
                        {item.error || "Error no especificado"}
                      </div>
                      {item.codigo && (
                        <div
                          style={{
                            marginTop: "4px",
                            color: "#64748b",
                            fontSize: "12px",
                          }}
                        >
                          Código: {item.codigo}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </section>
      ) : (
        <ModuloBloqueado
          titulo="Entrada documental"
          descripcion="Carga documental y OCR operativo."
        />
      )}



      {usuario?.rol === "master_admin" && masterDashboard?.saas && (
        <section id="mod-master" className="panel">
          <h2>Master Admin SaaS Executive</h2>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit,minmax(180px,1fr))",
              gap: "12px",
              marginTop: "12px",
            }}
          >

            <div className="metric-card">
              <strong>{masterDashboard.saas.tenants_totales}</strong>
              <p>Tenants</p>
            </div>

            <div className="metric-card">
              <strong>{masterDashboard.saas.tenants_activos}</strong>
              <p>Activos</p>
            </div>

            <div className="metric-card">
              <strong>{masterDashboard.saas.tenants_suspendidos}</strong>
              <p>Suspendidos</p>
            </div>

            <div className="metric-card">
              <strong>{masterDashboard.saas.usuarios_totales}</strong>
              <p>Usuarios</p>
            </div>

            <div className="metric-card">
              <strong>{masterDashboard.saas.operaciones_totales}</strong>
              <p>Operaciones</p>
            </div>

            <div className="metric-card">
              <strong>
                ${masterDashboard.saas.revenue_estimado_usd}
              </strong>
              <p>Revenue estimado</p>
            </div>

          </div>

          <div style={{ marginTop: "24px" }}>
            <h3>Distribución de planes</h3>

            <div
              style={{
                display: "flex",
                gap: "12px",
                flexWrap: "wrap",
                marginTop: "10px",
              }}
            >

              {Object.entries(masterDashboard.saas.planes || {}).map(
                ([plan, total]) => (
                  <div
                    key={plan}
                    style={{
                      border: "1px solid #cbd5e1",
                      borderRadius: "12px",
                      padding: "12px",
                      minWidth: "120px",
                    }}
                  >
                    <strong>{plan}</strong>
                    <p>{total} tenants</p>
                  </div>
                )
              )}

            </div>
          </div>

          <div style={{ marginTop: "24px" }}>
            <h3>Top tenants</h3>

            <div
              style={{
                display: "grid",
                gap: "10px",
              }}
            >

              {(masterDashboard.top_tenants || []).map((t, idx) => (
                <div
                  key={idx}
                  style={{
                    border: "1px solid #cbd5e1",
                    borderRadius: "12px",
                    padding: "12px",
                    background: "#fff",
                  }}
                >
                  <strong>{t.tenant}</strong>

                  <div>Plan actual: {t.plan}</div>
                  <div>Usuarios: {t.usuarios}</div>
                  <div>Operaciones: {t.operaciones_mes}</div>
                  <div>Licencia actual: {t.estado_licencia}</div>
                  <div>Vence: {t.fecha_vencimiento || "N/D"}</div>

                  <div style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(auto-fit,minmax(160px,1fr))",
                    gap: "8px",
                    marginTop: "12px"
                  }}>
                    <select
                      id={`tenant-plan-${idx}`}
                      defaultValue={t.plan || "ENTERPRISE"}
                      style={{ padding: "10px", borderRadius: "10px", border: "1px solid #cbd5e1" }}
                    >
                      <option value="BASIC">BASIC</option>
                      <option value="PRO">PRO</option>
                      <option value="ENTERPRISE">ENTERPRISE</option>
                      <option value="CUSTOM">CUSTOM</option>
                    </select>

                    <select
                      id={`tenant-lic-${idx}`}
                      defaultValue={t.estado_licencia || "ACTIVA"}
                      style={{ padding: "10px", borderRadius: "10px", border: "1px solid #cbd5e1" }}
                    >
                      <option value="ACTIVA">ACTIVA</option>
                      <option value="POR_VENCER">POR_VENCER</option>
                      <option value="SUSPENDIDA">SUSPENDIDA</option>
                      <option value="VENCIDA">VENCIDA</option>
                      <option value="BLOQUEADA">BLOQUEADA</option>
                      <option value="CANCELADA">CANCELADA</option>
                    </select>

                    <input
                      id={`tenant-venc-${idx}`}
                      type="date"
                      defaultValue={t.fecha_vencimiento || ""}
                      style={{ padding: "10px", borderRadius: "10px", border: "1px solid #cbd5e1" }}
                    />

                    <button
                      type="button"
                      onClick={() => actualizarTenantGlobal(t.tenant, {
                        plan: document.getElementById(`tenant-plan-${idx}`)?.value,
                        estado_licencia: document.getElementById(`tenant-lic-${idx}`)?.value,
                        fecha_vencimiento: document.getElementById(`tenant-venc-${idx}`)?.value,
                      })}
                      style={{ background: "#2563eb", color: "white", border: "none", borderRadius: "10px", padding: "10px" }}
                    >
                      Aplicar cambios
                    </button>
                  </div>
                </div>
              ))}

            </div>
          </div>

          <div style={{ marginTop: "24px" }}>
            <h3>Upgrade sugeridos</h3>

            <div
              style={{
                display: "grid",
                gap: "10px",
              }}
            >

              {(masterDashboard.upgrade_sugeridos || []).map((t, idx) => (
                <div
                  key={idx}
                  style={{
                    border: "1px solid #f59e0b",
                    borderRadius: "12px",
                    padding: "12px",
                    background: "#fffbeb",
                  }}
                >
                  <strong>{t.tenant}</strong>

                  <div>
                    {t.operaciones_mes} operaciones este mes
                  </div>

                  <div>
                    Plan actual: {t.plan}
                  </div>
                </div>
              ))}

            </div>
          </div>
        </section>
      )}



      {puede("dashboard") && (
        <section className="panel">
          <h2>Operaciones recientes</h2>

          {!dashboard?.operaciones?.length ? (
            <p>No hay operaciones registradas.</p>
          ) : (
            <div className="operaciones-list">
              {dashboard.operaciones.map((op, idx) => (
                <div
                  key={op.id_operacion || idx}
                  style={{
                    border: "1px solid #cbd5e1",
                    borderRadius: "12px",
                    padding: "14px",
                    marginBottom: "12px",
                    background: "#fff",
                  }}
                >
                  <div>
                    <strong>ID:</strong> {op.id_operacion}
                  </div>
                  <div>
                    <strong>Cliente:</strong> {op.cliente_nombre}
                  </div>
                  <div>
                    <strong>Estatus:</strong> {op.estatus_global}
                  </div>

                  <div>
                    <strong>Aprobada:</strong> {op.aprobada ? "✅ Sí" : "❌ No"}
                  </div>

                  {op.aprobada_por && (
                    <div>


                        <strong>Por:</strong> {op.aprobada_por}
                    </div>
                  )}

                  {op.fecha_aprobacion && (
                    <div>
                      <strong>Fecha:</strong> {op.fecha_aprobacion}
                    </div>
                  )}

                  {!op.aprobada && (rolUsuario === "supervisor" || esAdmin) && (
                    <button
                      style={{ marginTop: "10px" }}
                      onClick={() => aprobarOperacion(op.id_operacion)}
                    >
                      Aprobar operación
                    </button>
                  )}

              {op.reporte_ejecutivo?.descarga && (
                <a
                  href={op.reporte_ejecutivo.descarga}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{ textDecoration: "none" }}
                >
                  <button type="button" style={{ marginTop: "10px" }}>
                    Descargar reporte
                  </button>
                </a>
              )}
                </div>
              ))}
            </div>
          )}
        </section>
      )}

      {puede("incidencias") && (
        <section id="mod-incidencias" className="panel">
          <div style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            gap: "12px",
            flexWrap: "wrap",
            marginBottom: "14px",
          }}>
            <div>
              <h2 style={{ marginBottom: "4px" }}>Incidencias</h2>
              <p style={{ margin: 0, color: "#475569" }}>
                Gestión de incidencias críticas y seguimiento operativo.
              </p>
            </div>

            <button
              type="button"
              onClick={cargarIncidencias}
              disabled={incidenciasCargando}
              style={{
                background: "#0ea5e9",
                color: "white",
                border: "none",
                borderRadius: "12px",
                padding: "10px 14px",
              }}
            >
              {incidenciasCargando ? "Cargando..." : "Cargar incidencias"}
            </button>
          </div>

          {!incidencias.length ? (
            <p>No hay incidencias cargadas.</p>
          ) : (
            <div style={{ display: "grid", gap: "12px" }}>
              {incidencias.map((inc) => (
                <div
                  key={inc.id_operacion}
                  style={{
                    border: "1px solid #fecaca",
                    borderRadius: "14px",
                    padding: "14px",
                    background: "#fff7f7",
                  }}
                >
                  <div><strong>ID:</strong> {inc.id_operacion}</div>
                  <div><strong>Cliente:</strong> {inc.cliente || "N/D"}</div>
                  <div><strong>Operador:</strong> {inc.operador || "N/D"}</div>
                  <div><strong>Estado:</strong> {inc.estado || "N/D"}</div>
                  <div><strong>Riesgo:</strong> {inc.riesgo || "N/D"}</div>
                  <div><strong>Prioridad:</strong> {inc.prioridad || "N/D"}</div>

                  {inc.accion_sugerida && (
                    <div style={{ marginTop: "6px" }}>
                      <strong>Acción sugerida:</strong> {inc.accion_sugerida}
                    </div>
                  )}

                  {incidenciaEditando === inc.id_operacion ? (
                    <div style={{
                      marginTop: "12px",
                      display: "grid",
                      gap: "8px",
                      padding: "12px",
                      border: "1px solid #cbd5e1",
                      borderRadius: "12px",
                      background: "#fff",
                    }}>
                      <select
                        value={incidenciaForm.estado_incidencia}
                        onChange={(e) => setIncidenciaForm({
                          ...incidenciaForm,
                          estado_incidencia: e.target.value,
                        })}
                      >
                        <option value="ABIERTA">ABIERTA</option>
                        <option value="EN_REVISION">EN_REVISION</option>
                        <option value="RESUELTA">RESUELTA</option>
                        <option value="CERRADA">CERRADA</option>
                      </select>

                      <select
                        value={incidenciaForm.severidad}
                        onChange={(e) => setIncidenciaForm({
                          ...incidenciaForm,
                          severidad: e.target.value,
                        })}
                      >
                        <option value="BAJA">BAJA</option>
                        <option value="MEDIA">MEDIA</option>
                        <option value="ALTA">ALTA</option>
                        <option value="CRITICA">CRITICA</option>
                      </select>

                      <input
                        type="email"
                        placeholder="Asignado a"
                        value={incidenciaForm.asignado_a}
                        onChange={(e) => setIncidenciaForm({
                          ...incidenciaForm,
                          asignado_a: e.target.value,
                        })}
                      />

                      <textarea
                        placeholder="Comentario"
                        value={incidenciaForm.comentario}
                        onChange={(e) => setIncidenciaForm({
                          ...incidenciaForm,
                          comentario: e.target.value,
                        })}
                        rows={3}
                      />

                      <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                        <button
                          type="button"
                          onClick={() => guardarIncidencia(inc.id_operacion)}
                          style={{
                            background: "#16a34a",
                            color: "white",
                            border: "none",
                            borderRadius: "10px",
                            padding: "9px 12px",
                          }}
                        >
                          Guardar incidencia
                        </button>

                        <button
                          type="button"
                          onClick={() => setIncidenciaEditando(null)}
                          style={{
                            background: "#64748b",
                            color: "white",
                            border: "none",
                            borderRadius: "10px",
                            padding: "9px 12px",
                          }}
                        >
                          Cancelar
                        </button>
                      </div>
                    </div>
                  ) : (
                    <button
                      type="button"
                      onClick={() => {
                        setIncidenciaEditando(inc.id_operacion);
                        setIncidenciaForm({
                          estado_incidencia: "EN_REVISION",
                          severidad: "ALTA",
                          asignado_a: usuario?.email || "",
                          comentario: "",
                        });
                      }}
                      style={{
                        marginTop: "10px",
                        background: "#f59e0b",
                        color: "white",
                        border: "none",
                        borderRadius: "10px",
                        padding: "9px 12px",
                      }}
                    >
                      Gestionar incidencia
                    </button>
                  )}
                </div>
              ))}
            </div>
          )}
        </section>
      )}

      {puede("reportes") && (
        <section id="mod-reportes" className="panel">
          <div style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            gap: "12px",
            flexWrap: "wrap",
            marginBottom: "14px",
          }}>
            <div>
              <h2 style={{ marginBottom: "4px" }}>Centro de Reportes ARGO</h2>
              <p style={{ margin: 0, color: "#475569" }}>
                Reportes ejecutivos disponibles para las operaciones de tu tenant.
              </p>
            </div>

            <button
              type="button"
              onClick={cargarReportes}
              disabled={reportesCargando}
              style={{
                background: "#16a34a",
                color: "white",
                border: "none",
                borderRadius: "12px",
                padding: "10px 14px",
              }}
            >
              {reportesCargando ? "Cargando..." : "Cargar reportes"}
            </button>
          </div>

          {!reportes.length ? (
            <p>No hay reportes cargados.</p>
          ) : (
            <div style={{ display: "grid", gap: "12px" }}>
              {reportes.map((reporte) => (
                <div
                  key={reporte.id_operacion}
                  style={{
                    border: "1px solid #bbf7d0",
                    borderRadius: "14px",
                    padding: "14px",
                    background: "#f0fdf4",
                  }}
                >
                  <div>
                    <strong>Operación:</strong> {reporte.id_operacion}
                  </div>

                  <div>
                    <strong>Cliente:</strong>{" "}
                    {reporte.cliente_nombre || reporte.cliente_id || "N/D"}
                  </div>

                  <div>
                    <strong>Estatus:</strong>{" "}
                    {reporte.estatus_global || "N/D"}
                  </div>

                  <div>
                    <strong>Fecha:</strong>{" "}
                    {reporte.timestamp_local || "N/D"}
                  </div>

                  <div>
                    <strong>Archivo:</strong>{" "}
                    {reporte.archivo || "N/D"}
                  </div>

                  <div>
                    <strong>Storage:</strong>{" "}
                    {reporte.storage_disponible ? "Disponible" : "Local"}
                  </div>

                  <button
                    type="button"
                    onClick={() => descargarReporteProtegido(reporte)}
                    disabled={
                      reporteDescargando === reporte.id_operacion
                    }
                    style={{
                      marginTop: "10px",
                      background: "#0f766e",
                      color: "white",
                      border: "none",
                      borderRadius: "10px",
                      padding: "9px 12px",
                    }}
                  >
                    {reporteDescargando === reporte.id_operacion
                      ? "Descargando..."
                      : "Descargar reporte"}
                  </button>
                </div>
              ))}
            </div>
          )}
        </section>
      )}

      {(reporteEjecutivo?.storage?.signed_url || reporteEjecutivo?.descarga) && (
        <section className="panel">
          <h2>Reporte Ejecutivo ARGO</h2>
          <p>El reporte fue generado correctamente.</p>

          <a
            href={reporteEjecutivo?.storage?.signed_url || reporteEjecutivo?.descarga}
            target="_blank"
            rel="noopener noreferrer"
            style={{ textDecoration: "none" }}
          >
            <button type="button">Descargar Reporte Ejecutivo ARGO</button>
          </a>

          <p style={{ fontSize: "13px", color: "#475569" }}>
                width: "100%",
                flexBasis: "100%",
                textAlign: "center",
                marginTop: "4px",
            Archivo: {reporteEjecutivo.archivo}
          </p>
        </section>
      )}


        {puede("argo_connect") && esAdmin ? (
          <section id="mod-connect" className="panel" style={{ maxWidth: "1180px" }}>
            <h2>ARGO Connect V4</h2>
            <p style={{ color: "#475569" }}>
              Arma tu layout como una hoja de Excel: elige qué campo va en cada fila y guárdalo para reutilizarlo.
            </p>

            <div style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit,minmax(260px,1fr))",
              gap: "14px",
              marginTop: "18px",
              padding: "16px",
              border: "1px solid #dbeafe",
              borderRadius: "18px",
              background: "#f8fafc"
            }}>
              <div>
                <label style={{ fontWeight: "bold" }}>Nombre del layout</label>
                <input
                  value={connectNombre}
                  onChange={(e) => setConnectNombre(e.target.value)}
                  placeholder="Ej. SLAM CTL"
                  style={{ marginTop: "6px" }}
                />
              </div>

              <div>
                <label style={{ fontWeight: "bold" }}>Archivo de salida</label>
                <select
                  value={connectFormato}
                  onChange={(e) => setConnectFormato(e.target.value)}
                  style={{ width: "100%", padding: "14px", marginTop: "16px", borderRadius: "12px" }}
                >
                  <option value="xlsx">Excel XLSX</option>
                  <option value="csv">CSV</option>
                  <option value="txt">TXT</option>
                </select>
              </div>

              {(connectFormato === "csv" || connectFormato === "txt") && (
                <div>
                  <label style={{ fontWeight: "bold" }}>Separador</label>
                  <select
                    value={connectSeparador}
                    onChange={(e) => setConnectSeparador(e.target.value)}
                    style={{ width: "100%", padding: "14px", marginTop: "16px", borderRadius: "12px" }}
                  >
                    <option value=",">Coma (,)</option>
                    <option value=";">Punto y coma (;)</option>
                    <option value={"\\t"}>Tabulador</option>
                    <option value="|">Pipe (|)</option>
                  </select>
                </div>
              )}
            </div>

            <div style={{
              marginTop: "18px",
              border: "1px solid #cbd5e1",
              borderRadius: "18px",
              overflow: "hidden",
              background: "white"
            }}>
              <div style={{
                display: "grid",
                gridTemplateColumns: "110px 1fr 90px",
                background: "#0f172a",
                color: "white",
                fontWeight: "bold"
              }}>
                <div style={{ padding: "14px" }}>Posicion</div>
                <div style={{ padding: "14px" }}>Campo a exportar</div>
                <div style={{ padding: "14px", textAlign: "center" }}>Quitar</div>
              </div>

              {connectColumnas.length === 0 ? (
                <div style={{ padding: "18px", color: "#64748b" }}>
                  Selecciona los campos que quieres exportar.
                </div>
              ) : (
                connectColumnas.map((c, idx) => (
                  <div
                    key={`connect-v4-${idx}`}
                    style={{
                      display: "grid",
                      gridTemplateColumns: "110px 1fr 90px",
                      alignItems: "center",
                      borderTop: "1px solid #e2e8f0",
                      background: idx % 2 === 0 ? "#ffffff" : "#f8fafc"
                    }}
                  >
                    <div style={{ padding: "12px 14px", fontWeight: "bold" }}>
                      {idx + 1}
                    </div>

                    <div style={{ padding: "10px 14px" }}>
                      <select
                        value={c.campo || ""}
                        onChange={(e) => cambiarFilaConnect(idx, { campo: e.target.value })}
                        style={{ width: "100%", padding: "14px", borderRadius: "12px" }}
                      >
                        <option value="">Seleccionar campo ARGO...</option>
                        {(connectCatalogo || []).map((campo) => (
                          <option key={campo.campo} value={campo.campo}>
                            {campo.etiqueta} - {campo.grupo}
                          </option>
                        ))}
                      </select>
                    </div>

                    <div style={{ padding: "10px", textAlign: "center" }}>
                      <button
                        type="button"
                        onClick={() => quitarCampoConnect(idx)}
                        style={{ background: "#ef4444", boxShadow: "none", padding: "10px 12px", margin: 0 }}
                      >
                        X
                      </button>
                    </div>
                  </div>
                ))
              )}
            </div>

            <div style={{ display: "flex", gap: "10px", flexWrap: "wrap", marginTop: "16px" }}>
              <button type="button" onClick={agregarFilaConnect}>+ Agregar posicion</button>

              <button
                type="button"
                onClick={guardarPlantillaConnect}
                style={{ background: "#16a34a", boxShadow: "none" }}
              >
                {connectPlantillaEditando ? "Actualizar layout" : "Guardar layout"}
              </button>

              <button
                type="button"
                onClick={limpiarConnect}
                style={{ background: "#475569", boxShadow: "none" }}
              >
                Nuevo
              </button>
            </div>

            <div style={{
              marginTop: "22px",
              border: "1px solid #cbd5e1",
              borderRadius: "18px",
              padding: "16px",
              background: "#0f172a",
              color: "white"
            }}>
              <h3 style={{ marginTop: 0 }}>Layouts guardados</h3>

              {(connectPlantillas || []).length === 0 ? (
                <p>Aun no hay layouts guardados.</p>
              ) : (
                <div style={{ display: "grid", gap: "10px" }}>
                  {connectPlantillas.map((p) => (
                    <div key={p.id} style={{ background: "rgba(255,255,255,0.06)", borderRadius: "14px", padding: "12px" }}>
                      <strong>{p.nombre}</strong>
                      <div style={{ opacity: 0.8, fontSize: "13px", marginTop: "4px" }}>
                        Formato: {String(p.formato || "xlsx").toUpperCase()} - Posiciones: {(p.columnas || []).length}
                      </div>

                      <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", marginTop: "10px" }}>
                        <button type="button" onClick={() => editarPlantillaConnect(p)}>Editar</button>
                        <button type="button" onClick={() => vistaPreviaConnect(p)} style={{ background: "#0ea5e9", boxShadow: "none" }}>Vista previa</button>
                        <button type="button" onClick={() => exportarConnect(p, "xlsx")}>Excel</button>
                        <button type="button" onClick={() => exportarConnect(p, "csv")}>CSV</button>
                        <button type="button" onClick={() => exportarConnect(p, "txt")}>TXT</button>
                        <button type="button" onClick={() => eliminarPlantillaConnect(p)} style={{ background: "#ef4444", boxShadow: "none" }}>Eliminar</button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {connectPreview && (
              <div style={{
                marginTop: "18px",
                border: "1px solid #bae6fd",
                borderRadius: "18px",
                padding: "16px",
                background: "#f0f9ff",
                color: "#0f172a"
              }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: "10px", alignItems: "center", flexWrap: "wrap" }}>
                  <h3 style={{ margin: 0 }}>Vista previa: {connectPreview.nombre}</h3>
                  <button type="button" onClick={() => setConnectPreview(null)} style={{ background: "#475569", boxShadow: "none" }}>
                    Cerrar
                  </button>
                </div>

                <div style={{ overflowX: "auto", marginTop: "12px" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", background: "white" }}>
                    <tbody>
                      {(connectPreview.filas || []).map((fila, i) => (
                        <tr key={i}>
                          {(fila || []).map((celda, j) => (
                            <td key={j} style={{
                              border: "1px solid #cbd5e1",
                              padding: "8px",
                              fontWeight: i === 0 ? 700 : 400,
                              background: i === 0 ? "#e0f2fe" : "white"
                            }}>
                              {celda}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <p style={{ fontSize: "13px", color: "#475569", marginBottom: 0 }}>
                  Vista previa generada con las primeras filas del layout usando operaciones reales ARGO.
                </p>
              </div>
            )}
          </section>
        ) : null}

      {puede("camara_pro") ? (
        <section id="mod-camara" className="panel">
          <h2>ARGO Cámara PRO Manual</h2>

          <div className="camera-actions">
            {!camaraActiva ? (
              <button onClick={iniciarCamara} disabled={procesando}>
                Abrir cámara
              </button>
            ) : (
              <>
                <button onClick={capturarFoto} disabled={procesando}>
                  Capturar y validar
                </button>

                <button onClick={detenerCamara} disabled={procesando}>
                  Cerrar cámara
                </button>
              </>
            )}
          </div>

          <div className="scanner-status">
            {procesando ? "Procesando..." : scanStatus}
          </div>

          {camaraActiva && (
            <div className="camera-box">
              <video ref={videoRef} playsInline muted />
            </div>
          )}

          <canvas ref={canvasRef} style={{ display: "none" }} />
        </section>
      ) : (
        <ModuloBloqueado
          titulo="ARGO Cámara PRO Manual"
          descripcion="Captura manual premium con validación de calidad."
        />
      )}

      {preview && puede("camara_pro") && (
        <section className="panel">
          <h2>Última captura</h2>

          {calidad && (
            <div
              className="scanner-status"
              style={{
                background: calidad.ok ? "#dcfce7" : "#fee2e2",
                color: calidad.ok ? "#166534" : "#991b1b",
              }}
            >
              {calidad.ok
                ? "Calidad aceptada. Imagen enviada a OCR."
                : "Calidad rechazada. Repite la captura."}
              <br />
              Brillo: {calidad.brillo} · Contraste: {calidad.contraste} · Detalle:{" "}
              {calidad.detalle}%
              {!calidad.ok && (
                <>
                  <br />
                  {calidad.problemas.join(" · ")}
                </>
              )}
            </div>
          )}


          {lecturaMercancia && (
            <div
              style={{
                marginTop: "16px",
                marginBottom: "16px",
                padding: "16px",
                border: "1px solid #cbd5e1",
                borderRadius: "14px",
              }}
            >
              <h3 style={{ marginTop: 0 }}>
                Lectura de mercancía
              </h3>

              <div
                className="scanner-status"
                style={{
                  marginBottom: "14px",
                  background: lecturaConfirmada
                    ? "#dcfce7"
                    : "#fef9c3",
                  color: lecturaConfirmada
                    ? "#166534"
                    : "#854d0e",
                }}
              >
                {lecturaConfirmada
                  ? "Datos confirmados por el operador."
                  : "Verifica los datos contra la etiqueta física."}
              </div>

              {[
                ["marca", "Marca"],
                ["modelo", "Modelo"],
                ["numero_parte", "Número de parte"],
                ["cantidad_visible", "Cantidad visible"],
                ["unidad", "Unidad"],
                ["purchase_order", "Purchase Order"],
                ["partida", "Partida / línea"],
                ["lote", "Lote"],
                ["serie", "Serie"],
                ["pais_origen", "País de origen"],
                ["descripcion", "Descripción"],
              ].map(([campo, etiqueta]) => (
                <div
                  key={campo}
                  style={{
                    marginBottom: "10px",
                    paddingBottom: "8px",
                    borderBottom: "1px solid #e2e8f0",
                  }}
                >
                  <strong>{etiqueta}:</strong>{" "}

                  {editandoMercancia ? (
                    <input
                      value={lecturaMercancia?.[campo] ?? ""}
                      placeholder="No visible"
                      onChange={(e) =>
                        setLecturaMercancia((actual) => ({
                          ...(actual || {}),
                          [campo]:
                            e.target.value === ""
                              ? null
                              : e.target.value,
                        }))
                      }
                      style={{
                        width: "100%",
                        marginTop: "6px",
                        padding: "8px",
                        borderRadius: "8px",
                        border: "1px solid #94a3b8",
                      }}
                    />
                  ) : (
                    <span>
                      {lecturaMercancia?.[campo] ??
                        "No visible / no detectado"}
                    </span>
                  )}

                  {lecturaMercancia?.confianza?.[campo] != null && (
                    <div
                      style={{
                        fontSize: "12px",
                        marginTop: "3px",
                        opacity: 0.75,
                      }}
                    >
                      Confianza indicativa:{" "}
                      {Math.round(
                        Number(
                          lecturaMercancia.confianza[campo]
                        ) * 100
                      )}
                      %
                    </div>
                  )}
                </div>
              ))}

              {Array.isArray(
                lecturaMercancia.requiere_confirmacion
              ) &&
                lecturaMercancia.requiere_confirmacion.length > 0 && (
                  <div
                    className="scanner-status"
                    style={{
                      background: "#fee2e2",
                      color: "#991b1b",
                      marginTop: "12px",
                    }}
                  >
                    Requiere confirmación:{" "}
                    {lecturaMercancia.requiere_confirmacion.join(
                      ", "
                    )}
                  </div>
                )}

              {Array.isArray(
                lecturaMercancia.observaciones
              ) &&
                lecturaMercancia.observaciones.length > 0 && (
                  <div style={{ marginTop: "12px" }}>
                    <strong>Observaciones:</strong>
                    <br />
                    {lecturaMercancia.observaciones.join(" · ")}
                  </div>
                )}

              <div
                style={{
                  display: "flex",
                  flexWrap: "wrap",
                  gap: "8px",
                  marginTop: "16px",
                }}
              >
                <button
                  onClick={() => {
                    setLecturaConfirmada(true);
                    setEditandoMercancia(false);
                    setScanStatus(
                      "Datos confirmados por el operador."
                    );
                  }}
                >
                  Confirmar datos
                </button>

                <button
                  onClick={() => {
                    setEditandoMercancia(true);
                    setLecturaConfirmada(false);
                    setScanStatus(
                      "Modo corrección activo."
                    );
                  }}
                >
                  Corregir
                </button>

                <button
                  onClick={() => {
                    setLecturaMercancia(null);
                    setLecturaConfirmada(false);
                    setEditandoMercancia(false);
                    setPreview(null);
                    setCalidad(null);
                    setScanStatus(
                      "Listo para repetir captura."
                    );
                  }}
                >
                  Repetir foto
                </button>
              </div>

              <div
                style={{
                  fontSize: "12px",
                  marginTop: "12px",
                  opacity: 0.7,
                }}
              >
                Piloto Cámara PRO v2. La confirmación de esta
                fase todavía no registra una inspección definitiva.
              </div>
            </div>
          )}

          <img
            src={preview}
            alt="Última captura"
            style={{
              width: "100%",
              borderRadius: "14px",
              border: "1px solid #cbd5e1",
            }}
          />
        </section>
      )}






        {puede("dashboard") && (
          <section id="mod-dashboard" className="panel" style={{
            background: "linear-gradient(135deg,#020617 0%,#0f172a 55%,#1e293b 100%)",
            color: "white",
            border: "1px solid rgba(148,163,184,0.35)"
          }}>
            <h2>Dashboard</h2>
            <p style={{ color: "#cbd5e1" }}>
              Vista ejecutiva de operaciones, riesgos, aprobaciones y productividad del tenant.
            </p>

            <button onClick={cargarDashboard} disabled={procesando} style={{
              background: "#2563eb",
              color: "white",
              border: "none",
              borderRadius: "12px",
              padding: "12px 16px",
              fontWeight: "bold"
            }}>
              Actualizar dashboard
            </button>

            <div style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit,minmax(170px,1fr))",
              gap: "14px",
              marginTop: "18px"
            }}>
              <div style={{ background: "rgba(255,255,255,0.08)", border: "1px solid rgba(255,255,255,0.12)", borderRadius: "18px", padding: "16px" }}>
                <strong style={{ fontSize: "30px" }}>{dashboard?.resumen?.operaciones_total ?? 0}</strong>
                <p>Operaciones</p>
              </div>

              <div style={{ background: "rgba(22,163,74,0.16)", border: "1px solid rgba(22,163,74,0.35)", borderRadius: "18px", padding: "16px" }}>
                <strong style={{ fontSize: "30px", color: "#86efac" }}>{dashboard?.resumen?.operables ?? 0}</strong>
                <p>Operables</p>
              </div>

              <div style={{ background: "rgba(245,158,11,0.16)", border: "1px solid rgba(245,158,11,0.38)", borderRadius: "18px", padding: "16px" }}>
                <strong style={{ fontSize: "30px", color: "#fcd34d" }}>{dashboard?.resumen?.revision ?? 0}</strong>
                <p>En revision</p>
              </div>

              <div style={{ background: "rgba(239,68,68,0.16)", border: "1px solid rgba(239,68,68,0.38)", borderRadius: "18px", padding: "16px" }}>
                <strong style={{ fontSize: "30px", color: "#fca5a5" }}>{dashboard?.resumen?.criticas ?? 0}</strong>
                <p>Criticas</p>
              </div>
            </div>

            <div style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit,minmax(260px,1fr))",
              gap: "14px",
              marginTop: "18px"
            }}>
              <div style={{ background: "rgba(255,255,255,0.06)", borderRadius: "18px", padding: "16px" }}>
                <h3 style={{ marginTop: 0 }}>Semaforo operativo</h3>
                <div style={{ display: "grid", gap: "10px" }}>
                  <div style={{ display: "flex", justifyContent: "space-between", borderLeft: "6px solid #16a34a", paddingLeft: "10px" }}>
                    <span>Operable</span><strong>{dashboard?.resumen?.operables ?? 0}</strong>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between", borderLeft: "6px solid #f59e0b", paddingLeft: "10px" }}>
                    <span>Revision</span><strong>{dashboard?.resumen?.revision ?? 0}</strong>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between", borderLeft: "6px solid #ef4444", paddingLeft: "10px" }}>
                    <span>Critico</span><strong>{dashboard?.resumen?.criticas ?? 0}</strong>
                  </div>
                </div>
              </div>

              <div style={{ background: "rgba(255,255,255,0.06)", borderRadius: "18px", padding: "16px" }}>
                <h3 style={{ marginTop: 0 }}>Control operativo</h3>
                <p style={{ color: "#cbd5e1" }}>
                  Aprobaciones, incidencias y reportes integrados al dashboard para revision ejecutiva.
                </p>
                <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                  {puede("aprobaciones") && <span style={{ background: "#dbeafe", color: "#1e40af", borderRadius: "999px", padding: "6px 10px", fontWeight: 700 }}>Aprobaciones</span>}
                  {puede("incidencias") && <span style={{ background: "#fee2e2", color: "#991b1b", borderRadius: "999px", padding: "6px 10px", fontWeight: 700 }}>Incidencias</span>}
                  {puede("reportes") && <span style={{ background: "#dcfce7", color: "#166534", borderRadius: "999px", padding: "6px 10px", fontWeight: 700 }}>Reportes</span>}
                </div>
              </div>
            </div>
          </section>
        )}

        {puede("analytics_pro") ? (
          <section
            id="mod-analytics"
            className="panel"
            style={{
              background: "#f8fafc",
              border: "1px solid #cbd5e1"
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                gap: "12px",
                flexWrap: "wrap"
              }}
            >
              <div>
                <h2 id="mod-analytics-kpis" style={{ marginBottom: "4px" }}>
                  KPIs ejecutivos
                </h2>
                <p style={{ margin: 0, color: "#475569" }}>
                  Analytics operativo del tenant {usuario?.id_cliente || ""}.
                </p>
              </div>

              <button
                type="button"
                onClick={cargarDashboardPro}
                disabled={dashboardProCargando}
                style={{
                  background: "#2563eb",
                  color: "white"
                }}
              >
                {dashboardProCargando
                  ? "Actualizando Analytics..."
                  : "Actualizar Analytics"}
              </button>
            </div>

            {!dashboardPro ? (
              <div
                style={{
                  marginTop: "18px",
                  padding: "16px",
                  border: "1px solid #cbd5e1",
                  borderRadius: "14px",
                  background: "#fff"
                }}
              >
                Cargando Analytics PRO...
              </div>
            ) : (
              <>
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(auto-fit,minmax(190px,1fr))",
                    gap: "14px",
                    marginTop: "18px"
                  }}
                >
                  <div className="metric-card">
                    <strong>{dashboardPro?.kpis?.operaciones_total ?? 0}</strong>
                    <p>Operaciones</p>
                  </div>

                  <div className="metric-card">
                    <strong>{dashboardPro?.kpis?.operaciones_24h ?? 0}</strong>
                    <p>Últimas 24 horas</p>
                  </div>

                  <div className="metric-card">
                    <strong>{dashboardPro?.kpis?.operaciones_7d ?? 0}</strong>
                    <p>Últimos 7 días</p>
                  </div>

                  <div className="metric-card">
                    <strong>{dashboardPro?.kpis?.aprobadas ?? 0}</strong>
                    <p>Aprobadas</p>
                  </div>

                  <div className="metric-card">
                    <strong>{dashboardPro?.kpis?.operables ?? 0}</strong>
                    <p>Operables</p>
                  </div>

                  <div className="metric-card">
                    <strong>{dashboardPro?.kpis?.revision ?? 0}</strong>
                    <p>En revisión</p>
                  </div>

                  <div className="metric-card">
                    <strong>{dashboardPro?.kpis?.criticas ?? 0}</strong>
                    <p>Críticas</p>
                  </div>

                  <div className="metric-card">
                    <strong>{dashboardPro?.kpis?.incidencias_criticas ?? 0}</strong>
                    <p>Incidencias críticas</p>
                  </div>
                </div>

                <div style={{ marginTop: "26px" }}>
                  <h3>Ranking de operadores</h3>

                  {!(dashboardPro?.ranking_operadores || []).length ? (
                    <p>Sin operaciones con operador identificado.</p>
                  ) : (
                    <div style={{ display: "grid", gap: "10px" }}>
                      {(dashboardPro?.ranking_operadores || []).map((o, i) => (
                        <div
                          key={i}
                          style={{
                            border: "1px solid #cbd5e1",
                            borderRadius: "12px",
                            padding: "12px",
                            background: "#fff",
                            display: "flex",
                            justifyContent: "space-between",
                            gap: "12px"
                          }}
                        >
                          <strong>{o.operador || "N/D"}</strong>
                          <span>{o.operaciones ?? 0} operaciones</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                <div style={{ marginTop: "26px" }}>
                  <h3>Tendencia operativa</h3>

                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "repeat(auto-fit,minmax(180px,1fr))",
                      gap: "12px"
                    }}
                  >
                    <div className="metric-card">
                      <strong>{dashboardPro?.tendencias?.ultimas_24h ?? 0}</strong>
                      <p>Operaciones 24h</p>
                    </div>

                    <div className="metric-card">
                      <strong>{dashboardPro?.tendencias?.ultimos_7_dias ?? 0}</strong>
                      <p>Operaciones 7 días</p>
                    </div>
                  </div>
                </div>

                <div style={{ marginTop: "26px" }}>
                  <h3 id="mod-analytics-riesgos">Riesgos e incidencias críticas</h3>

                  {!(dashboardPro?.incidencias_criticas || []).length ? (
                    <p>Sin incidencias críticas.</p>
                  ) : (
                    <div style={{ display: "grid", gap: "10px" }}>
                      {(dashboardPro?.incidencias_criticas || [])
                        .slice(0, 12)
                        .map((r, i) => (
                          <div
                            key={r.id_operacion || i}
                            style={{
                              border: "1px solid #fecaca",
                              borderRadius: "12px",
                              padding: "12px",
                              background: "#fff"
                            }}
                          >
                            <strong>{r.id_operacion}</strong>
                            <div>Cliente: {r.cliente || "N/D"}</div>
                            <div>Operador: {r.operador || "N/D"}</div>
                            <div>Estado: {r.estado || "N/D"}</div>
                            <div>Riesgo: {r.riesgo || "N/D"}</div>
                            <div>Prioridad: {r.prioridad || "N/D"}</div>
                            <div>
                              Acción sugerida: {r.accion_sugerida || "N/D"}
                            </div>
                          </div>
                        ))}
                    </div>
                  )}
                </div>

                <div style={{ marginTop: "26px" }}>
                  <h3>Actividad reciente</h3>

                  {!(dashboardPro?.timeline_vivo || []).length ? (
                    <p>Sin actividad disponible.</p>
                  ) : (
                    <div style={{ display: "grid", gap: "10px" }}>
                      {(dashboardPro?.timeline_vivo || [])
                        .slice(0, 12)
                        .map((e, i) => (
                          <div
                            key={e.id_operacion || i}
                            style={{
                              border: "1px solid #cbd5e1",
                              borderRadius: "12px",
                              padding: "12px",
                              background: "#fff"
                            }}
                          >
                            <strong>{e.id_operacion}</strong>
                            <div>{e.cliente || "N/D"}</div>
                            <div>Operador: {e.operador || "N/D"}</div>
                            <div>Estado: {e.estado || "N/D"}</div>
                            <div>Fecha: {e.fecha || "N/D"}</div>
                          </div>
                        ))}
                    </div>
                  )}
                </div>

                <p
                  style={{
                    marginTop: "20px",
                    fontSize: "13px",
                    color: "#64748b"
                  }}
                >
                  Generado: {dashboardPro?.generado_en || "N/D"}
                </p>
              </>
            )}
          </section>
        ) : (
          <ModuloBloqueado
            titulo="Dashboard Ejecutivo PRO"
            descripcion="KPIs avanzados disponibles desde plan PRO."
          />
        )}

        {puede("admin_saas") && esAdmin ? (
          <section id="mod-admin" className="panel" style={{
            background: "linear-gradient(135deg,#020617 0%,#111827 65%,#1f2937 100%)",
            color: "white",
            border: "1px solid rgba(148,163,184,0.35)"
          }}>
            <h2>Admin SaaS Enterprise</h2>
            <p style={{ color: "#cbd5e1" }}>
              Administracion visual del tenant: usuarios, permisos, auditoria, licencias y control operativo.
            </p>

            <div style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit,minmax(210px,1fr))",
              gap: "14px",
              marginTop: "18px"
            }}>
              <div style={{ background: "rgba(255,255,255,0.08)", borderRadius: "18px", padding: "16px", border: "1px solid rgba(255,255,255,0.12)" }}>
                <h3 id="mod-admin-usuarios" style={{ marginTop: 0 }}>Usuarios del tenant</h3>
                <p style={{ color: "#cbd5e1" }}>Roles, estado, reset password y eliminación controlada.</p>

                <form
                  onSubmit={crearUsuarioAdmin}
                  style={{
                    display: "grid",
                    gap: "9px",
                    marginBottom: "16px",
                    padding: "12px",
                    borderRadius: "14px",
                    border: "1px solid rgba(96,165,250,0.45)",
                    background: "rgba(15,23,42,0.72)",
                  }}
                >
                  <strong>Crear nuevo usuario</strong>

                  <input
                    type="text"
                    placeholder="Nombre completo"
                    value={adminNuevoUsuario.nombre}
                    onChange={(e) => setAdminNuevoUsuario({
                      ...adminNuevoUsuario,
                      nombre: e.target.value,
                    })}
                    style={{
                      padding: "10px",
                      borderRadius: "10px",
                      border: "1px solid #475569",
                    }}
                  />

                  <input
                    type="email"
                    placeholder="correo@empresa.com"
                    value={adminNuevoUsuario.email}
                    onChange={(e) => setAdminNuevoUsuario({
                      ...adminNuevoUsuario,
                      email: e.target.value,
                    })}
                    style={{
                      padding: "10px",
                      borderRadius: "10px",
                      border: "1px solid #475569",
                    }}
                  />

                  <input
                    type="password"
                    placeholder="Password inicial"
                    value={adminNuevoUsuario.password}
                    onChange={(e) => setAdminNuevoUsuario({
                      ...adminNuevoUsuario,
                      password: e.target.value,
                    })}
                    style={{
                      padding: "10px",
                      borderRadius: "10px",
                      border: "1px solid #475569",
                    }}
                  />

                  <select
                    value={adminNuevoUsuario.rol}
                    onChange={(e) => setAdminNuevoUsuario({
                      ...adminNuevoUsuario,
                      rol: e.target.value,
                    })}
                    style={{
                      padding: "10px",
                      borderRadius: "10px",
                      border: "1px solid #475569",
                    }}
                  >
                    <option value="operador">Operador</option>
                    <option value="supervisor">Supervisor</option>
                    <option value="admin_cliente">Admin cliente</option>
                  </select>

                  <label style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "8px",
                    color: "#cbd5e1",
                    fontSize: "14px",
                  }}>
                    <input
                      type="checkbox"
                      checked={adminNuevoUsuario.activo !== false}
                      onChange={(e) => setAdminNuevoUsuario({
                        ...adminNuevoUsuario,
                        activo: e.target.checked,
                      })}
                    />
                    Usuario activo
                  </label>

                  <button
                    type="submit"
                    disabled={adminCreandoUsuario}
                    style={{
                      background: adminCreandoUsuario ? "#475569" : "#16a34a",
                      color: "white",
                      border: "none",
                      borderRadius: "12px",
                      padding: "10px 14px",
                      cursor: adminCreandoUsuario ? "wait" : "pointer",
                    }}
                  >
                    {adminCreandoUsuario
                      ? "Creando usuario..."
                      : "Crear usuario"}
                  </button>
                </form>

                <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", marginBottom: "12px" }}>
                  <button type="button" onClick={cargarUsuariosAdmin} style={{ background: "#2563eb", color: "white", border: "none", borderRadius: "12px", padding: "10px 14px" }}>
                    Cargar usuarios
                  </button>
                  <button type="button" onClick={cargarAuditoriaAdmin} style={{ background: "#0ea5e9", color: "white", border: "none", borderRadius: "12px", padding: "10px 14px" }}>
                    Historial permisos
                  </button>
                </div>

                <div style={{ display: "grid", gap: "10px" }}>
                  {(adminUsuarios || []).map((u, idx) => (
                    <div key={u.email || idx} style={{ background: "rgba(15,23,42,0.75)", border: "1px solid rgba(148,163,184,0.35)", borderRadius: "14px", padding: "12px" }}>
                      <strong>{u.nombre || u.email}</strong>
                      <div style={{ color: "#cbd5e1", fontSize: "13px" }}>{u.email}</div>
                      <div style={{ color: u.activo === false ? "#fca5a5" : "#86efac", fontSize: "13px", marginTop: "4px" }}>
                        {u.activo === false ? "INACTIVO" : "ACTIVO"} · {u.rol}
                      </div>

                      <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", marginTop: "10px" }}>
                        {["operador", "supervisor", "admin_cliente"].map((rolOpcion) => (
                          <button
                            key={rolOpcion}
                            type="button"
                            onClick={() => cambiarRolAdmin(u.email, rolOpcion)}
                            style={{
                              background: u.rol === rolOpcion ? "#16a34a" : "#334155",
                              color: "white",
                              border: "none",
                              borderRadius: "10px",
                              padding: "8px 10px",
                              fontSize: "13px"
                            }}
                          >
                            {rolOpcion}
                          </button>
                        ))}

                        <button type="button" onClick={() => cambiarEstadoUsuarioAdmin(u.email, !(u.activo !== false))} style={{ background: u.activo === false ? "#16a34a" : "#f59e0b", color: "white", border: "none", borderRadius: "10px", padding: "8px 10px" }}>
                          {u.activo === false ? "Activar" : "Desactivar"}
                        </button>

                        <button type="button" onClick={() => resetPasswordAdmin(u.email)} style={{ background: "#6366f1", color: "white", border: "none", borderRadius: "10px", padding: "8px 10px" }}>
                          Reset password
                        </button>

                        <button type="button" onClick={() => eliminarAccesoAdmin(u.email)} style={{ background: "#ef4444", color: "white", border: "none", borderRadius: "10px", padding: "8px 10px" }}>
                          Eliminar acceso
                        </button>
                      </div>
                    </div>
                  ))}
                </div>

                {(adminAuditoria || []).length > 0 && (
                  <div style={{ marginTop: "14px", borderTop: "1px solid rgba(148,163,184,0.35)", paddingTop: "12px" }}>
                    <h4 style={{ margin: "0 0 8px" }}>Historial de permisos</h4>
                    {(adminAuditoria || []).slice(0, 8).map((a, idx) => (
                      <div key={idx} style={{ fontSize: "13px", color: "#cbd5e1", marginBottom: "6px" }}>
                        <strong>{a.accion}</strong> · {a.actor_email || "sistema"} → {a.objetivo_email || a.tenant || "N/D"}
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div style={{ background: "rgba(255,255,255,0.08)", borderRadius: "18px", padding: "16px", border: "1px solid rgba(255,255,255,0.12)", gridColumn: "1 / -1" }}>
                <h3 id="mod-admin-permisos" style={{ marginTop: 0 }}>Permisos por módulo</h3>
                <p style={{ color: "#cbd5e1" }}>Control por plan, rol y modulo SaaS.</p>
                <div style={{ display: "flex", gap: "6px", flexWrap: "wrap" }}>
                  {modulosPermitidos.map((m) => (
                    <span key={m} style={{ background: "#1e293b", border: "1px solid #334155", borderRadius: "999px", padding: "5px 8px", fontSize: "12px" }}>
                      {m}
                    </span>
                  ))}
                </div>
              </div>

              <div style={{ background: "rgba(255,255,255,0.08)", borderRadius: "18px", padding: "16px", border: "1px solid rgba(255,255,255,0.12)" }}>
                <h3 id="mod-admin-auditoria" style={{ marginTop: 0 }}>Auditoría Enterprise</h3>
                <p style={{ color: "#cbd5e1" }}>Filtros, búsqueda, timeline visual y exportación.</p>

                <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", marginBottom: "12px" }}>
                  <button type="button" onClick={cargarAuditoriaAdmin} style={{ background: "#0ea5e9", color: "white", border: "none", borderRadius: "12px", padding: "10px 14px" }}>
                    Cargar auditoría
                  </button>
                  <button type="button" onClick={exportarAuditoriaCSV} style={{ background: "#16a34a", color: "white", border: "none", borderRadius: "12px", padding: "10px 14px" }}>
                    Exportar CSV
                  </button>
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(150px,1fr))", gap: "8px", marginBottom: "12px" }}>
                  <input
                    placeholder="Buscar evento..."
                    value={auditoriaFiltroTexto}
                    onChange={(e) => setAuditoriaFiltroTexto(e.target.value)}
                    style={{ padding: "9px", borderRadius: "10px", border: "1px solid #334155" }}
                  />

                  <select
                    value={auditoriaFiltroAccion}
                    onChange={(e) => setAuditoriaFiltroAccion(e.target.value)}
                    style={{ padding: "9px", borderRadius: "10px", border: "1px solid #334155" }}
                  >
                    <option value="">Todas las acciones</option>
                    {[...new Set((adminAuditoria || []).map((a) => a.accion).filter(Boolean))].map((accion) => (
                      <option key={accion} value={accion}>{accion}</option>
                    ))}
                  </select>

                  <select
                    value={auditoriaFiltroActor}
                    onChange={(e) => setAuditoriaFiltroActor(e.target.value)}
                    style={{ padding: "9px", borderRadius: "10px", border: "1px solid #334155" }}
                  >
                    <option value="">Todos los actores</option>
                    {[...new Set((adminAuditoria || []).map((a) => a.actor_email).filter(Boolean))].map((actor) => (
                      <option key={actor} value={actor}>{actor}</option>
                    ))}
                  </select>
                </div>

                <div style={{ display: "grid", gap: "8px", maxHeight: "360px", overflowY: "auto" }}>
                  {auditoriaFiltrada().length === 0 ? (
                    <div style={{ color: "#cbd5e1" }}>Sin eventos para mostrar.</div>
                  ) : (
                    auditoriaFiltrada().slice(0, 25).map((a, idx) => (
                      <div key={idx} style={{
                        borderLeft: a.modulo === "DASHBOARD_PRO" ? "5px solid #f59e0b" : "5px solid #38bdf8",
                        background: "rgba(15,23,42,0.72)",
                        borderRadius: "12px",
                        padding: "10px"
                      }}>
                        <strong>{a.accion || "EVENTO"}</strong>
                        <div style={{ color: "#cbd5e1", fontSize: "13px" }}>
                          {a.fecha || "Sin fecha"}
                        </div>
                        <div style={{ color: "#cbd5e1", fontSize: "13px" }}>
                          Actor: {a.actor_email || "sistema"}
                        </div>
                        <div style={{ color: "#cbd5e1", fontSize: "13px" }}>
                          Objetivo: {a.objetivo_email || a.tenant || "N/D"}
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>

              <div style={{ background: "rgba(255,255,255,0.08)", borderRadius: "18px", padding: "16px", border: "1px solid rgba(255,255,255,0.12)" }}>
                <h3 style={{ marginTop: 0 }}>Billing & Licencias</h3>
                <p style={{ color: "#cbd5e1" }}>Estado SaaS, renovacion y suspension operativa.</p>
                <div style={{ display: "grid", gap: "8px" }}>
                  <div style={{ background: "#052e16", color: "#86efac", borderRadius: "12px", padding: "8px" }}>ACTIVA</div>
                  <div style={{ background: "#451a03", color: "#fcd34d", borderRadius: "12px", padding: "8px" }}>POR VENCER</div>
                  <div style={{ background: "#450a0a", color: "#fca5a5", borderRadius: "12px", padding: "8px" }}>SUSPENDIDA</div>
                </div>
              </div>
            </div>
          </section>
        ) : (
          <ModuloBloqueado
            titulo="Admin SaaS"
            descripcion="Administracion de usuarios, permisos y auditoria."
          />
        )}
          </main>
        </div>
      </div>
    );
  }
export default App;
