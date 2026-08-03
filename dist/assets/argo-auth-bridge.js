/*
 * ARGO_AUTH_BRIDGE
 *
 * Puente transitorio para el frontend compilado:
 * 1. Captura session_token en la respuesta de /argo/login.
 * 2. Conserva el token en sessionStorage.
 * 3. Agrega Authorization: Bearer a solicitudes posteriores.
 * 4. No modifica el bundle React compilado.
 */

(function instalarArgoAuthBridge() {
    "use strict";

    const TOKEN_KEY = "argo_session_token";
    const LOGIN_PATH = "/argo/login";
    const originalFetch = window.fetch.bind(window);

    function obtenerUrl(input) {
        if (typeof input === "string") {
            return input;
        }

        if (input instanceof URL) {
            return input.toString();
        }

        if (input instanceof Request) {
            return input.url;
        }

        return String(input || "");
    }

    function esLogin(url) {
        try {
            return new URL(url, window.location.origin).pathname === LOGIN_PATH;
        } catch (_error) {
            return false;
        }
    }

    function construirSolicitudConToken(input, init, token) {
        const headersOriginales =
            init && init.headers
                ? init.headers
                : input instanceof Request
                  ? input.headers
                  : undefined;

        const headers = new Headers(headersOriginales);
        headers.set("Authorization", `Bearer ${token}`);

        if (input instanceof Request) {
            return {
                input: new Request(input, {
                    ...(init || {}),
                    headers,
                }),
                init: undefined,
            };
        }

        return {
            input,
            init: {
                ...(init || {}),
                headers,
            },
        };
    }

    window.fetch = async function argoFetchSeguro(input, init) {
        const url = obtenerUrl(input);
        const solicitudLogin = esLogin(url);
        const token = sessionStorage.getItem(TOKEN_KEY);

        let solicitud = {
            input,
            init,
        };

        if (token && !solicitudLogin) {
            solicitud = construirSolicitudConToken(
                input,
                init,
                token,
            );
        }

        const response = await originalFetch(
            solicitud.input,
            solicitud.init,
        );

        /*
         * Se procesa la copia antes de entregar la respuesta a React.
         * Así el token ya está almacenado cuando React cambia al dashboard
         * y ejecuta sus solicitudes posteriores.
         */
        if (solicitudLogin) {
            try {
                const datos = await response.clone().json();

                if (
                    response.ok &&
                    datos &&
                    datos.ok === true &&
                    typeof datos.session_token === "string" &&
                    datos.session_token.length > 20
                ) {
                    sessionStorage.setItem(
                        TOKEN_KEY,
                        datos.session_token,
                    );
                } else if (datos && datos.ok === false) {
                    sessionStorage.removeItem(TOKEN_KEY);
                }
            } catch (_error) {
                /*
                 * La respuesta original sigue llegando intacta a React.
                 * No se interrumpe el login por un error al leer la copia.
                 */
            }
        }

        return response;
    };

    window.ARGO_AUTH_BRIDGE = Object.freeze({
        version: "1.0.0",

        tieneSesion: function tieneSesion() {
            return Boolean(sessionStorage.getItem(TOKEN_KEY));
        },

        cerrarSesion: function cerrarSesion() {
            sessionStorage.removeItem(TOKEN_KEY);
        },
    });
})();
