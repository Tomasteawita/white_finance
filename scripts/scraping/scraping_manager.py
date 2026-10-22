"""
Módulo de Automatización Web Scraping (Balanz Productores & Brokers)
====================================================================
QUÉ HACE:
    Automatiza la extracción y descarga masiva de estados de Cuenta Corriente (.xlsx)
    desde el portal de productores de Balanz (https://productores.balanz.com/) para
    todos los clientes registrados en `data/balanz/`.

POR QUÉ (Lógica de Negocio y Trazabilidad CNV):
    La Cuenta Corriente es el registro contable y operativo que refleja exclusivamente
    los movimientos de liquidez de cada inversor (suscripciones/rescates de FCIs,
    boletos, cupones, recibos de cobro y comprobantes de pago). Automatizar su descarga
    elimina errores operativos manuales, garantiza la integridad de la ingesta histórica
    (`Cuenta Corriente-YYYYMMDD.xlsx`) y mantiene trazabilidad completa en
    `logs/balanz_pipelines.log` para auditoría normativa.
"""

import argparse
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from playwright.sync_api import Browser, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


class ScrapingManager:
    """
    Gestor centralizado de Web Scraping para descarga de Cuentas Corrientes y Cotizaciones.
    """

    def __init__(self, base_path: Optional[Path] = None) -> None:
        self.base_path: Path = base_path or Path(__file__).resolve().parent.parent.parent
        self.data_balanz_dir: Path = self.base_path / "data" / "balanz"
        self.clients_map_path: Path = self.data_balanz_dir / "clients_search_map.json"
        self.logs_dir: Path = self.base_path / "logs"
        self.downloads_dir: Path = Path.home() / "Downloads"

        self.balanz_url: str = "https://productores.balanz.com/"
        self.bullmarket_url: str = "https://inversiones.bullmarket.com.ar/Security/SignIn"

        self.logger: logging.Logger = self._setup_logger()
        load_dotenv(self.base_path / ".env")

    def _setup_logger(self) -> logging.Logger:
        """
        Configura logging dual (consola + archivo `logs/balanz_pipelines.log`)
        con timestamps ISO para trazabilidad de auditoría CNV.
        """
        if hasattr(sys.stdout, "reconfigure"):
            try:
                sys.stdout.reconfigure(encoding="utf-8", errors="replace")
                sys.stderr.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

        self.logs_dir.mkdir(parents=True, exist_ok=True)
        logger = logging.getLogger("BalanzScrapingManager")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            file_handler = logging.FileHandler(
                self.logs_dir / "balanz_pipelines.log", encoding="utf-8"
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    @staticmethod
    def validate_and_parse_date(date_str: str) -> Tuple[str, datetime]:
        """
        Valida la fecha de inicio ingresada (soporta YYYY-MM-DD o DD/MM/YYYY) y
        retorna una tupla con (fecha_dd_mm_yyyy, objeto_datetime).

        Falla ruidosamente si el formato es inválido o si la fecha es futura.
        """
        if not date_str or not isinstance(date_str, str):
            raise ValueError("La fecha 'desde' no puede ser nula ni vacía.")

        clean_str = date_str.strip()
        parsed_dt: Optional[datetime] = None

        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
            try:
                parsed_dt = datetime.strptime(clean_str, fmt)
                break
            except ValueError:
                continue

        if parsed_dt is None:
            raise ValueError(
                f"Formato de fecha inválido: '{date_str}'. Use 'YYYY-MM-DD' o 'DD/MM/YYYY'."
            )

        today = datetime.now()
        if parsed_dt.date() > today.date():
            raise ValueError(
                f"La fecha 'desde' ({parsed_dt.strftime('%Y-%m-%d')}) no puede ser posterior al día de hoy ({today.strftime('%Y-%m-%d')})."
            )

        return parsed_dt.strftime("%d/%m/%Y"), parsed_dt

    def load_client_search_map(self) -> Dict[str, str]:
        """
        Carga el diccionario de mapeo opcional `data/balanz/clients_search_map.json`
        que vincula el nombre de carpeta del cliente con el término de búsqueda en Balanz.
        """
        if not self.clients_map_path.exists():
            return {}

        try:
            with open(self.clients_map_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return {str(k): str(v) for k, v in data.items()}
                return {}
        except Exception as exc:
            self.logger.warning(
                f"No se pudo leer {self.clients_map_path}: {exc}. Se usará formateo estándar."
            )
            return {}

    def get_balanz_clients(
        self, selected_clients: Optional[List[str]] = None
    ) -> List[Tuple[str, str, Path]]:
        """
        Obtiene la lista de clientes a procesar desde `data/balanz/`.

        Retorna una lista de tuplas:
            (folder_name, search_query_title, client_cuenta_corriente_dir)
        """
        if not self.data_balanz_dir.exists():
            raise FileNotFoundError(
                f"El directorio de clientes Balanz no existe: {self.data_balanz_dir}"
            )

        search_map = self.load_client_search_map()
        all_folders = sorted(
            [
                d.name
                for d in self.data_balanz_dir.iterdir()
                if d.is_dir() and not d.name.startswith(".")
            ]
        )

        if not all_folders:
            raise ValueError(
                f"No se encontraron carpetas de clientes dentro de {self.data_balanz_dir}"
            )

        if selected_clients:
            selected_set = {c.strip() for c in selected_clients if c.strip()}
            folders = [f for f in all_folders if f in selected_set]
            missing = selected_set - set(folders)
            if missing:
                self.logger.warning(
                    f"Carpetas solicitadas no encontradas en data/balanz: {sorted(missing)}"
                )
        else:
            folders = all_folders

        result: List[Tuple[str, str, Path]] = []
        for folder in folders:
            mapped_name = search_map.get(folder, "").strip()
            if mapped_name:
                search_query = mapped_name
            else:
                search_query = folder.replace("_", " ").title()

            cc_dir = self.data_balanz_dir / folder / "Cuenta Corriente"
            result.append((folder, search_query, cc_dir))

        return result

    def _get_balanz_credentials(self) -> Tuple[str, str]:
        """
        Obtiene y valida las credenciales USERNAME_BALANZ y PASSWORD_BALANZ desde `.env`.
        """
        username = (
            os.getenv("USERNAME_BALANZ")
            or os.getenv("BALANZ_USERNAME")
            or os.getenv("USER_BALANZ")
            or ""
        ).strip()
        password = (
            os.getenv("PASSWORD_BALANZ")
            or os.getenv("BALANZ_PASSWORD")
            or os.getenv("PASS_BALANZ")
            or ""
        ).strip()

        if not username or not password:
            raise ValueError(
                "Credenciales de Balanz faltantes. Configure USERNAME_BALANZ y PASSWORD_BALANZ "
                f"en el archivo {self.base_path / '.env'} antes de ejecutar la descarga."
            )
        return username, password

    def _launch_browser(self, playwright_instance, headless: bool) -> Tuple[Browser, bool]:
        """
        Inicia Google Chrome nativo (`channel='chrome'`) en Windows o hace fallback
        a Chromium si se ejecuta en un entorno contenedorizado sin Chrome instalado.
        Retorna (browser, effective_headless).
        """
        is_linux_no_display = (
            sys.platform.startswith("linux") and not os.environ.get("DISPLAY")
        )
        effective_headless = True if is_linux_no_display else headless
        launch_args = ["--window-size=1920,1080"]
        if not effective_headless:
            launch_args.append("--start-maximized")

        try:
            self.logger.info(
                f"Abriendo navegador Google Chrome (headless={effective_headless})..."
            )
            browser = playwright_instance.chromium.launch(
                channel="chrome",
                headless=effective_headless,
                args=launch_args,
            )
            return browser, effective_headless
        except Exception as exc:
            self.logger.warning(
                f"No se pudo iniciar canal 'chrome' ({exc}). Usando Chromium integrado (headless={effective_headless})..."
            )
            browser = playwright_instance.chromium.launch(
                headless=effective_headless,
                args=launch_args,
            )
            return browser, effective_headless

    def _login_balanz(self, page: Page, username: str, password: str) -> str:
        """
        Ejecuta los pasos 1, 2 y 3 del flujo de login en https://productores.balanz.com/:
            1. Navega a https://productores.balanz.com/
            2. Ingresa usuario en `input.login-username[name='username']` y clickea `a#btn-login`
            3. Ingresa clave en `input#login-password[name='password']` y clickea `a#btn-login`
        Retorna la URL del menú principal de productores una vez autenticado.
        """
        self.logger.info(f"Paso 1: Navegando a {self.balanz_url}...")
        page.goto(self.balanz_url, wait_until="domcontentloaded", timeout=60000)

        # Paso 2: Ingresar usuario
        self.logger.info("Paso 2: Ingresando nombre de usuario...")
        user_input = page.locator("input.login-username[name='username']").first
        user_input.wait_for(state="visible", timeout=30000)
        user_input.fill(username)

        login_btn = page.locator("a#btn-login").first
        login_btn.click()

        # Paso 3: Ingresar contraseña
        self.logger.info("Paso 3: Ingresando contraseña...")
        pass_input = page.locator("input#login-password[name='password']").first
        pass_input.wait_for(state="visible", timeout=30000)
        pass_input.fill(password)

        login_btn.click()

        # Confirmar ingreso al menú principal esperando el buscador de clientes
        self.logger.info("Esperando carga del menú principal de productores...")
        search_input = page.locator("input.textbox-search").first
        search_input.wait_for(state="visible", timeout=45000)

        producer_home_url = page.url
        self.logger.info(f"✅ Login exitoso en Balanz Productores ({producer_home_url}).")
        return producer_home_url

    def _set_cuenta_corriente_start_date(
        self, client_page: Page, start_date_ddmmyyyy: str, start_dt: datetime
    ) -> None:
        """
        Interactúa con el componente ShieldUI DatePicker (`input.sui-picker-input` dentro de `.box-fecha`)
        para establecer la fecha 'desde' en la interfaz de Cuenta Corriente y aguarda la actualización
        de la grilla `.div-cuenta-corriente`.
        """
        self.logger.info(f"Paso 7a: Configurando fecha desde '{start_date_ddmmyyyy}'...")

        # Seleccionar el primer input.sui-picker-input (correspondiente a date-picker-from)
        from_input = client_page.locator(
            "section.cuenta .box-fecha:has(.date-picker-from) input.sui-picker-input"
        ).first
        if from_input.count() == 0:
            from_input = client_page.locator("section.cuenta input.sui-picker-input").first

        from_input.wait_for(state="visible", timeout=30000)

        # 1. Interacción por teclado sobre el input
        from_input.click()
        from_input.press("Control+A")
        from_input.press("Backspace")
        from_input.type(start_date_ddmmyyyy, delay=40)
        from_input.press("Enter")
        from_input.press("Tab")

        # 2. Sincronización con widget ShieldUI / jQuery en caso de requerir evento programático
        client_page.evaluate(
            """(args) => {
                const { ddmmyyyy, year, month, day } = args;
                const inputs = document.querySelectorAll('section.cuenta input.sui-picker-input');
                const inp = inputs.length > 0 ? inputs[0] : null;
                if (inp && inp.value !== ddmmyyyy) {
                    inp.value = ddmmyyyy;
                }
                if (window.jQuery) {
                    const $ = window.jQuery;
                    const $picker = $('.date-picker-from');
                    if ($picker.length && typeof $picker.swidget === 'function' && $picker.swidget()) {
                        try {
                            $picker.swidget().value(new Date(year, month - 1, day));
                            $picker.swidget().trigger('change');
                        } catch (e) {}
                    }
                    if (inp) {
                        $(inp).trigger('input').trigger('change').trigger('blur');
                    }
                } else if (inp) {
                    inp.dispatchEvent(new Event('input', { bubbles: true }));
                    inp.dispatchEvent(new Event('change', { bubbles: true }));
                    inp.dispatchEvent(new Event('blur', { bubbles: true }));
                }
            }""",
            {
                "ddmmyyyy": start_date_ddmmyyyy,
                "year": start_dt.year,
                "month": start_dt.month,
                "day": start_dt.day,
            },
        )

        # 3. Esperar actualización del label "Entre el DD/MM/YYYY al ..." y de la red
        try:
            client_page.wait_for_function(
                """(expectedDate) => {
                    const el = document.querySelector('section.cuenta .label-title');
                    return el && el.innerText && el.innerText.includes(expectedDate);
                }""",
                arg=start_date_ddmmyyyy,
                timeout=10000,
            )
            self.logger.info(
                f"Rango de fechas actualizado en la vista: {client_page.locator('section.cuenta .label-title').first.inner_text()}"
            )
        except Exception:
            self.logger.info(
                "Continuando tras espera de estabilización de grilla para la fecha seleccionada..."
            )

        try:
            client_page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        client_page.wait_for_timeout(1500)

    def _process_single_balanz_client(
        self,
        context: BrowserContext,
        main_page: Page,
        producer_home_url: str,
        folder_name: str,
        search_query: str,
        cc_dir: Path,
        start_date_ddmmyyyy: str,
        start_dt: datetime,
        download_date_suffix: str,
    ) -> Path:
        """
        Ejecuta los pasos 4 a 7 para un cliente individual y mueve el archivo Excel descargado
        a `data/balanz/<CLIENTE>/Cuenta Corriente/Cuenta Corriente-YYYYMMDD.xlsx`.
        """
        self.logger.info(
            f"=== Procesando cliente '{folder_name}' (Búsqueda: '{search_query}') ==="
        )

        # Garantizar que estamos en la pantalla limpia del buscador de productores
        if "/home" not in main_page.url:
            main_page.goto(producer_home_url, wait_until="load", timeout=45000)

        try:
            main_page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        main_page.wait_for_timeout(800)

        search_input = main_page.locator("input.textbox-search").first
        search_input.wait_for(state="visible", timeout=30000)

        # Paso 4 y 5: Ingresar nombre del cliente, hacer click en Buscar y esperar la fila del cliente actual
        # (evitando clickear la fila previa que Balanz mantiene en caché de sesión al volver a /home)
        tokens = [t.upper() for t in search_query.split() if t.strip()]
        matched_row_info = None

        for attempt in range(1, 4):
            self.logger.info(
                f"Paso 4: Buscando cliente '{search_query}' (intento {attempt})..."
            )
            search_input.click()
            search_input.fill("")
            search_input.fill(search_query)
            main_page.wait_for_timeout(300)

            if search_input.input_value().strip() != search_query.strip():
                search_input.fill(search_query)

            search_btn = main_page.locator(
                "a.btn-search[title='Buscar'], a.btn-search:has-text('Buscar')"
            ).first
            search_btn.wait_for(state="visible", timeout=15000)
            search_btn.click()

            try:
                handle = main_page.wait_for_function(
                    """(expectedTokens) => {
                        const buttons = Array.from(document.querySelectorAll("button.balanz-button[title='Acceder']"));
                        for (let i = 0; i < buttons.length; i++) {
                            const btn = buttons[i];
                            if (btn.offsetParent === null) continue;
                            const tr = btn.closest('tr') || btn.parentElement;
                            const text = (tr ? tr.innerText : '').toUpperCase();
                            if (expectedTokens.every(tok => text.includes(tok))) {
                                return { index: i, rowText: text.replace(/\\s+/g, ' ').trim() };
                            }
                        }
                        return false;
                    }""",
                    arg=tokens,
                    timeout=8000,
                )
                matched_row_info = handle.json_value()
                break
            except Exception:
                if attempt == 3:
                    raise RuntimeError(
                        f"No se encontró una fila con botón 'Acceder' que coincida con '{search_query}' tras 3 intentos."
                    )

        btn_index = int(matched_row_info["index"]) if matched_row_info else 0
        row_summary = matched_row_info.get("rowText", "") if matched_row_info else ""
        self.logger.info(
            f"Paso 5: Accediendo a la posición consolidada del cliente ({row_summary})..."
        )
        acceder_btn = main_page.locator("button.balanz-button[title='Acceder']").nth(btn_index)

        pages_before = len(context.pages)
        acceder_btn.click()
        main_page.wait_for_timeout(1500)

        # Detectar si Balanz abrió una nueva pestaña o navegó en la misma pestaña
        if len(context.pages) > pages_before:
            client_page = context.pages[-1]
            client_page.bring_to_front()
            client_page.wait_for_load_state("domcontentloaded", timeout=45000)
        else:
            client_page = main_page
            try:
                client_page.wait_for_url("**/posicionconsolidada**", timeout=30000)
            except Exception:
                pass

        try:
            # Paso 6: En #topMain desplegar MI CUENTA y clickear CUENTA CORRIENTE
            self.logger.info("Paso 6: Navegando a MI CUENTA -> CUENTA CORRIENTE...")
            top_main = client_page.locator("ul#topMain").first
            if top_main.is_visible():
                mi_cuenta_toggle = client_page.locator(
                    "ul#topMain a.dropdown-toggle:has-text('MI CUENTA')"
                ).first
                mi_cuenta_toggle.hover()
                mi_cuenta_toggle.click()

                cc_link = client_page.locator(
                    "ul#topMain ul.dropdown-menu a:has-text('CUENTA CORRIENTE')"
                ).first
                cc_link.click(force=True)
            else:
                client_page.goto(
                    "https://productores.balanz.com/micuenta/cuentacorriente",
                    wait_until="domcontentloaded",
                    timeout=45000,
                )

            try:
                client_page.wait_for_url("**/cuentacorriente**", timeout=30000)
            except Exception:
                pass

            # Paso 7: Esperar sección de Cuenta Corriente, filtrar fecha y Exportar a Excel
            self.logger.info("Paso 7: Esperando carga de la tabla de Cuenta Corriente...")
            client_page.locator("section.cuenta").first.wait_for(
                state="visible", timeout=45000
            )
            client_page.locator("div.div-cuenta-corriente.sui-grid").first.wait_for(
                state="visible", timeout=45000
            )

            self._set_cuenta_corriente_start_date(
                client_page, start_date_ddmmyyyy, start_dt
            )

            export_btn = client_page.locator(
                "section.cuenta button.sui-button:has-text('Exportar a Excel')"
            ).first
            export_btn.wait_for(state="visible", timeout=20000)

            self.logger.info("Paso 7b: Exportando a Excel...")
            self.downloads_dir.mkdir(parents=True, exist_ok=True)
            cc_dir.mkdir(parents=True, exist_ok=True)

            with client_page.expect_download(timeout=60000) as download_info:
                export_btn.click()

            download = download_info.value

            # Guardamos primero en el directorio de Descargas con su nombre original "Cuenta Corriente.xlsx"
            raw_download_path = self.downloads_dir / "Cuenta Corriente.xlsx"
            download.save_as(str(raw_download_path))

            if not raw_download_path.exists() or raw_download_path.stat().st_size == 0:
                raise RuntimeError(
                    f"El archivo descargado para {folder_name} está vacío o no se generó correctamente."
                )

            # Renombramos agregando -YYYYMMDD antes de .xlsx y después de Corriente
            renamed_filename = f"Cuenta Corriente-{download_date_suffix}.xlsx"
            renamed_download_path = self.downloads_dir / renamed_filename
            if renamed_download_path.exists():
                renamed_download_path.unlink()
            raw_download_path.rename(renamed_download_path)

            # Movemos el archivo a la carpeta Cuenta Corriente del cliente en data\balanz\<CLIENTE>\Cuenta Corriente\
            final_dest_path = cc_dir / renamed_filename
            if final_dest_path.exists():
                final_dest_path.unlink()
            shutil.move(str(renamed_download_path), str(final_dest_path))

            self.logger.info(
                f"✅ Excel descargado y almacenado exitosamente en: {final_dest_path}"
            )
            return final_dest_path

        finally:
            # Si se abrió una pestaña secundaria para el cliente, la cerramos;
            # si fue en la misma pestaña, regresamos al inicio de productores.
            if client_page != main_page:
                try:
                    client_page.close()
                except Exception:
                    pass
                main_page.bring_to_front()
            else:
                try:
                    main_page.goto(
                        producer_home_url,
                        wait_until="domcontentloaded",
                        timeout=45000,
                    )
                except Exception as nav_exc:
                    self.logger.warning(
                        f"Advertencia al regresar al menú principal: {nav_exc}"
                    )

    def download_cuentas_corrientes(
        self,
        broker: str,
        start_date: str,
        selected_clients: Optional[List[str]] = None,
        headless: bool = False,
        download_date_suffix: Optional[str] = None,
        unify_historical: bool = True,
    ) -> Dict[str, List[str]]:
        """
        Descarga las cuentas corrientes desde `start_date` hasta hoy para todos los clientes
        del broker especificado y unifica automáticamente `cuenta_corriente_historico.csv`.

        Retorna un diccionario con listas de clientes exitosos (`success`) y fallidos (`failed`).
        """
        if broker.lower() != "balanz":
            if broker.lower() == "bull market":
                self.logger.info("Lógica de Bull Market aún no implementada.")
                return {"success": [], "failed": []}
            raise ValueError(f"Broker no soportado: {broker}")

        start_date_ddmmyyyy, start_dt = self.validate_and_parse_date(start_date)
        suffix = download_date_suffix or datetime.now().strftime("%Y%m%d")
        username, password = self._get_balanz_credentials()
        clients = self.get_balanz_clients(selected_clients=selected_clients)

        self.logger.info(
            f"Iniciando descarga automática de Cuentas Corrientes Balanz | "
            f"Fecha desde: {start_date_ddmmyyyy} | Sufijo archivo: -{suffix} | "
            f"Clientes a procesar: {len(clients)}"
        )

        results: Dict[str, List[str]] = {"success": [], "failed": []}

        with sync_playwright() as p:
            browser, effective_headless = self._launch_browser(p, headless=headless)
            context = browser.new_context(
                accept_downloads=True,
                viewport={"width": 1920, "height": 1080} if effective_headless else None,
                no_viewport=not effective_headless,
            )
            page = context.new_page()

            try:
                producer_home_url = self._login_balanz(page, username, password)

                for folder_name, search_query, cc_dir in clients:
                    try:
                        dest_file = self._process_single_balanz_client(
                            context=context,
                            main_page=page,
                            producer_home_url=producer_home_url,
                            folder_name=folder_name,
                            search_query=search_query,
                            cc_dir=cc_dir,
                            start_date_ddmmyyyy=start_date_ddmmyyyy,
                            start_dt=start_dt,
                            download_date_suffix=suffix,
                        )
                        results["success"].append(f"{folder_name} -> {dest_file}")
                    except Exception as client_exc:
                        self.logger.error(
                            f"❌ Error procesando cliente '{folder_name}' (búsqueda '{search_query}'): {client_exc}"
                        )
                        try:
                            err_img = (
                                self.logs_dir
                                / f"error_balanz_{folder_name}_{suffix}.png"
                            )
                            page.screenshot(path=str(err_img), full_page=True)
                            self.logger.info(
                                f"Captura de diagnóstico guardada en: {err_img}"
                            )
                        except Exception:
                            pass
                        results["failed"].append(f"{folder_name}: {client_exc}")
            finally:
                context.close()
                browser.close()
                self.logger.info("Navegador cerrado.")

        self.logger.info(
            f"Resumen de descarga Balanz: {len(results['success'])} exitosos, "
            f"{len(results['failed'])} con error."
        )

        if unify_historical and results["success"]:
            self.logger.info(
                "Paso 2/4: Iniciando consolidación automática de históricos (cuenta_corriente_historico.csv)..."
            )
            try:
                if str(self.base_path) not in sys.path:
                    sys.path.insert(0, str(self.base_path))
                from scripts.layers.balanz.process_balanz_cuentas import BalanzAccountProcessor
                from scripts.layers.balanz.client_portfolio_evolution import run_all_clients_evolution
                from scripts.layers.balanz.list_client_holdings import run_all_clients_latest_holdings

                processor = BalanzAccountProcessor()
                processor.process_all_clients()
                self.logger.info("✅ Consolidación de cuenta_corriente_historico.csv completada.")

                self.logger.info(
                    "Paso 3/4: Iniciando generación de holdings diarios y evolución del patrimonio por cliente..."
                )
                run_all_clients_evolution(selected_clients=selected_clients)
                self.logger.info(
                    "✅ Generación de holdings diarios y evolución del patrimonio completada para todos los clientes."
                )

                self.logger.info(
                    "Paso 4/4: Generando listado de activos por cliente al último día disponible..."
                )
                run_all_clients_latest_holdings(selected_clients=selected_clients)
                self.logger.info(
                    "✅ Listado de activos al último día disponible generado exitosamente."
                )
            except Exception as unify_exc:
                self.logger.error(
                    f"⚠️ Error durante la consolidación de históricos / generación de holdings: {unify_exc}"
                )

        return results

    def extract_cotizaciones(self) -> None:
        """Extracción de cotizaciones (placeholder existente)."""
        self.logger.info("Iniciando extracción de cotizaciones...")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            self.logger.info("Extracción de cotizaciones completada (Simulada).")
            browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Web Scraping Manager - Descarga automática de Cuentas Corrientes"
    )
    parser.add_argument(
        "--task",
        choices=["cuentas_corrientes", "cotizaciones"],
        required=True,
        help="Tarea a ejecutar",
    )
    parser.add_argument(
        "--broker",
        choices=["Balanz", "Bull Market"],
        default="Balanz",
        help="Broker objetivo (por defecto: Balanz)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Fecha desde la cual descargar la cuenta corriente (formato YYYY-MM-DD o DD/MM/YYYY)",
    )
    parser.add_argument(
        "--clients",
        nargs="*",
        default=None,
        help="Lista opcional de carpetas de clientes en data/balanz (si se omite, descarga todos)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Ejecutar Google Chrome en segundo plano (por defecto se abre visible)",
    )
    parser.add_argument(
        "--skip-unify",
        action="store_true",
        help="Omitir la consolidación automática de cuenta_corriente_historico.csv al finalizar",
    )

    args = parser.parse_args()
    manager = ScrapingManager()

    if args.task == "cuentas_corrientes":
        results = manager.download_cuentas_corrientes(
            broker=args.broker,
            start_date=args.start_date,
            selected_clients=args.clients,
            headless=args.headless,
            unify_historical=not args.skip_unify,
        )
        if results["failed"] and not results["success"]:
            sys.exit(1)
    elif args.task == "cotizaciones":
        manager.extract_cotizaciones()


if __name__ == "__main__":
    main()
