"""
Suite de Pruebas de Validación para ScrapingManager (Balanz Productores)
========================================================================
QUÉ HACE:
    Valida casos borde (edge cases), resolución de clientes en `data/balanz`,
    parseo de fechas y coincidencia exacta de selectores DOM de Playwright
    contra la estructura HTML real de Balanz Productores en Google Chrome.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from scripts.scraping.scraping_manager import ScrapingManager


def test_date_validation() -> None:
    manager = ScrapingManager(base_path=root_path)

    # Casos válidos
    ddmmyyyy, dt = manager.validate_and_parse_date("2026-09-16")
    assert ddmmyyyy == "16/09/2026" and dt.year == 2026 and dt.month == 9 and dt.day == 16

    ddmmyyyy2, _ = manager.validate_and_parse_date("16/09/2026")
    assert ddmmyyyy2 == "16/09/2026"

    # Casos borde: fecha vacía
    try:
        manager.validate_and_parse_date("")
        assert False, "Debió fallar con fecha vacía"
    except ValueError:
        pass

    # Casos borde: fecha futura
    future_str = (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d")
    try:
        manager.validate_and_parse_date(future_str)
        assert False, "Debió fallar con fecha futura"
    except ValueError:
        pass

    print("✅ test_date_validation OK")


def test_client_discovery_and_mapping() -> None:
    manager = ScrapingManager(base_path=root_path)
    clients = manager.get_balanz_clients()
    assert len(clients) >= 5, f"Se esperaban al menos 5 clientes, encontrados: {len(clients)}"

    client_dict = {folder: (query, cc_dir) for folder, query, cc_dir in clients}
    assert "ARCE_ZULMA_ELIZABET" in client_dict
    assert client_dict["ARCE_ZULMA_ELIZABET"][0] == "Arce Zulma Elizabet"
    assert "VILLAFAÑE SANTIAGO" in client_dict
    assert client_dict["VILLAFAÑE SANTIAGO"][0] == "Villafañe Santiago"
    assert client_dict["VILLAFAÑE SANTIAGO"][1].name == "Cuenta Corriente"

    print(f"✅ test_client_discovery_and_mapping OK ({len(clients)} clientes validados)")


def test_dom_selectors_in_google_chrome() -> None:
    manager = ScrapingManager(base_path=root_path)

    mock_html = """
    <!DOCTYPE html>
    <html>
    <body>
        <!-- Login -->
        <input type="text" class="form-control login-username" name="username" value="" placeholder="Ingrese su nombre de Usuario">
        <input id="login-password" type="password" class="form-control login-password" name="password" placeholder="Ingrese su Clave">
        <a id="btn-login" class="btn btn-primary" data-loading-text="Loading...">Ingresar</a>

        <!-- Buscador -->
        <input type="text" placeholder="" class="textbox-search">
        <a style="margin-bottom:10px;" data-toggle="modal" class="btn btn-primary btn-search" title="Buscar">Buscar</a>
        <button type="button" title="Acceder" class="btn-sm balanz-button"><span class="glyphicon glyphicon-resize-full"></span></button>

        <!-- Navbar -->
        <ul class="nav nav-pills nav-main" id="topMain">
            <li class="dropdown">
                <a class="dropdown-toggle">MI CUENTA</a>
                <ul class="dropdown-menu">
                    <li class="dropdown"><a>POSICION CONSOLIDADA</a></li>
                    <li class="dropdown"><a>MOVIMIENTOS</a></li>
                    <li class="dropdown"><a id="cc-clicked" onclick="window._ccClicked = true;">CUENTA CORRIENTE</a></li>
                </ul>
            </li>
        </ul>

        <!-- Cuenta Corriente -->
        <section class="cuenta">
            <div class="container">
                <div class="row">
                    <div class="col-lg-12">
                        <div class="heading-title">
                            <h1 class="col-md-8 col-sm-6 col-xs-12">Cuenta Corriente</h1>
                            <div class="col-md-2 col-sm-6 col-xs-12 box-fecha pad0">
                                <span class="sui-datepicker"><input class="sui-picker-input" type="text" onchange="document.querySelector('.label-title').innerText = 'Entre el ' + this.value + ' al 17/10/2026';"><span class="sui-icon-wrapper" unselectable="on" title=""><span class="sui-sprite sui-calendar-icon" unselectable="on">select</span></span><div class="col-md-12 col-sm-12 col-xs-12 pad0 date-picker-from" style="display: none;"></div></span>
                            </div>
                            <div class="col-md-2 col-sm-6 col-xs-12 box-fecha pad0">
                                <span class="sui-datepicker"><input class="sui-picker-input" type="text"><span class="sui-icon-wrapper" unselectable="on" title=""><span class="sui-sprite sui-calendar-icon" unselectable="on">select</span></span><div class="col-md-12 col-sm-12 col-xs-12 pad0 date-picker-to" style="display: none;"></div></span>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-12 col-sm-12 col-xs-12 cuenta-tabla">
                        <div class="heading-table bgGris3 blanco radiusTop pad0" style="height:30px;">
                            <div class="col-md-12 col-sm-12 col-xs-12">
                                <span class="title label-title">Entre el 16/09/2026 al 17/10/2026</span>
                            </div>
                        </div>
                        <div class="div-cuenta-corriente sui-grid sui-grid-core">
                            <div class="sui-toolbar"><button type="button" class="sui-button" onclick="window._exported = true;">Exportar a Excel</button></div>
                        </div>
                    </div>
                </div>
            </div>
        </section>
    </body>
    </html>
    """

    with sync_playwright() as p:
        browser, _ = manager._launch_browser(p, headless=True)
        page = browser.new_page()
        page.set_content(mock_html)

        # Validar selectores de Login
        page.locator("input.login-username[name='username']").first.fill("usuario_test")
        page.locator("input#login-password[name='password']").first.fill("clave_test")
        page.locator("a#btn-login").first.click()

        # Validar selectores de Búsqueda y Acceso
        page.locator("input.textbox-search").first.fill("Villafañe Santiago")
        page.locator("a.btn-search[title='Buscar']").first.click()
        page.locator("button.balanz-button[title='Acceder']").first.click()

        # Validar navegación a CUENTA CORRIENTE
        page.locator("ul#topMain a.dropdown-toggle:has-text('MI CUENTA')").first.click()
        page.locator("ul#topMain ul.dropdown-menu a:has-text('CUENTA CORRIENTE')").first.click()
        assert page.evaluate("window._ccClicked === true")

        # Validar carga de fecha desde en el datepicker
        ddmmyyyy, dt = manager.validate_and_parse_date("2026-08-01")
        manager._set_cuenta_corriente_start_date(page, ddmmyyyy, dt)

        from_val = page.locator("section.cuenta input.sui-picker-input").first.input_value()
        assert from_val == "01/08/2026", f"Valor inesperado en datepicker: {from_val}"

        # Validar botón Exportar a Excel
        page.locator("section.cuenta button.sui-button:has-text('Exportar a Excel')").first.click()
        assert page.evaluate("window._exported === true")

        browser.close()

    print("✅ test_dom_selectors_in_google_chrome OK")


if __name__ == "__main__":
    test_date_validation()
    test_client_discovery_and_mapping()
    test_dom_selectors_in_google_chrome()
    print("\n🎉 Todas las pruebas pasaron exitosamente.")
