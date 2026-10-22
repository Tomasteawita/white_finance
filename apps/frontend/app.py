import streamlit as st
import subprocess
import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

BASE_PATH = Path(__file__).resolve().parent.parent.parent
DATA_BALANZ_DIR = BASE_PATH / "data" / "balanz"
CLIENTS_MAP_PATH = DATA_BALANZ_DIR / "clients_search_map.json"

st.set_page_config(page_title="White Finance - Control Panel", layout="wide")

st.title("📈 Panel de Control - White Finance")
st.markdown("Herramienta de ejecución de pipelines y extracción de datos.")

st.sidebar.title("Navegación")
opcion = st.sidebar.radio("Ir a:", ["Web Scraping", "Ejecución de Pipelines", "Marketing"])

if opcion == "Web Scraping":
    st.header("Extracción de Datos mediante Web Scraping")
    
    st.subheader("Cuentas Corrientes")
    col_broker, col_fecha = st.columns(2)
    with col_broker:
        broker = st.selectbox("Seleccione el Broker", ["Balanz", "Bull Market"])
    with col_fecha:
        fecha_desde_cc = st.date_input(
            "Fecha desde (Cuenta Corriente)",
            value=datetime.today() - timedelta(days=30),
            max_value=datetime.today(),
            help="Fecha inicial desde la cual se filtrará y exportará la Cuenta Corriente hasta el día de hoy.",
        )

    available_clients = []
    if DATA_BALANZ_DIR.exists():
        available_clients = sorted(
            [d.name for d in DATA_BALANZ_DIR.iterdir() if d.is_dir() and not d.name.startswith(".")]
        )

    selected_clients = st.multiselect(
        "Clientes a descargar (en data/balanz)",
        options=available_clients,
        default=available_clients,
        help="Por defecto selecciona todos los clientes. Puedes filtrar uno o varios si lo deseas.",
    )

    is_docker = os.path.exists("/.dockerenv")
    col_opt1, col_opt2 = st.columns(2)
    with col_opt1:
        mostrar_chrome = st.checkbox(
            "Abrir Google Chrome visible durante el proceso",
            value=not is_docker,
            disabled=is_docker,
            help=(
                "En contenedor Docker (Linux) el navegador se ejecuta en segundo plano (1920x1080). "
                "Para ver la ventana de Google Chrome en pantalla, ejecuta el panel desde Windows con .\\venv\\Scripts\\streamlit run apps\\frontend\\app.py"
                if is_docker
                else "Desmarca esta opción si prefieres que se ejecute en segundo plano (headless)."
            ),
        )
    with col_opt2:
        unificar_al_finalizar = st.checkbox(
            "Generar históricos, holdings y evolución al finalizar",
            value=True,
            help="Ejecuta automáticamente process_balanz_cuentas.py (Paso 2) y client_portfolio_evolution.py (Paso 3) tras la descarga.",
        )

    if is_docker:
        st.info(
            "🐳 **Ejecutando desde contenedor Docker (`white_finance-frontend`)**: "
            "La descarga se ejecutará en segundo plano dentro del contenedor con resolución de escritorio (`1920x1080`). "
            "Si deseas ver la ventana de Google Chrome abriéndose en vivo en tu escritorio de Windows, ejecuta por consola: "
            "`.\\venv\\Scripts\\python -m scripts.scraping.scraping_manager --task cuentas_corrientes --broker Balanz --start-date YYYY-MM-DD`"
        )

    with st.expander("⚙️ Configurar mapeo de búsqueda de clientes en Balanz (clients_search_map.json)"):
        st.caption(
            "Define cómo se busca cada carpeta de `data/balanz` en el buscador de `productores.balanz.com`. "
            "Por defecto reemplaza `_` por espacios en formato `.title()`."
        )
        current_map = {}
        if CLIENTS_MAP_PATH.exists():
            try:
                with open(CLIENTS_MAP_PATH, "r", encoding="utf-8") as f:
                    current_map = json.load(f)
            except Exception:
                current_map = {}
        for c in available_clients:
            if c not in current_map:
                current_map[c] = c.replace("_", " ").title()

        edited_map_str = st.text_area(
            "JSON de Mapeo (Carpeta -> Nombre/Comitente en Balanz)",
            value=json.dumps(current_map, indent=4, ensure_ascii=False),
            height=180,
        )
        if st.button("💾 Guardar Mapeo"):
            try:
                parsed_map = json.loads(edited_map_str)
                CLIENTS_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
                with open(CLIENTS_MAP_PATH, "w", encoding="utf-8") as f:
                    json.dump(parsed_map, f, indent=4, ensure_ascii=False)
                st.success("✅ Mapeo guardado en data/balanz/clients_search_map.json")
            except Exception as ex:
                st.error(f"❌ JSON inválido: {ex}")

    if st.button("📥 Descargar Excels Cuentas Corrientes", type="primary"):
        if broker == "Balanz" and not selected_clients:
            st.warning("⚠️ Selecciona al menos un cliente para descargar.")
        else:
            fecha_desde_str = fecha_desde_cc.strftime("%Y-%m-%d")
            cmd = [
                sys.executable,
                "-m",
                "scripts.scraping.scraping_manager",
                "--broker",
                broker,
                "--task",
                "cuentas_corrientes",
                "--start-date",
                fecha_desde_str,
            ]
            if broker == "Balanz" and selected_clients:
                cmd.append("--clients")
                cmd.extend(selected_clients)
            if not mostrar_chrome:
                cmd.append("--headless")
            cmd.append("--skip-unify")

            with st.spinner(
                f"Paso 1/4: Iniciando descarga en {broker} desde {fecha_desde_cc.strftime('%d/%m/%Y')} para {len(selected_clients)} cliente(s)..."
            ):
                try:
                    result = subprocess.run(
                        cmd,
                        cwd=str(BASE_PATH),
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        check=True,
                    )
                    st.success(f"✅ Paso 1/4: Descarga de Cuentas Corrientes ({broker}) completada.")
                    with st.expander("Ver Logs de Descarga", expanded=True):
                        st.code(result.stdout)

                    if broker == "Balanz" and unificar_al_finalizar:
                        with st.spinner("Paso 2/4: Unificando cuenta_corriente_historico.csv para cada cliente..."):
                            unify_res = subprocess.run(
                                [sys.executable, str(BASE_PATH / "scripts" / "layers" / "balanz" / "process_balanz_cuentas.py")],
                                cwd=str(BASE_PATH),
                                capture_output=True,
                                text=True,
                                encoding="utf-8",
                                errors="replace",
                                check=True,
                            )
                            st.success("✅ Paso 2/4: Unificación histórica completada.")
                            with st.expander("Ver Logs de Unificación"):
                                st.code((unify_res.stdout or "") + "\n" + (unify_res.stderr or ""))

                        with st.spinner("Paso 3/4: Generando holdings diarios y evolución del patrimonio por cliente..."):
                            evo_cmd = [
                                sys.executable,
                                str(BASE_PATH / "scripts" / "layers" / "balanz" / "client_portfolio_evolution.py"),
                                "--all",
                            ]
                            if selected_clients:
                                evo_cmd.extend(selected_clients)
                            evo_res = subprocess.run(
                                evo_cmd,
                                cwd=str(BASE_PATH),
                                capture_output=True,
                                text=True,
                                encoding="utf-8",
                                errors="replace",
                                check=True,
                            )
                            st.success("✅ Paso 3/4: Holdings diarios y evolución del patrimonio generados exitosamente.")
                            with st.expander("Ver Logs de Holdings y Evolución"):
                                st.code((evo_res.stdout or "") + "\n" + (evo_res.stderr or ""))

                        with st.spinner("Paso 4/4: Generando listado de activos por cliente al último día disponible..."):
                            list_cmd = [
                                sys.executable,
                                str(BASE_PATH / "scripts" / "layers" / "balanz" / "list_client_holdings.py"),
                            ]
                            if selected_clients:
                                list_cmd.extend(selected_clients)
                            list_res = subprocess.run(
                                list_cmd,
                                cwd=str(BASE_PATH),
                                capture_output=True,
                                text=True,
                                encoding="utf-8",
                                errors="replace",
                                check=True,
                            )
                            st.success("✅ Paso 4/4: Listado de activos al último día disponible generado exitosamente.")
                            with st.expander("Ver Listado de Activos por Cliente (Último Día)", expanded=True):
                                st.code((list_res.stdout or "") + "\n" + (list_res.stderr or ""))

                except subprocess.CalledProcessError as e:
                    st.error(f"❌ Error durante la ejecución del pipeline ({broker}).")
                    with st.expander("Ver Detalle del Error / Logs", expanded=True):
                        st.code((e.stdout or "") + "\n" + (e.stderr or ""))

    st.subheader("Cotizaciones")
    if st.button("Extraer Cotizaciones del Día"):
        with st.spinner("Extrayendo cotizaciones..."):
            try:
                result = subprocess.run(
                    [sys.executable, "-m", "scripts.scraping.scraping_manager", "--task", "cotizaciones"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                st.success("✅ Cotizaciones extraídas correctamente.")
                with st.expander("Ver Logs"):
                    st.code(result.stdout)
            except subprocess.CalledProcessError as e:
                st.error("❌ Error al extraer cotizaciones.")
                with st.expander("Ver Errores"):
                    st.code(e.stderr)

elif opcion == "Ejecución de Pipelines":
    st.header("Ejecución de execute_all_pipelines")
    
    fecha_ejecucion = st.date_input("Seleccione la fecha de partición", datetime.today())
    
    if st.button("Ejecutar Pipelines"):
        fecha_str = fecha_ejecucion.strftime("%Y-%m-%d")
        with st.spinner(f"Ejecutando pipelines para la fecha {fecha_str}..."):
            try:
                # Se llama al script pasándole la fecha como argumento
                result = subprocess.run(
                    [sys.executable, "scripts/pipelines/execute_all_pipelines.py", "--date", fecha_str],
                    capture_output=True,
                    text=True,
                    check=True
                )
                st.success("✅ Pipelines ejecutados correctamente.")
                st.code(result.stdout)
            except subprocess.CalledProcessError as e:
                st.error("❌ Ocurrió un error durante la ejecución de los pipelines.")
                st.code(e.stderr)

elif opcion == "Marketing":
    st.header("Reportes de Marketing (Instagram)")
    
    st.subheader("Generar Reporte Semanal de Setting")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        start_date = st.date_input("Fecha de inicio (--start)", datetime.today())
    with col2:
        end_date = st.date_input("Fecha de fin (--end)", datetime.today())
    with col3:
        process_date = st.date_input("Fecha de proceso (--process-date)", datetime.today())
        
    if st.button("Ejecutar Reporte"):
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        process_str = process_date.strftime("%Y-%m-%d")
        
        with st.spinner(f"Generando reporte de setting (Start: {start_str}, End: {end_str}, Process: {process_str})..."):
            try:
                result = subprocess.run(
                    [sys.executable, "scripts/layers/marketing/weekly_reports/generate_setting_report.py", "--start", start_str, "--end", end_str, "--process-date", process_str],
                    capture_output=True,
                    text=True,
                    check=True
                )
                st.success("✅ Reporte generado correctamente.")
                st.code(result.stdout)
            except subprocess.CalledProcessError as e:
                st.error("❌ Ocurrió un error al generar el reporte.")
                st.code(e.stderr)

    st.markdown("---")
    st.subheader("Ingeniería de Conversión (Pipeline NLP)")
    st.markdown("Extrae patrones de cierre, estructuras y preguntas clave de las conversaciones.")
    col1_conv, col2_conv, col3_conv = st.columns(3)
    
    with col1_conv:
        start_date_conv = st.date_input("Fecha de inicio de los chats", datetime.today(), key="start_conv")
    with col2_conv:
        end_date_conv = st.date_input("Fecha de fin de los chats", datetime.today(), key="end_conv")
    with col3_conv:
        process_date_conv = st.text_input("Carpeta de proceso (ej. 20260912)", value=datetime.today().strftime("%Y%m%d"))
        
    if st.button("Ejecutar Ingeniería de Conversión"):
        start_str_conv = start_date_conv.strftime("%Y-%m-%d")
        end_str_conv = end_date_conv.strftime("%Y-%m-%d")
        
        with st.spinner(f"Extrayendo y consolidando patrones con Gemini (puede tardar varios minutos)..."):
            try:
                result = subprocess.run(
                    [sys.executable, "scripts/marketing/conversion_engineering/run_conversion_pipeline.py", 
                     "--start_date", start_str_conv, 
                     "--end_date", end_str_conv, 
                     "--process_date", process_date_conv],
                    capture_output=True,
                    text=True,
                    check=True
                )
                st.success("✅ Pipeline de Ingeniería de Conversión completado con éxito. Los CSV están listos en 'reports/marketing/'.")
                with st.expander("Ver Logs del Proceso"):
                    st.code(result.stdout)
            except subprocess.CalledProcessError as e:
                st.error("❌ Ocurrió un error al ejecutar el pipeline de IA.")
                with st.expander("Ver Errores"):
                    st.code(e.stderr)

