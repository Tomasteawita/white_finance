import streamlit as st
import subprocess
import os
import sys
from datetime import datetime

st.set_page_config(page_title="White Finance - Control Panel", layout="wide")

st.title("📈 Panel de Control - White Finance")
st.markdown("Herramienta de ejecución de pipelines y extracción de datos.")

st.sidebar.title("Navegación")
opcion = st.sidebar.radio("Ir a:", ["Web Scraping", "Ejecución de Pipelines"])

if opcion == "Web Scraping":
    st.header("Extracción de Datos mediante Web Scraping")
    
    st.subheader("Cuentas Corrientes")
    broker = st.selectbox("Seleccione el Broker", ["Balanz", "Bull Market"])
    
    if st.button("Descargar Excels Cuentas Corrientes"):
        with st.spinner(f"Iniciando scraper para {broker}... (Esto puede tomar unos minutos)"):
            try:
                # Aquí llamaremos a scripts.scraping.scraping_manager
                # Por ahora, usamos subprocess para llamar a un entrypoint
                result = subprocess.run(
                    [sys.executable, "-m", "scripts.scraping.scraping_manager", "--broker", broker, "--task", "cuentas_corrientes"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                st.success(f"✅ Descarga de {broker} completada.")
                with st.expander("Ver Logs"):
                    st.code(result.stdout)
            except subprocess.CalledProcessError as e:
                st.error(f"❌ Error al ejecutar el scraper para {broker}.")
                with st.expander("Ver Errores"):
                    st.code(e.stderr)

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
