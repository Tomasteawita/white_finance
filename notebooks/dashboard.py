"""
Dashboard de Ganancias Realizadas
==================================
Replica interactivamente todos los gráficos del notebook ganancias_realizadas.ipynb.
Usa Plotly Dash + Plotly Express para gráficos interactivos (hover, zoom).
Se auto-actualiza cada 24 horas cuando los archivos CSV cambian.

Rutas de datos (dentro del contenedor Docker):
    /home/jovyan/data/ → montado desde ./data/analytics/ en el host
"""

import os
import json
import logging

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Rutas de los archivos CSV ─────────────────────────────────────────────────
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))

CSV_PROFIT        = os.path.join(DATA_DIR, "profit.csv")
CSV_PESOS         = os.path.join(DATA_DIR, "cuenta_corriente_historico.csv")
CSV_DOLARES       = os.path.join(DATA_DIR, "cuenta_corriente_dolares_historico.csv")
CSV_DOLARES_CABLE = os.path.join(DATA_DIR, "cuenta_corriente_dolares_cable_historico.csv")

ALL_CSVS = [CSV_PROFIT, CSV_PESOS, CSV_DOLARES, CSV_DOLARES_CABLE]

# Poll cada 24 horas (los archivos se actualizan ~sábado)
POLL_INTERVAL_MS = 24 * 60 * 60 * 1000


# ── Lógica de cálculo (replicada del notebook) ────────────────────────────────

def calculate_profit(df_cc: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula ganancias/pérdidas realizadas por operaciones de compra/venta (FIFO).
    """
    for col in ["Cantidad", "Precio", "Importe"]:
        if col in df_cc.columns and df_cc[col].dtype == "object":
            df_cc[col] = df_cc[col].str.replace(",", "", regex=False).astype(float)

    df_cc["Comprobante"] = df_cc["Comprobante"].str.strip()
    df_cc = df_cc.sort_values(by=["Especie", "Operado"], ascending=[True, True])

    COMPRAS = {"COMPRA NORMAL", "COMPRA EXTERIOR V"}
    VENTAS  = {"VENTA", "VENTA EXTERIOR V"}

    operaciones = df_cc[df_cc["Comprobante"].isin(COMPRAS | VENTAS)].copy()

    cartera    = {}
    resultados = []

    for _, op in operaciones.iterrows():
        especie     = op["Especie"]
        cantidad    = op["Cantidad"]
        importe     = op["Importe"]
        precio_op   = op["Precio"]
        comprobante = op["Comprobante"]

        if especie not in cartera:
            if comprobante in VENTAS:
                logger.warning(f"Venta sin compra previa: {especie}. Se omite.")
                continue
            cartera[especie] = {"cantidad_total": 0.0, "costo_total": 0.0}

        if comprobante in COMPRAS:
            cartera[especie]["cantidad_total"] += abs(cantidad)
            cartera[especie]["costo_total"]    += abs(importe)

        elif comprobante in VENTAS:
            if abs(cartera[especie]["cantidad_total"]) < abs(cantidad):
                logger.warning(f"Cantidad insuficiente para vender {especie}. Se omite.")
                cartera.pop(especie, None)
                continue

            ppc = (
                cartera[especie]["costo_total"] / cartera[especie]["cantidad_total"]
                if cartera[especie]["cantidad_total"] > 0 else 0
            )
            costo_venta      = ppc * abs(cantidad)
            ganancia_perdida = abs(importe) - abs(costo_venta)

            resultados.append({
                "Fecha Venta":                  op["Operado"],
                "Activo":                       especie,
                "Cantidad Vendida":             cantidad,
                "Precio Venta":                 precio_op,
                "Precio Promedio Compra (PPC)": ppc,
                "Costo Total Venta":            abs(costo_venta),
                "Ganancia/Perdida ($)":         ganancia_perdida,
            })

            cartera[especie]["cantidad_total"] -= abs(cantidad)
            cartera[especie]["costo_total"]    -= abs(costo_venta)

            if cartera[especie]["cantidad_total"] <= 0:
                cartera.pop(especie, None)

    return pd.DataFrame(resultados)


def get_mtimes() -> dict:
    return {csv: os.path.getmtime(csv) if os.path.exists(csv) else 0 for csv in ALL_CSVS}


# ── Carga de datos ────────────────────────────────────────────────────────────

def load_all_data() -> dict:
    """Carga y procesa todos los DataFrames. Devuelve también la lista de años disponibles."""
    out   = {}
    years = set()

    # ── profit.csv ────────────────────────────────────────────────────────────
    try:
        df_profit = pd.read_csv(CSV_PROFIT)
        df_profit["Fecha Venta"] = pd.to_datetime(df_profit["Fecha Venta"])
        df_profit["Anio-Mes"]    = df_profit["Fecha Venta"].dt.to_period("M").astype(str)
        df_profit["Year"]        = df_profit["Fecha Venta"].dt.year
        years.update(df_profit["Year"].dropna().unique().tolist())

        # Profit por mes
        out["profit_por_mes"] = (
            df_profit.groupby(["Year", "Anio-Mes"])["Ganancia/Perdida ($)"]
            .sum().reset_index().rename(columns={"Ganancia/Perdida ($)": "Importe"})
        )

        # Profit por activo (mes anterior al actual)
        hoy      = pd.Timestamp.now()
        mes_prev = (hoy - pd.DateOffset(months=1)).to_period("M")
        df_activo = (
            df_profit
            .groupby(["Year", "Anio-Mes", "Activo"])["Ganancia/Perdida ($)"]
            .sum().reset_index()
        )
        df_activo["Label"] = df_activo["Anio-Mes"] + " - " + df_activo["Activo"]
        out["profit_por_activo_base"] = df_activo   # se filtra por año más tarde
        out["profit_por_activo_mes_prev"] = str(mes_prev)

    except FileNotFoundError:
        logger.warning(f"No encontrado: {CSV_PROFIT}")
        out["profit_por_mes"]          = pd.DataFrame()
        out["profit_por_activo_base"]  = pd.DataFrame()
        out["profit_por_activo_mes_prev"] = ""

    # ── cuenta_corriente_historico.csv  →  dividendos pesos ───────────────────
    try:
        df_pesos = pd.read_csv(CSV_PESOS)
        df_pesos["Liquida"] = pd.to_datetime(df_pesos["Liquida"], errors="coerce")
        if df_pesos["Importe"].dtype == "object":
            df_pesos["Importe"] = df_pesos["Importe"].str.replace(",", "", regex=False).astype(float)

        df_pesos["Anio-Mes"] = df_pesos["Liquida"].dt.to_period("M").astype(str)
        df_pesos["Year"]     = df_pesos["Liquida"].dt.year
        years.update(df_pesos["Year"].dropna().unique().tolist())

        # Dividendos + caución agrupados
        df_div = df_pesos[df_pesos["Comprobante"].str.strip().isin(["DIVIDENDOS", "INTERES POR CAUCION"])].copy()
        out["dividendos_pesos"] = (
            df_div.groupby(["Year", "Anio-Mes"])["Importe"].sum().reset_index()
        )

        # Intereses por caución (para el gráfico 4)
        df_caucion = df_pesos[df_pesos["Comprobante"].str.strip() == "INTERES POR CAUCION"].copy()
        out["intereses_pesos"] = (
            df_caucion.groupby(["Year", "Anio-Mes"])["Importe"].sum().reset_index()
            .rename(columns={"Importe": "Importe_intereses"})
        )

    except FileNotFoundError:
        logger.warning(f"No encontrado: {CSV_PESOS}")
        out["dividendos_pesos"] = pd.DataFrame()
        out["intereses_pesos"]  = pd.DataFrame()

    # ── cuenta_corriente_dolares_historico.csv  →  dividendos USD ─────────────
    try:
        df_dol = pd.read_csv(CSV_DOLARES)
        if df_dol["Importe"].dtype == "object":
            df_dol["Importe"] = df_dol["Importe"].str.replace(",", "", regex=False).astype(float)
        df_dol_div = df_dol[df_dol["Comprobante"].str.strip().isin(["DIVIDENDOS", "RENTA Y AMORTIZ"])].copy()
        df_dol_div["Operado_dt"] = pd.to_datetime(df_dol_div["Operado"], errors="coerce")
        df_dol_div["Year"]       = df_dol_div["Operado_dt"].dt.year
        years.update(df_dol_div["Year"].dropna().unique().tolist())
        out["dividendos_dolares"] = df_dol_div[["Operado", "Importe", "Year"]].sort_values("Operado")

    except FileNotFoundError:
        logger.warning(f"No encontrado: {CSV_DOLARES}")
        out["dividendos_dolares"] = pd.DataFrame()

    # ── cuenta_corriente_dolares_cable_historico.csv ───────────────────────────
    try:
        df_cable = pd.read_csv(CSV_DOLARES_CABLE)
        for col in ["Cantidad", "Precio", "Importe"]:
            if col in df_cable.columns and df_cable[col].dtype == "object":
                df_cable[col] = df_cable[col].str.replace(",", "", regex=False).astype(float)

        df_profit_cable = calculate_profit(df_cable.copy())
        if not df_profit_cable.empty:
            df_profit_cable["Fecha Venta"] = pd.to_datetime(df_profit_cable["Fecha Venta"])
            df_profit_cable["Anio-Mes"]    = df_profit_cable["Fecha Venta"].dt.to_period("M").astype(str)
            df_profit_cable["Year"]        = df_profit_cable["Fecha Venta"].dt.year
            years.update(df_profit_cable["Year"].dropna().unique().tolist())
            out["profit_cable_por_mes"] = (
                df_profit_cable.groupby(["Year", "Anio-Mes"])["Ganancia/Perdida ($)"]
                .sum().reset_index().rename(columns={"Ganancia/Perdida ($)": "Importe"})
            )
        else:
            out["profit_cable_por_mes"] = pd.DataFrame()

        df_div_cable = df_cable[df_cable["Comprobante"].str.strip().isin(["DIVIDENDOS", "RENTA Y AMORTIZ"])].copy()
        df_div_cable["Operado_dt"] = pd.to_datetime(df_div_cable["Operado"], errors="coerce")
        df_div_cable["Year"]       = df_div_cable["Operado_dt"].dt.year
        years.update(df_div_cable["Year"].dropna().unique().tolist())
        out["dividendos_cable"] = df_div_cable[["Operado", "Importe", "Year"]].sort_values("Operado")

    except FileNotFoundError:
        logger.warning(f"No encontrado: {CSV_DOLARES_CABLE}")
        out["profit_cable_por_mes"] = pd.DataFrame()
        out["dividendos_cable"]     = pd.DataFrame()

    out["available_years"] = sorted([int(y) for y in years if pd.notna(y)])
    return out


# Cache de datos en memoria (se renueva cada 24h)
_cached_data: dict   = {}
_cached_mtimes: dict = {}


def get_data() -> dict:
    global _cached_data, _cached_mtimes
    current = get_mtimes()
    if not _cached_data or current != _cached_mtimes:
        logger.info("Recargando datos desde CSV...")
        _cached_data   = load_all_data()
        _cached_mtimes = current
    return _cached_data


# ── Helpers para gráficos ─────────────────────────────────────────────────────

def filter_by_year(df: pd.DataFrame, year) -> pd.DataFrame:
    """Filtra un DataFrame por la columna Year. Si year es None/'Todos' devuelve todo."""
    if not year or year == "Todos" or df.empty or "Year" not in df.columns:
        return df
    return df[df["Year"] == int(year)]


def make_bar_chart(df: pd.DataFrame, x_col: str, y_col: str, title: str,
                   x_label: str, y_label: str) -> go.Figure:
    if df.empty:
        return go.Figure().update_layout(title=f"{title} — sin datos",
                                         plot_bgcolor="#F8F9FA", paper_bgcolor="#FFF")
    fig = px.bar(
        df, x=x_col, y=y_col, title=title,
        labels={x_col: x_label, y_col: y_label},
        color=df[y_col].apply(lambda v: "Ganancia" if v >= 0 else "Pérdida"),
        color_discrete_map={"Ganancia": "#2ECC71", "Pérdida": "#E74C3C"},
        text=df[y_col].apply(lambda v: f"{v:,.0f}"),
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(xaxis_tickangle=-45, showlegend=False,
                      plot_bgcolor="#F8F9FA", paper_bgcolor="#FFFFFF",
                      margin={"t": 60, "b": 80})
    return fig


def make_simple_bar(df: pd.DataFrame, x_col: str, y_col: str, title: str,
                    x_label: str, y_label: str) -> go.Figure:
    if df.empty:
        return go.Figure().update_layout(title=f"{title} — sin datos",
                                         plot_bgcolor="#F8F9FA", paper_bgcolor="#FFF")
    fig = px.bar(
        df, x=x_col, y=y_col, title=title,
        labels={x_col: x_label, y_col: y_label},
        text=df[y_col].apply(lambda v: f"{v:,.2f}"),
        color_discrete_sequence=["#3498DB"],
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(xaxis_tickangle=-45, showlegend=False,
                      plot_bgcolor="#F8F9FA", paper_bgcolor="#FFFFFF",
                      margin={"t": 60, "b": 80})
    return fig


# ── App Dash ──────────────────────────────────────────────────────────────────

app = Dash(__name__, title="Dashboard · Ganancias Realizadas")

SECTION_STYLE = {
    "backgroundColor": "#FFFFFF",
    "borderRadius": "8px",
    "padding": "20px",
    "marginBottom": "24px",
    "boxShadow": "0 2px 6px rgba(0,0,0,0.08)",
}
HEADER_STYLE = {
    "fontSize": "20px",
    "fontWeight": "700",
    "color": "#2C3E50",
    "borderBottom": "3px solid #3498DB",
    "paddingBottom": "8px",
    "marginBottom": "16px",
}
PAGE_STYLE = {
    "fontFamily": "'Segoe UI', Arial, sans-serif",
    "backgroundColor": "#F0F2F5",
    "minHeight": "100vh",
    "padding": "24px 32px",
}
ROW_STYLE = {"display": "flex", "gap": "16px", "alignItems": "flex-start"}
HALF_STYLE = {"flex": "1", "minWidth": "0"}

# Carga inicial para poblar el dropdown de años
_initial_data = get_data()
_initial_years = _initial_data.get("available_years", [])
YEAR_OPTIONS = [{"label": "Todos los años", "value": "Todos"}] + [
    {"label": str(y), "value": y} for y in _initial_years
]

app.layout = html.Div(
    style=PAGE_STYLE,
    children=[
        html.H1("📈 Dashboard de Ganancias Realizadas",
                style={"color": "#1A252F", "marginBottom": "8px"}),
        html.P("Se actualiza automáticamente cada 24 horas cuando los archivos CSV cambian.",
               style={"color": "#7F8C8D", "marginBottom": "16px"}),

        # ── Filtro global por año ─────────────────────────────────────────────
        html.Div(
            style={**SECTION_STYLE, "padding": "14px 20px", "marginBottom": "20px",
                   "display": "flex", "alignItems": "center", "gap": "16px"},
            children=[
                html.Label("🗓️ Filtrar por año:",
                           style={"fontWeight": "600", "color": "#2C3E50",
                                  "whiteSpace": "nowrap"}),
                dcc.Dropdown(
                    id="year-filter",
                    options=YEAR_OPTIONS,
                    value="Todos",
                    clearable=False,
                    style={"width": "220px"},
                ),
            ],
        ),

        # Stores
        dcc.Store(id="csv-mtimes", data=json.dumps(get_mtimes())),
        dcc.Interval(id="interval", interval=POLL_INTERVAL_MS, n_intervals=0),

        # ── Sección PESOS ─────────────────────────────────────────────────────
        html.Div(style=SECTION_STYLE, children=[
            html.H2("🇦🇷 PESOS", style=HEADER_STYLE),

            # Gráficos 1 y 2 lado a lado
            html.Div(style=ROW_STYLE, children=[
                html.Div(style=HALF_STYLE, children=[dcc.Graph(id="chart-profit-mes")]),
                html.Div(style=HALF_STYLE, children=[dcc.Graph(id="chart-profit-activo")]),
            ]),

            html.Hr(style={"margin": "24px 0"}),
            dcc.Graph(id="chart-dividendos-pesos"),
            html.Hr(style={"margin": "24px 0"}),
            dcc.Graph(id="chart-total-pesos"),
        ]),

        # ── Sección DÓLARES ───────────────────────────────────────────────────
        html.Div(style=SECTION_STYLE, children=[
            html.H2("💵 DÓLARES", style=HEADER_STYLE),
            dcc.Graph(id="chart-dividendos-dolares"),
        ]),

        # ── Sección DÓLARES CABLE ─────────────────────────────────────────────
        html.Div(style=SECTION_STYLE, children=[
            html.H2("🔌 DÓLARES CABLE", style=HEADER_STYLE),
            dcc.Graph(id="chart-profit-cable"),
            html.Hr(style={"margin": "24px 0"}),
            dcc.Graph(id="chart-dividendos-cable"),
            html.Hr(style={"margin": "24px 0"}),
            dcc.Graph(id="chart-total-cable"),
        ]),
    ],
)


# ── Callback principal ────────────────────────────────────────────────────────

@app.callback(
    [
        Output("chart-profit-mes",         "figure"),
        Output("chart-profit-activo",      "figure"),
        Output("chart-dividendos-pesos",   "figure"),
        Output("chart-total-pesos",        "figure"),
        Output("chart-dividendos-dolares", "figure"),
        Output("chart-profit-cable",       "figure"),
        Output("chart-dividendos-cable",   "figure"),
        Output("chart-total-cable",        "figure"),
        Output("csv-mtimes",               "data"),
    ],
    Input("interval",     "n_intervals"),
    Input("year-filter",  "value"),
    State("csv-mtimes",   "data"),
)
def update_charts(n_intervals, selected_year, stored_mtimes_json):
    current_mtimes = get_mtimes()
    stored_mtimes  = json.loads(stored_mtimes_json) if stored_mtimes_json else {}

    if n_intervals == 0 or current_mtimes != stored_mtimes:
        logger.info("Recargando datos (cambio detectado o primera carga)...")

    data = get_data()
    y    = selected_year  # "Todos" o un entero de año

    # ── Gráfico 1: Profit por mes ─────────────────────────────────────────────
    df1  = filter_by_year(data["profit_por_mes"], y)
    fig1 = make_bar_chart(df1, "Anio-Mes", "Importe",
                          "Ganancias/Pérdidas por Mes", "Mes", "Importe ($)")

    # ── Gráfico 2: Por activo (mes anterior, filtrado por año) ────────────────
    df2_base = filter_by_year(data["profit_por_activo_base"], y)
    mes_prev = data.get("profit_por_activo_mes_prev", "")
    if not df2_base.empty and mes_prev:
        df2 = df2_base[df2_base["Anio-Mes"] == mes_prev].copy()
        fig2 = make_bar_chart(
            df2, "Label", "Ganancia/Perdida ($)",
            f"Ganancia/Pérdida por Activo — {mes_prev}", "Activo", "Importe ($)",
        )
    else:
        fig2 = go.Figure().update_layout(
            title="Ganancia/Pérdida por Activo — sin datos",
            plot_bgcolor="#F8F9FA", paper_bgcolor="#FFF",
        )

    # ── Gráfico 3: Dividendos pesos ───────────────────────────────────────────
    df3  = filter_by_year(data["dividendos_pesos"], y)
    fig3 = make_simple_bar(df3, "Anio-Mes", "Importe",
                           "Ingresos por Dividendos/Intereses por Mes (Pesos)",
                           "Mes", "Importe ($)")

    # ── Gráfico 4: Total pesos ────────────────────────────────────────────────
    df_cv  = filter_by_year(data["profit_por_mes"],   y).rename(columns={"Importe": "Importe_cv"})
    df_div = filter_by_year(data["dividendos_pesos"], y).rename(columns={"Importe": "Importe_div"})
    df_int = filter_by_year(data.get("intereses_pesos", pd.DataFrame()), y)

    if not df_cv.empty or not df_div.empty:
        df4 = df_cv.merge(df_div[["Anio-Mes", "Importe_div"]], on="Anio-Mes", how="outer")
        if not df_int.empty:
            df4 = df4.merge(df_int[["Anio-Mes", "Importe_intereses"]], on="Anio-Mes", how="outer")
        else:
            df4["Importe_intereses"] = 0
        df4["Importe"] = (
            df4.get("Importe_cv",          pd.Series(0, index=df4.index)).fillna(0)
            + df4.get("Importe_div",        pd.Series(0, index=df4.index)).fillna(0)
            + df4.get("Importe_intereses",  pd.Series(0, index=df4.index)).fillna(0)
        )
        df4 = df4.sort_values("Anio-Mes")
        fig4 = make_bar_chart(df4, "Anio-Mes", "Importe",
                              "Total Ganancias — Compra/Venta + Dividendos + Intereses (Pesos)",
                              "Mes", "Importe ($)")
    else:
        fig4 = go.Figure().update_layout(title="Total Ganancias — sin datos",
                                          plot_bgcolor="#F8F9FA", paper_bgcolor="#FFF")

    # ── Gráfico 5: Dividendos USD ─────────────────────────────────────────────
    df5  = filter_by_year(data["dividendos_dolares"], y)
    fig5 = make_simple_bar(df5, "Operado", "Importe",
                           "Ganancias por Dividendos y Amortizaciones (USD)",
                           "Fecha", "Importe (USD)")

    # ── Gráfico 6: Profit cable ───────────────────────────────────────────────
    df6  = filter_by_year(data["profit_cable_por_mes"], y)
    fig6 = make_bar_chart(df6, "Anio-Mes", "Importe",
                          "Ganancia/Pérdida Compra/Venta de Activos (USD Cable)",
                          "Mes", "Importe (USD)")

    # ── Gráfico 7: Dividendos cable ───────────────────────────────────────────
    df7  = filter_by_year(data["dividendos_cable"], y)
    fig7 = make_simple_bar(df7, "Operado", "Importe",
                           "Ingresos por Dividendos y Amortizaciones (USD Cable)",
                           "Fecha", "Importe (USD)")

    # ── Gráfico 8: Total cable ────────────────────────────────────────────────
    df6b = df6.copy()
    df7b = filter_by_year(data["dividendos_cable"], y).copy()

    if not df6b.empty:
        df7b["Operado_dt"] = pd.to_datetime(df7b["Operado"], errors="coerce")
        df7b["Anio-Mes"]   = df7b["Operado_dt"].dt.to_period("M").astype(str)
        df7b_m = df7b.groupby("Anio-Mes")["Importe"].sum().reset_index().rename(
            columns={"Importe": "Importe_div"}
        )
        df8 = df6b.merge(df7b_m, on="Anio-Mes", how="outer")
        df8["Importe"] = df8["Importe"].fillna(0) + df8["Importe_div"].fillna(0)
        df8 = df8.sort_values("Anio-Mes")
        fig8 = make_bar_chart(df8, "Anio-Mes", "Importe",
                              "Total Ganancias — Compra/Venta + Dividendos (USD Cable)",
                              "Mes", "Importe (USD)")
    else:
        fig8 = go.Figure().update_layout(title="Total Ganancias USD Cable — sin datos",
                                          plot_bgcolor="#F8F9FA", paper_bgcolor="#FFF")

    return fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, json.dumps(current_mtimes)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    host  = os.environ.get("DASH_HOST",  "0.0.0.0")
    port  = int(os.environ.get("DASH_PORT", 8050))
    debug = os.environ.get("DASH_DEBUG", "false").lower() == "true"
    logger.info(f"Iniciando Dashboard en http://{host}:{port}")
    app.run(host=host, port=port, debug=debug)
