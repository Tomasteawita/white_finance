import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

def create_premium_dashboard():
    csv_path = r'c:\Users\tomas\white_finance\data\analytics\portfolio_vcp_history.csv'
    if not os.path.exists(csv_path):
        print("El archivo VCP no se encuentra. Ejecutá el pipeline matemático primero.")
        return

    df = pd.read_csv(csv_path)
    df['Operado'] = pd.to_datetime(df['Operado'])
    
    # Excluir primeros días irrelevantes (limpiar cola inactiva)
    # Buscamos el primer día que el portfolio superó los $100
    df_filtered = df[df['Valor_Cuotaparte_USD'] > 100].copy()

    # Premium Color Palette (Sleek Dark Mode & Neon Highlights)
    bg_color = '#0b0f19'
    paper_bg = '#0b0f19'
    grid_color = '#1f2937'
    font_color = '#9ca3af'
    
    color_total = '#00f2fe'  # Cyan gradient base
    color_safe = '#10b981'   # Emerald
    color_growth = '#f43f5e' # Rose / Convexity

    # Crear figura con 2 subplots verticales
    fig = make_subplots(
        rows=2, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.1,
        subplot_titles=("💸 Curva de Patrimonio Histórico (Hard Currency)", "⚖️ Estrategia Barbell (Preservación vs Convexidad)"),
        row_heights=[0.6, 0.4]
    )

    # ==========================================
    # CHART 1: Total Equity Curve (Valor Cuotaparte)
    # ==========================================
    fig.add_trace(go.Scatter(
        x=df_filtered['Operado'], 
        y=df_filtered['Valor_Cuotaparte_USD'],
        mode='lines',
        name='Total Equity (USD)',
        line=dict(width=3, color=color_total),
        fill='tozeroy',
        fillcolor='rgba(0, 242, 254, 0.1)' # Soft cyan gradient
    ), row=1, col=1)

    # ==========================================
    # CHART 2: Barbell Stacked Area (Safe vs Growth)
    # ==========================================
    fig.add_trace(go.Scatter(
        x=df_filtered['Operado'], 
        y=df_filtered['Total_Safe_Valuation'],
        mode='lines',
        name='Safe Base (Bonds, Cash, FCIs)',
        stackgroup='one',
        line=dict(width=0, color=color_safe),
        fillcolor='rgba(16, 185, 129, 0.5)'
    ), row=2, col=1)

    fig.add_trace(go.Scatter(
        x=df_filtered['Operado'], 
        y=df_filtered['Total_Growth_Valuation'],
        mode='lines',
        name='Growth (Equities, Long Bonds)',
        stackgroup='one',
        line=dict(width=0, color=color_growth),
        fillcolor='rgba(244, 63, 94, 0.5)'
    ), row=2, col=1)

    # ==========================================
    # LAYOUT & AESTHETICS IMPLANTATION
    # ==========================================
    fig.update_layout(
        template='plotly_dark',
        plot_bgcolor=bg_color,
        paper_bgcolor=paper_bg,
        font=dict(family="Inter, Roboto, sans-serif", color=font_color, size=13),
        title_font=dict(size=22, color='#ffffff', family="Inter, Roboto, sans-serif"),
        title_text="White Finance: Quant Dashboard",
        title_x=0.02,
        margin=dict(l=40, r=40, t=80, b=40),
        hovermode="x unified",
        legend=dict(
            orientation="h", x=0.01, y=-0.15,
            bgcolor='rgba(0,0,0,0)', font=dict(color=font_color)
        ),
    )

    # Grid Lines and Axes Formatting
    for i in [1, 2]:
        fig.update_yaxes(
            title_text="USD ($)", row=i, col=1, 
            showgrid=True, gridwidth=1, gridcolor=grid_color,
            zeroline=False, tickprefix="$", tickformat=",.0f"
        )
        fig.update_xaxes(
            row=i, col=1, 
            showgrid=True, gridwidth=1, gridcolor=grid_color,
            zeroline=False
        )

    # Salida a HTML Interactivo (Standalone)
    output_html = r'c:\Users\tomas\white_finance\notebooks\portfolio_dashboard.html'
    fig.write_html(output_html, auto_open=False)
    print(f"✅ Dashboard Premium generado exitosamente en: {output_html}")
    
if __name__ == "__main__":
    create_premium_dashboard()
