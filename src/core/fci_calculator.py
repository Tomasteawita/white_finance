"""
Calculadora de Valor Cuotaparte (VCP) - Sistema de FCI
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime


class FCICalculator:
    """
    Calcula el Valor Cuotaparte (VCP) de cada cliente como un FCI
    Genera tanto la "Foto" (estado actual) como la "Película" (evolución temporal)
    """
    
    def __init__(self, processed_data_path: Path, analytics_path: Path):
        self.processed_data_path = processed_data_path
        self.analytics_path = analytics_path
        self.reports_path = analytics_path / "reports"
        self.reports_path.mkdir(parents=True, exist_ok=True)
    
    def calculate_vcp(self, client_id: str, account_history: pd.DataFrame, 
                      prices: pd.DataFrame, base_date: Optional[datetime] = None) -> Dict:
        """
        Calcula el Valor Cuotaparte (VCP) para un cliente
        
        VCP = Patrimonio Total / Cuotapartes
        Patrimonio = Σ(Cantidad_i × Precio_i) + Efectivo
        """
        if base_date is None:
            base_date = datetime.now()
        
        # Calcular posición actual (holdings)
        holdings = self._calculate_holdings(account_history, base_date)
        
        # Valorizar holdings con precios actuales
        portfolio_value = self._value_holdings(holdings, prices, base_date)
        
        # Calcular efectivo disponible
        cash = self._calculate_cash(account_history, base_date)
        
        # Patrimonio total
        total_patrimony = portfolio_value + cash
        
        # Calcular cuotapartes (flujos de capital)
        cuotapartes = self._calculate_cuotapartes(account_history, base_date)
        
        # VCP
        vcp = total_patrimony / cuotapartes if cuotapartes > 0 else 0
        
        return {
            "cliente": client_id,
            "fecha": base_date,
            "patrimonio_total": total_patrimony,
            "valor_cartera": portfolio_value,
            "efectivo": cash,
            "cuotapartes": cuotapartes,
            "vcp": vcp,
            "holdings": holdings
        }
    
    def _calculate_holdings(self, account_history: pd.DataFrame, date: datetime) -> pd.DataFrame:
        """
        Calcula las tenencias actuales (cantidad por ticker)
        """
        # Filtrar hasta la fecha indicada
        df = account_history[account_history['fecha'] <= date].copy()
        
        # Agrupar por ticker y sumar cantidades (compras + ventas-)
        holdings = df.groupby('ticker').agg({
            'cantidad': 'sum'
        }).reset_index()
        
        # Filtrar solo posiciones abiertas
        holdings = holdings[holdings['cantidad'] > 0]
        
        return holdings
    
    def _value_holdings(self, holdings: pd.DataFrame, prices: pd.DataFrame, 
                       date: datetime) -> float:
        """
        Valoriza las tenencias con precios de mercado
        """
        total_value = 0
        
        for _, row in holdings.iterrows():
            ticker = row['ticker']
            quantity = row['cantidad']
            
            # Buscar precio más reciente
            ticker_prices = prices[prices['ticker'] == ticker]
            ticker_prices = ticker_prices[ticker_prices['date'] <= date]
            
            if not ticker_prices.empty:
                price = ticker_prices.sort_values('date').iloc[-1]['close']
                total_value += quantity * price
        
        return total_value
    
    def _calculate_cash(self, account_history: pd.DataFrame, date: datetime) -> float:
        """
        Calcula el efectivo disponible
        """
        df = account_history[account_history['fecha'] <= date]
        
        if 'saldo' in df.columns and not df.empty:
            return df.sort_values('fecha').iloc[-1]['saldo']
        
        return 0
    
    def _calculate_cuotapartes(self, account_history: pd.DataFrame, date: datetime) -> float:
        """
        Calcula las cuotapartes (flujos de capital acumulados)
        """
        df = account_history[account_history['fecha'] <= date].copy()
        
        # Identificar aportes y retiros
        aportes = df[df['tipo_operacion'].isin(['aporte', 'deposito', 'transferencia_in'])]
        retiros = df[df['tipo_operacion'].isin(['retiro', 'extraccion', 'transferencia_out'])]
        
        total_aportes = aportes['monto'].sum() if not aportes.empty else 0
        total_retiros = retiros['monto'].sum() if not retiros.empty else 0
        
        # Cuotapartes = Capital Neto Aportado
        cuotapartes = total_aportes - total_retiros
        
        return max(cuotapartes, 1)  # Mínimo 1 para evitar división por 0
    
    def generate_pelicula(self, client_id: str, account_history: pd.DataFrame,
                         prices: pd.DataFrame, start_date: datetime, 
                         end_date: datetime, frequency: str = 'D') -> pd.DataFrame:
        """
        Genera la "Película" - Evolución del VCP a lo largo del tiempo
        """
        # Generar fechas según frecuencia
        date_range = pd.date_range(start=start_date, end=end_date, freq=frequency)
        
        results = []
        
        for date in date_range:
            vcp_data = self.calculate_vcp(client_id, account_history, prices, date)
            results.append({
                'fecha': date,
                'vcp': vcp_data['vcp'],
                'patrimonio': vcp_data['patrimonio_total'],
                'efectivo': vcp_data['efectivo'],
                'valor_cartera': vcp_data['valor_cartera']
            })
        
        df = pd.DataFrame(results)
        
        # Calcular rendimientos
        df['rendimiento_diario'] = df['vcp'].pct_change()
        df['rendimiento_acumulado'] = (df['vcp'] / df['vcp'].iloc[0]) - 1
        
        return df
    
    def save_report(self, client_id: str, pelicula: pd.DataFrame):
        """
        Guarda el reporte de rendimiento
        """
        file_path = self.reports_path / f"{client_id}_rendimiento_mensual.csv"
        pelicula.to_csv(file_path, index=False)
        print(f"✓ Reporte guardado: {file_path}")
