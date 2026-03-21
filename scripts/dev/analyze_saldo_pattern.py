import pandas as pd

path_prev = "/home/jovyan/data/cuentas_corrientes/clean_Cuenta corriente historico en pesos.csv"
path_act = "/home/jovyan/data/cuenta_corriente_historico.csv"

df_prev = pd.read_csv(path_prev)
df_act = pd.read_csv(path_act)

col_num_prev = [c for c in df_prev.columns if 'Numero' in c][0]
col_num_act = [c for c in df_act.columns if 'Numero' in c][0]

df_prev = df_prev[df_prev[col_num_prev].notna()].copy()
df_act = df_act[df_act[col_num_act].notna()].copy()

# Fix types to correctly join numbers (e.g 1023.0 vs 1023 or strings)
df_prev[col_num_prev] = pd.to_numeric(df_prev[col_num_prev], errors='coerce')
df_act[col_num_act] = pd.to_numeric(df_act[col_num_act], errors='coerce')

df_prev = df_prev.dropna(subset=[col_num_prev])
df_act = df_act.dropna(subset=[col_num_act])

# Joineamos por numero
merged = pd.merge(df_prev, df_act, left_on=col_num_prev, right_on=col_num_act, suffixes=('_prev', '_act'))

col_saldo_prev = [c for c in merged.columns if 'Saldo_prev' in c][0]
col_saldo_act = [c for c in merged.columns if 'Saldo_act' in c][0]

merged['Diferencia_Saldo'] = merged[col_saldo_act] - merged[col_saldo_prev]

print(f"Join exitoso. Total registros cruzados: {len(merged)}")
print("Muestra de Saldos cruzados por Nro. de Operación:")
print(merged[[col_num_prev, col_saldo_prev, col_saldo_act, 'Diferencia_Saldo']].head(10).to_string())

corr = merged[col_saldo_prev].corr(merged[col_saldo_act])
print(f"\nCorrelación lineal entre Saldos: {corr}")

# Si la diferencia es constante, podemos inferir que están desfasados por un Saldo Inicial
diffs = merged['Diferencia_Saldo'].round(4)
print("\nDiferencias más comunes (Saldo Actual - Saldo Previa):")
print(diffs.value_counts().head(5).to_string())

merged.to_csv("/home/jovyan/data/cuentas_corrientes/saldo_comparison.csv", index=False)
