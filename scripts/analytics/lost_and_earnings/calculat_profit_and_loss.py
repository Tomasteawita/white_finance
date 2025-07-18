"""
Genera un dataframe con el restulado de ganancias/perdidas de operaciones de compra/venta de activos,
a partir de un archivo CSV, proveniente del excel que brinda BullMarket de nuestra cuenta corriente.
"""
import pandas as pd

def calculat_profit_and_loss(ruta_archivo_csv: str):
    """
    Calcula la ganancia o pérdida realizada para cada operación de venta
    de activos a partir de un archivo CSV de cuenta corriente.

    Args:
        ruta_archivo_csv (str): La ruta al archivo CSV exportado del broker.

    Returns:
        pandas.DataFrame: Un DataFrame con el resultado de cada venta.
    """
    # 1. Cargar y preparar los datos del CSV
    df = pd.read_csv(
        ruta_archivo_csv,
        delimiter=',',  # El delimitador parece ser punto y coma
        decimal='.'     # Usar coma como separador decimal
    )

    # Convertir columnas a los tipos de datos correctos
    df['Operado'] = pd.to_datetime(df['Operado'], format='mixed', dayfirst=True, errors='coerce')
    # Asegurarse que las columnas numéricas sean floats
    for col in ['Cantidad', 'Precio', 'Importe']:
        if df[col].dtype == 'object':
            df[col] = df[col].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)

    # 2. Filtrar solo operaciones de compra y venta
    operaciones = df[df['Comprobante'].isin(['COMPRA NORMAL', 'VENTA'])].copy()
    operaciones = operaciones.sort_values(by='Operado', ascending=True)

    # 3. Lógica principal: Iterar y calcular
    cartera = {}  # Diccionario para seguir el estado de cada activo
                    # Ejemplo: {'GGAL': {'cantidad': 100, 'costo_total': 45000}}
    resultados = [] # Lista para guardar los resultados de las ventas

    print("Procesando operaciones...")

    for index, op in operaciones.iterrows():
        especie = op['Especie']
        cantidad = op['Cantidad']
        importe = op['Importe']
        precio_op = op['Precio']

        # Inicializar el activo en la cartera si no existe
        if especie not in cartera and op['Comprobante'] == 'VENTA':
            print(f"ADVERTENCIA: Se intentó vender {cantidad} de {especie}, pero no hay registro de compra. Se omitirá.")
            continue

        elif especie not in cartera:
            cartera[especie] = {'cantidad_total': 0.0, 'costo_total': 0.0}

        # Si es una COMPRA
        if op['Comprobante'] == 'COMPRA NORMAL':
            cartera[especie]['cantidad_total'] += abs(cantidad)
            cartera[especie]['costo_total'] += abs(importe) # El importe de compra es negativo
            print(f"Compra: {cantidad:.2f} de {especie} a ${precio_op:.2f}")


        # Si es una VENTA
        elif op['Comprobante'] == 'VENTA':
            if abs(cartera[especie]['cantidad_total']) < abs(cantidad):
                print(f"ADVERTENCIA: Se intentó vender {cantidad} de {especie}, pero solo hay {cartera[especie]['cantidad_total']} en cartera. Se omitirá.")
                cartera.pop(especie, None)
                print(f"Se eliminó {especie} de la cartera por falta de cantidad suficiente.")
                continue

            # Calcular el Precio Promedio de Compra (PPC) al momento de la venta
            if cartera[especie]['cantidad_total'] > 0:
                ppc = cartera[especie]['costo_total'] / cartera[especie]['cantidad_total']
            else:
                ppc = 0 # Evitar división por cero

            # Calcular el costo de los activos vendidos
            costo_de_venta = ppc * cantidad
            # La ganancia es el importe de la venta (positivo) menos el costo
            ganancia_perdida = abs(importe) - abs(costo_de_venta)

            print(f"Venta: {cantidad:.2f} de {especie} a ${precio_op:.2f}. PPC: ${ppc:.2f}. Resultado: ${ganancia_perdida:.2f}")

            # Registrar el resultado de la operación
            resultados.append({
                'Fecha Venta': op['Operado'].date(),
                'Activo': especie,
                'Cantidad Vendida': cantidad,
                'Precio Venta': precio_op,
                'Precio Promedio Compra (PPC)': ppc,
                'Costo Total Venta': abs(costo_de_venta),
                'Ganancia/Perdida ($)': ganancia_perdida
            })

            # Actualizar la cartera después de la venta
            cartera[especie]['cantidad_total'] -= cantidad
            cartera[especie]['costo_total'] -= costo_de_venta

            # Si se vendió todo, se puede resetear el costo para evitar errores de flotantes
            if cartera[especie]['cantidad_total'] < 1e-9: # Un número muy pequeño
                cartera[especie]['cantidad_total'] = 0
                cartera[especie]['costo_total'] = 0


    print("\n¡Cálculo finalizado!")
    return pd.DataFrame(resultados)

# --- EJECUCIÓN DEL SCRIPT ---
# Reemplaza 'ruta/a/tu/archivo.csv' con la ruta real de tu archivo
# Como subiste el archivo, podemos usar su nombre directamente.
nombre_archivo = '/home/jovyan/data/cuenta_corriente_historic.csv'
resultados_df = calculat_profit_and_loss(nombre_archivo)

# Mostrar el DataFrame con los resultados
if isinstance(resultados_df, pd.DataFrame) and not resultados_df.empty:
    print("\n--- Resumen de Ganancias y Pérdidas Realizadas ---")
    # Formatear los números para mejor lectura
    pd.options.display.float_format = '{:,.2f}'.format
    print(resultados_df.to_string()) # Usamos to_string para ver todas las filas y columnas
else:
    print("\nNo se encontraron operaciones de venta para calcular o hubo un error.")
    if not isinstance(resultados_df, pd.DataFrame):
        print(resultados_df) # Muestra el mensaje de error