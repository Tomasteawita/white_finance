import pandas as pd
import boto3
from io import BytesIO

def check_duplicates_s3():
    s3 = boto3.client('s3')
    bucket = 'withefinance-integrated'
    prefix = 'cuenta_corriente_historico/'
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if 'Contents' not in response:
        print("No se encontraron archivos en el bucket/prefijo especificado.")
        return
        
    for obj in response['Contents']:
        key = obj['Key']
        if not key.endswith('.csv'):
            continue
            
        print(f"\n==============================================")
        print(f"🔍 Analizando integridad en: {key}")
        print(f"==============================================")
        
        try:
            # Descargamos el archivo entero en memoria RAM
            obj_response = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(BytesIO(obj_response['Body'].read()))
            
            if 'Numero' not in df.columns:
                print("⚠️ Este archivo no contiene una columna 'Numero' para evaluar.")
                continue
                
            # Filtramos todos los registros cuyo 'Numero' aparece maÌs de una vez
            duplicates = df[df.duplicated(subset=['Numero'], keep=False)]
            
            if duplicates.empty:
                print("✅ Integridad Perfecta: No hay números de operación repetidos.")
            else:
                print(f"🚨 ¡ALERTA! Se hallaron {len(duplicates)} registros en conflicto (comparten Número de Comprobante).")
                print("----------------------------------------------")
                
                cols_to_show = [col for col in ['Liquida', 'Operado', 'Comprobante', 'Numero', 'Especie', 'Cantidad', 'Importe', 'Saldo'] if col in duplicates.columns]
                sorted_dups = duplicates.sort_values('Numero')[cols_to_show]
                print(sorted_dups.to_string(index=False))
                print("----------------------------------------------\n")
                
        except Exception as e:
            print(f"Error procesando {key}: {e}")

if __name__ == "__main__":
    check_duplicates_s3()
