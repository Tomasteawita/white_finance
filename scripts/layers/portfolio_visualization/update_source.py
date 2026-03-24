import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r'c:\Users\tomas\white_finance\.env')
user = os.getenv('POSTGRE_USER', 'postgres')
pwd = os.getenv('POSTGRE_PASSWORD', 'postgres')
host = os.getenv('POSTGRE_HOST', 'localhost')
port = os.getenv('POSTGRE_PORT', '5432')
db = os.getenv('POSTGRE_DB', 'postgres')

conn_str = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
engine = create_engine(conn_str)

with engine.begin() as conn:
    # See what is matching first
    res = conn.execute(text("SELECT ticker, source FROM earnings.historical_prices WHERE ticker LIKE '%SNSBO%' LIMIT 5")).fetchall()
    print("Found rows before update:")
    for r in res:
        print(f"Ticker: '{r[0]}', Source: '{r[1]}'")
        
    res_update = conn.execute(text("UPDATE earnings.historical_prices SET source = 'Cohen' WHERE ticker LIKE '%SNSBO%'"))
    print(f"\nFilas actualizadas: {res_update.rowcount}")
