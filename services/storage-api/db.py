import os, psycopg2
from contextlib import contextmanager

# La cadena de conexión se arma con variables de entorno que define docker-compose
# host=postgres funciona porque el servicio de Postgres en compose se llama "postgres"
DSN = f"dbname={os.getenv('PG_DB')} user={os.getenv('PG_USER')} password={os.getenv('PG_PASS')} host=postgres"

@contextmanager
def conn_cursor():
    """
    Abre conexión y cursor a Postgres; hace commit y cierra automáticamente.
    Lo usamos con 'with' para no olvidar cerrar nada.
    """
    conn = psycopg2.connect(DSN)
    try:
        yield conn, conn.cursor()
        conn.commit()
    finally:
        conn.close()

def init_db():
    """
    Lee el archivo models.sql y lo ejecuta para crear tablas/vistas si no existen.
    Se llama una sola vez al iniciar la app.
    """
    with open("/app/models.sql","r",encoding="utf-8") as f:
        ddl = f.read()
    with conn_cursor() as (c,cur):
        cur.execute(ddl)
