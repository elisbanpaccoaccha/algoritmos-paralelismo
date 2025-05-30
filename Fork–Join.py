import mysql.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Datos de conexión
DB_CONFIG = {
    'host':     'localhost',
    'user':     'root',
    'password': '',
    'database': 'sumaparalela',
    'autocommit': True
}

def contar_ciudades_por_pais(pais):
    """Consulta a MySQL y devuelve (pais, número_de_ciudades_distintas)."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(DISTINCT City) FROM customers WHERE Country = %s",
        (pais,)
    )
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return pais, count

if __name__ == '__main__':
    # 1) Recuperamos la lista de países
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT Country FROM customers")
    paises = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    # 2) Lanzamos un pool de hilos
    num_threads = min(32, len(paises))  # no más hilos que países ni un número excesivo
    start = time.time()
    resultados = {}
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Fork: lanzamos una tarea por cada país
        futuros = {executor.submit(contar_ciudades_por_pais, p): p for p in paises}

        # Join: vamos recogiendo los resultados
        for futuro in as_completed(futuros):
            pais, cuenta = futuro.result()
            resultados[pais] = cuenta

    total_time = time.time() - start

    # 3) Mostramos resultados y tiempo
    for pais, cuenta in resultados.items():
        print(f"{pais}: {cuenta} ciudades")
    print(f"\nTiempo total (paralelo con hilos): {total_time:.2f} s")
