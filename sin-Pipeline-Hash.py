import mysql.connector
import time
import hashlib

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'sumaparalela',
    'autocommit': True
}

BATCH_SIZE = 100_000
NUM_PARTS = 4
HASH_ITERATIONS = 1000  # Mismo número de iteraciones que la versión paralela

def hash_intensive(email):
    """Función intensiva que realiza múltiples hashes"""
    result = email.encode()
    for _ in range(HASH_ITERATIONS):
        result = hashlib.md5(result).digest()
    return result

def serial_pipeline_hash():
    # Inicializar contadores
    subtotales = {i: 0 for i in range(NUM_PARTS)}
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    offset = 0
    total_processed = 0

    print("[Serial] Iniciando procesamiento...")
    
    while True:
        cur.execute(
            "SELECT email FROM customers LIMIT %s OFFSET %s",
            (BATCH_SIZE, offset)
        )
        rows = cur.fetchall()
        if not rows:
            break

        batch_count = 0
        for (email,) in rows:
            if email:
                # Proceso intensivo para cada email
                hash_intensive(email)
                
                # Determinar partición
                h = int(hashlib.md5(email.encode()).hexdigest(), 16) % NUM_PARTS
                subtotales[h] += 1
                batch_count += 1
        
        total_processed += batch_count
        offset += BATCH_SIZE
        if total_processed % 50000 == 0:
            print(f"[Serial] Procesados {total_processed} emails...")

    cur.close()
    conn.close()
    return subtotales, total_processed

if __name__ == '__main__':
    print("Iniciando pipeline serial...")
    print(f"Realizando {HASH_ITERATIONS} iteraciones de hash por email")
    
    t0 = time.time()
    subtotales, total = serial_pipeline_hash()
    tiempo_total = time.time() - t0

    print(f"\n=== RESULTADOS FINALES ===")
    print(f"[Pipeline-serial] Total registros = {total}")
    for parte, subtotal in subtotales.items():
        print(f"  Partición {parte}: {subtotal} registros")
    print("=" * 30)
    print(f"\nTiempo total (Pipeline-serial): {tiempo_total:.2f} segundos")
