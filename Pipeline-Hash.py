import mysql.connector
import time
from threading import Thread, Event
from queue import Queue
import hashlib
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'sumaparalela',
    'autocommit': True
}
BATCH_SIZE = 100_000
NUM_WORKERS = multiprocessing.cpu_count()  # Usar número de CPUs disponibles
QUEUE_SIZE = BATCH_SIZE 
HASH_ITERATIONS = 1000

def hash_intensive(email):
    """Función intensiva que realiza múltiples hashes"""
    result = email.encode()
    for _ in range(HASH_ITERATIONS):
        result = hashlib.md5(result).digest()
    return int(hashlib.md5(email.encode()).hexdigest(), 16) % NUM_WORKERS, result

def process_batch(batch):
    """Procesa un lote completo de emails y retorna conteos por worker"""
    counts = [0] * NUM_WORKERS
    for email in batch:
        if email:
            worker_id, _ = hash_intensive(email)
            counts[worker_id] += 1
    return counts

def reader_and_processor(result_queue, finished_event):
    """Lee emails de la BD y los procesa en lotes"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        offset = 0
        total_read = 0
        
        # Crear pool de threads para procesar lotes
        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            print(f"[Reader] Iniciando lectura y procesamiento de emails...")
            
            while True:
                cur.execute(
                    "SELECT email FROM customers LIMIT %s OFFSET %s",
                    (BATCH_SIZE, offset)
                )
                rows = cur.fetchall()
                if not rows:
                    break
                
                # Extraer emails del resultado
                batch = [email[0] for email in rows if email[0]]
                
                # Procesar el lote y obtener resultados
                counts = process_batch(batch)
                result_queue.put(counts)
                
                total_read += len(batch)
                offset += BATCH_SIZE
                print(f"[Reader] Procesados {total_read} emails...")
            
        cur.close()
        conn.close()
        
        print(f"[Reader] Terminado. Total emails procesados: {total_read}")
        result_queue.put(None)  # Señal de fin
        
    except Exception as e:
        print(f"[Reader] Error: {e}")
        result_queue.put(None)
    finally:
        finished_event.set()

def collector(result_queue, finished_event):
    """Recolecta y suma los resultados de todos los lotes"""
    print(f"[Collector] Esperando resultados...")
    
    totals = [0] * NUM_WORKERS
    
    while True:
        counts = result_queue.get()
        if counts is None:
            break
            
        for i, count in enumerate(counts):
            totals[i] += count
    
    print(f"\n=== RESULTADOS FINALES ===")
    print(f"[Pipeline-paralelo] Total registros = {sum(totals)}")
    for i, count in enumerate(totals):
        print(f"  Worker {i}: {count} registros")
    print("=" * 30)
    
    finished_event.set()

if __name__ == '__main__':
    print("Iniciando pipeline paralelo...")
    print(f"Realizando {HASH_ITERATIONS} iteraciones de hash por email")
    print(f"Usando {NUM_WORKERS} workers")
    
    result_queue = Queue()
    reader_finished = Event()
    collector_finished = Event()
    
    t0 = time.time()
    
    # Iniciar threads
    reader_thread = Thread(target=reader_and_processor, args=(result_queue, reader_finished))
    collector_thread = Thread(target=collector, args=(result_queue, collector_finished))
    
    reader_thread.start()
    collector_thread.start()
    
    # Esperar a que terminen
    reader_thread.join()
    collector_thread.join()
    
    tiempo_total = time.time() - t0
    print(f"\nTiempo total (Pipeline-paralelo): {tiempo_total:.2f} segundos")