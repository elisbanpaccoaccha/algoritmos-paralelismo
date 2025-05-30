import mysql.connector
import time
from multiprocessing import Process, Barrier, Manager
from datetime import datetime, timedelta

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'sumaparalela',
    'autocommit': True
}

def worker_bsp(date_start, date_end, barrier, results, idx):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Convertir fechas a formato string para la consulta SQL
        date_start_str = date_start.strftime('%Y-%m-%d')
        date_end_str = date_end.strftime('%Y-%m-%d')
        
        # Ejecutar la consulta principal
        query = "SELECT COUNT(*) FROM customers WHERE subscription_date >= %s AND subscription_date < %s"
        cur.execute(query, (date_start_str, date_end_str))
        cnt = cur.fetchone()[0]
        cur.close()
        conn.close()

        # Almacenar resultado
        results[idx] = (date_start.strftime('%Y-%m'), cnt)
        print(f"Proceso {idx}: Período {date_start.strftime('%Y-%m')}, Registros: {cnt}")
        
    except Exception as e:
        print(f"Error en proceso {idx}: {e}")
        results[idx] = (date_start.strftime('%Y-%m'), 0)
    
    # Sincronizar todos los procesos
    barrier.wait()

    # Solo el proceso 0 imprime el total
    if idx == 0:
        total = sum(v for _, v in results.values())
        print(f"\n[BSP-paralelo] Total registros último año = {total}")
        print("Desglose por mes:")
        for i in range(len(results)):
            mes, cantidad = results[i]
            print(f"  {mes}: {cantidad} registros")

def generate_monthly_ranges():
    """Genera 12 rangos mensuales basados en los datos reales de la BD"""
    # Basándose en el diagnóstico: datos van de 2020-01-01 a 2022-05-29
    # Vamos a tomar los últimos 12 meses de datos disponibles
    
    # Comenzar desde mayo 2022 (último mes con datos) y retroceder 12 meses
    end_date = datetime(2022, 6, 1)  # Primer día después del último mes con datos
    fechas = []
    
    for i in range(12):
        # Calcular el mes final
        year = end_date.year
        month = end_date.month - i
        
        # Ajustar año si el mes es negativo o cero
        while month <= 0:
            month += 12
            year -= 1
        
        # Primer día del mes
        fin = datetime(year, month, 1)
        
        # Primer día del siguiente mes (para el rango)
        if month == 1:
            inicio = datetime(year - 1, 12, 1)
        else:
            inicio = datetime(year, month - 1, 1)
        
        fechas.append((inicio, fin))
    
    # Invertir para tener orden cronológico ascendente
    return list(reversed(fechas))


if __name__ == '__main__':
    print("Iniciando procesamiento BSP paralelo...")
    
    
    # Generar 12 particiones mensuales
    fechas = generate_monthly_ranges()
    
    print(f"Procesando {len(fechas)} períodos mensuales:")
    for i, (inicio, fin) in enumerate(fechas):
        print(f"  {i}: {inicio.strftime('%Y-%m-%d')} a {fin.strftime('%Y-%m-%d')}")
    
    N = len(fechas)
    mgr = Manager()
    results = mgr.dict()
    barrier = Barrier(N)
    procs = []

    print(f"\nIniciando {N} procesos paralelos...")
    t0 = time.time()
    
    # Crear y lanzar procesos
    for i, (ds, de) in enumerate(fechas):
        p = Process(target=worker_bsp, args=(ds, de, barrier, results, i))
        p.start()
        procs.append(p)
    
    # Esperar a que terminen todos los procesos
    for p in procs:
        p.join()
    
    tiempo_total = time.time() - t0
    print(f"\nTiempo total (BSP-paralelo): {tiempo_total:.2f} segundos")