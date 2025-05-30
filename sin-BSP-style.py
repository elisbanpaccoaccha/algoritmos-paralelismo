import mysql.connector
import time
from datetime import datetime, timedelta

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'sumaparalela',
    'autocommit': True
}

def suma_mes(start, end):
    """Cuenta registros en un rango de fechas (versión serial)"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Convertir fechas a formato string para la consulta SQL
        start_str = start.strftime('%Y-%m-%d')
        end_str = end.strftime('%Y-%m-%d')
        
        # Usar la misma consulta que BSP-style
        query = "SELECT COUNT(*) FROM customers WHERE subscription_date >= %s AND subscription_date < %s"
        cur.execute(query, (start_str, end_str))
        cnt = cur.fetchone()[0]
        cur.close()
        conn.close()
        return cnt
        
    except Exception as e:
        print(f"Error en consulta para período {start.strftime('%Y-%m')}: {e}")
        return 0

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
    print("Iniciando procesamiento serial BSP-style...")
    
    # Generar los intervalos mensuales
    fechas = generate_monthly_ranges()
    
    print(f"Procesando {len(fechas)} períodos mensuales:")
    for i, (inicio, fin) in enumerate(fechas):
        print(f"  {i}: {inicio.strftime('%Y-%m-%d')} a {fin.strftime('%Y-%m-%d')}")
    
    print("\nProcesando secuencialmente...")
    t0 = time.time()
    resultados = {}
    
    for i, (start, end) in enumerate(fechas):
        label = start.strftime('%Y-%m')
        cantidad = suma_mes(start, end)
        resultados[i] = (label, cantidad)
        print(f"Período {label}: {cantidad} registros")

    total_global = sum(v for _, v in resultados.values())
    tiempo_total = time.time() - t0
    
    print(f"\n[Serial-BSP] Total registros último año = {total_global}")
    print("Desglose por mes:")
    for i in range(len(resultados)):
        mes, cantidad = resultados[i]
        print(f"  {mes}: {cantidad} registros")
    
    print(f"\nTiempo total (serial BSP-style): {tiempo_total:.2f} segundos")