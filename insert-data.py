import pandas as pd
import mysql.connector
from mysql.connector import Error
import numpy as np
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
import threading

class DatabaseManager:
    def __init__(self, host='localhost', database='sumaparalela', 
                 user='root', password=''):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
    
    def create_connection(self):
        """Crear conexión a MySQL"""
        try:
            connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            return connection
        except Error as e:
            print(f"Error conectando a MySQL: {e}")
            return None
    
    def create_customers_table(self):
        """Crear tabla customers si no existe"""
        connection = self.create_connection()
        if connection:
            try:
                cursor = connection.cursor()
                create_table_query = """
                CREATE TABLE IF NOT EXISTS customers (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    index_field INT,
                    customer_id VARCHAR(50),
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    company VARCHAR(200),
                    city VARCHAR(100),
                    country VARCHAR(100),
                    phone_1 VARCHAR(20),
                    phone_2 VARCHAR(20),
                    email VARCHAR(150),
                    subscription_date DATE,
                    website VARCHAR(200)
                )
                """
                cursor.execute(create_table_query)
                connection.commit()
                print("Tabla 'customers' creada exitosamente")
            except Error as e:
                print(f"Error creando tabla: {e}")
            finally:
                cursor.close()
                connection.close()

class CSVProcessor:
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def insert_batch(self, batch_data, batch_number):
        """Insertar un lote de datos"""
        connection = self.db_manager.create_connection()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            
            # Query de inserción
            insert_query = """
            INSERT INTO customers (index_field, customer_id, first_name, last_name, 
                                 company, city, country, phone_1, phone_2, email, 
                                 subscription_date, website)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Preparar datos para inserción
            batch_values = []
            for _, row in batch_data.iterrows():
                # Convertir fecha si existe
                sub_date = None
                if pd.notna(row.get('Subscription Date')):
                    try:
                        sub_date = pd.to_datetime(row['Subscription Date']).date()
                    except:
                        sub_date = None
                
                values = (
                    int(row.get('Index', 0)) if pd.notna(row.get('Index')) else 0,
                    str(row.get('Customer Id', ''))[:50],
                    str(row.get('First Name', ''))[:100],
                    str(row.get('Last Name', ''))[:100],
                    str(row.get('Company', ''))[:200],
                    str(row.get('City', ''))[:100],
                    str(row.get('Country', ''))[:100],
                    str(row.get('Phone 1', ''))[:20],
                    str(row.get('Phone 2', ''))[:20],
                    str(row.get('Email', ''))[:150],
                    sub_date,
                    str(row.get('Website', ''))[:200]
                )
                batch_values.append(values)
            
            # Ejecutar inserción por lotes
            cursor.executemany(insert_query, batch_values)
            connection.commit()
            
            print(f"Lote {batch_number}: {len(batch_values)} registros insertados")
            return True
            
        except Error as e:
            print(f"Error insertando lote {batch_number}: {e}")
            return False
        finally:
            cursor.close()
            connection.close()
    
    def process_csv_in_batches(self, csv_file_path, batch_size=1000):
        """Procesar CSV en lotes"""
        try:
            # Leer CSV completo para obtener información
            print("Leyendo archivo CSV...")
            df = pd.read_csv(csv_file_path)
            total_rows = len(df)
            print(f"Total de registros: {total_rows}")
            
            # Procesar en lotes
            num_batches = (total_rows + batch_size - 1) // batch_size
            print(f"Procesando en {num_batches} lotes de {batch_size} registros")
            
            for i in range(0, total_rows, batch_size):
                batch_data = df.iloc[i:i+batch_size]
                batch_number = (i // batch_size) + 1
                
                success = self.insert_batch(batch_data, batch_number)
                if not success:
                    print(f"Error procesando lote {batch_number}")
                    break
                
                # Pequeña pausa entre lotes para no sobrecargar
                time.sleep(0.1)
            
            print("Procesamiento completado!")
            
        except Exception as e:
            print(f"Error procesando CSV: {e}")

class ParallelSummer:
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def get_numeric_data(self, limit=1000000):
        """Obtener datos numéricos para suma (usando Index como ejemplo)"""
        connection = self.db_manager.create_connection()
        if not connection:
            return []
        
        try:
            cursor = connection.cursor()
            query = f"SELECT index_field FROM customers LIMIT {limit}"
            cursor.execute(query)
            results = cursor.fetchall()
            return [row[0] for row in results if row[0] is not None]
        except Error as e:
            print(f"Error obteniendo datos: {e}")
            return []
        finally:
            cursor.close()
            connection.close()
    
    def sum_chunk(self, chunk):
        """Sumar un pedazo de datos"""
        return sum(chunk)
    
    def parallel_sum_threads(self, data, num_threads=4):
        """Suma paralela usando threads"""
        if not data:
            return 0
        
        chunk_size = len(data) // num_threads
        chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(self.sum_chunk, chunk) for chunk in chunks]
            results = [future.result() for future in futures]
        
        return sum(results)
    
    def parallel_sum_processes(self, data, num_processes=4):
        """Suma paralela usando procesos"""
        if not data:
            return 0
        
        chunk_size = len(data) // num_processes
        chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
        
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            futures = [executor.submit(self.sum_chunk, chunk) for chunk in chunks]
            results = [future.result() for future in futures]
        
        return sum(results)
    
    def benchmark_sums(self, data):
        """Comparar diferentes métodos de suma"""
        print(f"\n=== BENCHMARK DE SUMAS ({len(data)} elementos) ===")
        
        # Suma secuencial
        start_time = time.time()
        sequential_sum = sum(data)
        sequential_time = time.time() - start_time
        print(f"Suma secuencial: {sequential_sum} - Tiempo: {sequential_time:.4f}s")
        
        # Suma paralela con threads
        start_time = time.time()
        thread_sum = self.parallel_sum_threads(data, 4)
        thread_time = time.time() - start_time
        print(f"Suma con threads: {thread_sum} - Tiempo: {thread_time:.4f}s")
        
        # Suma paralela con procesos
        start_time = time.time()
        process_sum = self.parallel_sum_processes(data, 4)
        process_time = time.time() - start_time
        print(f"Suma con procesos: {process_sum} - Tiempo: {process_time:.4f}s")
        
        # Suma con NumPy (optimizada)
        np_data = np.array(data)
        start_time = time.time()
        numpy_sum = np.sum(np_data)
        numpy_time = time.time() - start_time
        print(f"Suma con NumPy: {int(numpy_sum)} - Tiempo: {numpy_time:.4f}s")

def main():
    """Función principal"""
    # Configurar base de datos
    db_manager = DatabaseManager(
        host='localhost',
        database='sumaparalela',  # Cambia por tu base de datos
        user='root',
        password=''  # Cambia por tu password
    )
    
    # Crear tabla
    db_manager.create_customers_table()
    
    # Procesar CSV
    csv_processor = CSVProcessor(db_manager)
    
    # Cambiar por la ruta de tu archivo CSV
    csv_file_path = 'customers-1000000.csv'  
    
    print("=== INSERTANDO DATOS EN LOTES ===")
    csv_processor.process_csv_in_batches(csv_file_path, batch_size=500)
    
    # Realizar sumas paralelas
    print("\n=== REALIZANDO SUMAS PARALELAS ===")
    summer = ParallelSummer(db_manager)
    
    # Obtener datos para sumar
    print("Obteniendo datos de la base de datos...")
    numeric_data = summer.get_numeric_data(1000000)
    
    if numeric_data:
        print(f"Datos obtenidos: {len(numeric_data)} registros")
        summer.benchmark_sums(numeric_data)
    else:
        print("No se encontraron datos para sumar")

if __name__ == "__main__":
    main()