# Análisis de Patrones de Programación Paralela en Python

Este proyecto implementa y compara diferentes patrones de programación paralela utilizando Python para procesar grandes conjuntos de datos almacenados en MySQL. Se implementan tres patrones principales: Pipeline, Fork-Join y BSP (Bulk Synchronous Parallel), cada uno con su versión serial correspondiente para comparación de rendimiento.

## 🚀 Características

- Implementación de tres patrones de programación paralela:
  - **Pipeline Pattern**: Procesamiento de emails con hashing intensivo
  - **Fork-Join Pattern**: Análisis de ciudades por país
  - **BSP Pattern**: Procesamiento de datos por períodos mensuales

- Comparación de rendimiento entre versiones:
  - Implementación paralela vs serial
  - Medición de tiempos de ejecución
  - Distribución de carga entre workers

## 📋 Requisitos Previos

- Python 3.13.3
- MySQL Server -XAMPP
- Bibliotecas Python:
  ```
  mysql-connector-python
  multiprocessing
  concurrent.futures
  ```

## 🔧 Configuración

1. Configurar la base de datos MySQL:
```python
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'sumaparalela',
    'autocommit': True
}
```

2. Cargar datos de prueba:
```bash
python insert-data.py
```

## 🔍 Estructura del Proyecto

```
├── Pipeline-Hash.py          # Implementación paralela con pipeline
├── sin-Pipeline-Hash.py      # Versión serial del pipeline
├── Fork-Join.py             # Implementación paralela fork-join
├── sin-Fork-Join.py         # Versión serial fork-join
├── BSP-style.py            # Implementación paralela BSP
├── sin-BSP-style.py        # Versión serial BSP
├── insert-data.py          # Script para cargar datos de prueba
└── README.md               # Esta documentación
```

## 📝 Notas

- Los tiempos de ejecución pueden variar según el hardware
- La configuración de la base de datos puede afectar el rendimiento
- El tamaño del conjunto de datos influye en la efectividad de la paralelización
