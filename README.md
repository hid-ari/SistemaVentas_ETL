# SistemaVentas_ETL

## Introducción
Este documento describe de forma detallada el proceso de **carga de dimensiones** dentro del Data Warehouse del Sistema de Ventas.

Las dimensiones constituyen la base estructural del modelo estrella, proporcionando el contexto descriptivo necesario para analizar las métricas almacenadas en las tablas de hechos.  
El proceso ETL garantiza que las dimensiones se carguen correctamente, sin duplicados, manteniendo la integridad de datos y utilizando múltiples fuentes.

**Fuentes utilizadas para la carga de dimensiones:**
- Archivos CSV  
- Base de datos SQL Server (OrigenVentas)  
- API REST  

Las cargas se realizan de forma **incremental e idempotente**, permitiendo ejecutar el proceso varias veces sin generar registros duplicados.

---

## Arquitectura para la Carga de Dimensiones
El proceso ETL orientado a dimensiones se organiza en tres componentes principales:

### 1. Extractores
Responsables de obtener datos desde diferentes orígenes:
- `CsvExtractor`
- `DatabaseExtractor`
- `ApiExtractor`

### 2. Transformación y Validación
Se encarga de:
- Normalizar los datos
- Aplicar validaciones de contenido
- Generar claves surrogadas cuando corresponde

### 3. DimensionLoader
Componente encargado de la lógica de carga de dimensiones. Sus funciones incluyen:
- Verificar si un registro ya existe
- Insertar nuevas entradas cuando corresponda
- Retornar la clave existente o generada

Este diseño garantiza:
- No duplicación de datos  
- Integridad referencial  
- Idempotencia del proceso ETL  

---

## Dimensiones Definidas en el Data Warehouse
El Sistema de Ventas utiliza las siguientes dimensiones:

- **DimDate**
- **DimProduct**
- **DimCustomer**
- **DimStore**
- **DimSalesperson**

Cada una responde a un área funcional del negocio y se carga siguiendo reglas específicas definidas para su naturaleza y origen de datos.

---
