# SistemaVentas_ETL  
## Documentación Unificada del Proceso ETL para Dimensiones y Tabla de Hechos

---

## Introducción

Este documento describe de manera detallada el funcionamiento del proceso ETL utilizado para cargar tanto las **dimensiones** como la **tabla de hechos** dentro del Data Warehouse del Sistema de Ventas.

Las **dimensiones** constituyen la base del modelo estrella, proporcionando el contexto descriptivo necesario para analizar las métricas almacenadas en las tablas de hechos.  
La **tabla de hechos FactSales** contiene las transacciones comerciales y métricas cuantitativas del negocio.

El proceso ETL está diseñado para garantizar:

- Cargas sin duplicados  
- Integridad referencial en todo el modelo estrella  
- Idempotencia, permitiendo ejecutar el proceso múltiples veces sin inconsistencias  
- Integración de datos provenientes de múltiples fuentes  

---

## Fuentes Utilizadas en el Proceso ETL

El sistema integra datos provenientes de tres orígenes principales:

- **Archivos CSV**  
- **Base de datos SQL Server (OrigenVentas)**  
- **API REST**

Estas fuentes se emplean tanto para la carga de **dimensiones** como para **hechos**, manteniendo una estructura coherente de extracción.

---

# Arquitectura General del Proceso ETL

El proceso ETL está organizado en tres componentes principales, utilizados tanto para dimensiones como para hechos.

---

## 1. Extractores / Servicios de Origen de Datos

Son responsables de obtener información desde las fuentes disponibles.

### Para dimensiones:
- `CsvExtractor`  
- `DatabaseExtractor`  
- `ApiExtractor`

### Para hechos:
- `CsvDataSourceService`  
- `DatabaseDataSourceService`  
- `ApiDataSourceService`

Su función principal es **leer, interpretar y normalizar** los datos provenientes de cada tipo de origen.

---

## 2. Transformación, Validación y Resolución de Claves

Esta capa ejecuta las operaciones esenciales para garantizar la calidad e integridad del Data Warehouse:

- Normalización de datos  
- Validación de contenido  
- Resolución de claves surrogadas con `DimensionLoader`  
- Aplicación de reglas específicas de negocio  
- Cálculo de métricas derivadas (solo para hechos)

En la carga de hechos se valida además la **existencia previa del registro** mediante su clave de negocio.

---

## 3. DimensionLoader

El `DimensionLoader` cumple una función central dentro del proceso ETL.

### Para dimensiones:
- Verifica si un registro ya existe  
- Inserta nuevas entradas cuando corresponde  
- Retorna la clave surrogada existente o generada  
- Garantiza idempotencia y evita duplicados  

### Para hechos:
- Resuelve claves surrogadas de todas las dimensiones asociadas  
- Verifica si el hecho ya existe mediante su clave de negocio  
- Inserta nuevos registros de ventas  
- Previene duplicados en FactSales  

Este componente asegura coherencia e integridad referencial dentro de todo el Data Warehouse.

---

# Carga de Dimensiones del Data Warehouse

El Sistema de Ventas utiliza cinco dimensiones fundamentales:

- `DimDate`  
- `DimProduct`  
- `DimCustomer`  
- `DimStore`  
- `DimSalesperson`

Cada dimensión se carga bajo reglas específicas según su origen y naturaleza.  
Las cargas son:

- **Incrementales**  
- **Idempotentes**  
- **Referenciales** (toda dimensión debe existir antes de cargar hechos asociados)

---

# Carga de la Tabla de Hechos: FactSales

## Definición

`FactSales` es la tabla de hechos principal y almacena cada línea de venta o transacción comercial.

### Claves foráneas hacia dimensiones:
- Fecha de la venta  
- Producto vendido  
- Cliente  
- Tienda  
- Vendedor  

### Clave de negocio:
- **Número de factura + Número de línea**  
  (garantiza unicidad y permite prevenir duplicados)

### Métricas almacenadas:
- Cantidad vendida  
- Precio unitario  
- Monto total de la línea de venta  

---

## Flujo General de Carga de Hechos

1. **Extracción:**  
   Los servicios leen y normalizan los datos desde CSV, SQL Server o API.

2. **Resolución de claves:**  
   Para cada venta se resuelven todas las claves dimensionales mediante el `DimensionLoader`.  
   Si una dimensión no existe, se inserta automáticamente antes de cargar el hecho.

3. **Validación contra duplicados:**  
   Se verifica si ya existe la combinación número de factura + línea.  
   - Si existe → se omite  
   - Si no existe → se inserta  

4. **Inserción en FactSales:**  
   Se almacena el registro con claves foráneas resueltas y métricas calculadas.

---

# Integración Total del Proceso ETL

La arquitectura unificada garantiza que:

- Las dimensiones estén correctamente cargadas antes de registrar hechos  
- La tabla FactSales mantenga integridad referencial  
- No existan duplicados ni inconsistencias  
- El proceso pueda ejecutarse múltiples veces sin afectar la calidad del Data Warehouse  
- Todo opere bajo un modelo estrella robusto, limpio y analíticamente confiable  

---

