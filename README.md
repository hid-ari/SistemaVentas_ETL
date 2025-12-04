# SistemaVentas_ETL
INTRODUCCIÓN
Este documento describe de forma detallada el proceso de carga de dimensiones dentro
del Data Warehouse del Sistema de Ventas.
Las dimensiones constituyen la base estructural del modelo estrella, ya que proporcionan
el contexto descriptivo necesario para analizar las métricas almacenadas en las tablas de
hechos. El proceso ETL garantiza que las dimensiones se carguen de manera correcta, sin
duplicados, con integridad de datos y provenientes de diversas fuentes.
Fuentes utilizadas para la carga de dimensiones:
• Archivos CSV
• Base de datos SQL Server (OrigenVentas)
• API REST
Las cargas se realizan de forma incremental y validada, permitiendo ejecutar el proceso
múltiples veces sin crear registros duplicados.
ARQUITECTURA PARA LA CARGA DE DIMENSIONES
El proceso ETL orientado a dimensiones está estructurado en tres componentes:
1. Extractores
Obtienen datos desde las distintas fuentes:
• CsvExtractor
• DatabaseExtractor
• ApiExtractor
2. Transformación y Validación
Normaliza datos, valida contenido y genera claves surrogadas cuando aplica.
3. DimensionLoader
Es la capa encargada de:
• Verificar si un registro ya existe
• Insertar nuevas entradas cuando corresponda
• Retornar la clave generada o existente
Este patrón asegura:
• No duplicación de datos
• Integridad referencial
• Idempotencia del proceso
DIMENSIONES DEFINIDAS EN EL DATA WAREHOUSE
Las dimensiones del Sistema de Ventas son:
• DimDate
• DimProduct
• DimCustomer
• DimStore
• DimSalesperson
Cada una responde a un área funcional del negocio y se carga mediante reglas
específicas.
