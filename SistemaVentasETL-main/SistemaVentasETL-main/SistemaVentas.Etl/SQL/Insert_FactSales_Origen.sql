USE [OrigenVentas];
GO

DECLARE @FechaInicio DATE = '2025-01-01';
DECLARE @FechaFin DATE = '2025-03-31';
DECLARE @FechaActual DATE = @FechaInicio;

WHILE @FechaActual <= @FechaFin
BEGIN
    IF NOT EXISTS (SELECT 1 FROM [Origen].[DimDate] WHERE full_date = @FechaActual)
    BEGIN
        INSERT INTO [Origen].[DimDate] (date_key, full_date, month_num, quarter_num, year_num)
        VALUES (
            CAST(FORMAT(@FechaActual, 'yyyyMMdd') AS INT),
            @FechaActual,
            MONTH(@FechaActual),
            CEILING(MONTH(@FechaActual) / 3.0),
            YEAR(@FechaActual)
        );
    END
    SET @FechaActual = DATEADD(DAY, 1, @FechaActual);
END
GO

DELETE FROM [Origen].[DimProduct] WHERE product_code LIKE 'P-%';

INSERT INTO [Origen].[DimProduct] (product_code, product_name, category)
VALUES
('P-001', 'Laptop Dell Inspiron 15',      'Computadoras'),
('P-002', 'Mouse Logitech MX Master',     'Accesorios'),
('P-003', 'Teclado Mecánico Corsair',     'Accesorios'),
('P-004', 'Monitor Samsung 27 pulgadas',  'Monitores'),
('P-005', 'Impresora HP LaserJet',        'Impresoras'),
('P-006', 'Disco Duro Externo 1TB',       'Almacenamiento'),
('P-007', 'Memoria RAM 16GB DDR4',        'Componentes'),
('P-008', 'Webcam Logitech C920',         'Accesorios'),
('P-009', 'Audífonos Sony WH-1000XM4',    'Audio'),
('P-010', 'Tablet Samsung Galaxy Tab',    'Tablets'),
('P-011', 'SSD Kingston 500GB',           'Almacenamiento'),
('P-012', 'Router TP-Link AC1750',        'Redes'),
('P-013', 'Cable HDMI 2.0',               'Cables'),
('P-014', 'Mousepad Gaming RGB',          'Accesorios'),
('P-015', 'Silla Gamer Ergonómica',       'Mobiliario');
GO

DELETE FROM [Origen].[DimCustomer] WHERE customer_name IN (
    'TechStore RD', 'Computadoras del Caribe', 'Office Solutions SA', 
    'Digital World', 'Mega Electrónica', 'Distribuidora Nacional',
    'Corporación Tecnológica', 'La Casa del Computador', 'Sistemas Integrados', 'SuperTech'
);

INSERT INTO [Origen].[DimCustomer] (customer_code, customer_name, customer_type, country)
VALUES
('CLI-001', 'TechStore RD',                'Retail',     'República Dominicana'),
('CLI-002', 'Computadoras del Caribe',     'Wholesale',  'República Dominicana'),
('CLI-003', 'Office Solutions SA',         'Corporate',  'República Dominicana'),
('CLI-004', 'Digital World',               'Retail',     'República Dominicana'),
('CLI-005', 'Mega Electrónica',            'Retail',     'República Dominicana'),
('CLI-006', 'Distribuidora Nacional',      'Wholesale',  'República Dominicana'),
('CLI-007', 'Corporación Tecnológica',     'Corporate',  'República Dominicana'),
('CLI-008', 'La Casa del Computador',      'Retail',     'República Dominicana'),
('CLI-009', 'Sistemas Integrados',         'Corporate',  'República Dominicana'),
('CLI-010', 'SuperTech',                   'Retail',     'República Dominicana');
GO

DELETE FROM [Origen].[DimStore] WHERE store_code LIKE 'TIENDA-%';

INSERT INTO [Origen].[DimStore] (store_code, store_name, country, region, city)
VALUES
('TIENDA-001', 'Sucursal Santo Domingo Centro',    'República Dominicana', 'Región Metropolitana', 'Santo Domingo'),
('TIENDA-002', 'Sucursal Santiago',                'República Dominicana', 'Región Cibao',         'Santiago'),
('TIENDA-003', 'Sucursal La Romana',               'República Dominicana', 'Región Este',          'La Romana'),
('TIENDA-004', 'Sucursal Puerto Plata',            'República Dominicana', 'Región Norte',         'Puerto Plata'),
('TIENDA-005', 'Sucursal San Pedro de Macorís',    'República Dominicana', 'Región Este',          'San Pedro de Macorís'),
('TIENDA-006', 'Sucursal Higüey',                  'República Dominicana', 'Región Este',          'Higüey'),
('TIENDA-007', 'Sucursal San Francisco',           'República Dominicana', 'Región Cibao',         'San Francisco de Macorís'),
('TIENDA-008', 'Sucursal Moca',                    'República Dominicana', 'Región Cibao',         'Moca'),
('TIENDA-009', 'Sucursal Barahona',                'República Dominicana', 'Región Suroeste',      'Barahona'),
('TIENDA-010', 'Sucursal Bonao',                   'República Dominicana', 'Región Cibao',         'Bonao');
GO

DELETE FROM [Origen].[DimSalesperson] WHERE salesperson_code LIKE 'VEN-%';

INSERT INTO [Origen].[DimSalesperson] (salesperson_code, salesperson_name)
VALUES
('VEN-001', 'Juan Carlos Pérez'),
('VEN-002', 'María González'),
('VEN-003', 'Pedro Rodríguez'),
('VEN-004', 'Ana Martínez'),
('VEN-005', 'Luis Fernández'),
('VEN-006', 'Carmen Sánchez'),
('VEN-007', 'José López'),
('VEN-008', 'Laura Díaz'),
('VEN-009', 'Carlos Ramírez'),
('VEN-010', 'Isabel Torres');
GO

DECLARE @ProductKeyMap TABLE (ProductCode VARCHAR(10), ProductKey INT);
DECLARE @CustomerKeyMap TABLE (CustomerName VARCHAR(200), CustomerKey INT);
DECLARE @StoreKeyMap TABLE (StoreName VARCHAR(200), StoreKey INT);
DECLARE @SalespersonKeyMap TABLE (SalespersonName VARCHAR(100), SalespersonKey INT);

-- Cargar mapeos de claves
INSERT INTO @ProductKeyMap SELECT product_code, product_key FROM [Origen].[DimProduct] WHERE product_code LIKE 'P-%';
INSERT INTO @CustomerKeyMap SELECT customer_name, customer_key FROM [Origen].[DimCustomer] WHERE customer_code LIKE 'CLI-%';
INSERT INTO @StoreKeyMap SELECT store_name, store_key FROM [Origen].[DimStore] WHERE store_code LIKE 'TIENDA-%';
INSERT INTO @SalespersonKeyMap SELECT salesperson_name, salesperson_key FROM [Origen].[DimSalesperson] WHERE salesperson_code LIKE 'VEN-%';

-- Limpiar ventas existentes de 2025
DELETE FROM [Origen].[FactSales] WHERE invoice_number LIKE 'INV-2025-%';

-- INSERTAR 120 REGISTROS DE VENTAS
INSERT INTO [Origen].[FactSales] 
(date_key, product_key, customer_key, store_key, salesperson_key, invoice_number, line_number, quantity, unit_price, total_amount)
SELECT 
    CAST(FORMAT(CAST(sale_date AS DATE), 'yyyyMMdd') AS INT),
    p.ProductKey,
    c.CustomerKey,
    s.StoreKey,
    sp.SalespersonKey,
    invoice_number,
    ROW_NUMBER() OVER (PARTITION BY invoice_number ORDER BY invoice_number), 
    quantity,
    unit_price,
    total_amount
FROM (VALUES
    ('INV-2025-001', 'P-001', 'TechStore RD',                 'Sucursal Santo Domingo Centro',   'Juan Carlos Pérez',  2,  45000.00,  90000.00,  '2025-01-05'),
    ('INV-2025-002', 'P-004', 'Computadoras del Caribe',      'Sucursal Santiago',               'María González',     5,  12500.00,  62500.00,  '2025-01-05'),
    ('INV-2025-003', 'P-007', 'Office Solutions SA',          'Sucursal Santo Domingo Centro',   'Juan Carlos Pérez', 10,   3500.00,  35000.00,  '2025-01-06'),
    ('INV-2025-004', 'P-002', 'Digital World',                'Sucursal La Romana',              'Pedro Rodríguez',    8,    850.00,   6800.00,  '2025-01-06'),
    ('INV-2025-005', 'P-010', 'Mega Electrónica',             'Sucursal Puerto Plata',           'Ana Martínez',       3,  18500.00,  55500.00,  '2025-01-07'),
    ('INV-2025-006', 'P-005', 'Distribuidora Nacional',       'Sucursal San Pedro de Macorís',   'Luis Fernández',     4,   8500.00,  34000.00,  '2025-01-07'),
    ('INV-2025-007', 'P-003', 'TechStore RD',                 'Sucursal Santo Domingo Centro',   'Carmen Sánchez',     6,   2200.00,  13200.00,  '2025-01-08'),
    ('INV-2025-008', 'P-009', 'Corporación Tecnológica',      'Sucursal Santiago',               'José López',        12,  15500.00, 186000.00,  '2025-01-08'),
    ('INV-2025-009', 'P-006', 'La Casa del Computador',       'Sucursal Higüey',                 'Pedro Rodríguez',    7,   4200.00,  29400.00,  '2025-01-09'),
    ('INV-2025-010', 'P-011', 'Sistemas Integrados',          'Sucursal San Francisco',          'María González',    15,   3800.00,  57000.00,  '2025-01-09'),
    ('INV-2025-011', 'P-012', 'SuperTech',                    'Sucursal Moca',                   'José López',         4,   5500.00,  22000.00,  '2025-01-10'),
    ('INV-2025-012', 'P-008', 'TechStore RD',                 'Sucursal Barahona',               'Laura Díaz',         5,   3200.00,  16000.00,  '2025-01-10'),
    ('INV-2025-013', 'P-001', 'Office Solutions SA',          'Sucursal Santo Domingo Centro',   'Carlos Ramírez',     3,  45000.00, 135000.00,  '2025-01-11'),
    ('INV-2025-014', 'P-013', 'Digital World',                'Sucursal Bonao',                  'Isabel Torres',     20,    280.00,   5600.00,  '2025-01-11'),
    ('INV-2025-015', 'P-014', 'Mega Electrónica',             'Sucursal Santiago',               'María González',    10,    650.00,   6500.00,  '2025-01-12'),
    ('INV-2025-016', 'P-015', 'Computadoras del Caribe',      'Sucursal Santo Domingo Centro',   'Juan Carlos Pérez',  2,  18500.00,  37000.00,  '2025-01-12'),
    ('INV-2025-017', 'P-004', 'TechStore RD',                 'Sucursal La Romana',              'Pedro Rodríguez',    6,  12500.00,  75000.00,  '2025-01-13'),
    ('INV-2025-018', 'P-007', 'Distribuidora Nacional',       'Sucursal Puerto Plata',           'Ana Martínez',      18,   3500.00,  63000.00,  '2025-01-13'),
    ('INV-2025-019', 'P-002', 'La Casa del Computador',       'Sucursal San Pedro de Macorís',   'Luis Fernández',    12,    850.00,  10200.00,  '2025-01-14'),
    ('INV-2025-020', 'P-010', 'Sistemas Integrados',          'Sucursal Santo Domingo Centro',   'Carmen Sánchez',     5,  18500.00,  92500.00,  '2025-01-14'),
    ('INV-2025-021', 'P-005', 'SuperTech',                    'Sucursal Santiago',               'José López',         7,   8500.00,  59500.00,  '2025-01-15'),
    ('INV-2025-022', 'P-003', 'Corporación Tecnológica',      'Sucursal Higüey',                 'Pedro Rodríguez',    9,   2200.00,  19800.00,  '2025-01-15'),
    ('INV-2025-023', 'P-009', 'Digital World',                'Sucursal San Francisco',          'María González',     4,  15500.00,  62000.00,  '2025-01-16'),
    ('INV-2025-024', 'P-006', 'TechStore RD',                 'Sucursal Moca',                   'José López',        11,   4200.00,  46200.00,  '2025-01-16'),
    ('INV-2025-025', 'P-011', 'Office Solutions SA',          'Sucursal Barahona',               'Laura Díaz',         8,   3800.00,  30400.00,  '2025-01-17'),
    ('INV-2025-026', 'P-012', 'Mega Electrónica',             'Sucursal Bonao',                  'Isabel Torres',      6,   5500.00,  33000.00,  '2025-01-17'),
    ('INV-2025-027', 'P-008', 'Computadoras del Caribe',      'Sucursal Santo Domingo Centro',   'Carlos Ramírez',    15,   3200.00,  48000.00,  '2025-01-18'),
    ('INV-2025-028', 'P-001', 'Distribuidora Nacional',       'Sucursal Santiago',               'María González',     4,  45000.00, 180000.00,  '2025-01-18'),
    ('INV-2025-029', 'P-013', 'La Casa del Computador',       'Sucursal La Romana',              'Pedro Rodríguez',   25,    280.00,   7000.00,  '2025-01-19'),
    ('INV-2025-030', 'P-014', 'Sistemas Integrados',          'Sucursal Puerto Plata',           'Ana Martínez',      14,    650.00,   9100.00,  '2025-01-19'),
    ('INV-2025-031', 'P-015', 'SuperTech',                    'Sucursal San Pedro de Macorís',   'Luis Fernández',     3,  18500.00,  55500.00,  '2025-01-20'),
    ('INV-2025-032', 'P-004', 'TechStore RD',                 'Sucursal Santo Domingo Centro',   'Carmen Sánchez',     8,  12500.00, 100000.00,  '2025-01-20'),
    ('INV-2025-033', 'P-007', 'Corporación Tecnológica',      'Sucursal Higüey',                 'Pedro Rodríguez',   22,   3500.00,  77000.00,  '2025-01-21'),
    ('INV-2025-034', 'P-002', 'Digital World',                'Sucursal San Francisco',          'María González',    16,    850.00,  13600.00,  '2025-01-21'),
    ('INV-2025-035', 'P-010', 'Office Solutions SA',          'Sucursal Moca',                   'José López',         6,  18500.00, 111000.00,  '2025-01-22'),
    ('INV-2025-036', 'P-005', 'Mega Electrónica',             'Sucursal Barahona',               'Laura Díaz',         9,   8500.00,  76500.00,  '2025-01-22'),
    ('INV-2025-037', 'P-003', 'Computadoras del Caribe',      'Sucursal Bonao',                  'Isabel Torres',     11,   2200.00,  24200.00,  '2025-01-23'),
    ('INV-2025-038', 'P-009', 'La Casa del Computador',       'Sucursal Santo Domingo Centro',   'Carlos Ramírez',     7,  15500.00, 108500.00,  '2025-01-23'),
    ('INV-2025-039', 'P-006', 'Distribuidora Nacional',       'Sucursal Santiago',               'María González',    13,   4200.00,  54600.00,  '2025-01-24'),
    ('INV-2025-040', 'P-011', 'TechStore RD',                 'Sucursal La Romana',              'Pedro Rodríguez',   19,   3800.00,  72200.00,  '2025-01-24'),
    ('INV-2025-041', 'P-012', 'Sistemas Integrados',          'Sucursal Puerto Plata',           'Ana Martínez',       5,   5500.00,  27500.00,  '2025-01-25'),
    ('INV-2025-042', 'P-008', 'SuperTech',                    'Sucursal San Pedro de Macorís',   'Luis Fernández',    12,   3200.00,  38400.00,  '2025-01-25'),
    ('INV-2025-043', 'P-001', 'Corporación Tecnológica',      'Sucursal Santo Domingo Centro',   'Juan Carlos Pérez',  5,  45000.00, 225000.00,  '2025-01-26'),
    ('INV-2025-044', 'P-013', 'Digital World',                'Sucursal Higüey',                 'Pedro Rodríguez',   30,    280.00,   8400.00,  '2025-01-26'),
    ('INV-2025-045', 'P-014', 'Office Solutions SA',          'Sucursal San Francisco',          'María González',    18,    650.00,  11700.00,  '2025-01-27'),
    ('INV-2025-046', 'P-015', 'Mega Electrónica',             'Sucursal Moca',                   'José López',         4,  18500.00,  74000.00,  '2025-01-27'),
    ('INV-2025-047', 'P-004', 'TechStore RD',                 'Sucursal Barahona',               'Laura Díaz',        10,  12500.00, 125000.00,  '2025-01-28'),
    ('INV-2025-048', 'P-007', 'Computadoras del Caribe',      'Sucursal Bonao',                  'Isabel Torres',     24,   3500.00,  84000.00,  '2025-01-28'),
    ('INV-2025-049', 'P-002', 'La Casa del Computador',       'Sucursal Santo Domingo Centro',   'Carlos Ramírez',    20,    850.00,  17000.00,  '2025-01-29'),
    ('INV-2025-050', 'P-010', 'Distribuidora Nacional',       'Sucursal Santiago',               'María González',     8,  18500.00, 148000.00,  '2025-01-29'),
    ('INV-2025-051', 'P-005', 'Sistemas Integrados',          'Sucursal La Romana',              'Pedro Rodríguez',   11,   8500.00,  93500.00,  '2025-01-30'),
    ('INV-2025-052', 'P-003', 'SuperTech',                    'Sucursal Puerto Plata',           'Ana Martínez',      14,   2200.00,  30800.00,  '2025-01-30'),
    ('INV-2025-053', 'P-009', 'TechStore RD',                 'Sucursal San Pedro de Macorís',   'Luis Fernández',     9,  15500.00, 139500.00,  '2025-01-31'),
    ('INV-2025-054', 'P-006', 'Corporación Tecnológica',      'Sucursal Santo Domingo Centro',   'Carmen Sánchez',    16,   4200.00,  67200.00,  '2025-01-31'),
    ('INV-2025-055', 'P-011', 'Digital World',                'Sucursal Higüey',                 'Pedro Rodríguez',   21,   3800.00,  79800.00,  '2025-02-01'),
    ('INV-2025-056', 'P-012', 'Office Solutions SA',          'Sucursal San Francisco',          'María González',     7,   5500.00,  38500.00,  '2025-02-01'),
    ('INV-2025-057', 'P-008', 'Mega Electrónica',             'Sucursal Moca',                   'José López',        13,   3200.00,  41600.00,  '2025-02-02'),
    ('INV-2025-058', 'P-001', 'Computadoras del Caribe',      'Sucursal Barahona',               'Laura Díaz',         6,  45000.00, 270000.00,  '2025-02-02'),
    ('INV-2025-059', 'P-013', 'La Casa del Computador',       'Sucursal Bonao',                  'Isabel Torres',     35,    280.00,   9800.00,  '2025-02-03'),
    ('INV-2025-060', 'P-014', 'Distribuidora Nacional',       'Sucursal Santo Domingo Centro',   'Carlos Ramírez',    22,    650.00,  14300.00,  '2025-02-03'),
    ('INV-2025-061', 'P-015', 'TechStore RD',                 'Sucursal Santiago',               'María González',     5,  18500.00,  92500.00,  '2025-02-04'),
    ('INV-2025-062', 'P-004', 'Sistemas Integrados',          'Sucursal La Romana',              'Pedro Rodríguez',   12,  12500.00, 150000.00,  '2025-02-04'),
    ('INV-2025-063', 'P-007', 'SuperTech',                    'Sucursal Puerto Plata',           'Ana Martínez',      26,   3500.00,  91000.00,  '2025-02-05'),
    ('INV-2025-064', 'P-002', 'Corporación Tecnológica',      'Sucursal San Pedro de Macorís',   'Luis Fernández',    24,    850.00,  20400.00,  '2025-02-05'),
    ('INV-2025-065', 'P-010', 'Digital World',                'Sucursal Santo Domingo Centro',   'Juan Carlos Pérez', 10,  18500.00, 185000.00,  '2025-02-06'),
    ('INV-2025-066', 'P-005', 'Office Solutions SA',          'Sucursal Higüey',                 'Pedro Rodríguez',   13,   8500.00, 110500.00,  '2025-02-06'),
    ('INV-2025-067', 'P-003', 'Mega Electrónica',             'Sucursal San Francisco',          'María González',    17,   2200.00,  37400.00,  '2025-02-07'),
    ('INV-2025-068', 'P-009', 'TechStore RD',                 'Sucursal Moca',                   'José López',        11,  15500.00, 170500.00,  '2025-02-07'),
    ('INV-2025-069', 'P-006', 'Computadoras del Caribe',      'Sucursal Barahona',               'Laura Díaz',        19,   4200.00,  79800.00,  '2025-02-08'),
    ('INV-2025-070', 'P-011', 'La Casa del Computador',       'Sucursal Bonao',                  'Isabel Torres',     23,   3800.00,  87400.00,  '2025-02-08'),
    ('INV-2025-071', 'P-012', 'Distribuidora Nacional',       'Sucursal Santo Domingo Centro',   'Carlos Ramírez',     9,   5500.00,  49500.00,  '2025-02-09'),
    ('INV-2025-072', 'P-008', 'Sistemas Integrados',          'Sucursal Santiago',               'María González',    15,   3200.00,  48000.00,  '2025-02-09'),
    ('INV-2025-073', 'P-001', 'SuperTech',                    'Sucursal La Romana',              'Pedro Rodríguez',    7,  45000.00, 315000.00,  '2025-02-10'),
    ('INV-2025-074', 'P-013', 'Corporación Tecnológica',      'Sucursal Puerto Plata',           'Ana Martínez',      40,    280.00,  11200.00,  '2025-02-10'),
    ('INV-2025-075', 'P-014', 'Digital World',                'Sucursal San Pedro de Macorís',   'Luis Fernández',    25,    650.00,  16250.00,  '2025-02-11'),
    ('INV-2025-076', 'P-015', 'TechStore RD',                 'Sucursal Santo Domingo Centro',   'Carmen Sánchez',     6,  18500.00, 111000.00,  '2025-02-11'),
    ('INV-2025-077', 'P-004', 'Office Solutions SA',          'Sucursal Higüey',                 'Pedro Rodríguez',   14,  12500.00, 175000.00,  '2025-02-12'),
    ('INV-2025-078', 'P-007', 'Mega Electrónica',             'Sucursal San Francisco',          'María González',    28,   3500.00,  98000.00,  '2025-02-12'),
    ('INV-2025-079', 'P-002', 'Computadoras del Caribe',      'Sucursal Moca',                   'José López',        28,    850.00,  23800.00,  '2025-02-13'),
    ('INV-2025-080', 'P-010', 'La Casa del Computador',       'Sucursal Barahona',               'Laura Díaz',        12,  18500.00, 222000.00,  '2025-02-13'),
    ('INV-2025-081', 'P-005', 'Distribuidora Nacional',       'Sucursal Bonao',                  'Isabel Torres',     15,   8500.00, 127500.00,  '2025-02-14'),
    ('INV-2025-082', 'P-003', 'Sistemas Integrados',          'Sucursal Santo Domingo Centro',   'Carlos Ramírez',    20,   2200.00,  44000.00,  '2025-02-14'),
    ('INV-2025-083', 'P-009', 'SuperTech',                    'Sucursal Santiago',               'María González',    13,  15500.00, 201500.00,  '2025-02-15'),
    ('INV-2025-084', 'P-006', 'TechStore RD',                 'Sucursal La Romana',              'Pedro Rodríguez',   22,   4200.00,  92400.00,  '2025-02-15'),
    ('INV-2025-085', 'P-011', 'Corporación Tecnológica',      'Sucursal Puerto Plata',           'Ana Martínez',      25,   3800.00,  95000.00,  '2025-02-16'),
    ('INV-2025-086', 'P-012', 'Digital World',                'Sucursal San Pedro de Macorís',   'Luis Fernández',    11,   5500.00,  60500.00,  '2025-02-16'),
    ('INV-2025-087', 'P-008', 'Office Solutions SA',          'Sucursal Santo Domingo Centro',   'Juan Carlos Pérez', 17,   3200.00,  54400.00,  '2025-02-17'),
    ('INV-2025-088', 'P-001', 'Mega Electrónica',             'Sucursal Higüey',                 'Pedro Rodríguez',    8,  45000.00, 360000.00,  '2025-02-17'),
    ('INV-2025-089', 'P-013', 'Computadoras del Caribe',      'Sucursal San Francisco',          'María González',    45,    280.00,  12600.00,  '2025-02-18'),
    ('INV-2025-090', 'P-014', 'La Casa del Computador',       'Sucursal Moca',                   'José López',        30,    650.00,  19500.00,  '2025-02-18'),
    ('INV-2025-091', 'P-015', 'TechStore RD',                 'Sucursal Barahona',               'Laura Díaz',         7,  18500.00, 129500.00,  '2025-02-19'),
    ('INV-2025-092', 'P-004', 'Distribuidora Nacional',       'Sucursal Bonao',                  'Isabel Torres',     16,  12500.00, 200000.00,  '2025-02-19'),
    ('INV-2025-093', 'P-007', 'Sistemas Integrados',          'Sucursal Santo Domingo Centro',   'Carlos Ramírez',    30,   3500.00, 105000.00,  '2025-02-20'),
    ('INV-2025-094', 'P-002', 'SuperTech',                    'Sucursal Santiago',               'María González',    32,    850.00,  27200.00,  '2025-02-20'),
    ('INV-2025-095', 'P-010', 'Corporación Tecnológica',      'Sucursal La Romana',              'Pedro Rodríguez',   14,  18500.00, 259000.00,  '2025-02-21'),
    ('INV-2025-096', 'P-005', 'Digital World',                'Sucursal Puerto Plata',           'Ana Martínez',      17,   8500.00, 144500.00,  '2025-02-21'),
    ('INV-2025-097', 'P-003', 'Office Solutions SA',          'Sucursal San Pedro de Macorís',   'Luis Fernández',    23,   2200.00,  50600.00,  '2025-02-22'),
    ('INV-2025-098', 'P-009', 'TechStore RD',                 'Sucursal Santo Domingo Centro',   'Carmen Sánchez',    15,  15500.00, 232500.00,  '2025-02-22'),
    ('INV-2025-099', 'P-006', 'Mega Electrónica',             'Sucursal Higüey',                 'Pedro Rodríguez',   25,   4200.00, 105000.00,  '2025-02-23'),
    ('INV-2025-100', 'P-011', 'Computadoras del Caribe',      'Sucursal San Francisco',          'María González',    27,   3800.00, 102600.00,  '2025-02-23'),
    ('INV-2025-101', 'P-012', 'La Casa del Computador',       'Sucursal Moca',                   'José López',        13,   5500.00,  71500.00,  '2025-02-24'),
    ('INV-2025-102', 'P-008', 'Distribuidora Nacional',       'Sucursal Barahona',               'Laura Díaz',        19,   3200.00,  60800.00,  '2025-02-24'),
    ('INV-2025-103', 'P-001', 'Sistemas Integrados',          'Sucursal Bonao',                  'Isabel Torres',      9,  45000.00, 405000.00,  '2025-02-25'),
    ('INV-2025-104', 'P-013', 'SuperTech',                    'Sucursal Santo Domingo Centro',   'Carlos Ramírez',    50,    280.00,  14000.00,  '2025-02-25'),
    ('INV-2025-105', 'P-014', 'TechStore RD',                 'Sucursal Santiago',               'María González',    35,    650.00,  22750.00,  '2025-02-26'),
    ('INV-2025-106', 'P-015', 'Corporación Tecnológica',      'Sucursal La Romana',              'Pedro Rodríguez',    8,  18500.00, 148000.00,  '2025-02-26'),
    ('INV-2025-107', 'P-004', 'Digital World',                'Sucursal Puerto Plata',           'Ana Martínez',      18,  12500.00, 225000.00,  '2025-02-27'),
    ('INV-2025-108', 'P-007', 'Office Solutions SA',          'Sucursal San Pedro de Macorís',   'Luis Fernández',    32,   3500.00, 112000.00,  '2025-02-27'),
    ('INV-2025-109', 'P-002', 'Mega Electrónica',             'Sucursal Santo Domingo Centro',   'Juan Carlos Pérez', 36,    850.00,  30600.00,  '2025-02-28'),
    ('INV-2025-110', 'P-010', 'Computadoras del Caribe',      'Sucursal Higüey',                 'Pedro Rodríguez',   16,  18500.00, 296000.00,  '2025-02-28'),
    ('INV-2025-111', 'P-005', 'La Casa del Computador',       'Sucursal San Francisco',          'María González',    19,   8500.00, 161500.00,  '2025-03-01'),
    ('INV-2025-112', 'P-003', 'TechStore RD',                 'Sucursal Moca',                   'José López',        26,   2200.00,  57200.00,  '2025-03-01'),
    ('INV-2025-113', 'P-009', 'Distribuidora Nacional',       'Sucursal Barahona',               'Laura Díaz',        17,  15500.00, 263500.00,  '2025-03-02'),
    ('INV-2025-114', 'P-006', 'Sistemas Integrados',          'Sucursal Bonao',                  'Isabel Torres',     28,   4200.00, 117600.00,  '2025-03-02'),
    ('INV-2025-115', 'P-011', 'SuperTech',                    'Sucursal Santo Domingo Centro',   'Carlos Ramírez',    29,   3800.00, 110200.00,  '2025-03-03'),
    ('INV-2025-116', 'P-012', 'Corporación Tecnológica',      'Sucursal Santiago',               'María González',    15,   5500.00,  82500.00,  '2025-03-03'),
    ('INV-2025-117', 'P-008', 'Digital World',                'Sucursal La Romana',              'Pedro Rodríguez',   21,   3200.00,  67200.00,  '2025-03-04'),
    ('INV-2025-118', 'P-001', 'Office Solutions SA',          'Sucursal Puerto Plata',           'Ana Martínez',      10,  45000.00, 450000.00,  '2025-03-04'),
    ('INV-2025-119', 'P-013', 'TechStore RD',                 'Sucursal San Pedro de Macorís',   'Luis Fernández',    55,    280.00,  15400.00,  '2025-03-05'),
    ('INV-2025-120', 'P-014', 'Mega Electrónica',             'Sucursal Santo Domingo Centro',   'Carmen Sánchez',    40,    650.00,  26000.00,  '2025-03-05')
) AS V(invoice_number, product_code, customer_name, store_name, salesperson_name, quantity, unit_price, total_amount, sale_date)
LEFT JOIN @ProductKeyMap p ON V.product_code = p.ProductCode
LEFT JOIN @CustomerKeyMap c ON V.customer_name = c.CustomerName
LEFT JOIN @StoreKeyMap s ON V.store_name = s.StoreName
LEFT JOIN @SalespersonKeyMap sp ON V.salesperson_name = sp.SalespersonName;
GO

SELECT 
    'Fechas' AS Tabla,
    COUNT(*) AS TotalRegistros
FROM [Origen].[DimDate]
WHERE date_key >= 20250101 AND date_key <= 20250331
UNION ALL
SELECT 
    'Productos' AS Tabla,
    COUNT(*) AS TotalRegistros
FROM [Origen].[DimProduct]
WHERE product_code LIKE 'P-%'
UNION ALL
SELECT 
    'Clientes' AS Tabla,
    COUNT(*) AS TotalRegistros
FROM [Origen].[DimCustomer]
WHERE customer_code LIKE 'CLI-%'
UNION ALL
SELECT 
    'Tiendas' AS Tabla,
    COUNT(*) AS TotalRegistros
FROM [Origen].[DimStore]
WHERE store_code LIKE 'TIENDA-%'
UNION ALL
SELECT 
    'Vendedores' AS Tabla,
    COUNT(*) AS TotalRegistros
FROM [Origen].[DimSalesperson]
WHERE salesperson_code LIKE 'VEN-%'
UNION ALL
SELECT 
    'Ventas (FactSales)' AS Tabla,
    COUNT(*) AS TotalRegistros
FROM [Origen].[FactSales]
WHERE invoice_number LIKE 'INV-2025-%';
GO
