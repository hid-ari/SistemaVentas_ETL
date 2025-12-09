using System.Globalization;

namespace SistemaVentas.Etl.Services
{
    /// <summary>
    /// Servicio unificado para el origen de datos CSV.
    /// Maneja tanto la extracción como la carga de dimensiones y hechos desde archivos CSV.
    /// </summary>
    public class CsvDataSourceService
    {
        private readonly DimensionLoader _dimensionLoader;
        private readonly string _dataFolderPath;

        public CsvDataSourceService(
            DimensionLoader dimensionLoader,
            string dataFolderPath)
        {
            _dimensionLoader = dimensionLoader;
            _dataFolderPath = dataFolderPath;
        }

        #region Dimensiones

        public async Task<int> LoadAllDimensionsAsync()
        {
            Console.WriteLine($"[CSV] Leyendo archivos CSV de dimensiones desde: {_dataFolderPath}");
            Console.WriteLine();

            int totalDimensionsLoaded = 0;

            totalDimensionsLoaded += await LoadDimDateAsync();
            totalDimensionsLoaded += await LoadDimProductAsync();
            totalDimensionsLoaded += await LoadDimCustomerAsync();
            totalDimensionsLoaded += await LoadDimStoreAsync();
            totalDimensionsLoaded += await LoadDimSalespersonAsync();

            Console.WriteLine($"[CSV] Total dimensiones cargadas: {totalDimensionsLoaded} registros");
            return totalDimensionsLoaded;
        }

        private async Task<int> LoadDimDateAsync()
        {
            var csvPath = Path.Combine(_dataFolderPath, "DimDate.csv");
            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"[CSV] No se encontró: {csvPath}");
                return 0;
            }

            Console.WriteLine($"[CSV] Cargando fechas desde: {Path.GetFileName(csvPath)}");
            var lines = await File.ReadAllLinesAsync(csvPath);
            int count = 0;

            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    var cols = lines[i].Split(',', StringSplitOptions.TrimEntries);
                    var fullDate = DateTime.Parse(cols[0], CultureInfo.InvariantCulture);

                    await _dimensionLoader.LoadDateAsync(fullDate);
                    count++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CSV] Error línea {i}: {ex.Message}");
                }
            }

            Console.WriteLine($"[CSV] {count} fechas cargadas");
            return count;
        }

        private async Task<int> LoadDimProductAsync()
        {
            var csvPath = Path.Combine(_dataFolderPath, "DimProducts.csv");
            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"[CSV] No se encontró: {csvPath}");
                return 0;
            }

            Console.WriteLine($"[CSV] Cargando productos desde: {Path.GetFileName(csvPath)}");
            var lines = await File.ReadAllLinesAsync(csvPath);
            int count = 0;

            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    var cols = lines[i].Split(',', StringSplitOptions.TrimEntries);
                    var productCode = cols[0];
                    var productName = cols.Length > 1 ? cols[1] : null;
                    var category = cols.Length > 2 ? cols[2] : null;

                    await _dimensionLoader.LoadProductAsync(productCode, productName, category);
                    count++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CSV] Error línea {i}: {ex.Message}");
                }
            }

            Console.WriteLine($"[CSV] {count} productos cargados");
            return count;
        }

        private async Task<int> LoadDimCustomerAsync()
        {
            var csvPath = Path.Combine(_dataFolderPath, "DimCustomers.csv");
            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"[CSV] No se encontró: {csvPath}");
                return 0;
            }

            Console.WriteLine($"[CSV] Cargando clientes desde: {Path.GetFileName(csvPath)}");
            var lines = await File.ReadAllLinesAsync(csvPath);
            int count = 0;

            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    var cols = lines[i].Split(',', StringSplitOptions.TrimEntries);
                    var customerName = cols[0];
                    var customerType = cols.Length > 1 ? cols[1] : "Retail";
                    var country = cols.Length > 2 ? cols[2] : "República Dominicana";

                    await _dimensionLoader.LoadCustomerAsync(customerName, customerType, country);
                    count++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CSV] Error línea {i}: {ex.Message}");
                }
            }

            Console.WriteLine($"[CSV] {count} clientes cargados");
            return count;
        }

        private async Task<int> LoadDimStoreAsync()
        {
            var csvPath = Path.Combine(_dataFolderPath, "DimStores.csv");
            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"[CSV] No se encontró: {csvPath}");
                return 0;
            }

            Console.WriteLine($"[CSV] Cargando tiendas desde: {Path.GetFileName(csvPath)}");
            var lines = await File.ReadAllLinesAsync(csvPath);
            int count = 0;

            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    if (string.IsNullOrWhiteSpace(lines[i])) continue;
                    
                    var cols = lines[i].Split(',', StringSplitOptions.TrimEntries);
                    if (cols.Length < 1 || string.IsNullOrWhiteSpace(cols[0])) continue;
                    
                    var storeName = cols[0];
                    var country = cols.Length > 1 && !string.IsNullOrWhiteSpace(cols[1]) ? cols[1] : "República Dominicana";
                    var region = cols.Length > 2 && !string.IsNullOrWhiteSpace(cols[2]) ? cols[2] : "Sin Región";
                    var city = cols.Length > 3 && !string.IsNullOrWhiteSpace(cols[3]) ? cols[3] : "Sin Ciudad";

                    await _dimensionLoader.LoadStoreAsync(storeName, country, region, city);
                    count++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CSV] Error línea {i}: {ex.Message}");
                }
            }

            Console.WriteLine($"[CSV] {count} tiendas cargadas");
            return count;
        }

        private async Task<int> LoadDimSalespersonAsync()
        {
            var csvPath = Path.Combine(_dataFolderPath, "DimSalespeople.csv");
            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"[CSV] No se encontró: {csvPath}");
                return 0;
            }

            Console.WriteLine($"[CSV] Cargando vendedores desde: {Path.GetFileName(csvPath)}");
            var lines = await File.ReadAllLinesAsync(csvPath);
            int count = 0;

            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    var cols = lines[i].Split(',', StringSplitOptions.TrimEntries);
                    var salespersonName = cols[0];

                    await _dimensionLoader.LoadSalespersonAsync(salespersonName);
                    count++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CSV] Error línea {i}: {ex.Message}");
                }
            }

            Console.WriteLine($"[CSV] {count} vendedores cargados");
            return count;
        }

        #endregion

        #region FactSales

        public async Task<int> LoadFactSalesAsync()
        {
            var csvPath = Path.Combine(_dataFolderPath, "FactSales.csv");
            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"[CSV] No se encontró: {csvPath}");
                return 0;
            }

            Console.WriteLine($"[CSV] Cargando ventas desde: {Path.GetFileName(csvPath)}");
            var lines = await File.ReadAllLinesAsync(csvPath);
            int count = 0;

            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    if (string.IsNullOrWhiteSpace(lines[i])) continue;
                    
                    var cols = lines[i].Split(',', StringSplitOptions.TrimEntries);
                    
                    if (cols.Length < 9)
                    {
                        Console.WriteLine($"[CSV] Error línea {i}: Formato incompleto (esperadas 9 columnas, encontradas {cols.Length})");
                        continue;
                    }
                    
                    // Formato: invoice_number,product_code,customer_name,store_name,salesperson_name,quantity,unit_price,total_amount,sale_date
                    var invoiceNumber = cols[0];
                    var productCode = cols[1];
                    var customerName = cols[2];
                    var storeName = cols[3];
                    var salespersonName = cols[4];
                    var quantity = int.Parse(cols[5]);
                    var unitPrice = decimal.Parse(cols[6], CultureInfo.InvariantCulture);
                    var totalAmount = decimal.Parse(cols[7], CultureInfo.InvariantCulture);
                    var saleDate = DateTime.Parse(cols[8], CultureInfo.InvariantCulture);

                    // Cargar dimensiones y obtener las claves
                    var keys = await _dimensionLoader.LoadAllDimensionsAsync(
                        saleDate, productCode, customerName, storeName, salespersonName);

                    // Cargar el hecho de venta directamente
                    bool wasInserted = await LoadSaleToWarehouseAsync(
                        keys["DateKey"],
                        keys["ProductKey"],
                        keys["CustomerKey"],
                        keys["StoreKey"],
                        keys["SalespersonKey"],
                        invoiceNumber,
                        1, // line_number
                        quantity,
                        unitPrice,
                        totalAmount
                    );

                    if (wasInserted)
                    {
                        count++;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CSV] Error línea {i}: {ex.Message}");
                }
            }

            Console.WriteLine($"[CSV] {count} ventas cargadas");
            return count;
        }

        private async Task<bool> LoadSaleToWarehouseAsync(
            int dateKey,
            int productKey,
            int customerKey,
            int storeKey,
            int salespersonKey,
            string invoiceNumber,
            int lineNumber,
            int quantity,
            decimal unitPrice,
            decimal totalAmount)
        {
            using var conn = new Microsoft.Data.SqlClient.SqlConnection(_dimensionLoader.GetConnectionString());
            await conn.OpenAsync();

            var checkCmd = new Microsoft.Data.SqlClient.SqlCommand(@"
                SELECT 1 FROM Fact.FactSales 
                WHERE invoice_number = @InvoiceNumber 
                AND line_number = @LineNumber", conn);
            
            checkCmd.Parameters.AddWithValue("@InvoiceNumber", invoiceNumber);
            checkCmd.Parameters.AddWithValue("@LineNumber", lineNumber);

            var exists = await checkCmd.ExecuteScalarAsync();

            if (exists == null)
            {
                var insertCmd = new Microsoft.Data.SqlClient.SqlCommand(@"
                    INSERT INTO Fact.FactSales (
                        date_key, product_key, customer_key, store_key, salesperson_key,
                        invoice_number, line_number, quantity, unit_price, total_amount
                    )
                    VALUES (
                        @DateKey, @ProductKey, @CustomerKey, @StoreKey, @SalespersonKey,
                        @InvoiceNumber, @LineNumber, @Quantity, @UnitPrice, @TotalAmount
                    )", conn);

                insertCmd.Parameters.AddWithValue("@DateKey", dateKey);
                insertCmd.Parameters.AddWithValue("@ProductKey", productKey);
                insertCmd.Parameters.AddWithValue("@CustomerKey", customerKey);
                insertCmd.Parameters.AddWithValue("@StoreKey", storeKey);
                insertCmd.Parameters.AddWithValue("@SalespersonKey", salespersonKey);
                insertCmd.Parameters.AddWithValue("@InvoiceNumber", invoiceNumber);
                insertCmd.Parameters.AddWithValue("@LineNumber", lineNumber);
                insertCmd.Parameters.AddWithValue("@Quantity", quantity);
                insertCmd.Parameters.AddWithValue("@UnitPrice", unitPrice);
                insertCmd.Parameters.AddWithValue("@TotalAmount", totalAmount);

                await insertCmd.ExecuteNonQueryAsync();
                return true; // Se insertó correctamente
            }
            
            return false; // Ya existía, no se insertó
        }

        #endregion
    }
}
