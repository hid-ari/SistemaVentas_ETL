using Microsoft.Data.SqlClient;

namespace SistemaVentas.Etl.Services
{
    /// <summary>
    /// Servicio unificado para el origen de datos Database (SQL Server).
    /// Maneja tanto la extracción como la carga de dimensiones y hechos desde la base de datos fuente.
    /// </summary>
    public class DatabaseDataSourceService
    {
        private readonly string _sourceConnection;
        private readonly DimensionLoader _dimensionLoader;

        public DatabaseDataSourceService(
            string sourceConnection,
            DimensionLoader dimensionLoader)
        {
            _sourceConnection = sourceConnection;
            _dimensionLoader = dimensionLoader;
        }

        #region Dimensiones

        public async Task<int> LoadAllDimensionsAsync()
        {
            Console.WriteLine("[DATABASE] Conectando a base de datos OrigenVentas...");

            using var sourceConn = new SqlConnection(_sourceConnection);
            await sourceConn.OpenAsync();

            int dimensionsLoaded = 0;

            dimensionsLoaded += await LoadDimDateAsync(sourceConn);
            dimensionsLoaded += await LoadDimProductAsync(sourceConn);
            dimensionsLoaded += await LoadDimCustomerAsync(sourceConn);
            dimensionsLoaded += await LoadDimStoreAsync(sourceConn);
            dimensionsLoaded += await LoadDimSalespersonAsync(sourceConn);

            Console.WriteLine($"[DATABASE] Total dimensiones cargadas: {dimensionsLoaded} registros");
            return dimensionsLoaded;
        }

        private async Task<int> LoadDimDateAsync(SqlConnection sourceConn)
        {
            Console.WriteLine("[DATABASE] Cargando fechas desde Origen.DimDate...");
            
            using var cmdDate = new SqlCommand(@"
                SELECT DISTINCT date_key, full_date, month_num, quarter_num, year_num
                FROM Origen.DimDate
                ORDER BY date_key", sourceConn);

            int count = 0;
            using var reader = await cmdDate.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var dateKey = reader.GetInt32(0);
                var fullDate = reader.GetDateTime(1);
                var monthNum = reader.GetInt32(2);
                var quarterNum = reader.GetInt32(3);
                var yearNum = reader.GetInt32(4);

                await _dimensionLoader.LoadDateDimensionAsync(dateKey, fullDate, monthNum, quarterNum, yearNum);
                count++;
            }

            Console.WriteLine($"[DATABASE] {count} fechas cargadas");
            return count;
        }

        private async Task<int> LoadDimProductAsync(SqlConnection sourceConn)
        {
            Console.WriteLine("[DATABASE] Cargando productos desde Origen.DimProduct...");
            
            using var cmdProduct = new SqlCommand(@"
                SELECT product_key, product_code, product_name, category
                FROM Origen.DimProduct
                ORDER BY product_key", sourceConn);

            int count = 0;
            using var reader = await cmdProduct.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var productCode = reader.GetString(1);
                var productName = reader.GetString(2);
                var category = reader.IsDBNull(3) ? "" : reader.GetString(3);

                await _dimensionLoader.LoadProductDimensionAsync(productCode, productName, category);
                count++;
            }

            Console.WriteLine($"[DATABASE] {count} productos cargados");
            return count;
        }

        private async Task<int> LoadDimCustomerAsync(SqlConnection sourceConn)
        {
            Console.WriteLine("[DATABASE] Cargando clientes desde Origen.DimCustomer...");
            
            using var cmdCustomer = new SqlCommand(@"
                SELECT customer_key, customer_code, customer_name, customer_type, country
                FROM Origen.DimCustomer
                ORDER BY customer_key", sourceConn);

            int count = 0;
            using var reader = await cmdCustomer.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var customerCode = reader.GetString(1);
                var customerName = reader.GetString(2);
                var customerType = reader.GetString(3);
                var country = reader.GetString(4);

                await _dimensionLoader.LoadCustomerDimensionAsync(customerCode, customerName, customerType, country);
                count++;
            }

            Console.WriteLine($"[DATABASE] {count} clientes cargados");
            return count;
        }

        private async Task<int> LoadDimStoreAsync(SqlConnection sourceConn)
        {
            Console.WriteLine("[DATABASE] Cargando tiendas desde Origen.DimStore...");
            
            using var cmdStore = new SqlCommand(@"
                SELECT store_key, store_code, store_name, country, region, city
                FROM Origen.DimStore
                ORDER BY store_key", sourceConn);

            int count = 0;
            using var reader = await cmdStore.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var storeCode = reader.GetString(1);
                var storeName = reader.GetString(2);
                var country = reader.GetString(3);
                var region = reader.IsDBNull(4) ? "Sin Región" : reader.GetString(4);
                var city = reader.IsDBNull(5) ? "Sin Ciudad" : reader.GetString(5);

                await _dimensionLoader.LoadStoreDimensionAsync(storeCode, storeName, country, region, city);
                count++;
            }

            Console.WriteLine($"[DATABASE] {count} tiendas cargadas");
            return count;
        }

        private async Task<int> LoadDimSalespersonAsync(SqlConnection sourceConn)
        {
            Console.WriteLine("[DATABASE] Cargando vendedores desde Origen.DimSalesperson...");
            
            using var cmdSalesperson = new SqlCommand(@"
                SELECT salesperson_key, salesperson_code, salesperson_name
                FROM Origen.DimSalesperson
                ORDER BY salesperson_key", sourceConn);

            int count = 0;
            using var reader = await cmdSalesperson.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var salespersonCode = reader.GetString(1);
                var salespersonName = reader.GetString(2);

                await _dimensionLoader.LoadSalespersonDimensionAsync(salespersonCode, salespersonName);
                count++;
            }

            Console.WriteLine($"[DATABASE] {count} vendedores cargados");
            return count;
        }

        #endregion

        #region FactSales

        public async Task<int> LoadFactSalesAsync()
        {
            Console.WriteLine("[DATABASE] Conectando a base de datos OrigenVentas para FactSales...");

            using var sourceConn = new SqlConnection(_sourceConnection);
            await sourceConn.OpenAsync();

            int factSalesLoaded = 0;

            Console.WriteLine("[DATABASE] Cargando ventas desde Origen.FactSales...");
            using (var cmdSales = new SqlCommand(@"
                SELECT 
                    fs.invoice_number,
                    fs.line_number,
                    od.date_key,
                    op.product_code,
                    oc.customer_code,
                    ost.store_code,
                    osp.salesperson_code,
                    fs.quantity,
                    fs.unit_price,
                    fs.total_amount
                FROM Origen.FactSales fs
                INNER JOIN Origen.DimDate od ON fs.date_key = od.date_key
                INNER JOIN Origen.DimProduct op ON fs.product_key = op.product_key
                INNER JOIN Origen.DimCustomer oc ON fs.customer_key = oc.customer_key
                INNER JOIN Origen.DimStore ost ON fs.store_key = ost.store_key
                INNER JOIN Origen.DimSalesperson osp ON fs.salesperson_key = osp.salesperson_key
                ORDER BY fs.invoice_number, fs.line_number", sourceConn))
            {
                using var reader = await cmdSales.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    try
                    {
                        var invoiceNumber = reader.GetString(0);
                        var lineNumber = reader.GetInt32(1);
                        var dateKey = reader.GetInt32(2);
                        var productCode = reader.GetString(3);
                        var customerCode = reader.GetString(4);
                        var storeCode = reader.GetString(5);
                        var salespersonCode = reader.GetString(6);
                        var quantity = reader.GetInt32(7);
                        var unitPrice = reader.GetDecimal(8);
                        var totalAmount = reader.GetDecimal(9);

                        // Obtener las claves del DW usando los códigos
                        var productKey = await _dimensionLoader.GetProductKeyByCodeAsync(productCode);
                        var customerKey = await _dimensionLoader.GetCustomerKeyByCodeAsync(customerCode);
                        var storeKey = await _dimensionLoader.GetStoreKeyByCodeAsync(storeCode);
                        var salespersonKey = await _dimensionLoader.GetSalespersonKeyByCodeAsync(salespersonCode);

                        if (productKey.HasValue && customerKey.HasValue && storeKey.HasValue && salespersonKey.HasValue)
                        {
                            bool wasInserted = await LoadSaleToWarehouseAsync(
                                dateKey,
                                productKey.Value,
                                customerKey.Value,
                                storeKey.Value,
                                salespersonKey.Value,
                                invoiceNumber,
                                lineNumber,
                                quantity,
                                unitPrice,
                                totalAmount
                            );
                            
                            if (wasInserted)
                            {
                                factSalesLoaded++;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[DATABASE] Error al cargar venta desde DB: {ex.Message}");
                    }
                }
            }

            Console.WriteLine($"[DATABASE] Total FactSales cargadas: {factSalesLoaded} registros");
            return factSalesLoaded;
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
            using var conn = new SqlConnection(_dimensionLoader.GetConnectionString());
            await conn.OpenAsync();

            var checkCmd = new SqlCommand(@"
                SELECT 1 FROM Fact.FactSales 
                WHERE invoice_number = @InvoiceNumber 
                AND line_number = @LineNumber", conn);
            
            checkCmd.Parameters.AddWithValue("@InvoiceNumber", invoiceNumber);
            checkCmd.Parameters.AddWithValue("@LineNumber", lineNumber);

            var exists = await checkCmd.ExecuteScalarAsync();

            if (exists == null)
            {
                var insertCmd = new SqlCommand(@"
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
