using SistemaVentas.Etl.Models;
using System.Net.Http.Json;

namespace SistemaVentas.Etl.Services
{
    /// <summary>
    /// Servicio unificado para el origen de datos API REST.
    /// Maneja tanto la extracción como la carga de dimensiones y hechos desde la API.
    /// </summary>
    public class ApiDataSourceService
    {
        private readonly HttpClient _httpClient;
        private readonly DimensionLoader _dimensionLoader;

        public ApiDataSourceService(
            HttpClient httpClient,
            DimensionLoader dimensionLoader)
        {
            _httpClient = httpClient;
            _dimensionLoader = dimensionLoader;
        }

        #region Dimensiones

        public async Task<int> LoadAllDimensionsAsync()
        {
            Console.WriteLine("[API] Conectando a API REST...");
            
            int totalDimensionsLoaded = 0;

            try
            {
                totalDimensionsLoaded += await LoadDimDateAsync();
                totalDimensionsLoaded += await LoadDimProductAsync();
                totalDimensionsLoaded += await LoadDimCustomerAsync();
                totalDimensionsLoaded += await LoadDimStoreAsync();
                totalDimensionsLoaded += await LoadDimSalespersonAsync();

                Console.WriteLine($"[API] Total dimensiones cargadas: {totalDimensionsLoaded} registros");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[API] Error al conectar con la API: {ex.Message}");
                Console.WriteLine("[API] Verifique que la API esté ejecutándose");
            }

            return totalDimensionsLoaded;
        }

        private async Task<int> LoadDimDateAsync()
        {
            try
            {
                Console.WriteLine("[API] Cargando fechas desde API...");
                var response = await _httpClient.GetAsync("DimDate");
                
                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"[API] Error al obtener DimDate: {response.StatusCode}");
                    return 0;
                }

                var dates = await response.Content.ReadFromJsonAsync<List<DimDate>>();
                
                if (dates == null || dates.Count == 0)
                {
                    Console.WriteLine("[API] No se obtuvieron fechas desde la API");
                    return 0;
                }

                int count = dates.Count;
                for (int i = 0; i < count; i++)
                {
                    await _dimensionLoader.LoadDateAsync(dates[i].FullDate);
                }

                Console.WriteLine($"[API] {count} fechas cargadas");
                return count;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[API] Error al cargar DimDate: {ex.Message}");
                return 0;
            }
        }

        private async Task<int> LoadDimProductAsync()
        {
            try
            {
                Console.WriteLine("[API] Cargando productos desde API...");
                var response = await _httpClient.GetAsync("DimProduct");
                
                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"[API] Error al obtener DimProduct: {response.StatusCode}");
                    return 0;
                }

                var products = await response.Content.ReadFromJsonAsync<List<DimProduct>>();
                
                if (products == null || products.Count == 0)
                {
                    Console.WriteLine("[API] No se obtuvieron productos desde la API");
                    return 0;
                }

                int count = products.Count;
                for (int i = 0; i < count; i++)
                {
                    await _dimensionLoader.LoadProductAsync(
                        products[i].ProductCode, 
                        products[i].ProductName, 
                        products[i].Category);
                }

                Console.WriteLine($"[API] {count} productos cargados");
                return count;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[API] Error al cargar DimProduct: {ex.Message}");
                return 0;
            }
        }

        private async Task<int> LoadDimCustomerAsync()
        {
            try
            {
                Console.WriteLine("[API] Cargando clientes desde API...");
                var response = await _httpClient.GetAsync("DimCustomer");
                
                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"[API] Error al obtener DimCustomer: {response.StatusCode}");
                    return 0;
                }

                var customers = await response.Content.ReadFromJsonAsync<List<DimCustomer>>();
                
                if (customers == null || customers.Count == 0)
                {
                    Console.WriteLine("[API] No se obtuvieron clientes desde la API");
                    return 0;
                }

                int count = customers.Count;
                for (int i = 0; i < count; i++)
                {
                    await _dimensionLoader.LoadCustomerAsync(
                        customers[i].CustomerName, 
                        customers[i].CustomerType, 
                        customers[i].Country);
                }

                Console.WriteLine($"[API] {count} clientes cargados");
                return count;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[API] Error al cargar DimCustomer: {ex.Message}");
                return 0;
            }
        }

        private async Task<int> LoadDimStoreAsync()
        {
            try
            {
                Console.WriteLine("[API] Cargando tiendas desde API...");
                var response = await _httpClient.GetAsync("DimStore");
                
                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"[API] Error al obtener DimStore: {response.StatusCode}");
                    return 0;
                }

                var stores = await response.Content.ReadFromJsonAsync<List<DimStore>>();
                
                if (stores == null || stores.Count == 0)
                {
                    Console.WriteLine("[API] No se obtuvieron tiendas desde la API");
                    return 0;
                }

                int count = stores.Count;
                for (int i = 0; i < count; i++)
                {
                    await _dimensionLoader.LoadStoreAsync(
                        stores[i].StoreName, 
                        stores[i].Country, 
                        stores[i].Region, 
                        stores[i].City);
                }

                Console.WriteLine($"[API] {count} tiendas cargadas");
                return count;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[API] Error al cargar DimStore: {ex.Message}");
                return 0;
            }
        }

        private async Task<int> LoadDimSalespersonAsync()
        {
            try
            {
                Console.WriteLine("[API] Cargando vendedores desde API...");
                var response = await _httpClient.GetAsync("DimSalesperson");
                
                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"[API] Error al obtener DimSalesperson: {response.StatusCode}");
                    return 0;
                }

                var salespeople = await response.Content.ReadFromJsonAsync<List<DimSalesperson>>();
                
                if (salespeople == null || salespeople.Count == 0)
                {
                    Console.WriteLine("[API] No se obtuvieron vendedores desde la API");
                    return 0;
                }

                int count = salespeople.Count;
                for (int i = 0; i < count; i++)
                {
                    await _dimensionLoader.LoadSalespersonAsync(salespeople[i].SalespersonName);
                }

                Console.WriteLine($"[API] {count} vendedores cargados");
                return count;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[API] Error al cargar DimSalesperson: {ex.Message}");
                return 0;
            }
        }

        #endregion

        #region FactSales

        public async Task<int> LoadFactSalesAsync()
        {
            try
            {
                Console.WriteLine("[API] Obteniendo FactSales desde API...");
                var response = await _httpClient.GetAsync("FactSales?top=1000");
                
                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"[API] Error al obtener FactSales: {response.StatusCode}");
                    return 0;
                }

                var factSales = await response.Content.ReadFromJsonAsync<List<FactSales>>();
                
                if (factSales == null || !factSales.Any())
                {
                    Console.WriteLine("[API] No se obtuvieron FactSales desde la API");
                    return 0;
                }

                Console.WriteLine($"[API] Registros obtenidos desde API: {factSales.Count}");
                Console.WriteLine("[API] Cargando a Data Warehouse...");
                
                int successCount = 0;
                int errorCount = 0;

                foreach (var sale in factSales)
                {
                    try
                    {
                        await LoadSaleToWarehouseAsync(sale);
                        successCount++;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[API] Error al cargar venta {sale.InvoiceNumber}: {ex.Message}");
                        errorCount++;
                    }
                }

                Console.WriteLine($"[API] FactSales procesados: {successCount} cargados, {errorCount} errores");
                return successCount;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[API] Error al cargar FactSales: {ex.Message}");
                return 0;
            }
        }

        private async Task LoadSaleToWarehouseAsync(FactSales sale)
        {
            using var conn = new Microsoft.Data.SqlClient.SqlConnection(_dimensionLoader.GetConnectionString());
            await conn.OpenAsync();

            var checkCmd = new Microsoft.Data.SqlClient.SqlCommand(@"
                SELECT 1 FROM Fact.FactSales 
                WHERE invoice_number = @InvoiceNumber 
                AND line_number = @LineNumber", conn);
            
            checkCmd.Parameters.AddWithValue("@InvoiceNumber", sale.InvoiceNumber ?? string.Empty);
            checkCmd.Parameters.AddWithValue("@LineNumber", sale.LineNumber);

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

                insertCmd.Parameters.AddWithValue("@DateKey", sale.DateKey);
                insertCmd.Parameters.AddWithValue("@ProductKey", sale.ProductKey);
                insertCmd.Parameters.AddWithValue("@CustomerKey", sale.CustomerKey);
                insertCmd.Parameters.AddWithValue("@StoreKey", sale.StoreKey);
                insertCmd.Parameters.AddWithValue("@SalespersonKey", sale.SalespersonKey);
                insertCmd.Parameters.AddWithValue("@InvoiceNumber", sale.InvoiceNumber ?? string.Empty);
                insertCmd.Parameters.AddWithValue("@LineNumber", sale.LineNumber);
                insertCmd.Parameters.AddWithValue("@Quantity", sale.Quantity);
                insertCmd.Parameters.AddWithValue("@UnitPrice", sale.UnitPrice);
                insertCmd.Parameters.AddWithValue("@TotalAmount", sale.TotalAmount);

                await insertCmd.ExecuteNonQueryAsync();
            }
        }

        #endregion
    }
}
