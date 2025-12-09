using Microsoft.Data.SqlClient;
using System.Data;

namespace SistemaVentas.Etl.Services
{
    public class DatabaseExtractor
    {
        private readonly string _sourceConnection;
        private readonly string _targetConnection;
        private readonly DimensionLoader _dimensionLoader;

        public DatabaseExtractor(string sourceConnection, string targetConnection, DimensionLoader dimensionLoader)
        {
            _sourceConnection = sourceConnection;
            _targetConnection = targetConnection;
            _dimensionLoader = dimensionLoader;
        }

        public async Task<int> LoadDimensionsFromDatabaseAsync()
        {
            Console.WriteLine("Conectando a base de datos OrigenVentas...");

            using var sourceConn = new SqlConnection(_sourceConnection);
            await sourceConn.OpenAsync();

            int dimensionsLoaded = 0;

            // 1. Cargar DimDate
            Console.WriteLine("Cargando fechas desde Origen.OrigenDate...");
            using (var cmdDate = new SqlCommand(@"
                SELECT DISTINCT date_key, full_date, month_num, quarter_num, year_num
                FROM Origen.OrigenDate
                ORDER BY date_key", sourceConn))
            {
                using var reader = await cmdDate.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var dateKey = reader.GetInt32(0);
                    var fullDate = reader.GetDateTime(1);
                    var monthNum = reader.GetInt32(2);
                    var quarterNum = reader.GetInt32(3);
                    var yearNum = reader.GetInt32(4);

                    await _dimensionLoader.LoadDateDimensionAsync(dateKey, fullDate, monthNum, quarterNum, yearNum);
                    dimensionsLoaded++;
                }
            }

            // 2. Cargar DimProduct
            Console.WriteLine("Cargando productos desde Origen.OrigenProduct...");
            using (var cmdProduct = new SqlCommand(@"
                SELECT product_key, product_code, product_name, category
                FROM Origen.OrigenProduct
                ORDER BY product_key", sourceConn))
            {
                using var reader = await cmdProduct.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var productCode = reader.GetString(1);
                    var productName = reader.GetString(2);
                    var category = reader.IsDBNull(3) ? "" : reader.GetString(3);

                    await _dimensionLoader.LoadProductDimensionAsync(productCode, productName, category);
                    dimensionsLoaded++;
                }
            }

            // 3. Cargar DimCustomer
            Console.WriteLine("Cargando clientes desde Origen.OrigenCustomer...");
            using (var cmdCustomer = new SqlCommand(@"
                SELECT customer_key, customer_code, customer_name, customer_type, country
                FROM Origen.OrigenCustomer
                ORDER BY customer_key", sourceConn))
            {
                using var reader = await cmdCustomer.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var customerCode = reader.GetString(1);
                    var customerName = reader.GetString(2);
                    var customerType = reader.GetString(3);
                    var country = reader.GetString(4);

                    await _dimensionLoader.LoadCustomerDimensionAsync(customerCode, customerName, customerType, country);
                    dimensionsLoaded++;
                }
            }

            // 4. Cargar DimStore
            Console.WriteLine("Cargando tiendas desde Origen.OrigenStore...");
            using (var cmdStore = new SqlCommand(@"
                SELECT store_key, store_code, store_name, country, region, city
                FROM Origen.OrigenStore
                ORDER BY store_key", sourceConn))
            {
                using var reader = await cmdStore.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var storeCode = reader.GetString(1);
                    var storeName = reader.GetString(2);
                    var country = reader.GetString(3);
                    var region = reader.GetString(4);
                    var city = reader.GetString(5);

                    await _dimensionLoader.LoadStoreDimensionAsync(storeCode, storeName, country, region, city);
                    dimensionsLoaded++;
                }
            }

            // 5. Cargar DimSalesperson
            Console.WriteLine("Cargando vendedores desde Origen.OrigenSalesperson...");
            using (var cmdSalesperson = new SqlCommand(@"
                SELECT salesperson_key, salesperson_code, salesperson_name
                FROM Origen.OrigenSalesperson
                ORDER BY salesperson_key", sourceConn))
            {
                using var reader = await cmdSalesperson.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var salespersonCode = reader.GetString(1);
                    var salespersonName = reader.GetString(2);

                    await _dimensionLoader.LoadSalespersonDimensionAsync(salespersonCode, salespersonName);
                    dimensionsLoaded++;
                }
            }

            Console.WriteLine($"Total dimensiones cargadas desde DB: {dimensionsLoaded} registros");
            return dimensionsLoaded;
        }
    }
}
