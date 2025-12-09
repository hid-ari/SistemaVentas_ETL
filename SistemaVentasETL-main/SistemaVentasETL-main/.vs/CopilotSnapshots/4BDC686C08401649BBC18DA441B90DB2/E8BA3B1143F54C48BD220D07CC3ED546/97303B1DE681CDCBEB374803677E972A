using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;

namespace SistemaVentas.Etl.Services
{
    public class CsvExtractor
    {
        private readonly string _connectionString;
        private readonly DimensionLoader _dimensionLoader;

        public CsvExtractor(string connectionString, DimensionLoader dimensionLoader)
        {
            _connectionString = connectionString;
            _dimensionLoader = dimensionLoader;
        }

        public async Task<int> LoadDimensionsFromCsvAsync(string dataFolderPath)
        {
            Console.WriteLine($"Leyendo archivos CSV de dimensiones desde: {dataFolderPath}");

            int totalDimensionsLoaded = 0;

            totalDimensionsLoaded += await LoadDimDateFromCsvAsync(Path.Combine(dataFolderPath, "DimDate.csv"));
            totalDimensionsLoaded += await LoadDimProductFromCsvAsync(Path.Combine(dataFolderPath, "DimProducts.csv"));
            totalDimensionsLoaded += await LoadDimCustomerFromCsvAsync(Path.Combine(dataFolderPath, "DimCustomers.csv"));
            totalDimensionsLoaded += await LoadDimStoreFromCsvAsync(Path.Combine(dataFolderPath, "DimStores.csv"));
            totalDimensionsLoaded += await LoadDimSalespersonFromCsvAsync(Path.Combine(dataFolderPath, "DimSalespeople.csv"));

            Console.WriteLine($"Total dimensiones CSV: {totalDimensionsLoaded} registros");
            return totalDimensionsLoaded;
        }

        private async Task<int> LoadDimDateFromCsvAsync(string csvPath)
        {
            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"No se encontro: {csvPath}");
                return 0;
            }

            Console.WriteLine($"Cargando fechas desde: {Path.GetFileName(csvPath)}");
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
                    Console.WriteLine($"Error linea {i}: {ex.Message}");
                }
            }

            Console.WriteLine($"{count} fechas cargadas");
            return count;
        }

        private async Task<int> LoadDimProductFromCsvAsync(string csvPath)
        {
            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"No se encontro: {csvPath}");
                return 0;
            }

            Console.WriteLine($"Cargando productos desde: {Path.GetFileName(csvPath)}");
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
                    Console.WriteLine($"Error linea {i}: {ex.Message}");
                }
            }

            Console.WriteLine($"{count} productos cargados");
            return count;
        }

        private async Task<int> LoadDimCustomerFromCsvAsync(string csvPath)
        {
            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"No se encontro: {csvPath}");
                return 0;
            }

            Console.WriteLine($"Cargando clientes desde: {Path.GetFileName(csvPath)}");
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
                    Console.WriteLine($"Error linea {i}: {ex.Message}");
                }
            }

            Console.WriteLine($"{count} clientes cargados");
            return count;
        }

        private async Task<int> LoadDimStoreFromCsvAsync(string csvPath)
        {
            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"No se encontro: {csvPath}");
                return 0;
            }

            Console.WriteLine($"Cargando tiendas desde: {Path.GetFileName(csvPath)}");
            var lines = await File.ReadAllLinesAsync(csvPath);
            int count = 0;

            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    var cols = lines[i].Split(',', StringSplitOptions.TrimEntries);
                    var storeName = cols[0];
                    var country = cols.Length > 1 ? cols[1] : "República Dominicana";
                    var region = cols.Length > 2 ? cols[2] : null;
                    var city = cols.Length > 3 ? cols[3] : null;

                    await _dimensionLoader.LoadStoreAsync(storeName, country, region, city);
                    count++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error linea {i}: {ex.Message}");
                }
            }

            Console.WriteLine($"{count} tiendas cargadas");
            return count;
        }

        private async Task<int> LoadDimSalespersonFromCsvAsync(string csvPath)
        {
            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"No se encontro: {csvPath}");
                return 0;
            }

            Console.WriteLine($"Cargando vendedores desde: {Path.GetFileName(csvPath)}");
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
                    Console.WriteLine($"Error linea {i}: {ex.Message}");
                }
            }

            Console.WriteLine($"{count} vendedores cargados");
            return count;
        }

        public async Task<int> LoadFactSalesFromCsvAsync(string csvPath, ApiDataLoader apiDataLoader)
        {
            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"No se encontro: {csvPath}");
                return 0;
            }

            Console.WriteLine($"Cargando ventas desde: {Path.GetFileName(csvPath)}");
            var lines = await File.ReadAllLinesAsync(csvPath);
            int count = 0;

            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    var cols = lines[i].Split(',', StringSplitOptions.TrimEntries);
                    
                    // Formato: invoice_number,product_code,customer_name,store_name,salesperson,quantity,unit_price,total_amount,sale_date
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

                    // Cargar el hecho de venta
                    await apiDataLoader.LoadSaleAsync(
                        keys["DateKey"],
                        keys["ProductKey"],
                        keys["CustomerKey"],
                        keys["StoreKey"],
                        keys["SalespersonKey"],
                        invoiceNumber,
                        1, // line_number (contador interno)
                        quantity,
                        unitPrice,
                        totalAmount
                    );

                    count++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error linea {i}: {ex.Message}");
                }
            }

            Console.WriteLine($"{count} ventas cargadas");
            return count;
        }
    }
}
