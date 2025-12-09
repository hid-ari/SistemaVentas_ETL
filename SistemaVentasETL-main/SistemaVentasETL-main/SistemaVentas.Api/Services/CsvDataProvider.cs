using SistemaVentas.Api.Models;
using System.Globalization;

namespace SistemaVentas.Api.Services
{
    public class CsvDataProvider
    {
        private readonly string _dataPath;

        public CsvDataProvider(string dataPath)
        {
            _dataPath = dataPath;
        }

        public List<DimDate> LoadDimDates()
        {
            var dates = new List<DimDate>();
            var filePath = Path.Combine(_dataPath, "DimDate.csv");

            if (!File.Exists(filePath))
            {
                Console.WriteLine($"Archivo no encontrado: {filePath}");
                return dates;
            }

            var lines = File.ReadAllLines(filePath);
            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    var cols = lines[i].Split(',');
                    var fullDate = DateTime.Parse(cols[0], CultureInfo.InvariantCulture);
                    
                    dates.Add(new DimDate
                    {
                        DateKey = int.Parse(fullDate.ToString("yyyyMMdd")),
                        FullDate = fullDate,
                        MonthNum = fullDate.Month,
                        QuarterNum = (fullDate.Month - 1) / 3 + 1,
                        YearNum = fullDate.Year
                    });
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error en linea {i} de DimDate.csv: {ex.Message}");
                }
            }

            Console.WriteLine($"Cargadas {dates.Count} fechas desde CSV");
            return dates;
        }

        public List<DimProduct> LoadDimProducts()
        {
            var products = new List<DimProduct>();
            var filePath = Path.Combine(_dataPath, "DimProducts.csv");

            if (!File.Exists(filePath))
            {
                Console.WriteLine($"Archivo no encontrado: {filePath}");
                return products;
            }

            var lines = File.ReadAllLines(filePath);
            int id = 1;
            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    var cols = lines[i].Split(',');
                    products.Add(new DimProduct
                    {
                        ProductKey = id++,
                        ProductCode = cols[0].Trim(),
                        ProductName = cols.Length > 1 ? cols[1].Trim() : null,
                        Category = cols.Length > 2 ? cols[2].Trim() : null
                    });
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error en linea {i} de DimProducts.csv: {ex.Message}");
                }
            }

            Console.WriteLine($"Cargados {products.Count} productos desde CSV");
            return products;
        }

        public List<DimCustomer> LoadDimCustomers()
        {
            var customers = new List<DimCustomer>();
            var filePath = Path.Combine(_dataPath, "DimCustomers.csv");

            if (!File.Exists(filePath))
            {
                Console.WriteLine($"Archivo no encontrado: {filePath}");
                return customers;
            }

            var lines = File.ReadAllLines(filePath);
            int id = 1;
            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    var cols = lines[i].Split(',');
                    customers.Add(new DimCustomer
                    {
                        CustomerKey = id,
                        CustomerCode = $"C-{id}",
                        CustomerName = cols[0].Trim(),
                        CustomerType = cols.Length > 1 ? cols[1].Trim() : "Retail",
                        Country = cols.Length > 2 ? cols[2].Trim() : "República Dominicana"
                    });
                    id++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error en linea {i} de DimCustomers.csv: {ex.Message}");
                }
            }

            Console.WriteLine($"Cargados {customers.Count} clientes desde CSV");
            return customers;
        }

        public List<DimStore> LoadDimStores()
        {
            var stores = new List<DimStore>();
            var filePath = Path.Combine(_dataPath, "DimStores.csv");

            if (!File.Exists(filePath))
            {
                Console.WriteLine($"Archivo no encontrado: {filePath}");
                return stores;
            }

            var lines = File.ReadAllLines(filePath);
            int id = 1;
            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    var cols = lines[i].Split(',');
                    stores.Add(new DimStore
                    {
                        StoreKey = id,
                        StoreCode = $"S-{id}",
                        StoreName = cols[0].Trim(),
                        Country = cols.Length > 1 ? cols[1].Trim() : "República Dominicana",
                        Region = cols.Length > 2 ? cols[2].Trim() : null,
                        City = cols.Length > 3 ? cols[3].Trim() : null
                    });
                    id++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error en linea {i} de DimStores.csv: {ex.Message}");
                }
            }

            Console.WriteLine($"Cargadas {stores.Count} tiendas desde CSV");
            return stores;
        }

        public List<DimSalesperson> LoadDimSalespeople()
        {
            var salespeople = new List<DimSalesperson>();
            var filePath = Path.Combine(_dataPath, "DimSalespeople.csv");

            if (!File.Exists(filePath))
            {
                Console.WriteLine($"Archivo no encontrado: {filePath}");
                return salespeople;
            }

            var lines = File.ReadAllLines(filePath);
            int id = 1;
            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    var cols = lines[i].Split(',');
                    salespeople.Add(new DimSalesperson
                    {
                        SalespersonKey = id,
                        SalespersonCode = $"SP-{id}",
                        SalespersonName = cols[0].Trim()
                    });
                    id++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error en linea {i} de DimSalespeople.csv: {ex.Message}");
                }
            }

            Console.WriteLine($"Cargados {salespeople.Count} vendedores desde CSV");
            return salespeople;
        }

        public List<FactSales> LoadFactSales()
        {
            var factSales = new List<FactSales>();
            var filePath = Path.Combine(_dataPath, "FactSales.csv");

            if (!File.Exists(filePath))
            {
                Console.WriteLine($"Archivo no encontrado: {filePath}");
                return factSales;
            }

            var lines = File.ReadAllLines(filePath);
            int id = 1;
            
            for (int i = 1; i < lines.Length; i++)
            {
                try
                {
                    var cols = lines[i].Split(',');
                    
                    // Formato: invoice_number,product_code,customer_name,store_name,salesperson_name,quantity,unit_price,total_amount,sale_date
                    var invoiceNumber = cols[0].Trim();
                    var productCode = cols[1].Trim();
                    var customerName = cols[2].Trim();
                    var storeName = cols[3].Trim();
                    var salespersonName = cols[4].Trim();
                    var quantity = int.Parse(cols[5].Trim());
                    var unitPrice = decimal.Parse(cols[6].Trim(), CultureInfo.InvariantCulture);
                    var totalAmount = decimal.Parse(cols[7].Trim(), CultureInfo.InvariantCulture);
                    var saleDate = DateTime.Parse(cols[8].Trim(), CultureInfo.InvariantCulture);
                    
                    factSales.Add(new FactSales
                    {
                        SalesKey = id++,
                        DateKey = int.Parse(saleDate.ToString("yyyyMMdd")),
                        ProductKey = GetProductKeyByCode(productCode),
                        CustomerKey = GetCustomerKeyByName(customerName),
                        StoreKey = GetStoreKeyByName(storeName),
                        SalespersonKey = GetSalespersonKeyByName(salespersonName),
                        InvoiceNumber = invoiceNumber,
                        LineNumber = 1,
                        Quantity = quantity,
                        UnitPrice = unitPrice,
                        TotalAmount = totalAmount
                    });
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error en linea {i} de FactSales.csv: {ex.Message}");
                }
            }

            Console.WriteLine($"Cargadas {factSales.Count} ventas desde CSV");
            return factSales;
        }

        private int GetProductKeyByCode(string code)
        {
            return int.Parse(code.Split('-')[1]);
        }

        private int GetCustomerKeyByName(string name)
        {
            return Math.Abs(name.GetHashCode() % 10) + 1;
        }

        private int GetStoreKeyByName(string name)
        {
            return Math.Abs(name.GetHashCode() % 10) + 1;
        }

        private int GetSalespersonKeyByName(string name)
        {
            return Math.Abs(name.GetHashCode() % 10) + 1;
        }
    }
}
