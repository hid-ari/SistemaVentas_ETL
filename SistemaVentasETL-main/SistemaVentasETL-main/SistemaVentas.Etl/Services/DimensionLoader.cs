using Microsoft.Data.SqlClient;
using System.Data;

namespace SistemaVentas.Etl.Services
{
    public class DimensionLoader
    {
        private readonly string _connectionString;

        public DimensionLoader(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<int> LoadDateAsync(DateTime date)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            int dateKey = int.Parse(date.ToString("yyyyMMdd"));

            var checkCmd = new SqlCommand(
                "SELECT date_key FROM Dimension.DimDate WHERE date_key = @DateKey", conn);
            checkCmd.Parameters.AddWithValue("@DateKey", dateKey);

            var exists = await checkCmd.ExecuteScalarAsync();

            if (exists == null)
            {
                var insertCmd = new SqlCommand(@"
                    INSERT INTO Dimension.DimDate (date_key, full_date, month_num, quarter_num, year_num)
                    VALUES (@DateKey, @FullDate, @MonthNum, @QuarterNum, @YearNum)", conn);

                insertCmd.Parameters.AddWithValue("@DateKey", dateKey);
                insertCmd.Parameters.AddWithValue("@FullDate", date.Date);
                insertCmd.Parameters.AddWithValue("@MonthNum", date.Month);
                insertCmd.Parameters.AddWithValue("@QuarterNum", (date.Month - 1) / 3 + 1);
                insertCmd.Parameters.AddWithValue("@YearNum", date.Year);

                await insertCmd.ExecuteNonQueryAsync();
            }

            return dateKey;
        }

        public async Task<int> LoadDateDimensionAsync(int dateKey, DateTime fullDate, int monthNum, int quarterNum, int yearNum)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var checkCmd = new SqlCommand(
                "SELECT date_key FROM Dimension.DimDate WHERE date_key = @DateKey", conn);
            checkCmd.Parameters.AddWithValue("@DateKey", dateKey);

            var exists = await checkCmd.ExecuteScalarAsync();

            if (exists == null)
            {
                var insertCmd = new SqlCommand(@"
                    INSERT INTO Dimension.DimDate (date_key, full_date, month_num, quarter_num, year_num)
                    VALUES (@DateKey, @FullDate, @MonthNum, @QuarterNum, @YearNum)", conn);

                insertCmd.Parameters.AddWithValue("@DateKey", dateKey);
                insertCmd.Parameters.AddWithValue("@FullDate", fullDate);
                insertCmd.Parameters.AddWithValue("@MonthNum", monthNum);
                insertCmd.Parameters.AddWithValue("@QuarterNum", quarterNum);
                insertCmd.Parameters.AddWithValue("@YearNum", yearNum);

                await insertCmd.ExecuteNonQueryAsync();
            }

            return dateKey;
        }

        public async Task<int> LoadProductAsync(string productCode, string? productName = null, string? category = null)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var checkCmd = new SqlCommand(
                "SELECT product_key FROM Dimension.DimProduct WHERE product_code = @ProductCode", conn);
            checkCmd.Parameters.AddWithValue("@ProductCode", productCode);

            var result = await checkCmd.ExecuteScalarAsync();

            if (result != null)
            {
                return Convert.ToInt32(result);
            }

            var insertCmd = new SqlCommand(@"
                INSERT INTO Dimension.DimProduct (product_code, product_name, category)
                VALUES (@ProductCode, @ProductName, @Category);
                SELECT SCOPE_IDENTITY();", conn);

            insertCmd.Parameters.AddWithValue("@ProductCode", productCode);
            insertCmd.Parameters.AddWithValue("@ProductName", (object?)productName ?? DBNull.Value);
            insertCmd.Parameters.AddWithValue("@Category", (object?)category ?? DBNull.Value);

            var newKey = await insertCmd.ExecuteScalarAsync();
            return Convert.ToInt32(newKey);
        }

        public async Task<int> LoadProductDimensionAsync(string productCode, string productName, string category)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var checkCmd = new SqlCommand(
                "SELECT product_key FROM Dimension.DimProduct WHERE product_code = @ProductCode", conn);
            checkCmd.Parameters.AddWithValue("@ProductCode", productCode);

            var result = await checkCmd.ExecuteScalarAsync();

            if (result != null)
            {
                return Convert.ToInt32(result);
            }

            var insertCmd = new SqlCommand(@"
                INSERT INTO Dimension.DimProduct (product_code, product_name, category)
                VALUES (@ProductCode, @ProductName, @Category);
                SELECT SCOPE_IDENTITY();", conn);

            insertCmd.Parameters.AddWithValue("@ProductCode", productCode);
            insertCmd.Parameters.AddWithValue("@ProductName", productName);
            insertCmd.Parameters.AddWithValue("@Category", (object?)category ?? DBNull.Value);

            var newKey = await insertCmd.ExecuteScalarAsync();
            return Convert.ToInt32(newKey);
        }

        public async Task<int> LoadCustomerAsync(string customerName, string customerType = "Retail", string country = "República Dominicana")
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var checkCmd = new SqlCommand(
                "SELECT customer_key FROM Dimension.DimCustomer WHERE customer_name = @CustomerName", conn);
            checkCmd.Parameters.AddWithValue("@CustomerName", customerName);

            var result = await checkCmd.ExecuteScalarAsync();

            if (result != null)
            {
                return Convert.ToInt32(result);
            }

            var getMaxKeyCmd = new SqlCommand(
                "SELECT ISNULL(MAX(customer_key), 0) + 1 FROM Dimension.DimCustomer", conn);
            var nextKey = Convert.ToInt32(await getMaxKeyCmd.ExecuteScalarAsync());

            var customerCode = $"C-{nextKey}";

            var insertCmd = new SqlCommand(@"
                INSERT INTO Dimension.DimCustomer (customer_code, customer_name, customer_type, country)
                VALUES (@CustomerCode, @CustomerName, @CustomerType, @Country);
                SELECT SCOPE_IDENTITY();", conn);

            insertCmd.Parameters.AddWithValue("@CustomerCode", customerCode);
            insertCmd.Parameters.AddWithValue("@CustomerName", customerName);
            insertCmd.Parameters.AddWithValue("@CustomerType", customerType);
            insertCmd.Parameters.AddWithValue("@Country", country);

            var newKey = await insertCmd.ExecuteScalarAsync();
            return Convert.ToInt32(newKey);
        }

        public async Task<int> LoadCustomerDimensionAsync(string customerCode, string customerName, string customerType, string country)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var checkCmd = new SqlCommand(
                "SELECT customer_key FROM Dimension.DimCustomer WHERE customer_code = @CustomerCode", conn);
            checkCmd.Parameters.AddWithValue("@CustomerCode", customerCode);

            var result = await checkCmd.ExecuteScalarAsync();

            if (result != null)
            {
                return Convert.ToInt32(result);
            }

            var insertCmd = new SqlCommand(@"
                INSERT INTO Dimension.DimCustomer (customer_code, customer_name, customer_type, country)
                VALUES (@CustomerCode, @CustomerName, @CustomerType, @Country);
                SELECT SCOPE_IDENTITY();", conn);

            insertCmd.Parameters.AddWithValue("@CustomerCode", customerCode);
            insertCmd.Parameters.AddWithValue("@CustomerName", customerName);
            insertCmd.Parameters.AddWithValue("@CustomerType", customerType);
            insertCmd.Parameters.AddWithValue("@Country", country);

            var newKey = await insertCmd.ExecuteScalarAsync();
            return Convert.ToInt32(newKey);
        }

        public async Task<int> LoadStoreAsync(string storeName, string country = "República Dominicana", string? region = null, string? city = null)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var checkCmd = new SqlCommand(
                "SELECT store_key FROM Dimension.DimStore WHERE store_name = @StoreName", conn);
            checkCmd.Parameters.AddWithValue("@StoreName", storeName);

            var result = await checkCmd.ExecuteScalarAsync();

            if (result != null)
            {
                return Convert.ToInt32(result);
            }

            var getMaxKeyCmd = new SqlCommand(
                "SELECT ISNULL(MAX(store_key), 0) + 1 FROM Dimension.DimStore", conn);
            var nextKey = Convert.ToInt32(await getMaxKeyCmd.ExecuteScalarAsync());

            var storeCode = $"S-{nextKey}";

            var insertCmd = new SqlCommand(@"
                INSERT INTO Dimension.DimStore (store_code, store_name, country, region, city)
                VALUES (@StoreCode, @StoreName, @Country, @Region, @City);
                SELECT SCOPE_IDENTITY();", conn);

            insertCmd.Parameters.AddWithValue("@StoreCode", storeCode);
            insertCmd.Parameters.AddWithValue("@StoreName", storeName);
            insertCmd.Parameters.AddWithValue("@Country", country);
            insertCmd.Parameters.AddWithValue("@Region", (object?)region ?? DBNull.Value);
            insertCmd.Parameters.AddWithValue("@City", (object?)city ?? DBNull.Value);

            var newKey = await insertCmd.ExecuteScalarAsync();
            return Convert.ToInt32(newKey);
        }

        public async Task<int> LoadStoreDimensionAsync(string storeCode, string storeName, string country, string region, string city)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var checkCmd = new SqlCommand(
                "SELECT store_key FROM Dimension.DimStore WHERE store_code = @StoreCode", conn);
            checkCmd.Parameters.AddWithValue("@StoreCode", storeCode);

            var result = await checkCmd.ExecuteScalarAsync();

            if (result != null)
            {
                return Convert.ToInt32(result);
            }

            var insertCmd = new SqlCommand(@"
                INSERT INTO Dimension.DimStore (store_code, store_name, country, region, city)
                VALUES (@StoreCode, @StoreName, @Country, @Region, @City);
                SELECT SCOPE_IDENTITY();", conn);

            insertCmd.Parameters.AddWithValue("@StoreCode", storeCode);
            insertCmd.Parameters.AddWithValue("@StoreName", storeName);
            insertCmd.Parameters.AddWithValue("@Country", country);
            insertCmd.Parameters.AddWithValue("@Region", region);
            insertCmd.Parameters.AddWithValue("@City", city);

            var newKey = await insertCmd.ExecuteScalarAsync();
            return Convert.ToInt32(newKey);
        }

        public async Task<int> LoadSalespersonAsync(string salespersonName)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var checkCmd = new SqlCommand(
                "SELECT salesperson_key FROM Dimension.DimSalesperson WHERE salesperson_name = @SalespersonName", conn);
            checkCmd.Parameters.AddWithValue("@SalespersonName", salespersonName);

            var result = await checkCmd.ExecuteScalarAsync();

            if (result != null)
            {
                return Convert.ToInt32(result);
            }

            var getMaxKeyCmd = new SqlCommand(
                "SELECT ISNULL(MAX(salesperson_key), 0) + 1 FROM Dimension.DimSalesperson", conn);
            var nextKey = Convert.ToInt32(await getMaxKeyCmd.ExecuteScalarAsync());

            var salespersonCode = $"SP-{nextKey}";

            var insertCmd = new SqlCommand(@"
                INSERT INTO Dimension.DimSalesperson (salesperson_code, salesperson_name)
                VALUES (@SalespersonCode, @SalespersonName);
                SELECT SCOPE_IDENTITY();", conn);

            insertCmd.Parameters.AddWithValue("@SalespersonCode", salespersonCode);
            insertCmd.Parameters.AddWithValue("@SalespersonName", salespersonName);

            var newKey = await insertCmd.ExecuteScalarAsync();
            return Convert.ToInt32(newKey);
        }

        public async Task<int> LoadSalespersonDimensionAsync(string salespersonCode, string salespersonName)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var checkCmd = new SqlCommand(
                "SELECT salesperson_key FROM Dimension.DimSalesperson WHERE salesperson_code = @SalespersonCode", conn);
            checkCmd.Parameters.AddWithValue("@SalespersonCode", salespersonCode);

            var result = await checkCmd.ExecuteScalarAsync();

            if (result != null)
            {
                return Convert.ToInt32(result);
            }

            var insertCmd = new SqlCommand(@"
                INSERT INTO Dimension.DimSalesperson (salesperson_code, salesperson_name)
                VALUES (@SalespersonCode, @SalespersonName);
                SELECT SCOPE_IDENTITY();", conn);

            insertCmd.Parameters.AddWithValue("@SalespersonCode", salespersonCode);
            insertCmd.Parameters.AddWithValue("@SalespersonName", salespersonName);

            var newKey = await insertCmd.ExecuteScalarAsync();
            return Convert.ToInt32(newKey);
        }

        public async Task<Dictionary<string, int>> LoadAllDimensionsAsync(
            DateTime saleDate,
            string productCode,
            string customerName,
            string storeName,
            string salespersonName)
        {
            var keys = new Dictionary<string, int>();

            var dateTask = LoadDateAsync(saleDate);
            var productTask = LoadProductAsync(productCode);
            var customerTask = LoadCustomerAsync(customerName);
            var storeTask = LoadStoreAsync(storeName);
            var salespersonTask = LoadSalespersonAsync(salespersonName);

            await Task.WhenAll(dateTask, productTask, customerTask, storeTask, salespersonTask);

            keys["DateKey"] = await dateTask;
            keys["ProductKey"] = await productTask;
            keys["CustomerKey"] = await customerTask;
            keys["StoreKey"] = await storeTask;
            keys["SalespersonKey"] = await salespersonTask;

            return keys;
        }

        public async Task ShowDimensionStatisticsAsync()
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            Console.WriteLine("Estadisticas de Dimensiones:");

            var dimCustomer = await GetCountAsync(conn, "Dimension.DimCustomer");
            var dimProduct = await GetCountAsync(conn, "Dimension.DimProduct");
            var dimStore = await GetCountAsync(conn, "Dimension.DimStore");
            var dimSalesperson = await GetCountAsync(conn, "Dimension.DimSalesperson");
            var dimDate = await GetCountAsync(conn, "Dimension.DimDate");

            Console.WriteLine($"  DimCustomer:     {dimCustomer,6} registros");
            Console.WriteLine($"  DimProduct:      {dimProduct,6} registros");
            Console.WriteLine($"  DimStore:        {dimStore,6} registros");
            Console.WriteLine($"  DimSalesperson:  {dimSalesperson,6} registros");
            Console.WriteLine($"  DimDate:         {dimDate,6} registros");
        }

        private async Task<int> GetCountAsync(SqlConnection conn, string tableName)
        {
            using var cmd = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", conn);
            var result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(result);
        }
    }
}
