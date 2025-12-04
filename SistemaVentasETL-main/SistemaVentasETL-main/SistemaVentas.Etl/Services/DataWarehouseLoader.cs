using Microsoft.Data.SqlClient;

namespace SistemaVentas.Etl.Services
{
    public class DataWarehouseLoader
    {
        private readonly string _connectionString;

        public DataWarehouseLoader(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task ShowStatisticsAsync()
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            Console.WriteLine("?? Estadísticas del Data Warehouse:");

            try
            {
                // Contar registros en dimensiones
                var dimCustomer = await GetCountAsync(conn, "Dimension.DimCustomer");
                var dimProduct = await GetCountAsync(conn, "Dimension.DimProduct");
                var dimStore = await GetCountAsync(conn, "Dimension.DimStore");
                var dimSalesperson = await GetCountAsync(conn, "Dimension.DimSalesperson");
                var dimDate = await GetCountAsync(conn, "Dimension.DimDate");
                var factSales = await GetCountAsync(conn, "Fact.FactSales");

                Console.WriteLine($"  • DimCustomer:     {dimCustomer,6} registros");
                Console.WriteLine($"  • DimProduct:      {dimProduct,6} registros");
                Console.WriteLine($"  • DimStore:        {dimStore,6} registros");
                Console.WriteLine($"  • DimSalesperson:  {dimSalesperson,6} registros");
                Console.WriteLine($"  • DimDate:         {dimDate,6} registros");
                Console.WriteLine($"  • FactSales:       {factSales,6} registros");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"? Error al obtener estadísticas: {ex.Message}");
            }
        }

        private async Task<int> GetCountAsync(SqlConnection conn, string tableName)
        {
            using var cmd = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", conn);
            var result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(result);
        }
    }
}
