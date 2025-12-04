using Microsoft.Data.SqlClient;
using SistemaVentas.Etl.Models;
using System.Data;

namespace SistemaVentas.Etl.Services
{
    public class ApiDataLoader
    {
        private readonly string _connectionString;

        public ApiDataLoader(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task LoadFactSalesAsync(IEnumerable<FactSales> sales)
        {
            if (!sales.Any())
            {
                Console.WriteLine("No hay datos del API para cargar");
                return;
            }

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            int successCount = 0;
            int errorCount = 0;

            foreach (var sale in sales)
            {
                try
                {
                    var checkCmd = new SqlCommand(@"
                        SELECT 1 FROM Fact.FactSales 
                        WHERE invoice_number = @InvoiceNumber 
                        AND line_number = @LineNumber", conn);
                    
                    checkCmd.Parameters.AddWithValue("@InvoiceNumber", sale.InvoiceNumber ?? string.Empty);
                    checkCmd.Parameters.AddWithValue("@LineNumber", sale.LineNumber);

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
                        successCount++;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error al cargar venta {sale.InvoiceNumber}: {ex.Message}");
                    errorCount++;
                }
            }

            Console.WriteLine($"API procesada: {successCount} registros cargados, {errorCount} errores");
        }

        public async Task LoadSaleAsync(
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
            using var conn = new SqlConnection(_connectionString);
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
            }
        }
    }
}
