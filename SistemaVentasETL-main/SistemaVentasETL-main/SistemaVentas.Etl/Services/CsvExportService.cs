using SistemaVentas.Etl.Models;
using System.Text;

namespace SistemaVentas.Etl.Services
{
    public class CsvExportService
    {
        public async Task ExportFactSalesAsync(IEnumerable<FactSales> sales, string filePath)
        {
            var sb = new StringBuilder();
            sb.AppendLine("sales_key,date_key,product_key,customer_key,store_key,salesperson_key,invoice_number,line_number,quantity,unit_price,total_amount");

            foreach (var s in sales)
            {
                sb.AppendLine($"{s.SalesKey},{s.DateKey},{s.ProductKey},{s.CustomerKey},{s.StoreKey},{s.SalespersonKey},{s.InvoiceNumber},{s.LineNumber},{s.Quantity},{s.UnitPrice},{s.TotalAmount}");
            }

            await File.WriteAllTextAsync(filePath, sb.ToString(), Encoding.UTF8);
        }
    }
}