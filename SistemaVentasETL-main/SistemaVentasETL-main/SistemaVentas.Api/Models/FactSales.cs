namespace SistemaVentas.Api.Models
{
    public class FactSales
    {
        public long SalesKey { get; set; }
        public int DateKey { get; set; }
        public int ProductKey { get; set; }
        public int CustomerKey { get; set; }
        public int StoreKey { get; set; }
        public int SalespersonKey { get; set; }
        public string InvoiceNumber { get; set; } = string.Empty;
        public int LineNumber { get; set; }
        public int Quantity { get; set; }
        public decimal UnitPrice { get; set; }
        public decimal TotalAmount { get; set; }

        public DimCustomer? Customer { get; set; }
        public DimProduct? Product { get; set; }
        public DimStore? Store { get; set; }
        public DimSalesperson? Salesperson { get; set; }
        public DimDate? Date { get; set; }
    }
}