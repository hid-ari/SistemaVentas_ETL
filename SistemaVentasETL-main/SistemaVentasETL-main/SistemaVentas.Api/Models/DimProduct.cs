namespace SistemaVentas.Api.Models
{
    public class DimProduct
    {
        public int ProductKey { get; set; }
        public string ProductCode { get; set; } = string.Empty;
        public string ProductName { get; set; } = string.Empty;
        public string? Category { get; set; }
    }
}