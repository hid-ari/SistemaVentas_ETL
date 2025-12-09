namespace SistemaVentas.Api.Models
{
    public class DimCustomer
    {
        public int CustomerKey { get; set; }
        public string CustomerCode { get; set; } = string.Empty;
        public string CustomerName { get; set; } = string.Empty;
        public string CustomerType { get; set; } = string.Empty;
        public string Country { get; set; } = string.Empty;
    }
}