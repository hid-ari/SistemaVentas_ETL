namespace SistemaVentas.Etl.Models
{
    public class DimSalesperson
    {
        public int SalespersonKey { get; set; }
        public string SalespersonCode { get; set; } = string.Empty;
        public string SalespersonName { get; set; } = string.Empty;
    }
}
