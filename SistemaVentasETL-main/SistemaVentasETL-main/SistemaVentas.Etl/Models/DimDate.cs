namespace SistemaVentas.Etl.Models
{
    public class DimDate
    {
        public int DateKey { get; set; }
        public DateTime FullDate { get; set; }
        public int MonthNum { get; set; }
        public int QuarterNum { get; set; }
        public int YearNum { get; set; }
    }
}
