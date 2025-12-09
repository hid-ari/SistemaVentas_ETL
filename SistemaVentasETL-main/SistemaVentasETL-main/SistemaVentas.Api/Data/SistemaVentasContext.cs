using Microsoft.EntityFrameworkCore;
using SistemaVentas.Api.Models;

namespace SistemaVentas.Api.Data
{
    public class SistemaVentasContext : DbContext
    {
        public SistemaVentasContext(DbContextOptions<SistemaVentasContext> options)
            : base(options)
        {
        }

        public DbSet<DimCustomer> DimCustomers => Set<DimCustomer>();
        public DbSet<DimDate> DimDates => Set<DimDate>();
        public DbSet<DimProduct> DimProducts => Set<DimProduct>();
        public DbSet<DimSalesperson> DimSalespersons => Set<DimSalesperson>();
        public DbSet<DimStore> DimStores => Set<DimStore>();
        public DbSet<FactSales> FactSales => Set<FactSales>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<DimCustomer>(entity =>
            {
                entity.ToTable("DimCustomer", "Dimension");
                entity.HasKey(e => e.CustomerKey);
                entity.Property(e => e.CustomerKey).HasColumnName("customer_key");
                entity.Property(e => e.CustomerCode).HasColumnName("customer_code");
                entity.Property(e => e.CustomerName).HasColumnName("customer_name");
                entity.Property(e => e.CustomerType).HasColumnName("customer_type");
                entity.Property(e => e.Country).HasColumnName("country");
            });

            modelBuilder.Entity<DimDate>(entity =>
            {
                entity.ToTable("DimDate", "Dimension");
                entity.HasKey(e => e.DateKey);
                entity.Property(e => e.DateKey).HasColumnName("date_key");
                entity.Property(e => e.FullDate).HasColumnName("full_date");
                entity.Property(e => e.MonthNum).HasColumnName("month_num");
                entity.Property(e => e.QuarterNum).HasColumnName("quarter_num");
                entity.Property(e => e.YearNum).HasColumnName("year_num");
            });

            modelBuilder.Entity<DimProduct>(entity =>
            {
                entity.ToTable("DimProduct", "Dimension");
                entity.HasKey(e => e.ProductKey);
                entity.Property(e => e.ProductKey).HasColumnName("product_key");
                entity.Property(e => e.ProductCode).HasColumnName("product_code");
                entity.Property(e => e.ProductName).HasColumnName("product_name");
                entity.Property(e => e.Category).HasColumnName("category");
            });

            modelBuilder.Entity<DimSalesperson>(entity =>
            {
                entity.ToTable("DimSalesperson", "Dimension");
                entity.HasKey(e => e.SalespersonKey);
                entity.Property(e => e.SalespersonKey).HasColumnName("salesperson_key");
                entity.Property(e => e.SalespersonCode).HasColumnName("salesperson_code");
                entity.Property(e => e.SalespersonName).HasColumnName("salesperson_name");
            });

            modelBuilder.Entity<DimStore>(entity =>
            {
                entity.ToTable("DimStore", "Dimension");
                entity.HasKey(e => e.StoreKey);
                entity.Property(e => e.StoreKey).HasColumnName("store_key");
                entity.Property(e => e.StoreCode).HasColumnName("store_code");
                entity.Property(e => e.StoreName).HasColumnName("store_name");
                entity.Property(e => e.Country).HasColumnName("country");
                entity.Property(e => e.Region).HasColumnName("region");
                entity.Property(e => e.City).HasColumnName("city");
            });

            modelBuilder.Entity<FactSales>(entity =>
            {
                entity.ToTable("FactSales", "Fact");
                entity.HasKey(e => e.SalesKey);
                entity.Property(e => e.SalesKey).HasColumnName("sales_key");
                entity.Property(e => e.DateKey).HasColumnName("date_key");
                entity.Property(e => e.ProductKey).HasColumnName("product_key");
                entity.Property(e => e.CustomerKey).HasColumnName("customer_key");
                entity.Property(e => e.StoreKey).HasColumnName("store_key");
                entity.Property(e => e.SalespersonKey).HasColumnName("salesperson_key");
                entity.Property(e => e.InvoiceNumber).HasColumnName("invoice_number");
                entity.Property(e => e.LineNumber).HasColumnName("line_number");
                entity.Property(e => e.Quantity).HasColumnName("quantity");
                entity.Property(e => e.UnitPrice).HasColumnName("unit_price").HasColumnType("decimal(18,2)");
                entity.Property(e => e.TotalAmount).HasColumnName("total_amount").HasColumnType("decimal(18,2)");
            });
        }
    }
}