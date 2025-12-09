using Microsoft.EntityFrameworkCore;
using SistemaVentas.Api.Data;
using SistemaVentas.Api.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var useCsvData = builder.Configuration.GetValue<bool>("UseCsvData", true);

if (useCsvData)
{
    Console.WriteLine("API CONFIGURADA PARA USAR DATOS DESDE CSV");
    Console.WriteLine();
    
    builder.Services.AddDbContext<SistemaVentasContext>(options =>
        options.UseInMemoryDatabase("SistemaVentasInMemory"));
    
    var dataPath = builder.Configuration.GetValue<string>("CsvDataPath") 
        ?? Path.Combine(Directory.GetCurrentDirectory(), "Data");
    
    builder.Services.AddSingleton(new CsvDataProvider(dataPath));
    
    Console.WriteLine($"Ruta de datos CSV: {dataPath}");
    Console.WriteLine();
}
else
{
    Console.WriteLine("  API CONFIGURADA PARA USAR SQL SERVER");

    builder.Services.AddDbContext<SistemaVentasContext>(options =>
        options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection")));
}

var app = builder.Build();

if (useCsvData)
{
    using (var scope = app.Services.CreateScope())
    {
        var context = scope.ServiceProvider.GetRequiredService<SistemaVentasContext>();
        var csvProvider = scope.ServiceProvider.GetRequiredService<CsvDataProvider>();
        
        Console.WriteLine();
        Console.WriteLine("Cargando datos desde archivos CSV...");
        Console.WriteLine();
        
        context.DimDates.AddRange(csvProvider.LoadDimDates());
        context.DimProducts.AddRange(csvProvider.LoadDimProducts());
        context.DimCustomers.AddRange(csvProvider.LoadDimCustomers());
        context.DimStores.AddRange(csvProvider.LoadDimStores());
        context.DimSalespersons.AddRange(csvProvider.LoadDimSalespeople());
        context.FactSales.AddRange(csvProvider.LoadFactSales());
        
        context.SaveChanges();

        Console.WriteLine();
        Console.WriteLine("Datos CSV cargados en memoria correctamente");
        Console.WriteLine();
    }
}

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
