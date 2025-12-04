using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using SistemaVentas.Etl.Services;

var builder = Host.CreateApplicationBuilder(args);

var sourceConn = builder.Configuration.GetConnectionString("SourceConnection") ?? "";
var dwConn = builder.Configuration.GetConnectionString("StagingConnection") ?? "";
var apiBaseUrl = builder.Configuration["ApiSettings:BaseUrl"] ?? "";

builder.Services.AddSingleton(new DimensionLoader(dwConn));
builder.Services.AddSingleton<CsvExtractor>(sp => 
    new CsvExtractor(dwConn, sp.GetRequiredService<DimensionLoader>()));
builder.Services.AddSingleton<DatabaseExtractor>(sp => 
    new DatabaseExtractor(sourceConn, dwConn, sp.GetRequiredService<DimensionLoader>()));
builder.Services.AddSingleton(new DataWarehouseLoader(dwConn));
builder.Services.AddSingleton(new ApiDataLoader(dwConn));

builder.Services.AddHttpClient<ApiExtractor>(client =>
{
    client.BaseAddress = new Uri(apiBaseUrl);
    client.Timeout = TimeSpan.FromSeconds(30);
}).AddTypedClient((httpClient, sp) =>
{
    var dimensionLoader = sp.GetRequiredService<DimensionLoader>();
    return new ApiExtractor(httpClient, dimensionLoader);
});

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var csvExtractor = scope.ServiceProvider.GetRequiredService<CsvExtractor>();
    var dbExtractor = scope.ServiceProvider.GetRequiredService<DatabaseExtractor>();
    var apiExtractor = scope.ServiceProvider.GetRequiredService<ApiExtractor>();
    var dimensionLoader = scope.ServiceProvider.GetRequiredService<DimensionLoader>();
    var dwLoader = scope.ServiceProvider.GetRequiredService<DataWarehouseLoader>();
    var apiLoader = scope.ServiceProvider.GetRequiredService<ApiDataLoader>();

    Console.WriteLine("  SISTEMA DE VENTAS - PROCESO ETL");
    Console.WriteLine();

    Console.WriteLine("  SELECCIONE EL ORIGEN DE DATOS");
    Console.WriteLine();
    Console.WriteLine("FUENTES DE DIMENSIONES:");
    Console.WriteLine("  1. Cargar desde archivos CSV");
    Console.WriteLine("  2. Cargar desde Base de Datos (VentasOrigen)");
    Console.WriteLine("  3. Cargar desde API REST");
    Console.WriteLine("  4. Cargar desde TODAS las fuentes (CSV + DB + API)");
    Console.WriteLine();
    Console.WriteLine("HECHOS (FactSales):");
    Console.WriteLine("  5. Cargar FactSales desde API");
    Console.WriteLine("  6. Cargar FactSales desde CSV");
    Console.WriteLine();
    Console.WriteLine("COMPLETO:");
    Console.WriteLine("  7. Proceso COMPLETO (Dimensiones + FactSales desde todas las fuentes)");
    Console.WriteLine();
    Console.WriteLine("  0. Salir");
    Console.WriteLine();
    Console.Write("Seleccione una opcion (0-7): ");

    string? opcion = Console.ReadLine();
    Console.WriteLine();

    if (opcion == "0")
    {
        Console.WriteLine("Proceso cancelado por el usuario.");
        return;
    }

    try
    {
        int totalDimensions = 0;
        int totalFacts = 0;
        List<string> fuentesProcesadas = new List<string>();

        bool cargarCsv = opcion == "1" || opcion == "4" || opcion == "7";
        bool cargarDatabase = opcion == "2" || opcion == "4" || opcion == "7";
        bool cargarApiDimensions = opcion == "3" || opcion == "4" || opcion == "7";
        bool cargarFactSalesApi = opcion == "5" || opcion == "7";
        bool cargarFactSalesCsv = opcion == "6" || opcion == "7";

        if (cargarCsv || cargarDatabase || cargarApiDimensions)
        {
            Console.WriteLine("    CARGA DE DIMENSIONES AL DATA WAREHOUSE");            
            Console.WriteLine();

            if (cargarCsv)
            {
                Console.WriteLine("[FUENTE: CSV] Cargando dimensiones...");
                var dataFolderPath = Path.Combine(AppContext.BaseDirectory, "Data");
                totalDimensions += await csvExtractor.LoadDimensionsFromCsvAsync(dataFolderPath);
                fuentesProcesadas.Add("CSV");
                Console.WriteLine();
            }

            if (cargarDatabase)
            {
                Console.WriteLine("[FUENTE: DATABASE] Cargando dimensiones...");
                totalDimensions += await dbExtractor.LoadDimensionsFromDatabaseAsync();
                fuentesProcesadas.Add("Database");
                Console.WriteLine();
            }

            if (cargarApiDimensions)
            {
                Console.WriteLine("[FUENTE: API] Cargando dimensiones...");
                totalDimensions += await apiExtractor.LoadDimensionsFromApiAsync();
                fuentesProcesadas.Add("API-Dimensions");
                Console.WriteLine();
            }

            Console.WriteLine("    ESTADISTICAS DE DIMENSIONES CARGADAS");
            await dimensionLoader.ShowDimensionStatisticsAsync();
            Console.WriteLine();
        }

        if (cargarFactSalesApi)
        {
            Console.WriteLine("    CARGA DE HECHOS (FACTSALES) DESDE API");
            Console.WriteLine("[FUENTE: API] Cargando FactSales...");
            
            totalFacts = await apiExtractor.LoadFactSalesFromApiAsync(apiLoader);
            
            if (totalFacts > 0)
            {
                if (!fuentesProcesadas.Contains("API-Dimensions"))
                {
                    fuentesProcesadas.Add("API-FactSales");
                }
                else
                {
                    fuentesProcesadas[fuentesProcesadas.IndexOf("API-Dimensions")] = "API";
                }
            }
            
            Console.WriteLine();

            Console.WriteLine("    ESTADISTICAS DEL DATA WAREHOUSE");
            await dwLoader.ShowStatisticsAsync();
            Console.WriteLine();
        }

        if (cargarFactSalesCsv)
        {
            Console.WriteLine("    CARGA DE HECHOS (FACTSALES) DESDE CSV");
            Console.WriteLine("[FUENTE: CSV] Cargando FactSales...");
            
            var dataFolderPath = Path.Combine(AppContext.BaseDirectory, "Data");
            var ventasExternasPath = Path.Combine(dataFolderPath, "VentasExternas.csv");
            totalFacts += await csvExtractor.LoadFactSalesFromCsvAsync(ventasExternasPath, apiLoader);
            
            if (!fuentesProcesadas.Contains("CSV"))
            {
                fuentesProcesadas.Add("CSV-FactSales");
            }
            
            Console.WriteLine();

            Console.WriteLine("    ESTADISTICAS DEL DATA WAREHOUSE");
            await dwLoader.ShowStatisticsAsync();
            Console.WriteLine();
        }

        if (!cargarCsv && !cargarDatabase && !cargarApiDimensions && !cargarFactSalesApi && !cargarFactSalesCsv)
        {
            Console.WriteLine("Opcion no valida. Seleccione una opcion entre 0-7.");
            return;
        }
        Console.WriteLine("   PROCESO ETL COMPLETADO");
        Console.WriteLine();
        Console.WriteLine("Resumen:");
        if (totalDimensions > 0)
        {
            Console.WriteLine($"   Dimensiones procesadas: {totalDimensions}");
        }
        if (totalFacts > 0)
        {
            Console.WriteLine($"   Hechos procesados: {totalFacts}");
        }
        Console.WriteLine($"   Fuentes procesadas: {string.Join(", ", fuentesProcesadas)}");
    }
    catch (Exception ex)
    {
        Console.WriteLine();
        Console.WriteLine("      ERROR EN EL PROCESO ETL");
        Console.WriteLine($"Error: {ex.Message}");
        Console.WriteLine($"Stack: {ex.StackTrace}");
    }
}


