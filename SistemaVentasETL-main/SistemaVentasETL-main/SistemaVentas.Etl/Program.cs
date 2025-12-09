using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using SistemaVentas.Etl.Services;

var builder = Host.CreateApplicationBuilder(args);

var sourceConn = builder.Configuration.GetConnectionString("SourceConnection") ?? "";
var dwConn = builder.Configuration.GetConnectionString("StagingConnection") ?? "";
var apiBaseUrl = builder.Configuration["ApiSettings:BaseUrl"] ?? "";
var dataFolderPath = Path.Combine(AppContext.BaseDirectory, "Data");

// Servicios compartidos
builder.Services.AddSingleton(new DimensionLoader(dwConn));

// Servicio para origen CSV
builder.Services.AddSingleton<CsvDataSourceService>(sp => 
    new CsvDataSourceService(
        sp.GetRequiredService<DimensionLoader>(),
        dataFolderPath));

// Servicio para origen Database
builder.Services.AddSingleton<DatabaseDataSourceService>(sp => 
    new DatabaseDataSourceService(
        sourceConn,
        sp.GetRequiredService<DimensionLoader>()));

// Servicio para origen API REST
builder.Services.AddHttpClient<ApiDataSourceService>(client =>
{
    client.BaseAddress = new Uri(apiBaseUrl);
    client.Timeout = TimeSpan.FromSeconds(30);
}).AddTypedClient((httpClient, sp) =>
{
    var dimensionLoader = sp.GetRequiredService<DimensionLoader>();
    return new ApiDataSourceService(httpClient, dimensionLoader);
});

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var csvService = scope.ServiceProvider.GetRequiredService<CsvDataSourceService>();
    var dbService = scope.ServiceProvider.GetRequiredService<DatabaseDataSourceService>();
    var apiService = scope.ServiceProvider.GetRequiredService<ApiDataSourceService>();
    var dimensionLoader = scope.ServiceProvider.GetRequiredService<DimensionLoader>();

    // Limpiar Data Warehouse antes de comenzar
    await dimensionLoader.CleanDataWarehouseAsync();

    Console.WriteLine("CARGAS COMPLETAS (DIMENSIONES + FACTSALES)");
    Console.WriteLine();
    Console.WriteLine("    1. Cargar Todo (Dimensiones + FactSales) desde CSV");
    Console.WriteLine("    2. Cargar Todo (Dimensiones + FactSales) desde Base de Datos");
    Console.WriteLine("    3. Cargar Todo (Dimensiones + FactSales) desde API REST");
    Console.WriteLine("    4. Cargar TODO desde TODAS las fuentes (CSV + DB + API)");
    Console.WriteLine();
    Console.WriteLine("    0. Salir");
    Console.WriteLine();
    Console.Write("Seleccione una opción (0-4): ");

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
        int totalFactSales = 0;
        List<string> fuentesProcesadas = new List<string>();

        Console.WriteLine();

        switch (opcion)
        {
            case "1": // CSV - Todo
                Console.WriteLine("CARGANDO TODO DESDE CSV");
                Console.WriteLine();
                Console.WriteLine("[CSV] Paso 1/2: Cargando Dimensiones...");
                totalDimensions = await csvService.LoadAllDimensionsAsync();
                Console.WriteLine();
                Console.WriteLine("[CSV] Paso 2/2: Cargando FactSales...");
                totalFactSales = await csvService.LoadFactSalesAsync();
                fuentesProcesadas.Add("CSV");
                break;

            case "2": // Database - Todo
                Console.WriteLine("CARGANDO TODO DESDE BASE DE DATOS");
                Console.WriteLine();
                Console.WriteLine("[DATABASE] Paso 1/2: Cargando Dimensiones...");
                totalDimensions = await dbService.LoadAllDimensionsAsync();
                Console.WriteLine();
                Console.WriteLine("[DATABASE] Paso 2/2: Cargando FactSales...");
                totalFactSales = await dbService.LoadFactSalesAsync();
                fuentesProcesadas.Add("Database");
                break;

            case "3": // API - Todo
                Console.WriteLine("CARGANDO TODO DESDE API");
                Console.WriteLine();
                Console.WriteLine("[API] Paso 1/2: Cargando Dimensiones...");
                totalDimensions = await apiService.LoadAllDimensionsAsync();
                Console.WriteLine();
                Console.WriteLine("[API] Paso 2/2: Cargando FactSales...");
                totalFactSales = await apiService.LoadFactSalesAsync();
                fuentesProcesadas.Add("API");
                break;

            case "4": // Todas las fuentes - Todo
                Console.WriteLine("CARGA COMPLETA DESDE TODAS LAS FUENTES");
                Console.WriteLine();
                
                // CSV
                Console.WriteLine("[1/3] PROCESANDO CSV...");
                Console.WriteLine("[CSV] Cargando Dimensiones...");
                totalDimensions += await csvService.LoadAllDimensionsAsync();
                Console.WriteLine("[CSV] Cargando FactSales...");
                totalFactSales += await csvService.LoadFactSalesAsync();
                fuentesProcesadas.Add("CSV");
                Console.WriteLine();

                // Database
                Console.WriteLine("[2/3] PROCESANDO BASE DE DATOS...");
                Console.WriteLine("[DATABASE] Cargando Dimensiones...");
                totalDimensions += await dbService.LoadAllDimensionsAsync();
                Console.WriteLine("[DATABASE] Cargando FactSales...");
                totalFactSales += await dbService.LoadFactSalesAsync();
                fuentesProcesadas.Add("Database");
                Console.WriteLine();

                // API
                Console.WriteLine("[3/3] PROCESANDO API...");
                Console.WriteLine("[API] Cargando Dimensiones...");
                totalDimensions += await apiService.LoadAllDimensionsAsync();
                Console.WriteLine("[API] Cargando FactSales...");
                totalFactSales += await apiService.LoadFactSalesAsync();
                fuentesProcesadas.Add("API");
                Console.WriteLine();
                break;

            default:
                Console.WriteLine("Opción no válida. Seleccione una opción entre 0-4.");
                return;
        }

        Console.WriteLine();
        Console.WriteLine("ESTADISTICAS DE DIMENSIONES EN DATA WAREHOUSE");
        await dimensionLoader.ShowDimensionStatisticsAsync();

        Console.WriteLine();
        Console.WriteLine("PROCESO ETL COMPLETADO");
        Console.WriteLine();
        Console.WriteLine("RESUMEN:");
        Console.WriteLine($"   Dimensiones procesadas: {totalDimensions}");
        Console.WriteLine($"   FactSales procesados:   {totalFactSales}");
        Console.WriteLine($"   Fuentes utilizadas:     {string.Join(", ", fuentesProcesadas)}");
        Console.WriteLine();
    }
    catch (Exception ex)
    {
        Console.WriteLine();
        Console.WriteLine("ERROR EN EL PROCESO ETL");
        Console.WriteLine();
        Console.WriteLine($"Error: {ex.Message}");
        Console.WriteLine();
        Console.WriteLine("Stack Trace:");
        Console.WriteLine($"{ex.StackTrace}");
    }
}


