using SistemaVentas.Etl.Models;
using System.Net.Http.Json;

namespace SistemaVentas.Etl.Services
{
    public class ApiClientService
    {
        private readonly HttpClient _httpClient;

        public ApiClientService(HttpClient httpClient)
        {
            _httpClient = httpClient;
        }

        public async Task<List<FactSales>> GetFactSalesAsync(int top = 100)
        {
            try
            {
                var response = await _httpClient.GetAsync($"FactSales?top={top}");
                response.EnsureSuccessStatusCode();
                var data = await response.Content.ReadFromJsonAsync<List<FactSales>>();
                return data ?? new List<FactSales>();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al obtener FactSales desde API: {ex.Message}");
                return new List<FactSales>();
            }
        }
    }
}