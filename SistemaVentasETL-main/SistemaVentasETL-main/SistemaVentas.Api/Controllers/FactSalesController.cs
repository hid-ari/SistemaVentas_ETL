using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using SistemaVentas.Api.Data;

namespace SistemaVentas.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class FactSalesController : ControllerBase
    {
        private readonly SistemaVentasContext _context;

        public FactSalesController(SistemaVentasContext context)
        {
            _context = context;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<object>>> Get([FromQuery] int top = 100)
        {
            // 🔹 devolvemos solo las columnas de la tabla Fact, no las navegaciones
            var data = await _context.FactSales
                .OrderByDescending(f => f.SalesKey)
                .Take(top)
                .Select(f => new
                {
                    f.SalesKey,
                    f.DateKey,
                    f.ProductKey,
                    f.CustomerKey,
                    f.StoreKey,
                    f.SalespersonKey,
                    f.InvoiceNumber,
                    f.LineNumber,
                    f.Quantity,
                    f.UnitPrice,
                    f.TotalAmount
                })
                .ToListAsync();

            return Ok(data);
        }
    }
}
