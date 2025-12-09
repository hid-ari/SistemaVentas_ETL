using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using SistemaVentas.Api.Data;
using SistemaVentas.Api.Models;

namespace SistemaVentas.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DimProductController : ControllerBase
    {
        private readonly SistemaVentasContext _context;

        public DimProductController(SistemaVentasContext context)
        {
            _context = context;
        }

        [HttpGet]
        public async Task<IEnumerable<DimProduct>> Get()
        {
            return await _context.DimProducts.ToListAsync();
        }
    }
}