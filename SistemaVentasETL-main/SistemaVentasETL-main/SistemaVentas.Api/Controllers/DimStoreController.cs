using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using SistemaVentas.Api.Data;
using SistemaVentas.Api.Models;

namespace SistemaVentas.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DimStoreController : ControllerBase
    {
        private readonly SistemaVentasContext _context;

        public DimStoreController(SistemaVentasContext context)
        {
            _context = context;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<DimStore>>> Get()
        {
            return await _context.DimStores.ToListAsync();
        }

        [HttpGet("{id:int}")]
        public async Task<ActionResult<DimStore>> Get(int id)
        {
            var item = await _context.DimStores.FindAsync(id);
            if (item == null) return NotFound();
            return item;
        }
    }
}
