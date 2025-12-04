using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using SistemaVentas.Api.Data;
using SistemaVentas.Api.Models;

namespace SistemaVentas.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DimSalespersonController : ControllerBase
    {
        private readonly SistemaVentasContext _context;

        public DimSalespersonController(SistemaVentasContext context)
        {
            _context = context;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<DimSalesperson>>> Get()
        {
            return await _context.DimSalespersons.ToListAsync();
        }

        [HttpGet("{id:int}")]
        public async Task<ActionResult<DimSalesperson>> Get(int id)
        {
            var item = await _context.DimSalespersons.FindAsync(id);
            if (item == null) return NotFound();
            return item;
        }
    }
}
