using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using SistemaVentas.Api.Data;
using SistemaVentas.Api.Models;

namespace SistemaVentas.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DimDateController : ControllerBase
    {
        private readonly SistemaVentasContext _context;

        public DimDateController(SistemaVentasContext context)
        {
            _context = context;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<DimDate>>> Get()
        {
            return await _context.DimDates.OrderBy(d => d.DateKey).ToListAsync();
        }

        [HttpGet("{id:int}")]
        public async Task<ActionResult<DimDate>> Get(int id)
        {
            var item = await _context.DimDates.FindAsync(id);
            if (item == null) return NotFound();
            return item;
        }

        [HttpGet("range")]
        public async Task<ActionResult<IEnumerable<DimDate>>> GetRange([FromQuery] int startYear, [FromQuery] int endYear)
        {
            var dates = await _context.DimDates
                .Where(d => d.YearNum >= startYear && d.YearNum <= endYear)
                .OrderBy(d => d.DateKey)
                .ToListAsync();
            
            return Ok(dates);
        }
    }
}
