using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using SistemaVentas.Api.Data;
using SistemaVentas.Api.Models;

namespace SistemaVentas.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DimCustomerController : ControllerBase
    {
        private readonly SistemaVentasContext _context;

        public DimCustomerController(SistemaVentasContext context)
        {
            _context = context;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<DimCustomer>>> Get()
        {
            return await _context.DimCustomers.ToListAsync();
        }

        [HttpGet("{id:int}")]
        public async Task<ActionResult<DimCustomer>> Get(int id)
        {
            var item = await _context.DimCustomers.FindAsync(id);
            if (item == null) return NotFound();
            return item;
        }
    }
}