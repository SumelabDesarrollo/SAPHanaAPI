using Microsoft.AspNetCore.Mvc;
using SAPHanaAPI.Services;
using System.Threading.Tasks;

namespace SAPHanaAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ObtenerClienteController : ControllerBase
    {
        private readonly SapHanaService _sapHanaService;

        public ObtenerClienteController(SapHanaService sapHanaService)
        {
            _sapHanaService = sapHanaService;
        }

        [HttpGet]
        public async Task<IActionResult> Get()
        {
            var data = await _sapHanaService.GetClientsFromSapHanaAsync();
            return Ok(data);
        }
    }
}
