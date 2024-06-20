using Microsoft.AspNetCore.Mvc;
using SAPHanaAPI.Services;

namespace SAPHanaAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SapHanaController : ControllerBase
    {
        private readonly SapHanaService _sapHanaService;

        public SapHanaController(SapHanaService sapHanaService)
        {
            _sapHanaService = sapHanaService;
        }

        [HttpGet]
        public async Task<IActionResult> Get()
        {
            var data = await _sapHanaService.GetDataFromSapHanaAsync();
            return Ok(data);
        }
    }
}
