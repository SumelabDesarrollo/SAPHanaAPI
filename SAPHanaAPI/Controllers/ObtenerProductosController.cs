using Microsoft.AspNetCore.Mvc;
using SAPHanaAPI.Services;
using System.Threading.Tasks;

namespace SAPHanaAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ObtenerProductosController : ControllerBase
    {
        private readonly ProductService _productService;

        public ObtenerProductosController(ProductService productService)
        {
            _productService = productService;
        }

        [HttpGet]
        public async Task<IActionResult> Get()
        {
            var data = await _productService.GetProductsFromSapHanaAsync();
            return Ok(data);
        }
    }
}
