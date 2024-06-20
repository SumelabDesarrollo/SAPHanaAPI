using Microsoft.AspNetCore.Mvc;
using InsertarOrdenes.Services;
using System.Threading.Tasks;

namespace InsertarOrdenes.Controllers
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

        [HttpPost("orders/{orderId}")]
        public async Task<IActionResult> PostOrder(int orderId)
        {
            try
            {
                var (docNum, jsonData) = await _sapHanaService.SendOrderToSapAsync(orderId);
                return Ok(new { message = "Order inserted successfully", docNum, jsonData });
            }
            catch (Exception ex)
            {
                return BadRequest(new { message = ex.Message });
            }
        }

        [HttpPost("orders")]
        public async Task<IActionResult> PostAllOrders()
        {
            try
            {
                var (successfulOrders, failedOrders) = await _sapHanaService.SendAllOrdersToSapAsync();
                return Ok(new
                {
                    message = "Orders processed",
                    successfulOrders,
                    failedOrders
                });
            }
            catch (Exception ex)
            {
                return BadRequest(new { message = ex.Message });
            }
        }
    }
}
