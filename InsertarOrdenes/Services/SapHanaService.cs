using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace InsertarOrdenes.Services
{
    public class SapHanaService
    {
        private readonly string _postgresConnectionString;
        private readonly HttpClient _httpClient;
        private readonly string _baseUrl;
        private readonly string _loginEndpoint;
        private readonly string _ordersEndpoint;
        private readonly string _docDate;
        private readonly string _docDueDate;
        private readonly ILogger<SapHanaService> _logger;
        private string _sessionId;

        public SapHanaService(string postgresConnectionString, IConfiguration configuration, ILogger<SapHanaService> logger)
        {
            _postgresConnectionString = postgresConnectionString;
            _httpClient = new HttpClient(new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (message, cert, chain, errors) => true
            });

            _baseUrl = configuration["SAP_API:BaseUrl"];
            _loginEndpoint = configuration["SAP_API:LoginEndpoint"];
            _ordersEndpoint = configuration["SAP_API:OrdersEndpoint"];
            _docDate = configuration["SAP_Document:DocDate"] ?? DateTime.Now.ToString("yyyy-MM-dd");
            _docDueDate = configuration["SAP_Document:DocDueDate"] ?? DateTime.Now.ToString("yyyy-MM-dd");
            _logger = logger;
        }

        private async Task<string> LoginToSapAsync()
        {
            var loginData = new
            {
                CompanyDB = "SBO_EC_SL_TEST",
                Password = "2022",
                UserName = "SISTEMAS2"
            };

            var content = new StringContent(JsonSerializer.Serialize(loginData), Encoding.UTF8, "application/json");

            var response = await _httpClient.PostAsync($"{_baseUrl}{_loginEndpoint}", content);

            if (!response.IsSuccessStatusCode)
            {
                throw new Exception($"Failed to log in to SAP. Status code: {response.StatusCode}");
            }

            var responseData = await response.Content.ReadAsStringAsync();
            var jsonDoc = JsonDocument.Parse(responseData);

            if (jsonDoc.RootElement.TryGetProperty("SessionId", out var sessionIdElement))
            {
                _sessionId = sessionIdElement.GetString();
                _httpClient.DefaultRequestHeaders.Add("Cookie", $"B1SESSION={_sessionId}");
                return _sessionId;
            }

            throw new Exception("Failed to log in to SAP.");
        }

        private async Task EnsureSessionIsValidAsync()
        {
            if (string.IsNullOrEmpty(_sessionId))
            {
                await LoginToSapAsync();
            }
        }

        public async Task<(int, string)> SendOrderToSapAsync(int orderId)
        {
            await EnsureSessionIsValidAsync();

            var orderData = await GetOrderDataFromPostgresAsync(orderId);

            if (orderData == null)
            {
                throw new Exception("Order not found.");
            }

            var jsonData = JsonSerializer.Serialize(orderData);
            _logger.LogInformation($"Sending JSON to SAP: {jsonData}");

            var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

            var response = await _httpClient.PostAsync($"{_baseUrl}{_ordersEndpoint}", content);

            if (!response.IsSuccessStatusCode)
            {
                var responseContent = await response.Content.ReadAsStringAsync();
                if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                {
                    await LoginToSapAsync();
                    content = new StringContent(jsonData, Encoding.UTF8, "application/json");
                    response = await _httpClient.PostAsync($"{_baseUrl}{_ordersEndpoint}", content);

                    if (!response.IsSuccessStatusCode)
                    {
                        responseContent = await response.Content.ReadAsStringAsync();
                        _logger.LogError($"Failed to send order to SAP. Status code: {response.StatusCode}, Response: {responseContent}, JSON Sent: {jsonData}");
                        throw new Exception($"Failed to send order to SAP. Status code: {response.StatusCode}, Response: {responseContent}, JSON Sent: {jsonData}");
                    }
                }
                else
                {
                    _logger.LogError($"Failed to send order to SAP. Status code: {response.StatusCode}, Response: {responseContent}");
                    throw new Exception($"Failed to send order to SAP. Status code: {response.StatusCode}, Response: {responseContent}, JSON Sent: {jsonData}");
                }
            }

            var responseData = await response.Content.ReadAsStringAsync();
            var jsonDoc = JsonDocument.Parse(responseData);

            if (jsonDoc.RootElement.TryGetProperty("DocNum", out var docNum))
            {
                await MarkOrderAsInsertedAsync(orderId);
                return (docNum.GetInt32(), jsonData);
            }

            throw new Exception($"Failed to retrieve DocNum from SAP. JSON Sent: {jsonData}");
        }

        public async Task<(List<(int orderId, int docNum)> successfulOrders, List<(int orderId, string error)> failedOrders)> SendAllOrdersToSapAsync()
        {
            await EnsureSessionIsValidAsync();

            var orderIds = await GetOrderIdsFromPostgresAsync();
            var successfulOrders = new List<(int orderId, int docNum)>();
            var failedOrders = new List<(int, string)>();

            foreach (var orderId in orderIds)
            {
                try
                {
                    var (docNum, jsonData) = await SendOrderToSapAsync(orderId);
                    _logger.LogInformation($"Orden {orderId} enviada correctamente a SAP con documento numero {docNum}.");
                    successfulOrders.Add((orderId, docNum));
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error al enviar la orden con pedido {orderId} to SAP: {ex.Message}");
                    failedOrders.Add((orderId, ex.Message));
                }
            }

            return (successfulOrders, failedOrders);
        }


        private async Task MarkOrderAsInsertedAsync(int orderId)
        {
            try
            {
                using (var connection = new NpgsqlConnection(_postgresConnectionString))
                {
                    await connection.OpenAsync();
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"
                            UPDATE pedido
                            SET insertado_sap = TRUE
                            WHERE idpedido = @orderId";

                        command.Parameters.AddWithValue("@orderId", orderId);

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (NpgsqlException ex)
            {
                _logger.LogError($"Error updating order {orderId} as inserted in PostgreSQL: {ex.Message}");
            }
        }

        private async Task<List<int>> GetOrderIdsFromPostgresAsync()
        {
            var orderIds = new List<int>();

            try
            {
                using (var connection = new NpgsqlConnection(_postgresConnectionString))
                {
                    await connection.OpenAsync();
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"
                            SELECT idpedido
                            FROM pedido
                            WHERE estado = 'Pedido de venta' AND insertado_sap = FALSE";

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                orderIds.Add(reader.GetInt32(0));
                            }
                        }
                    }
                }
            }
            catch (NpgsqlException ex)
            {
                _logger.LogError($"Error connecting to PostgreSQL: {ex.Message}");
            }

            return orderIds;
        }

        private async Task<dynamic> GetOrderDataFromPostgresAsync(int orderId)
        {
            try
            {
                using (var connection = new NpgsqlConnection(_postgresConnectionString))
                {
                    await connection.OpenAsync();
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"
                            SELECT 
                                c.nxt_id_erp AS partner_nxt_id_erp,
                                p.origen_venta,
                                p.user_id AS user_name,
                                c.name AS client_name,
                                p.fecha_creacion AS fecha_creacion,
                                p.date_order AS date_order,
                                dp.id_producto,
                                dp.product_uom_qty,
                                dp.price_unit,
                                dp.discount,
                                pr.nxt_id_erp AS product_nxt_id_erp,
                                dp.qty_order,
                                dp.qty_bonus,
                                pr.taxes_id
                            FROM pedido p
                            JOIN cliente c ON p.idcliente = c.idcliente
                            JOIN detallepedidos dp ON p.idpedido = dp.idpedido
                            JOIN producto pr ON dp.id_producto = pr.id_producto
                            WHERE p.idpedido = @orderId";

                        command.Parameters.AddWithValue("@orderId", orderId);

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            if (await reader.ReadAsync())
                            {
                                var orderData = new
                                {
                                    CardCode = reader["partner_nxt_id_erp"].ToString().Trim(),
                                    DocDate = reader["fecha_creacion"] != DBNull.Value ? Convert.ToDateTime(reader["fecha_creacion"]).ToString("yyyy-MM-dd") : _docDate,
                                    DocDueDate = reader["date_order"] != DBNull.Value ? Convert.ToDateTime(reader["date_order"]).ToString("yyyy-MM-dd") : _docDueDate,
                                    Comments = "Orden de prueba de pedido API",
                                    U_SL_ORI_VTA = MapOriginVenta(reader["origen_venta"].ToString()),
                                    U_SL_USER_ODOO = reader["user_name"].ToString().Trim(),
                                    DocumentLines = new List<object>()
                                };

                                var items = (List<object>)((dynamic)orderData).DocumentLines;

                                do
                                {
                                    items.Add(new
                                    {
                                        ItemCode = reader["product_nxt_id_erp"],
                                        Quantity = Convert.ToDecimal(reader["product_uom_qty"]),
                                        UnitPrice = Convert.ToDecimal(reader["price_unit"]),
                                        DiscountPercent = Convert.ToDecimal(reader["discount"]),
                                        WarehouseCode = "BOPL",
                                        TaxCode = reader["taxes_id"],
                                        U_SL_CANT_PED = Convert.ToDecimal(reader["qty_order"]),
                                        U_SL_CANT_BON = Convert.ToDecimal(reader["qty_bonus"])
                                    });
                                } while (await reader.ReadAsync());

                                return orderData;
                            }
                        }
                    }
                }
            }
            catch (NpgsqlException ex)
            {
                throw new Exception("Error al conectar a PostgreSQL: " + ex.Message);
            }

            return null;
        }

        private string MapOriginVenta(string origenVenta)
        {
            return origenVenta switch
            {
                "Call center" => "1",
                "Cliente" => "2",
                "eCommerce" => "3",
                "Farmareds" => "4",
                "Transferencia" => "5",
                "Vendedor" => "6",
                _ => throw new Exception($"Origen de venta desconocido: {origenVenta}")
            };
        }
    }
}
