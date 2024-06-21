using Sap.Data.Hana;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;

namespace SAPHanaAPI.Services
{
    public class ProductService
    {
        private readonly string _hanaConnectionString;
        private readonly string _postgresConnectionString;

        public ProductService(string hanaConnectionString, string postgresConnectionString)
        {
            _hanaConnectionString = hanaConnectionString;
            _postgresConnectionString = postgresConnectionString;
        }

        public async Task<List<object>> GetProductsFromSapHanaAsync()
        {
            var productList = new List<object>();

            try
            {
                using (var connection = new HanaConnection(_hanaConnectionString))
                {
                    await connection.OpenAsync();
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"
                        SELECT 
                            T0.""ItemCode"" AS ""nxt_id_erp"",
                            T1.""Producto"" AS ""name"",
                            T1.""IVA VENTAS"" AS ""taxes_id"",
                            T1.""GrupoProductos"" AS ""grupo"",
                            T1.""Laboratorio"" AS ""sl_marca"",
                            T1.""PVF"" AS ""list_price"",
                            T1.""PVP"" AS ""sl_product_pvp"",
                            T1.""Estado"" AS ""estado"",
                            T1.""Fraccionador"" AS ""fraccionador"",
                            T1.""Presentación"" AS ""presentacion"",
                            (SELECT SUM(S0.""OnHand"" - S0.""IsCommited"")
                             FROM ""SBO_EC_SL_TEST"".""OITW"" S0
                             WHERE S0.""ItemCode"" = T0.""ItemCode""
                               AND S0.""WhsCode"" = 'BOMI') AS ""stock""
                        FROM
                            ""SBO_EC_SL_TEST"".""OITM"" T0
                        INNER JOIN ""SBO_EC_SL_TEST"".""SL_PRODUCTOS_SAP_FOR_ORDERS"" T1 ON T0.""ItemCode"" = T1.""ItemCode""
                        WHERE
                            T0.""SellItem"" = 'Y'
                            AND T0.""InvntItem"" = 'Y'";

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var rowData = new
                                {
                                    nxt_id_erp = reader["nxt_id_erp"].ToString(),
                                    name = reader["name"].ToString(),
                                    taxes_id = reader["taxes_id"].ToString(),
                                    grupo = reader["grupo"].ToString(),
                                    sl_marca = reader["sl_marca"].ToString(),
                                    list_price = ConvertToDecimal(reader["list_price"].ToString()),
                                    sl_product_pvp = ConvertToDecimal(reader["sl_product_pvp"].ToString()),
                                    estado = reader["estado"].ToString(),
                                    fraccionador = reader["fraccionador"].ToString(),
                                    presentacion = reader["presentacion"].ToString(),
                                    stock = Convert.ToDecimal(reader["stock"])
                                };
                                productList.Add(rowData);

                                // Insertar o actualizar datos en PostgreSQL
                                await UpsertProductToPostgresAsync(rowData);
                            }
                        }
                    }
                }
            }
            catch (HanaException ex)
            {
                throw new Exception("Error al conectar a SAP HANA: " + ex.Message);
            }

            return productList;
        }

        private decimal ConvertToDecimal(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return 0;
            }

            // Reemplazar comas por puntos
            value = value.Replace(',', '.');

            if (decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var result))
            {
                return result;
            }

            throw new FormatException($"Cannot parse decimal value from '{value}'");
        }

        private async Task UpsertProductToPostgresAsync(dynamic rowData)
        {
            try
            {
                using (var connection = new NpgsqlConnection(_postgresConnectionString))
                {
                    await connection.OpenAsync();

                    // Comprobar si el producto ya existe
                    bool exists;
                    using (var checkCmd = connection.CreateCommand())
                    {
                        checkCmd.CommandText = "SELECT COUNT(1) FROM producto WHERE nxt_id_erp = @nxt_id_erp";
                        checkCmd.Parameters.AddWithValue("nxt_id_erp", rowData.nxt_id_erp);
                        exists = (long)await checkCmd.ExecuteScalarAsync() > 0;
                    }

                    // Insertar o actualizar el producto
                    if (exists)
                    {
                        using (var updateCmd = connection.CreateCommand())
                        {
                            updateCmd.CommandText = @"
                            UPDATE producto SET
                                name = @name,
                                taxes_id = @taxes_id,
                                grupo = @grupo,
                                sl_marca = @sl_marca,
                                list_price = @list_price,
                                sl_product_pvp = @sl_product_pvp,
                                estado = @estado,
                                fraccionador = @fraccionador,
                                presentacion = @presentacion,
                                stock = @stock
                            WHERE nxt_id_erp = @nxt_id_erp";

                            updateCmd.Parameters.AddWithValue("name", rowData.name);
                            updateCmd.Parameters.AddWithValue("taxes_id", rowData.taxes_id);
                            updateCmd.Parameters.AddWithValue("grupo", rowData.grupo);
                            updateCmd.Parameters.AddWithValue("sl_marca", rowData.sl_marca);
                            updateCmd.Parameters.AddWithValue("list_price", rowData.list_price);
                            updateCmd.Parameters.AddWithValue("sl_product_pvp", rowData.sl_product_pvp);
                            updateCmd.Parameters.AddWithValue("estado", rowData.estado);
                            updateCmd.Parameters.AddWithValue("fraccionador", rowData.fraccionador);
                            updateCmd.Parameters.AddWithValue("presentacion", rowData.presentacion);
                            updateCmd.Parameters.AddWithValue("stock", rowData.stock);
                            updateCmd.Parameters.AddWithValue("nxt_id_erp", rowData.nxt_id_erp);

                            await updateCmd.ExecuteNonQueryAsync();
                        }
                    }
                    else
                    {
                        using (var insertCmd = connection.CreateCommand())
                        {
                            insertCmd.CommandText = @"
                            INSERT INTO producto (nxt_id_erp, name, taxes_id, grupo, sl_marca, list_price, sl_product_pvp, estado, fraccionador, presentacion, stock) 
                            VALUES (@nxt_id_erp, @name, @taxes_id, @grupo, @sl_marca, @list_price, @sl_product_pvp, @estado, @fraccionador, @presentacion, @stock)";

                            insertCmd.Parameters.AddWithValue("nxt_id_erp", rowData.nxt_id_erp);
                            insertCmd.Parameters.AddWithValue("name", rowData.name);
                            insertCmd.Parameters.AddWithValue("taxes_id", rowData.taxes_id);
                            insertCmd.Parameters.AddWithValue("grupo", rowData.grupo);
                            insertCmd.Parameters.AddWithValue("sl_marca", rowData.sl_marca);
                            insertCmd.Parameters.AddWithValue("list_price", rowData.list_price);
                            insertCmd.Parameters.AddWithValue("sl_product_pvp", rowData.sl_product_pvp);
                            insertCmd.Parameters.AddWithValue("estado", rowData.estado);
                            insertCmd.Parameters.AddWithValue("fraccionador", rowData.fraccionador);
                            insertCmd.Parameters.AddWithValue("presentacion", rowData.presentacion);
                            insertCmd.Parameters.AddWithValue("stock", rowData.stock);

                            await insertCmd.ExecuteNonQueryAsync();
                        }
                    }
                }
            }
            catch (NpgsqlException ex)
            {
                throw new Exception("Error al insertar o actualizar datos en PostgreSQL: " + ex.Message);
            }
        }
    }
}
