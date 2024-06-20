using Sap.Data.Hana;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SAPHanaAPI.Services
{
    public class SapHanaService
    {
        private readonly string _hanaConnectionString;
        private readonly string _postgresConnectionString;

        public SapHanaService(string hanaConnectionString, string postgresConnectionString)
        {
            _hanaConnectionString = hanaConnectionString;
            _postgresConnectionString = postgresConnectionString;
        }

        public async Task<List<object>> GetDataFromSapHanaAsync()
        {
            var dataList = new List<object>();

            try
            {
                using (var connection = new HanaConnection(_hanaConnectionString))
                {
                    await connection.OpenAsync();
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"
    SELECT 
        'master' AS ""recordtype"",
        T0.""ItemCode"" AS ""nxt_id_erp"",
        T0.""ItemCode"" AS ""default_code"",
        T0.""ItemName"" AS ""name"",
        T0.""UserText"" AS ""description_sale"",
        'product' AS ""type"",
        'Marca' AS ""attribute_line_ids"",
        T1.""FirmName"" AS ""sl_marca"",
        T1.""FirmName"" AS ""attribute_value"",
        'always' AS ""inventory_availability"",
        CAST(T0.""TaxCodeAR"" AS VARCHAR(100)) AS ""taxes_id"",
        REPLACE(CAST((SELECT P0.""Price"" FROM ""SBO_EC_SL_TEST"".""ITM1"" P0 WHERE P0.""ItemCode"" = T0.""ItemCode"" and P0.""PriceList"" = '1') AS VARCHAR(100)), '.', ',') AS ""list_price"",    
        REPLACE(CAST((SELECT P0.""Price"" FROM ""SBO_EC_SL_TEST"".""ITM1"" P0 WHERE P0.""ItemCode"" = T0.""ItemCode"" and P0.""PriceList"" = '2') AS VARCHAR(100)), '.', ',') AS ""sl_product_pvp"",
        T0.""U_SL_CAR_ESP"" AS ""sl_caract_especial"",
        T0.""U_SL_TIP_CRE"" AS ""sl_tipo_credito"",
        T0.""U_SL_FRA_ART"" AS ""sl_fraccionador"",
        T0.""U_SL_ALM_DES"" AS ""sl_bodega_sap"",
        T2.""ItmsGrpNam"" AS ""sl_division"",
        T3.""Name"" AS ""sl_macrofamilia"",
        T4.""Name"" AS ""sl_familia"",
        T5.""Name"" AS ""sl_presentacion"",
        CAST(T0.""U_SL_CAN_MAX"" AS VARCHAR(50)) AS ""sl_cantidad_max_venta"",
        CAST(IFNULL((SELECT ROUND(SUM(S0.""AvgPrice""*S0.""OnHand"")/SUM(S0.""OnHand""), 2) 
            FROM ""SBO_EC_SL_TEST"".""OITW"" S0
            INNER JOIN ""SBO_EC_SL_TEST"".""OITM"" S3 ON S0.""ItemCode"" = S3.""ItemCode"" 
            WHERE S0.""AvgPrice"" > 0
                AND S0.""OnHand"" > 0
                AND S0.""ItemCode"" = T0.""ItemCode""
            GROUP BY S0.""ItemCode""),0.00) AS VARCHAR(100)) AS ""sl_costo_real"",
        (SELECT 
                SUM(S0.""OnHand"" - S0.""IsCommited"")
            FROM ""SBO_EC_SL_TEST"".""OITW"" S0
            WHERE S0.""ItemCode"" = T0.""ItemCode""
                AND S0.""WhsCode"" = 'BOMI') AS ""additional_stock""
    FROM
        ""SBO_EC_SL_TEST"".""OITM"" T0 
        INNER JOIN ""SBO_EC_SL_TEST"".""OMRC"" T1 ON T1.""FirmCode"" = T0.""FirmCode"" 
        INNER JOIN ""SBO_EC_SL_TEST"".""OITB"" T2 ON T0.""ItmsGrpCod"" = T2.""ItmsGrpCod""
        LEFT JOIN ""SBO_EC_SL_TEST"".""@SL_MACROFAMILIA"" T3 ON T3.""Code"" = T0.""U_SL_MAC_ART""
        LEFT JOIN ""SBO_EC_SL_TEST"".""@SL_FAMILIA"" T4 ON T4.""Code"" = T0.""U_SL_FAM_ART""
        LEFT JOIN ""SBO_EC_SL_TEST"".""@SL_PRESENTACIONES"" T5 ON T5.""Code"" = T0.""U_SL_PRE_ART""
    WHERE
        T0.""SellItem"" = 'Y'
        AND T0.""InvntItem"" = 'Y'";
                        

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                object additionalStockValue = null;
                                if (!reader.IsDBNull(reader.GetOrdinal("additional_stock")))
                                {
                                    decimal additionalStock = reader.GetDecimal(reader.GetOrdinal("additional_stock"));
                                    if (additionalStock % 1 == 0)
                                    {
                                        additionalStockValue = Convert.ToInt32(additionalStock);
                                    }
                                    else
                                    {
                                        additionalStockValue = additionalStock;
                                    }
                                }

                                var rowData = new
                                {
                                    recordtype = reader["recordtype"],
                                    nxt_id_erp = reader["nxt_id_erp"],
                                    default_code = reader["default_code"],
                                    name = reader["name"],
                                    description_sale = reader["description_sale"],
                                    type = reader["type"],
                                    attribute_line_ids = reader["attribute_line_ids"],
                                    sl_marca = reader["sl_marca"],
                                    attribute_value = reader["attribute_value"],
                                    inventory_availability = reader["inventory_availability"],
                                    taxes_id = reader["taxes_id"],
                                    list_price = reader["list_price"],
                                    sl_product_pvp = reader["sl_product_pvp"],
                                    sl_caract_especial = reader["sl_caract_especial"],
                                    sl_tipo_credito = reader["sl_tipo_credito"],
                                    sl_fraccionador = reader["sl_fraccionador"],
                                    sl_bodega_sap = reader["sl_bodega_sap"],
                                    sl_division = reader["sl_division"],
                                    sl_macrofamilia = reader["sl_macrofamilia"],
                                    sl_familia = reader["sl_familia"],
                                    sl_presentacion = reader["sl_presentacion"],
                                    sl_cantidad_max_venta = reader["sl_cantidad_max_venta"],
                                    sl_costo_real = reader["sl_costo_real"],
                                    additional_stock = additionalStockValue
                                };
                                dataList.Add(rowData);

                                // Insert data into PostgreSQL
                                await InsertDataToPostgresAsync(rowData);
                            }
                        }
                    }
                }
            }
            catch (HanaException ex)
            {
                // Manejar el error de conexión a SAP HANA
                throw new Exception("Error al conectar a SAP HANA: " + ex.Message);
            }

            return dataList;
        }

        private async Task InsertDataToPostgresAsync(dynamic rowData)
        {
            try
            {
                using (var connection = new NpgsqlConnection(_postgresConnectionString))
                {
                    await connection.OpenAsync();
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"
                    INSERT INTO producto (nxt_id_erp, name, list_price, sl_product_pvp, stock, taxes_id, sl_marca) 
                    VALUES (@nxt_id_erp, @name, @list_price, @sl_product_pvp, @stock, @taxes_id, @sl_marca)";

                        command.Parameters.AddWithValue("nxt_id_erp", (string)rowData.nxt_id_erp);
                        command.Parameters.AddWithValue("name", (string)rowData.name);
                        command.Parameters.AddWithValue("list_price", rowData.list_price != null ? double.Parse((string)rowData.list_price) : (double?)null);
                        command.Parameters.AddWithValue("sl_product_pvp", rowData.sl_product_pvp != null ? double.Parse((string)rowData.sl_product_pvp) : (double?)null);
                        command.Parameters.AddWithValue("stock", (double)rowData.additional_stock);
                        command.Parameters.AddWithValue("taxes_id", (string)rowData.taxes_id);
                        command.Parameters.AddWithValue("sl_marca", (string)rowData.sl_marca);

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (NpgsqlException ex)
            {
                // Manejar el error de conexión a PostgreSQL
                throw new Exception("Error al conectar a PostgreSQL: " + ex.Message);
            }
        }



    }
}
