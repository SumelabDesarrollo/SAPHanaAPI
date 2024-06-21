using Sap.Data.Hana;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Npgsql;

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

        public async Task SyncClients()
        {
            var clientsList = await GetClientsFromSapHanaAsync();

            foreach (var client in clientsList)
            {
                await UpsertClientInPostgresAsync(client);
            }
        }

        public async Task<List<object>> GetClientsFromSapHanaAsync()
        {
            var clientsList = new List<object>();

            try
            {
                using (var hanaConnection = new HanaConnection(_hanaConnectionString))
                {
                    await hanaConnection.OpenAsync();
                    using (var command = hanaConnection.CreateCommand())
                    {
                        command.CommandText = @"
                        SELECT
                            T0.""CardCode"",
                            T0.""CardName"",
                            T0.""LicTradNum"",
                            T0.""Balance"",
                            T0.""Vendedor"",
                            T0.""AsesorCredito"",
                            T0.""CallCenter"",
                            T0.""Estado"",
                            T0.""Unida de Negocio"",
                            T0.""Provincia"",
                            T0.""Ciudad"",
                            T0.""Parroquia"",
                            T0.""MaximoDiasVencido"",
                            T0.""CardFName"",
                            OCTG.""PymntGroup"" AS ""property_payment_term_id"",
                            CAST(T1.""CreditLine"" AS VARCHAR(100)) AS ""credit_limit"",
                            T1.""frozenFor"" AS ""state_id""
                        FROM
                            SBO_EC_SL_TEST.SL_CLIENTES_SAP_FOR_ORDERS T0
                            LEFT JOIN SBO_EC_SL_TEST.OCRD T1 ON T0.""CardCode"" = T1.""CardCode""
                            LEFT JOIN SBO_EC_SL_TEST.OCTG ON T1.""GroupNum"" = OCTG.""GroupNum""";

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var clientData = new
                                {
                                    nxt_id_erp = reader["CardCode"].ToString(),
                                    name = reader["CardName"].ToString(),
                                    vat = reader["LicTradNum"].ToString(),
                                    saldo = ConvertToDecimal(reader["Balance"].ToString()),
                                    user_id = reader["Vendedor"].ToString(),
                                    asesor_credito = reader["AsesorCredito"].ToString(),
                                    asesor_callcenter = reader["CallCenter"].ToString(),
                                    state_id = reader["state_id"].ToString(),
                                    estado = reader["Estado"].ToString(),
                                    sl_cla_cli = reader["Unida de Negocio"].ToString(),
                                    street = $"{reader["Provincia"]} {reader["Ciudad"]} {reader["Parroquia"]}",
                                    maximodias = ConvertToDecimal(reader["MaximoDiasVencido"].ToString()),
                                    x_studio_nombre_comercial_sap = reader["CardFName"].ToString(),
                                    property_payment_term_id = reader["property_payment_term_id"].ToString(),
                                    credit_limit = ConvertToDecimal(reader["credit_limit"].ToString())
                                };
                                clientsList.Add(clientData);
                            }
                        }
                    }
                }
            }
            catch (HanaException ex)
            {
                throw new Exception("Error al conectar a SAP HANA: " + ex.Message);
            }

            return clientsList;
        }

        private async Task UpsertClientInPostgresAsync(dynamic clientData)
        {
            try
            {
                using (var postgresConnection = new NpgsqlConnection(_postgresConnectionString))
                {
                    await postgresConnection.OpenAsync();

                    // Verificar o crear la dirección
                    var direccionId = await EnsureDireccionExistsAsync(postgresConnection, clientData.street);

                    var upsertClientCommand = new NpgsqlCommand(@"
                        INSERT INTO cliente (nxt_id_erp, name, vat, saldo, user_id, asesor_credito, asesor_callcenter, estado, sl_cla_cli, maximodias, x_studio_nombre_comercial_sap, property_payment_term_id, credit_limit, iddireccion, state_id)
                        VALUES (@nxt_id_erp, @name, @vat, @saldo, @user_id, @asesor_credito, @asesor_callcenter, @estado, @sl_cla_cli, @maximodias, @x_studio_nombre_comercial_sap, @property_payment_term_id, @credit_limit, @iddireccion, @state_id)
                        ON CONFLICT (nxt_id_erp) DO UPDATE SET
                            name = EXCLUDED.name,
                            vat = EXCLUDED.vat,
                            saldo = EXCLUDED.saldo,
                            user_id = EXCLUDED.user_id,
                            asesor_credito = EXCLUDED.asesor_credito,
                            asesor_callcenter = EXCLUDED.asesor_callcenter,
                            estado = EXCLUDED.estado,
                            sl_cla_cli = EXCLUDED.sl_cla_cli,
                            maximodias = EXCLUDED.maximodias,
                            x_studio_nombre_comercial_sap = EXCLUDED.x_studio_nombre_comercial_sap,
                            property_payment_term_id = EXCLUDED.property_payment_term_id,
                            credit_limit = EXCLUDED.credit_limit,
                            iddireccion = EXCLUDED.iddireccion,
                            state_id = EXCLUDED.state_id", postgresConnection);

                    upsertClientCommand.Parameters.AddWithValue("nxt_id_erp", clientData.nxt_id_erp);
                    upsertClientCommand.Parameters.AddWithValue("name", clientData.name);
                    upsertClientCommand.Parameters.AddWithValue("vat", clientData.vat);
                    upsertClientCommand.Parameters.AddWithValue("saldo", clientData.saldo);
                    upsertClientCommand.Parameters.AddWithValue("user_id", clientData.user_id);
                    upsertClientCommand.Parameters.AddWithValue("asesor_credito", clientData.asesor_credito);
                    upsertClientCommand.Parameters.AddWithValue("asesor_callcenter", clientData.asesor_callcenter);
                    upsertClientCommand.Parameters.AddWithValue("estado", clientData.estado);
                    upsertClientCommand.Parameters.AddWithValue("sl_cla_cli", clientData.sl_cla_cli);
                    upsertClientCommand.Parameters.AddWithValue("maximodias", clientData.maximodias);
                    upsertClientCommand.Parameters.AddWithValue("x_studio_nombre_comercial_sap", clientData.x_studio_nombre_comercial_sap);
                    upsertClientCommand.Parameters.AddWithValue("property_payment_term_id", clientData.property_payment_term_id);
                    upsertClientCommand.Parameters.AddWithValue("credit_limit", clientData.credit_limit);
                    upsertClientCommand.Parameters.AddWithValue("state_id", clientData.state_id);
                    upsertClientCommand.Parameters.AddWithValue("iddireccion", direccionId);

                    await upsertClientCommand.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error al insertar o actualizar datos en PostgreSQL: " + ex.Message);
            }
        }

        private async Task<int> EnsureDireccionExistsAsync(NpgsqlConnection postgresConnection, string street)
        {
            // Verificar si la dirección ya existe
            var selectCommand = new NpgsqlCommand("SELECT iddireccion FROM direccion WHERE street = @street LIMIT 1", postgresConnection);
            selectCommand.Parameters.AddWithValue("street", street);

            var result = await selectCommand.ExecuteScalarAsync();

            if (result != null)
            {
                return (int)result;
            }

            // Si la dirección no existe, crearla
            var insertCommand = new NpgsqlCommand("INSERT INTO direccion (street) VALUES (@street) RETURNING iddireccion", postgresConnection);
            insertCommand.Parameters.AddWithValue("street", street);

            var newId = await insertCommand.ExecuteScalarAsync();
            return (int)newId;
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
    }
}
