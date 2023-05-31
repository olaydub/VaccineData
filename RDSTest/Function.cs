//This is a test lambda to test querying the postgres db - currently connecting, but queries not executing

using System;
using System.Data;
using Npgsql;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using System.ComponentModel;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace RDSTest
{
    public class Function
    {
        public string FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            try
            {
                NpgsqlConnection conn = OpenConnection();
                if (conn.State == ConnectionState.Open)
                {
                    Console.WriteLine("Success: Connected to the database");
                    using (conn)
                    {
                        // Add a dummy row to the database
                        AddDummyRow(conn);
                    }
                    conn.Close();
                    conn.Dispose();
                }
                else
                {
                    Console.WriteLine("Failure: No connection to the database. State: {0}", Enum.GetName(typeof(ConnectionState), conn.State));
                }
                

                return "Dummy row added successfully.";
            }
            catch (NpgsqlException ex)
            {
                Console.WriteLine("Npgsql Error: {0}", ex.Message);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message);
            }

            return "Failed to add dummy row.";
        }

        private NpgsqlConnection OpenConnection()
        {
            string endpoint = "mod12pginstance.cd4aw9zafazv.us-east-1.rds.amazonaws.com";

            string connString = "Server=" + endpoint + ";" +
                "Port=5432;" +
                "Database=vaccineData;" +
                "User ID=postgres;" +
                "Password=cs455pass;" +
                "Timeout=15";

            //Create connection object and open connection
            NpgsqlConnection conn = new NpgsqlConnection(connString);
            conn.Open();
            return conn;


            //using var dataSource = NpgsqlDataSource.Create(connString);
            //return dataSource;
        }

        private async Task AddDummyRow(NpgsqlConnection conn)
        {
            conn.Open();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = conn;

            cmd.CommandText = "INSERT INTO public.sites(siteid, sitename, zipcode) VALUES (1, \"site1\", 123);";
            using (cmd)
            {
                await cmd.ExecuteNonQueryAsync();
            }
           


        }
    }
}
