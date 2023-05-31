using System;
using System.Data;
using Npgsql;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using System.IO;
using System.Text.Json;
using System.Xml;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace RDSFunction
{
    public class Function
    {
        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            try
            {
                NpgsqlConnection conn = OpenConnection();
                if (conn.State == ConnectionState.Open)
                {
                    Console.WriteLine("Success: Connected to the database");
                    conn.Close();
                    conn.Dispose();
                }
                else
                {
                    Console.WriteLine("Failure: No connection to the database. State: {0}", Enum.GetName(typeof(ConnectionState), conn.State));
                }

                // Retrieve the S3 bucket and object key from the event
            
                // Download the file content from S3
                string fileContent = "";
                var bucketName = "";
                var objectKey = "";
                using (AmazonS3Client s3Client = new AmazonS3Client())
                {
                    try
                    {
                        var latestObject = evnt.Records.OrderByDescending(r => r.EventTime).FirstOrDefault();
                        if (latestObject == null)
                        {
                            context.Logger.LogLine("No records found in the S3 event.");

                        }

                        bucketName = latestObject.S3.Bucket.Name;
                        objectKey = latestObject.S3.Object.Key;

                        // Perform further actions with the bucket name and object key
                        context.Logger.LogLine($"Bucket Name: {bucketName}");
                        context.Logger.LogLine($"Object Key: {objectKey}");

                        using (var response = await s3Client.GetObjectAsync(bucketName, objectKey))
                        using (var streamReader = new StreamReader(response.ResponseStream))
                        {
                            fileContent = await streamReader.ReadToEndAsync();

                            // Perform further actions with the object content
                            context.Logger.LogLine($"Object Content: {fileContent}");
                        }

                    }
                    catch (Exception e)
                    {
                        context.Logger.LogLine($"Error retrieving S3 object content: {e.Message}");
                    }


                }

                if (fileContent.Length == 0)
                {
                    Console.WriteLine("The file is empty.");
                }
                else
                {
                    if (objectKey.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                    {
                        try
                        {
                            // Try parsing the file content as JSON
                            MyData data = JsonSerializer.Deserialize<MyData>(fileContent);
                            Console.WriteLine("success: parsed as JSON");

                            // If parsing succeeds, process the JSON data and insert/update the database
                            ProcessJsonData(data, conn);
                        }
                        catch (Exception ex)
                        {
                            // If parsing fails, print the JSON error
                            Console.WriteLine("JSON error:");
                            Console.WriteLine(ex.Message);
                        }
                    }
                    else if (objectKey.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                    {
                        try
                        {
                            // Try parsing the file content as XML
                            XmlDocument xmlDocument = new XmlDocument();
                            xmlDocument.LoadXml(fileContent);

                            Console.WriteLine("Success: parsed as XML");

                            // If parsing succeeds, process the XML data and insert/update the database
                            ProcessXmlData(xmlDocument, conn);
                        }
                        catch (XmlException xmlException)
                        {
                            // If parsing fails, print the XML error
                            Console.WriteLine("XML error:");
                            Console.WriteLine(xmlException.Message);
                        }
                    }
                    else
                    {
                        Console.WriteLine("Invalid file format. Supported formats are xml and json.");
                    }
                }
            }
            catch (NpgsqlException ex)
            {
                Console.WriteLine("Npgsql Error: {0}", ex.Message);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message);
            }

            return String.Empty;
        }

        //Open connection to postgres server using credentials (could hard code into AWS security)
        private NpgsqlConnection OpenConnection()
        {
            string endpoint = "mod12pginstance.cd4aw9zafazv.us-east-1.rds.amazonaws.com";

            string connString = "Server=" + endpoint + ";" +
                "Port=5432;" +
                "Database=vaccineData;" +
                "User ID=postgres;" +
                "Password=cs455pass;" +
                "Timeout=15";

            // Create connection object and open connection
            NpgsqlConnection conn = new NpgsqlConnection(connString);
            conn.Open();
            return conn;
        }

        //to-do: get it working
        private void ProcessJsonData(MyData data, NpgsqlConnection conn)
        {
            using (NpgsqlCommand cmd = new NpgsqlCommand())
            {
                cmd.Connection = conn;

                foreach (var vaccineData in data.Vaccines)
                {
                    cmd.CommandText = "SELECT COUNT(*) FROM data WHERE SiteID = @siteID AND vaccineDate = @vaccineDate";
                    cmd.Parameters.AddWithValue("@siteID", data.site.id);
                    cmd.Parameters.AddWithValue("@vaccineDate", new DateTime(data.date.year, data.date.month, data.date.day));

                    int rowCount = Convert.ToInt32(cmd.ExecuteScalar());

                    if (rowCount > 0)
                    {
                        cmd.CommandText = "UPDATE data SET FirstShot = @firstShot, SecondShot = @secondShot WHERE SiteID = @siteID AND vaccineDate = @vaccineDate";
                    }
                    else
                    {
                        //to-do: may need to ensure site is in sites table first
                        //siteID, sitename zipcode

                        cmd.CommandText = "INSERT INTO data (SiteID, vaccineDate, FirstShot, SecondShot) VALUES (@siteID, @vaccineDate, @firstShot, @secondShot)";
                    }

                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("@siteID", data.site.id);
                    cmd.Parameters.AddWithValue("@vaccineDate", new DateTime(data.date.year, data.date.month, data.date.day));
                    cmd.Parameters.AddWithValue("@firstShot", vaccineData.firstShot);
                    cmd.Parameters.AddWithValue("@secondShot", vaccineData.secondShot);

                    try
                    {
                        cmd.ExecuteNonQuery();
                        Console.WriteLine("Database operation succeeded.");
                    }
                    catch (NpgsqlException ex)
                    {
                        Console.WriteLine("Database operation failed: " + ex.Message);
                    }
                }
            }
        }

        //to-do: get it working
        private void ProcessXmlData(XmlDocument xmlDocument, NpgsqlConnection conn)
        {
            using (NpgsqlCommand cmd = new NpgsqlCommand())
            {
                cmd.Connection = conn;

                XmlNodeList dataNodes = xmlDocument.SelectNodes("//data");

                foreach (XmlNode dataNode in dataNodes)
                {
                    int siteID = int.Parse(dataNode.SelectSingleNode("site/id").InnerText);
                    DateTime vaccineDate = new DateTime(
                        int.Parse(dataNode.Attributes["year"].Value),
                        int.Parse(dataNode.Attributes["month"].Value),
                        int.Parse(dataNode.Attributes["day"].Value)
                    );

                    cmd.CommandText = "SELECT COUNT(*) FROM data WHERE SiteID = @siteID AND vaccineDate = @vaccineDate";
                    cmd.Parameters.AddWithValue("@siteID", siteID);
                    cmd.Parameters.AddWithValue("@vaccineDate", vaccineDate.Date);

                    int rowCount = Convert.ToInt32(cmd.ExecuteScalar());

                    if (rowCount > 0)
                    {
                        cmd.CommandText = "UPDATE data SET FirstShot = @firstShot, SecondShot = @secondShot WHERE SiteID = @siteID AND vaccineDate = @vaccineDate";
                    }
                    else
                    {
                        cmd.CommandText = "INSERT INTO data (SiteID, vaccineDate, FirstShot, SecondShot) VALUES (@siteID, @vaccineDate, @firstShot, @secondShot)";
                    }

                    int firstShot = int.Parse(dataNode.SelectSingleNode("vaccines/brand/firstShot").InnerText);
                    int secondShot = int.Parse(dataNode.SelectSingleNode("vaccines/brand/secondShot").InnerText);

                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("@siteID", siteID);
                    cmd.Parameters.AddWithValue("@vaccineDate", vaccineDate.Date);
                    cmd.Parameters.AddWithValue("@firstShot", firstShot);
                    cmd.Parameters.AddWithValue("@secondShot", secondShot);

                    try
                    {
                        cmd.ExecuteNonQuery();
                        Console.WriteLine("Database operation succeeded.");
                    }
                    catch (NpgsqlException ex)
                    {
                        Console.WriteLine("Database operation failed: " + ex.Message);
                    }
                }
            }
        }


    }
}
