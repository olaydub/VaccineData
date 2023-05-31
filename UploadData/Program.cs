using System;
using System.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.S3.Util;
using Amazon.Runtime.Internal;

namespace UploadData
{
    class Program
    {
        static async Task Main(string[] args)
        {

            Console.WriteLine("Enter the file path:");
            string filePath = Console.ReadLine();

            Console.WriteLine("Enter the file format (xml/json):");
            string format = Console.ReadLine();

            if (!File.Exists(filePath))
            {
                Console.WriteLine("File not found.");
                return;
            }

            if (format.ToLower() == "xml")
            {
                await UploadFileToS3(filePath, "xml");
                Console.WriteLine("Uploading XML file: " + filePath);
            }
            else if (format.ToLower() == "json")
            {
                await UploadFileToS3(filePath, "json");
                Console.WriteLine("Uploading JSON file: " + filePath);
            }
            else
            {
                Console.WriteLine("Invalid file format. Supported formats are xml and json.");
                return;
            }

            Console.WriteLine("File upload completed successfully.");
        }

        static async Task UploadFileToS3(string filePath, string fileType)
        {
            // Get credentials to use to authenticate to AWS
            AWSCredentials credentials = GetAWSCredentialsByName("default");

            // Get object to interact with S3
            AmazonS3Client s3Client = new AmazonS3Client(credentials, RegionEndpoint.USEast1);

            // Set S3 bucket name and key for the uploaded file
            string bucketName = "vaccinedata69420";
            string key = Path.GetFileName(filePath);

            // Read file content
            byte[] fileBytes = File.ReadAllBytes(filePath);

            // Upload file to S3
            var putRequest = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                ContentType = GetContentType(fileType),
                InputStream = new MemoryStream(fileBytes),
            };
            Tag tag = new Tag()
            {
                Key = "type",
                Value = fileType
            };
            putRequest.TagSet.Add(tag);
            await s3Client.PutObjectAsync(putRequest);

        }

        static string GetContentType(string fileType)
        {
            if (fileType.ToLower() == "xml")
            {
                return "application/xml";
            }
            else if (fileType.ToLower() == "json")
            {
                return "application/json";
            }
            else
            {
                throw new ArgumentException("Invalid file type.");
            }
        }
        private static AWSCredentials GetAWSCredentialsByName(string profileName)
        {
            if (String.IsNullOrEmpty(profileName))
            {
                throw new ArgumentNullException("profileName cannot be null or empty");
            }

            SharedCredentialsFile credFile = new SharedCredentialsFile();
            CredentialProfile profile = credFile.ListProfiles().Find(p => p.Name.Equals(profileName));

            if (profile == null)
            {
                throw new Exception(String.Format("Profile named {0} not found", profileName));
            }
            return AWSCredentialsFactory.GetAWSCredentials(profile, new SharedCredentialsFile());
        }

    }
}
