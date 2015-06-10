/********************************************************************************************************
 *  AWCOPY:  COPY S3 OBJECTS TO AZURE BLOBS
 * 
 *  Copyright (c) Barry Briggs
 *  All Rights Reserved 
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the 
 *  License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 
 *  
 *  THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, 
 *  INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE, 
 *  MERCHANTABLITY OR NON-INFRINGEMENT. 
 *  
 *  See the Apache 2 License for the specific language governing permissions and limitations under the License. 
 *
 * 
 * 
 * ******************************************************************************************************/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Globalization; 

using Amazon;
using Amazon.S3;
using Amazon.S3.IO;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using Microsoft.WindowsAzure.Storage.Table;

public enum JobStatus
{
    NOTFOUND = -1,
    STARTED = 1,
    COMPLETED = 4,
    FAULTED = 5
}

namespace Globals
{
    public class Global
    {
    }

    /* convenience classes to keep the number of arguments down */
    [Serializable]
    public class S3ToAz
    {
        public Guid _guid; 
        public AmazonS3Data _s3data;
        public AzureBlobData _azdata;
        public S3ToBlobTransferParameters _s3azparms;
        public S3ToAz() { } 
        public S3ToAz(Guid guid, AmazonS3Data s3data, AzureBlobData az, S3ToBlobTransferParameters parms)
        {
            _guid = guid; 
            _s3data = s3data;
            _azdata = az;
            _s3azparms = parms; 
        }
    }

    [Serializable]
    public class AmazonS3Data
    {
        public string AccessKey { get; set; }
        public string Secret { get; set; }
        public string Region {get; set;}
        public string Bucket {get; set;}
        public string Path {get; set;}

        public AmazonS3Data() { }
        public AmazonS3Data(string accesskey, string secret, string region, string bucket, string path)
        {
            AccessKey = accesskey;
            Secret = secret;
            Region = region;
            Bucket = bucket;
            Path = path;
        }
    }
    [Serializable]
    public class AzureBlobData
    {
        public string Container { get; set; }
        public string Account { get; set; }
        public string Key { get; set;  }
        public AzureBlobData() { }
        public AzureBlobData(string container, string account, string key)
        {
            Container = container;
            Account = account;
            Key = key;
        }
    }
    [Serializable]
    public class S3ToBlobTransferParameters
    {
        public bool Encrypt { get; set; }
        public long BlockSize { get; set; }
        public bool Recurse { get; set; }
        public bool DeleteS3AfterCopy { get; set; }
        public bool Prompt { get; set; }
        public bool VerboseLogging { get; set; }
        public string Name { get; set;  }
        public string UserName { get; set; }
        public S3ToBlobTransferParameters() { }
 
        public S3ToBlobTransferParameters(bool encrypt, long blocksize, bool recurse, bool delete, bool prompt, bool verbose, string user="defaultuser", string name="default")
        {
            Encrypt = encrypt;
            BlockSize = blocksize;
            Recurse = recurse;
            DeleteS3AfterCopy = delete;
            Prompt = prompt;
            VerboseLogging = verbose;
            Name = name;                                /* Simple way to identify jobs in the UI. Does not have to be unique. */
            UserName = user; 
        }

    }
    /* Used by the DocumentDB version */ 
    public class OperationStatusRecord
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }
        public string Name { get; set; }
        public string Status { get; set; }
        public string Message { get; set; }
        public OperationStatusRecord() { }
        public OperationStatusRecord(Guid g, string status, string message, string name)
        {
                Id = g.ToString();
            Status = status;
            Message = message;
            Name=name; 
        }
    }
    /* Used by the Azure Table version (current) */ 
    public class TblOperationStatusRecord : TableEntity
    {
        public string Id { get; set;  }
        public string Name { get; set;  }
        public string UserName { get; set; }
        public string StartTime { get; set; }
        public string EndTime { get; set; }
        public string BytesXfer { get; set; }
        public string BytesSec { get; set;  }
        public string Status { get; set; }
        public string Message { get; set; }
        public TblOperationStatusRecord() { }
        public TblOperationStatusRecord(Guid g, string status, string message, string name, string user)
        {
            this.PartitionKey = g.ToString();
            this.RowKey = g.ToString(); 
            Id = g.ToString();
            Status = status;
            Message = message;
            Name = name;
            UserName = user; 
        }
    }

    public class Statics
    {
        public Statics()
        { }
        #region CopyOneObject: Block-oriented static copy of one AWS Object to corresponding Azure blob

        /* this is called with valid clients for S3 and Azure already created */ 
        public static async Task<bool> CopyOneObject(AmazonS3Data s3data, AzureBlobData azdata, S3ToBlobTransferParameters parms, AmazonS3Client client, CloudBlobClient blobClient, string file)
        {
            Amazon.S3.Model.GetObjectRequest amzreq = new Amazon.S3.Model.GetObjectRequest
            {
                BucketName = s3data.Bucket,
                Key = s3data.Path,
                ByteRange = new Amazon.S3.Model.ByteRange(0, parms.BlockSize)
            };

            CloudBlobContainer blobcontainer = blobClient.GetContainerReference(azdata.Container);
            CloudBlockBlob blockBlob = blobcontainer.GetBlockBlobReference(file);

            List<string> blockIds = new List<string>();

            S3DirectoryInfo rootDirectory = new S3DirectoryInfo(client, s3data.Bucket);
                S3FileInfo fileinfo = rootDirectory.GetFile(s3data.Path);
                if (!fileinfo.Exists)
                {
                    //TRACE SOMETHING HERE 
                    return false;
                }

                if (blockBlob.Exists() && parms.Prompt)
                {
                    //TRACE SOMETHING HERE 
                }

                long start = 0;
                long end = start + parms.BlockSize;

                amzreq.ByteRange.Start = start;
                amzreq.ByteRange.End = end;

                int blockNumber = 0;

                /* need to add async stuff here */
                while (amzreq.ByteRange.Start < fileinfo.Length)
                {
                    try
                    {
                        using (Amazon.S3.Model.GetObjectResponse response = await client.GetObjectAsync(amzreq))
                        {
                            using (BinaryReader reader = new BinaryReader(response.ResponseStream))
                            //using (StreamReader reader=new StreamReader(response.ResponseStream))
                            {
                                //reader.BaseStream.Position = 0;
                                //
                                // NOTE : NOT ASYNC!! There is no .NET binary async read method
                                //
                                //may have to change def of BlockSize to int (ya think?) 
                                byte[] buf = reader.ReadBytes((int)parms.BlockSize);

                                /* does this result in another copy? */
                                using (MemoryStream memStream = new MemoryStream(buf))
                                {
                                    var blockId = blockNumber++.ToString().PadLeft(10, '0');
                                    /* are you kidding me? */
                                    var blockIdBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(blockId.ToString(CultureInfo.InvariantCulture).PadLeft(32, '0')));
                                    await blockBlob.PutBlockAsync(blockIdBase64, memStream, null);
                                    blockIds.Add(blockIdBase64);
                                }

                            }
                        }
                    }
                    catch (Exception e)
                    {
                        //TRACESOMETHING HERE 
                    }
                    amzreq.ByteRange.Start += parms.BlockSize;
                    amzreq.ByteRange.End = amzreq.ByteRange.Start + parms.BlockSize;
                }
                blockBlob.PutBlockList(blockIds);

            return true;
        }
        /* caller must update amzreq */ 
        public static async Task<long> CopyOneBlock(AmazonS3Client client, Amazon.S3.Model.GetObjectRequest amzreq, CloudBlockBlob blockBlob, long blocksize, string blockIdBase64)
        {
            long read = 0L; 
            try
            {
                using (Amazon.S3.Model.GetObjectResponse response = await client.GetObjectAsync(amzreq))
                {
                    using (BinaryReader reader = new BinaryReader(response.ResponseStream))
                    {
                        byte[] buf = reader.ReadBytes((int)blocksize);
                        read = buf.Length; 
                        /* does this result in another copy? */
                        using (MemoryStream memStream = new MemoryStream(buf))
                        {
                            //var blockId = blockNumber++.ToString().PadLeft(10, '0');
                            /* are you kidding me? */
                            //var blockIdBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(blockId.ToString(CultureInfo.InvariantCulture).PadLeft(32, '0')));
                            await blockBlob.PutBlockAsync(blockIdBase64, memStream, null);
                            //blockIds.Add(blockIdBase64);
                        }

                    }
                }
            }
            catch (Exception e)
            {
                //TRACESOMETHING HERE
                return 0L;
            }

            return read; 
        }
        #endregion 

        #region Convert a string to a RegionEndpoint for AWS
        public static RegionEndpoint StringToRegion(string r)
        {
            switch (r)
            {
                case "us-west-1":
                    return RegionEndpoint.USWest1;
                case "us-west-2":
                    return RegionEndpoint.USWest2;
                case "us-east-1":
                    return RegionEndpoint.USEast1;
                case "ap-northeast-1":
                    return RegionEndpoint.APNortheast1;
                case "ap-southeast-1":
                    return RegionEndpoint.APSoutheast1;
                case "ap-southeast-2":
                    return RegionEndpoint.APSoutheast2;
                case "cn-north-1":
                    return RegionEndpoint.CNNorth1;
                case "eu_central-1":
                    return RegionEndpoint.EUCentral1;
                case "eu-west-1":
                    return RegionEndpoint.EUWest1;
                case "sa-east-1":
                    return RegionEndpoint.SAEast1;
                case "us-gov-west-1":
                    return RegionEndpoint.USGovCloudWest1;

                default:
                    return null;
            }
        }
        #endregion 

        #region DocumentDB accessors
        /* DocumentDB functions; not currently used but here if you would like to use DocDb for more sophisticated logging and tracking  */

        private static readonly string DocDbUrl = "YOURDOCUMENTDBURL:443";
        private static readonly string DocDbKey = "YOURDOCDBKEY";
        private static readonly string DocDbName = "YOURDOCDBDATABASE"; 
        private static async Task<Database>GetOrCreateDatabaseAsync(DocumentClient client, string id)
        {
            Database database = null;
            try
            {
                database = client.CreateDatabaseQuery().Where(db => db.Id == id).AsEnumerable().FirstOrDefault();
                if (database == null)
                    database = await client.CreateDatabaseAsync(new Database { Id = id });
            }
            catch(Exception e)
            {
                Console.WriteLine("Database creation failure " + e.Message); 
            }
            return database; 
        }
        private static async Task<DocumentCollection>GetOrCreateCollectionAsync(DocumentClient client, string dbLink, string id)
        {
            DocumentCollection collection = client.CreateDocumentCollectionQuery(dbLink).Where(c => c.Id == id).ToArray().FirstOrDefault(); 
            if(collection==null)
            {
                collection = await client.CreateDocumentCollectionAsync(dbLink, new DocumentCollection { Id = id });
            }
            return collection; 
        }
        async public static Task<int>WriteToDocumentDB(Guid g, string status, string message, string name)
        {
            var client = new DocumentClient(new Uri(DocDbUrl), DocDbKey);  
            Database database = await GetOrCreateDatabaseAsync(client, DocDbName);
            if(database==null)
            {
                return 1;
            }
            try
            {
                OperationStatusRecord rec = new OperationStatusRecord(g, status, message, name);
                DocumentCollection documentCollection = await GetOrCreateCollectionAsync(client, database.SelfLink, "awcopylog");
                await client.CreateDocumentAsync(documentCollection.DocumentsLink, rec);
            }
            catch(Exception e)
            {
                return 1;
            }
            return 0; 
        }
        async public static Task<string> GetJobStatus(Guid g)
        {
            var client = new DocumentClient(new Uri(DocDbUrl),DocDbKey);
            Database database = await GetOrCreateDatabaseAsync(client, DocDbName);
            DocumentCollection documentCollection = await GetOrCreateCollectionAsync(client, database.SelfLink, "awcopylog");
            dynamic rec1 = client.CreateDocumentQuery<Document>(documentCollection.SelfLink).Where(d => d.Id == g.ToString()).AsEnumerable().FirstOrDefault();
            return rec1.Status;
        }
        async public static Task UpdateJobStatus(Guid g, string status,string message=null)
        {
            try
            {
                var client = new DocumentClient(new Uri(DocDbUrl),DocDbKey);
                Database database = await GetOrCreateDatabaseAsync(client, DocDbName);
                DocumentCollection documentCollection = await GetOrCreateCollectionAsync(client, database.SelfLink, "awcopylog");
                dynamic rec1 = client.CreateDocumentQuery<Document>(documentCollection.SelfLink).Where(d => d.Id == g.ToString()).AsEnumerable().FirstOrDefault();
                rec1.Status = status;
                if (message != null)
                    rec1._message = message;
                dynamic doc = rec1;
                await client.ReplaceDocumentAsync(doc);
            }
            catch(Exception e)
            {
                Console.WriteLine("Update Job Status: " +e.Message); 
            }

        }
        async public static Task DeleteRecord(Guid g)
        {
            var client = new DocumentClient(new Uri(DocDbUrl), DocDbKey);
            Database database = await GetOrCreateDatabaseAsync(client, DocDbName);
            DocumentCollection documentCollection = await GetOrCreateCollectionAsync(client, database.SelfLink, "awcopylog");
            dynamic rec1 = client.CreateDocumentQuery<Document>(documentCollection.SelfLink).Where(d => d.Id == g.ToString()).AsEnumerable().FirstOrDefault();
            await client.DeleteDocumentAsync(rec1.SelfLink);

        }

        #endregion

        #region Azure Table Accessors

        private static readonly string AzTableName = "YOURAZURETABLENAME";
        private static readonly string AzTableKey = "YOURAZURESTORAGEKEY";
        async static public Task<CloudTable> GetOrCreateAzureTable()
        {
            StorageCredentials creds = new StorageCredentials(AzTableName,AzTableKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference("awscopylog");
            table.CreateIfNotExists();
            return table; 
        }

        async static public Task<int> CreateJobRecordInAzureTable(Guid g, string status, S3ToBlobTransferParameters parms, string message = null)
        {
            try
            {
                CloudTable tbl = await GetOrCreateAzureTable();
                TblOperationStatusRecord rec = new TblOperationStatusRecord(g, status, message, parms.Name, parms.UserName);
                rec.StartTime = DateTime.Now.ToString();                /* 2/27/2009 12:16:59 PM format */
                rec.EndTime = " "; 
                TableOperation insertOperation = TableOperation.Insert(rec);
                tbl.Execute(insertOperation);
                return 0; 
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
                return -1; 
            }
        }
        async static public Task<string> GetJobStatusFromAzureTable(Guid g)
        {
            try
            {
                CloudTable tbl = await GetOrCreateAzureTable();
                TableQuery<TblOperationStatusRecord> query = new TableQuery<TblOperationStatusRecord>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, g.ToString()));
                foreach (TblOperationStatusRecord entity in tbl.ExecuteQuery(query))
                {
                    return entity.Status;
                }
                return "NOTFOUND";
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return "ERROR";
            }
        }

        async static public Task UpdateJobStatusInAzureTable(Guid g, string status)
        {
            try
            {
                CloudTable tbl = await GetOrCreateAzureTable();
                TableQuery<TblOperationStatusRecord> query = new TableQuery<TblOperationStatusRecord>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, g.ToString()));
                foreach (TblOperationStatusRecord entity in tbl.ExecuteQuery(query))
                {
                    entity.Status = status;
                    if (status == "COMPLETED")
                        entity.EndTime = DateTime.Now.ToString(); 
                    TableOperation updateOperation = TableOperation.Replace(entity);
                    tbl.Execute(updateOperation);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

        }
        async static public Task<List<TblOperationStatusRecord>> GetAllStatus(int filter)
        {
            string[] s1;
            List<TblOperationStatusRecord> recs = new List<TblOperationStatusRecord>();
            try
            {
                TableContinuationToken continuationToken = null;
                TableQuery<TblOperationStatusRecord> query = null; 
                TableQuerySegment<TblOperationStatusRecord> tableQueryResult=null;
                CloudTable tbl = await GetOrCreateAzureTable();
                switch(filter) {
                    case 0:
                        {  
                            //return all data
                            query = new TableQuery<TblOperationStatusRecord>(); 
                            tableQueryResult = await tbl.ExecuteQuerySegmentedAsync(query, continuationToken);
                            break;
                        }
                    case 1:         //return 'started'
                        {
                            query = new TableQuery<TblOperationStatusRecord>().Where(TableQuery.GenerateFilterCondition("Status", QueryComparisons.Equal, "STARTED"));
                            tableQueryResult = await tbl.ExecuteQuerySegmentedAsync(query, continuationToken);
                            break;
                        }
                    case 2:         //return 'completed'
                        {
                            query = new TableQuery<TblOperationStatusRecord>().Where(TableQuery.GenerateFilterCondition("Status", QueryComparisons.Equal, "COMPLETED"));
                            tableQueryResult = await tbl.ExecuteQuerySegmentedAsync(query, continuationToken);
                            break;
                        }
                case 3:         // return 'faulted'
                        {
                            query = new TableQuery<TblOperationStatusRecord>().Where(TableQuery.GenerateFilterCondition("Status", QueryComparisons.Equal, "FAULTED"));
                            tableQueryResult = await tbl.ExecuteQuerySegmentedAsync(query, continuationToken);
                            break;
                        }
                default:
                    break; 
                }
                if (tableQueryResult==null || tableQueryResult.Results.Count == 0)
                    return null;
                s1 = new string[tableQueryResult.Results.Count];
                int i = 0; 
                foreach(TblOperationStatusRecord rec in tableQueryResult.Results)
                {
                    //string json = JsonConvert.SerializeObject(rec);
                    //s1[i++] = json;
                    recs.Add(rec); 
                }
                return recs; 
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            return null; 
        }
        async static public Task DeleteJobInAzureTable(Guid g)
        {

            return;
        }
        #endregion 
    }
}
