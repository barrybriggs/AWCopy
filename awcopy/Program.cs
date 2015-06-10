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
using System.Collections.Specialized;
using System.Net;
using System.Net.Http;
using System.Web;
using System.Threading; 
using System.Globalization;
using System.Net.Http.Headers;
using System.Net.Http.Formatting; 


using Newtonsoft.Json; 

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth; 
using Orleans;

using Amazon;
using Amazon.S3;
using Amazon.S3.IO;

using GrainInterfaces;
using Globals; 

namespace awcopy
{

    class Program
    {
        private static string help = "AWCopy help \n\n" +
                                      "Usage: awcopy arguments switches \n\n"+
                                   "   Arguments:\n\n"+
                                   "   AWS Parameters:\n" +
                                   "\t bucket:{aws-bucket-specification} path:{aws-object-path} \n\t accesskey:{aws s3 access key} secret:{aws s3 secret key} \n\t region:{aws-region}\n" +
                                   "   Azure Parameters:\n"+
                                   "\t container:{azure-container-specification} \n\t storageaccount:{azure storage account} \n\t storagekey:{azure storage key} \n\n" +
                                   "   Switches:\n" +
                                   "\t /C: Use the cloud batch service to bulk copy files\n" +
                                   "\t /B: Specify block size (default: 4MB); format nnnnK or nnnnM \n" +
                                   "\t /U: Simple copy (object downloaded locally then uploaded) (default)\n" +
                                   "\t /E: Encrypt blobs stored in Azure\n" +
                                   "\t /J: Job name (does not have to be unique; no spaces; default: \"default\""+
                                   "\t /D: Delete files in AWS after copy (confirmation required)\n" +
                                   "\t /S: Recursively copy\n" +
                                   "\t /P: Prompt to overwrite existing Azure blob\n" + 
                                   "\t /N: Display number of files to copy only (no actual copy)\n" +
                                   "\t /A: (Maximum) number of agents in Azure to handle copy (default:4) \n" +
                                   "\t /L: List contents of AWS bucket (Azure parameters ignored) with estimated egress charges\n" +
                                   "\t /H: Display help on any command\n" +
                                   "\t /Z: Log all activity to local journal file, format is awcopy-date-time.log\n" +
                                   "\t /V: Verbose logging\n" +
                                   "NOTE AWS charges will apply to data copied out of AWS\n"; 

        static void Main(string[] args)
        {
            int i;
            string bucket=null;
            string path = null; 
            string accesskey=null;
            string secret=null;
            string region = null; 
            string container=null;
            string account=null;
            string key=null;

            bool batch = false;
            bool simple = false ; 
            bool encrypt=false;
            bool delete=false;
            bool recurse = false; 
            bool prompt = false; 
            bool listazure=false; 
            int agents=4;
            bool verbose = false; 

            bool listbucket = false; 
            string logfile="";
            RegionEndpoint awsregion=null;
            StreamWriter logstream=null; 
            bool logging=false;
            long blocksize=4194304;             /* 2**22, or 4MB is the default */
            string jobname = "default"; 

            if(args.Length==0)
            {
                Console.WriteLine(help);
                Console.WriteLine("Press any key ... ");
                Console.ReadKey(); 
                return;
            }

            for (i = 0; i < args.Length; i++)
            {
                if (args[i] == "/H")
                {
                    Console.WriteLine(help);
                    return;
                }
                else if (args[i].StartsWith("bucket:"))
                    bucket = args[i].Substring(7, args[i].Length - 7);
                else if (args[i].StartsWith("path:"))
                    path = args[i].Substring(5, args[i].Length - 5);
                else if (args[i].StartsWith("accesskey:"))
                    accesskey = args[i].Substring(10, args[i].Length - 10);
                else if (args[i].StartsWith("secret:"))
                    secret = args[i].Substring(7, args[i].Length - 7);
                else if (args[i].StartsWith("region:"))
                {
                    region = args[i].Substring(7, args[i].Length - 7);
                    awsregion = Statics.StringToRegion(region);
                    if (awsregion == null)
                    {
                        Console.WriteLine("Illegal region specified");
                        return;
                    }
                }
                else if (args[i].StartsWith("container:"))
                    container = args[i].Substring(10, args[i].Length - 10);
                else if (args[i].StartsWith("storageaccount:"))
                    account = args[i].Substring(15, args[i].Length - 15);
                else if (args[i].StartsWith("storagekey:"))
                    key = args[i].Substring(11, args[i].Length - 11);
                else if (args[i].StartsWith("/C"))
                {
                    batch = true;
                    simple = false;
                }
                else if (args[i].StartsWith("/U"))
                {
                    batch = false;
                    simple = true;
                }

                else if (args[i].StartsWith("/B"))
                {
                    blocksize = ParseBlockSize(args[i]);
                    if (blocksize == -1) 
                    {
                        Console.WriteLine("Block size incorrectly specified; examples: /B:4M, /B:16K, /B:16384");
                        return;
                    }
                }
                else if (args[i].StartsWith("/J"))
                {
                    jobname = args[i].Substring(3);
                    if (jobname.Length == 0)
                    {
                        Logger(logstream, logging, "Zero length jobname specified; exiting");
                        return;
                    }
                }
                else if (args[i].StartsWith("/S"))
                    recurse = true;
                else if (args[i].StartsWith("/E"))
                    encrypt = true;
                else if (args[i].StartsWith("/D"))
                    delete = true;
                else if (args[i].StartsWith("/P"))
                    prompt = true;
                else if (args[i].StartsWith("/L"))
                    listbucket = true;
                else if (args[i].StartsWith("/V"))
                    verbose = true; 
                else if (args[i].StartsWith("/A"))
                    agents = ToNum(args[i].Substring(3, args[i].Length - 3));
                else if (args[i].StartsWith("/Z"))
                {
                    string exepath = System.IO.Path.GetDirectoryName(Environment.GetCommandLineArgs()[0]);
                    logfile = "awcopy-" + DateTime.Now.Month.ToString() +
                        DateTime.Now.Month.ToString() +
                        DateTime.Now.Day.ToString() +
                        DateTime.Now.Year.ToString() + "-" +
                        DateTime.Now.Hour.ToString() +
                        DateTime.Now.Minute.ToString() +
                        DateTime.Now.Second.ToString() + ".log";
                    string fullfile = exepath + logfile;
                    logstream = new StreamWriter(fullfile);
                    logging = true;
                    logstream.WriteLine("AWCopy Log " + DateTime.Now.ToLongDateString() + " " + DateTime.Now.ToLongTimeString());

                }
                /* undocumented features */
                else if (args[i].StartsWith("/F"))
                    listazure = true;

                /* unexpected argument */
                else
                {
                    Console.WriteLine("Unexpected argument or option at parameter" + (i + 1).ToString());
                    Console.WriteLine(help);
                    Console.WriteLine("Press any key ... ");
                    Console.ReadKey();
                    return;
                }
            }

            AWSCopy copier = new AWSCopy(logging, logstream);

            if(listbucket)
            {
                Console.WriteLine("Listing files only");
                string res=copier.ListAllAWSFiles(bucket, accesskey, secret, awsregion);
                Logger(logstream, logging, res);
            }
            if (listazure)
            {
                Console.WriteLine("Listing Azure files only");
                string res=copier.ListAllAzureFiles(container, account, key);
                Logger(logstream, logging, res);
 
            }

            else if (simple)
            {
                string res = copier.DoSimpleCopy(bucket, path, accesskey, secret, awsregion, container, account, key, encrypt, delete, logfile, prompt, recurse);
                Logger(logstream, logging, res);
            }
            else if (batch)
            {

                /* new */
                AmazonS3Data s3data = new AmazonS3Data(accesskey, secret, region, bucket, path);
                AzureBlobData blobdata = new AzureBlobData(container, account, key);
                S3ToBlobTransferParameters xferparms = new S3ToBlobTransferParameters(encrypt, blocksize, recurse, delete, prompt, verbose, Environment.UserName, jobname);

                /* for cloud version, we let the cloud look up the AWS region. There appears to be a bug in the marshalling of the RegionEndpoint */
                copier.DoBatchCopy(s3data, blobdata, xferparms);

            }
            Console.WriteLine("Press any key ... ");
            Console.ReadLine();

            if (logstream != null)
                logstream.Dispose(); 
        }
        static public void Logger(StreamWriter logstream, bool log, string text)
        {
            if (log)
                logstream.WriteLine(text);
            Console.WriteLine(text); 
        }
        static int ToNum(string s)
        {
            int n = 0;
            int i = 0;
            while (i < s.Length)
                if (s[i] >= 0x30 && s[i] <= 0x39)
                    n = n * 10 + (s[i] - 0x30);
                else
                    throw (new InvalidOperationException("Illegal number")); 
            return n; 
        }
        /* very little error checking here */ 
        static long ParseBlockSize(string s)
        {
            int i=3;    /* past /B: */
            int p=0;    /* power of 10 */
            int n=0;    /* result */ 

            s=s.ToLower(); 
            while (i<s.Length)
            {
               if (s[i] >= 0x30 && s[i] <= 0x39)
                    n = n * (int)Math.Pow(10,p) + (s[i] - 0x30);
                else if(s[i]=='k')
                    n=n*1024;
                else if(s[i]=='m')
                    n=n*1024*1024;
                else
                    return -1; 
                i++;
                p++; 
            }
            return n; 
        }
    }

    class AWSCopy
    {
        string _bucket;
        string _accesskey;
        string _secret;
        RegionEndpoint _region; 
        string _container; 
        string _account;
        string _key;
        bool _blog = false; 
        StreamWriter _log;
        string username = Environment.UserName;
        string azUrl = "YOURAZUREURLGOESHERE";

#if using_Orleans_locally
        bool _orlinit = false; 
#endif
        private ulong totalsize = 0;
        private double costpergigabyte = 0.09; 

        public AWSCopy() 
        {
  
        }
        public AWSCopy( bool blog, StreamWriter logger)
        {
            _blog = blog; 
            _log = logger; 
        }
        #region Local Copy Controller 

        /* This copy function downloads the object from AWS into local memory and the uploads it into Azure blob storage. 
         * Not recommended for very large bulk functions but handy for a file or two or a small directory. However it will move
         * very large AWS buckets and folders if you really want to go this route, but won't be as fast as the cloud/actor
         * version, which is parallelized. Same code however is used in the cloud actors, just runs multiple instances in parallel. */

        public string DoSimpleCopy(string bucket, string path, string accesskey, string secret, RegionEndpoint awsregion, string container, string account, string key,
            bool encrypt, bool delete, string logfile, bool prompt, bool recurse)
        {
            _bucket = bucket;
            _accesskey = accesskey;
            _secret = secret;
            _region = awsregion;
            _container = container;
            _account = account;
            _key = key;

            AmazonS3Client client;

            if (container == null || container == "")
                return("No container specified");
            if (path == null || path == "")
                return ("No file(s) specified");
            if (accesskey == null || accesskey == "" || secret == null || secret == "" || awsregion == null)
                return ("AWS parameters not specified"); 
            if (account == null || account == "" || key == null || key == "")
                return ("Azure parameters not specified (need both storage account and storage key)");

            StorageCredentials creds = new StorageCredentials(account, key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            /* do one (specified) file */
            using (client = new Amazon.S3.AmazonS3Client(accesskey, secret, awsregion))
            {
                S3DirectoryInfo rootDirectory = new S3DirectoryInfo(client, bucket);

                /* one file or all? (TODO: regular expression parsing) */ 
                if (path != "*.*")
                {
                    S3FileInfo fileinfo = rootDirectory.GetFile(path);
                    if (!fileinfo.Exists)
                    {
                        Program.Logger(_log, _blog, "Object does not exist in S3; note object names are case-sensitive");
                        return ("Error: no such object");
                    }

                    byte[] buf = ReadOneAWSObject(fileinfo);
                    if(buf!=null)
                    {
                       bool uploaded = UploadBufferToBlob(buf, container, fileinfo.Name, blobClient, prompt);
                       if (!uploaded)
                           return "Error on upload";
                    }
      
                }
                /* copying all files. check for recursion flag */ 
                else
                {
                    CopyDirectoryRecursive(rootDirectory, container, blobClient, prompt, recurse); 
                }
            }
            return "Success";
        }

        #endregion 

        #region Local Copy of Very Large Object

        /* this method copies blobs of arbitrary length; this is NOT RECOMMENDED and NOT DOCUMENTED but
         * is handy for debugging purposes. */
        public async Task<bool> CopyLargeObject(string acckey, string secret, RegionEndpoint region, string bucket, string file, string account, string key, string container)
        {
            var blocksize = 20000;                  /* set this via a switch later */
            Amazon.S3.Model.GetObjectRequest amzreq = new Amazon.S3.Model.GetObjectRequest
            {
                BucketName = bucket,
                Key = file, 
                ByteRange = new Amazon.S3.Model.ByteRange(0, blocksize)
            };

            StorageCredentials creds = new StorageCredentials(account, key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer blobcontainer = blobClient.GetContainerReference(container);
            CloudBlockBlob blockBlob = blobcontainer.GetBlockBlobReference(file); 

            List<string> blockIds = new List<string>(); 

            using (Amazon.S3.AmazonS3Client client = new Amazon.S3.AmazonS3Client(acckey, secret, region))
            {
                S3DirectoryInfo rootDirectory = new S3DirectoryInfo(client, bucket);
                S3FileInfo fileinfo = rootDirectory.GetFile(file);
                if (!fileinfo.Exists)
                {
                    Program.Logger(_log, _blog, "Object does not exist in S3; note object names are case-sensitive");
                    return false; 
                }

                long start = 0;
                long end = start + blocksize;

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
                            {
                                //reader.BaseStream.Position = 0;
                                byte[] buf = reader.ReadBytes(blocksize); 

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
                    catch(Exception e)
                    {
                        Program.Logger(_log, _blog, "Exception in large copy" + e.Message); 
                    }
                    amzreq.ByteRange.Start += blocksize;
                    amzreq.ByteRange.End = amzreq.ByteRange.Start + blocksize;
                }
                blockBlob.PutBlockList(blockIds);
            }
            return true;
        }
        #endregion

        #region Full Directory/Recursive Local Copy
        private bool CopyDirectoryRecursive(S3DirectoryInfo s3dir, string container, CloudBlobClient blobClient, bool prompt, bool recurse)
        {
            S3FileInfo[] s3files = s3dir.GetFiles();
            for (int i = 0; i < s3files.Length; i++)
            {
                Program.Logger(_log, _blog, s3files[i].FullName);
                byte[] buf = ReadOneAWSObject(s3files[i]);
                if (buf != null)
                {
                    /* the FullName includes the "folder" which we will replicate on Azure whose folder semantics are the same
                     * (there are no "real" folders in either AWS or Azure */
                    bool uploaded = UploadBufferToBlob(buf, container, s3files[i].FullName, blobClient, prompt);
                }
            }
            if (recurse)
            {
                S3DirectoryInfo[] s3subdirs = s3dir.GetDirectories();
                for (int i = 0; i < s3subdirs.Length; i++)
                {
                    Program.Logger(_log, _blog, s3subdirs[i].FullName);
                    /* does this directory exist in Azure? */
                    CopyDirectoryRecursive(s3subdirs[i], container, blobClient, prompt, recurse);
                }
            }

            return true;
        }
        #endregion 

        #region Read and Write One Object/Blob 
        private byte[] ReadOneAWSObject(S3FileInfo fi)
        {
            byte[] buf = new byte[fi.Length];
            /* NOTE StreamReader can only read <int> length files = 4GB */
            using (Stream sr = fi.OpenRead())
            {
                int ct = sr.Read(buf, 0, (int)fi.Length);
                if(ct!=(int)fi.Length)
                {
                    Program.Logger(_log, _blog, "Error on read from AWS: " + fi.FullName);
                    return null; 
                }
            }
            return buf; 
        }
        private bool UploadBufferToBlob(byte[] buf, string container, string name, CloudBlobClient blobClient , bool prompt)
        {
            try
            {
                // Retrieve reference to a previously created container.
                CloudBlobContainer blobcontainer = blobClient.GetContainerReference(container);

                // Retrieve reference to a blob.
                CloudBlockBlob blockBlob = blobcontainer.GetBlockBlobReference(name);
                if(blockBlob.Exists() && prompt==true)
                {
                    Console.WriteLine("Blob exists. Overwrite(Y/N)?");
                    ConsoleKeyInfo k=Console.ReadKey();
                    if (k.Key != ConsoleKey.Y)
                    {
                        Program.Logger(_log, _blog, "Blob exists, skipping per user response"); 
                        return false; 
                    }
                    Program.Logger(_log, _blog, "Blob exists, overwriting per user response"); 

                }

                // Create or overwrite the "myblob" blob with contents from a local file.
                using (MemoryStream sr = new MemoryStream(buf))
                {
                    blockBlob.UploadFromStream(sr);
                }
            }
            catch(Exception e)
            {
                Program.Logger(_log, _blog, "Error on upload to Azure " + e.Message);
                return false; 
            }
            return true; 
        }
        #endregion 

        #region Batch/Cloud Copy 
        //public string DoBatchCopy(string bucket, string path, string accesskey, string secret, string region, string container, string account, string key, 
         //   bool encrypt, bool delete, bool numfiles, bool list, int agents, int cores, string logfile)
        public string DoBatchCopy(AmazonS3Data s3data, AzureBlobData azblob, S3ToBlobTransferParameters parms)
        {
            if (string.IsNullOrEmpty(azblob.Container))
                return ("No container specified");
            if (string.IsNullOrEmpty(s3data.Path))
                return ("No file(s) specified");
            if (string.IsNullOrEmpty(s3data.AccessKey) || string.IsNullOrEmpty(s3data.Secret) || string.IsNullOrEmpty(s3data.Region))
                return ("AWS parameters not specified");
            if (string.IsNullOrEmpty(azblob.Account) || string.IsNullOrEmpty(azblob.Key))
                return ("Azure parameters not specified (need both storage account and storage key)");


            string baseurl = "http://localhost:8080";
            string azbase = azUrl;

            /* pick the appropriate URL here */ 
            string usingurl = baseurl;
            Program.Logger(_log, _blog, "Using URL = " + usingurl);
            Program.Logger(_log, _blog, "Invoking Azure cloud service to perform copy operation");

            Guid g = Guid.NewGuid();
            parms.UserName = this.username; 
            //int res=Statics.WriteToDocumentDB(g, "INITIALIZED", "").Result;
            int res = Statics.CreateJobRecordInAzureTable(g, "INITIALIZED", parms, "").Result;
            S3ToAz s3toaz=new S3ToAz(g,s3data, azblob, parms);
            string json = JsonConvert.SerializeObject(s3toaz);
            string postdata = '=' + JsonConvert.SerializeObject(s3toaz);

            int r=SendAndWait(usingurl, json).Result; 
            return "Success";
        }

        async private Task<int> SendAndWait(string url, string json)
        {
            string jobid = "";
            using (var client = new HttpClient())
            {
                try
                {
                    HttpResponseMessage response;
                    client.BaseAddress = new Uri(url);
                    client.DefaultRequestHeaders.Accept.Clear();
                    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    response = await client.PostAsJsonAsync("api/data", json);
                    if (response.IsSuccessStatusCode)
                    {
                        jobid = response.Content.ReadAsAsync<string>().Result;
                        Program.Logger(_log, _blog, "Job:" + jobid);
                    }
                    Guid g = Guid.Parse(jobid);
                    string s = "";
                    while(s!="COMPLETED" && s!="FAULTED")
                    {
                        Console.Write(".");
                        Thread.Sleep(1000);
                        s = await Statics.GetJobStatusFromAzureTable(g);
                    }
                    if (s == "COMPLETED")
                        Program.Logger(_log, _blog, "\nJob " + g.ToString() + " completed successfully");
                    else if (s == "FAULTED")
                        Program.Logger(_log, _blog, "\nJob " + g.ToString() + " faulted");
                }
                catch (FormatException fe)
                {
                 Console.WriteLine("Error: job creation failed"+fe.Message); 
                }
                catch (Exception e)
                {
                Console.WriteLine(e.Message);
                }
            }

            return 0;
        }
        #endregion

        #region Local List AWS Files
        public string ListAllAWSFiles(string bucket, string accesskey, string secret, RegionEndpoint awsregion)
        {
            if (accesskey == null || accesskey == "" || secret == null || secret == "" || awsregion == null)
                return ("AWS parameters not specified");

            totalsize = 0;
            String s = String.Format("{0,-16} {1,-16}  {2,-64} ", "Last Mod", "Size", "Name");
            Program.Logger(_log, _blog, s);
            AmazonS3Client client;
            try
            {
                using (client = new Amazon.S3.AmazonS3Client(accesskey,secret,awsregion))
                {
                    /* if bucket not specified list all buckets and their contents */ 
                    if (bucket == "" || bucket==null)
                    {
                        Amazon.S3.Model.ListBucketsResponse r = client.ListBuckets();
                        foreach (Amazon.S3.Model.S3Bucket b in r.Buckets)
                        {
                            Program.Logger(_log, _blog, " Bucket: " + b.BucketName+"\n");
                            S3DirectoryInfo rootDirectory = new S3DirectoryInfo(client, b.BucketName);
                            ListDirectory(rootDirectory);
                        }
                    }
                    /* list a specific bucket */ 
                    else
                    {
                        S3DirectoryInfo rootDirectory = new S3DirectoryInfo(client, bucket);
                        ListDirectory(rootDirectory);
                    }
                }
            }
            catch (Exception e)
            {
                Program.Logger(_log, _blog, "Error: " + e.Message);
            }
            if (totalsize < 104857600)
                Program.Logger(_log, _blog, "Total size is less than 100MB; costs negligible");
            else
                Program.Logger(_log, _blog, "Total size: "+(totalsize/1048576L).ToString()+ "MB Estimated transfer costs: $" + (totalsize * costpergigabyte / 1073741824).ToString("#.##")); 
            return "Complete"; 
        }

        /* recursive function to list a directory, its contents, and subdirectories and their contents */ 
        private void ListDirectory(S3DirectoryInfo s3dir)
        {
            S3FileInfo[] s3files = s3dir.GetFiles();
            for (int i = 0; i < s3files.Length; i++)
            {
                String s = String.Format("{0,16} {1,16}  {2,-64}", s3files[i].LastWriteTime, s3files[i].Length, s3files[i].Name);
                Program.Logger(_log, _blog, s);
                totalsize += (ulong)s3files[i].Length;

            }
            S3DirectoryInfo[] s3subdirs = s3dir.GetDirectories();
            for (int i = 0; i < s3subdirs.Length; i++)
            {
                Program.Logger(_log, _blog," Directory of  "+ s3subdirs[i].FullName+"\n");
                ListDirectory(s3subdirs[i]);
            }
        }
        #endregion 

        #region List Azure Directories and Files (undocumented; for dev purposes only)

        public string ListAllAzureFiles(string container, string account, string key)
        {
#if developmentstorage
            CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
            CloudBlobContainer blobContainer = storageAccount.CreateCloudBlobClient().GetContainerReference("wad-control-container");
#endif
            if (!ValidateAzureParameters(container, account, key))
                return ("Error: unable to complete request");

            StorageCredentials creds = new StorageCredentials(account, key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            if (container == "" || container==null)
            {
                foreach (CloudBlobContainer ct in blobClient.ListContainers())
                {
                    Console.WriteLine(ct.Name);
                    CloudBlobContainer blobContainer = blobClient.GetContainerReference(ct.Name);
                    ListAzureContainer(blobContainer);
                }
            }
            else
            {
                CloudBlobContainer blobContainer = blobClient.GetContainerReference(container);
                ListAzureContainer(blobContainer);
            }

            
            return "Success";
        }
        private void ListAzureContainer(CloudBlobContainer container)
        {
            foreach (IListBlobItem item in container.ListBlobs())
            {
                if (item.GetType() == typeof(CloudBlobDirectory))
                {
                    CloudBlobDirectory directory = (CloudBlobDirectory)item;
                    Program.Logger(_log, _blog, directory.Prefix);
                    ListAzureContainerDirectory(directory); 
                }
                else
                {
                    Program.Logger(_log, _blog, item.Uri.ToString());
                }
            }
        }
        private void ListAzureContainerDirectory(CloudBlobDirectory dir)
        {
            foreach(IListBlobItem item in dir.ListBlobs())
            {
                if (item.GetType() == typeof(CloudBlobDirectory))
                {
                    CloudBlobDirectory directory = (CloudBlobDirectory)item;
                    Program.Logger(_log, _blog,directory.Prefix);
                    ListAzureContainerDirectory(directory);
                }
                else
                {
                    Program.Logger(_log, _blog, item.Uri.ToString());
                }

            }
        }

        public string ListAzureDirectories(string container, string account, string key)
        {
#if developmentstorage
            CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
            CloudBlobContainer blobContainer = storageAccount.CreateCloudBlobClient().GetContainerReference("wad-control-container");
#endif 
            if (!ValidateAzureParameters(container, account, key))
                return ("Error: unable to complete request");

            StorageCredentials      creds = new StorageCredentials(account, key);
            CloudStorageAccount     storageAccount = new CloudStorageAccount(creds, true);
            CloudBlobClient         blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer      blobContainer =blobClient.GetContainerReference(container);
            try
            {
                string blobPrefix = null;
                bool useFlatBlobListing = false;
                var blobs = blobContainer.ListBlobs(blobPrefix, useFlatBlobListing, BlobListingDetails.None);
                var folders = blobs.Where(b => b as CloudBlobDirectory != null).ToList();
                foreach (var folder in folders)
                {
                    Program.Logger(_log, _blog, folder.Uri.ToString());
                }
            }
            catch(Exception e)
            {
                Program.Logger(_log, _blog, "Azure exception: " + e.Message);
                return ("Error: unable to complete request");
            }
            return ("Success"); 
        }

        #endregion 

        #region Parameter Checking
        public bool ValidateAWSParameters(string accesskey, string secret, string region)
        {
            if (string.IsNullOrEmpty(accesskey) || string.IsNullOrEmpty(secret) || string.IsNullOrEmpty(region))
            {
                Console.WriteLine( "AWS parameters not specified");
                return false;
            }
            return true;
        }
        public static bool ValidateAzureParameters(string container, string account, string key)
        {
            if (string.IsNullOrEmpty(account)|| string.IsNullOrEmpty(key))
            {
                Console.WriteLine("Azure parameters not specified (need both storage account and storage key)");
                return false;
            }
            return true; 

        }
        #endregion 
    }
}