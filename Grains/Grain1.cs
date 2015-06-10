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
using System.Threading; 
using System.Threading.Tasks;
using System.Text;
using System.Net;
using System.Net.Security;
using System.Diagnostics; 
 
using Orleans;
using Orleans.Concurrency;
using Orleans.Runtime;

using GrainInterfaces;
using System.IO;
using System.Globalization;
using Globals; 


using Amazon;
using Amazon.S3;
using Amazon.S3.IO;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Diagnostics;


namespace Grains
{
    #region Exceptions
    public class AWSAzureException : Exception
    {
        public AWSAzureException(string message)
        {
        } 
    }
    #endregion 

    #region AWSGrain: Moves blob(s) from AWS to Azure
    /// <summary>
    /// Orleans grain implementation class AWSGrain. This worker grain gets a list of files to move from AWS to Azure.
    /// TODO: Should be marked static 
    /// </summary>
    public class AWSGrain : Orleans.Grain, IAWSGrain 
    {
        /* instance variables added to support block-wise copy */
        private AmazonS3Client              _client;
        private AmazonS3Data                _s3data;
        private List<string>                _files;
        private AzureBlobData               _azblob;
        private S3ToBlobTransferParameters  _parms;
        private StorageCredentials          _azcreds;
        private CloudStorageAccount         _azact;
        private CloudBlobContainer          _blobcontainer;
        private CloudBlobClient             _azclient;
        private CloudBlockBlob              _blockblob; 
        private RegionEndpoint              _awsreg;
        private int                         _fileidx;
        private int                         _curblock;
        Amazon.S3.Model.GetObjectRequest    _amzreq;
        public override Task ActivateAsync()
        {
            return TaskDone.Done;
        }

        public override Task DeactivateAsync()
        {
            return TaskDone.Done;
        }

        public Task BeginMoveFiles(AmazonS3Data s3data, List<string> files, AzureBlobData azblob, S3ToBlobTransferParameters parms)
        {
            _s3data = s3data;
            _files = files;
            _azblob = azblob;
            _parms = parms;
            _azcreds = new StorageCredentials(azblob.Account, azblob.Key);
            _azact = new CloudStorageAccount(_azcreds, true);
            _azclient = _azact.CreateCloudBlobClient();
            _awsreg = Statics.StringToRegion(s3data.Region);
            _client = new AmazonS3Client(s3data.AccessKey, s3data.Secret, _awsreg);

            _fileidx = 0;
            _curblock = 0;
            _amzreq = new Amazon.S3.Model.GetObjectRequest
            {
                BucketName = s3data.Bucket,
                Key = files[0],
                ByteRange = new Amazon.S3.Model.ByteRange(0, parms.BlockSize)
            };
            _blobcontainer = _azclient.GetContainerReference(_azblob.Container);
            int colon = files[0].IndexOf(":");
            string myfile = files[0].Substring(colon + 2); 
            _blockblob = _blobcontainer.GetBlockBlobReference(myfile);

            return TaskDone.Done ; 
        }
        async public Task<long>MoveData()
        {
            var blockId = _curblock++.ToString().PadLeft(10, '0');
            /* are you kidding me? */
            string blockIdBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(blockId.ToString(CultureInfo.InvariantCulture).PadLeft(32, '0')));
            long copied=await Statics.CopyOneBlock(_client, _amzreq, _blockblob, _parms.BlockSize, blockIdBase64);
            if (copied < _parms.BlockSize)
            {
                _fileidx++;
                if (_fileidx >= _files.Count())
                {
                    /* make sure keys get gc'd */ 
                    _s3data = null;
                    _azcreds = null; 
                    _client.Dispose();
                    return 0; /* we're done */
                }
                /* move amz stuff to next object */
                _amzreq.ByteRange.Start = 0;
                _amzreq.Key = _files[_fileidx];
                /* move azure stuff to next blob */
                _curblock = 0;
                int colon = _files[_fileidx].IndexOf(":");
                string myfile = _files[_fileidx].Substring(colon + 2);
                _blockblob = _blobcontainer.GetBlockBlobReference(myfile);
            }
            else
                _amzreq.ByteRange.Start += _parms.BlockSize; 
            return copied;
        }
        async public Task<int> MoveFiles(AmazonS3Data s3data, List<string> files, AzureBlobData azblob, S3ToBlobTransferParameters parms)
        {
            StorageCredentials creds = new StorageCredentials(azblob.Account, azblob.Key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            int count=0;
            try
            {
                RegionEndpoint awsregion = Statics.StringToRegion(s3data.Region);
                using (AmazonS3Client client = new AmazonS3Client(s3data.AccessKey, s3data.Secret, awsregion))
                {
                    foreach (string file in files)
                    {
                        /* take off the bucket name */
                        int colon = file.IndexOf(":");
                        string myfile = file.Substring(colon + 2); 
                        /* 3rd argument to constructor is called 'objectkey' assume that's the file (?) */
                        S3FileInfo fi = new S3FileInfo(client, s3data.Bucket, myfile);
                        await Statics.CopyOneObject(s3data, azblob, parms, client, blobClient, myfile);
                        count++; 
                    }
                }
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }

            return count; 
        }
    }

    #endregion

    #region AWSDispatcher: Gets list of files to move and dispatches to worker grains 
    [Reentrant]
    /* this needs to be reentrant so that we can ask for status while the job is running */ 
    public class AWSDispatcher : Orleans.Grain, IAWSDispatcher
    {

        /* we do this here because the apicontroller is stateless */ 
        private Dictionary<Guid,int> _currentjobs=new Dictionary<Guid, int>(); 
        public override Task ActivateAsync()
        {
            return TaskDone.Done;
        }
        public override Task DeactivateAsync()
        {
            return TaskDone.Done;
        }
        async public Task<int> CopyDispatcher(Guid guid, AmazonS3Data s3data, AzureBlobData azblob, S3ToBlobTransferParameters parms)
        {
            Trace.Write("Starting copy","Information");
            await Statics.UpdateJobStatusInAzureTable(guid, "STARTED");
            StorageCredentials creds = new StorageCredentials(azblob.Account, azblob.Key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            RegionEndpoint awsregion = Statics.StringToRegion(s3data.Region);

            try
            {
                using (AmazonS3Client client = new Amazon.S3.AmazonS3Client(s3data.AccessKey,s3data.Secret, awsregion))
                {
                    S3DirectoryInfo rootDirectory = new S3DirectoryInfo(client, s3data.Bucket);
                    if (s3data.Path != "*.*")
                    {
                        S3FileInfo fileinfo = rootDirectory.GetFile(s3data.Path);
                        if (!fileinfo.Exists)
                        {
                            return 28; //FIX
                        }
                        else
                        {
                            /* move just one file. still use the AWSGrain as it understands blocking, encryption, etc. */ 
                            IAWSGrain iawsgrain = AWSGrainFactory.GetGrain(Guid.NewGuid());
                            List<string> file = new List<string>();
                            file.Add(fileinfo.FullName);
                            /* this is temporary until we get block mode working */ 
                            
                            int ct = await iawsgrain.MoveFiles(s3data, file, azblob, parms);
                            if (ct != 1)
                                return -1; 
                        }
                    }
                    else
                        /* move many files possibly recursively through subdirectories */ 
                        await CopyAWSToAzureRecursive(s3data, rootDirectory, azblob, parms);
                }
                await Statics.UpdateJobStatusInAzureTable(guid, "COMPLETED");
                Trace.WriteLine("AWSDispatcher Completion ", "Information");

                return 0;
            }
            catch(Exception e)
            {
                Statics.UpdateJobStatusInAzureTable(guid, "FAULTED");
                Trace.WriteLine("AWSDispatcher Fault: "+e.Message,"Error");
                Console.WriteLine(e.Message);
                return -1; 
            }
        }
        /* debugging */
        async public Task<int> CopyDispatcherStub(Guid guid, AmazonS3Data s3data, AzureBlobData azblob, S3ToBlobTransferParameters parms, int waittime)
        {
            await Statics.UpdateJobStatusInAzureTable(guid, "STARTED");

            //Thread.Sleep(20000); 

            await Statics.UpdateJobStatusInAzureTable(guid, "COMPLETED"); 
            return 0; 
        }

        #region Main Loop for AWS to Azure Copy (recursion controlled by parms 'recurse' flag)  
        async public Task CopyAWSToAzureRecursive(AmazonS3Data s3data, S3DirectoryInfo s3dir, AzureBlobData azblob, S3ToBlobTransferParameters parms)
        {  

            try
            {
                S3FileInfo[] s3files = s3dir.GetFiles();
                for (int i = 0; i < s3files.Length; i++)
                {
                    /* using a 'random' GUID prevents multiple users from accidentally 'reusing' grains in use by others */ 
                    IAWSGrain iawsgrain = AWSGrainFactory.GetGrain(Guid.NewGuid());
                    List<string> awsfiles = new List<string>();
                    for (int g = 0; g < 10; g++)
                    {
                        if ((i * 10 + g) < s3files.Length)
                            awsfiles.Add(s3files[i * 10 + g].FullName);
                        else
                            break; 
                    }
                    int ct = await iawsgrain.MoveFiles(s3data, awsfiles, azblob, parms);
                }
            }
            catch(Exception e)
            {
                Console.WriteLine("AWS read error: " + e.Message);
            }
            if (parms.Recurse)
            {
                S3DirectoryInfo[] s3subdirs = s3dir.GetDirectories();
                for (int i = 0; i < s3subdirs.Length; i++)
                {
                    await CopyAWSToAzureRecursive(s3data, s3subdirs[i], azblob, parms);
                }
            }
            return; 
        }
        #endregion
 


        #region Single File Copy Functions
        /* these are not currently used but possible we may find a use for them later so leaving 'em in for now */        
        public static byte[] ReadOneAWSObject(S3FileInfo fi)
        {
            byte[] buf = new byte[fi.Length];
            /* NOTE StreamReader can only read <int> length files = 4GB */
            using (Stream sr = fi.OpenRead())
            {
                int ct = sr.Read(buf, 0, (int)fi.Length);
                if(ct!=(int)fi.Length)
                {
                    return null; 
                }
            }
            return buf; 
        }
        public static bool UploadBufferToBlob(byte[] buf, string container, string name, CloudBlobClient blobClient)
        {
            try
            {
                // Retrieve reference to a previously created container.
                CloudBlobContainer blobcontainer = blobClient.GetContainerReference(container);

                // Retrieve reference to a blob.
                CloudBlockBlob blockBlob = blobcontainer.GetBlockBlobReference(name);

                // Create or overwrite the "myblob" blob with contents from a local file.
                using (MemoryStream sr = new MemoryStream(buf))
                {
                    blockBlob.UploadFromStream(sr);
                }
            }
            catch(Exception e)
            {
                Console.WriteLine("Error on upload to Azure " + e.Message);
                return false; 
            }
            return true; 
        }
#endregion 
    }
        #endregion 
}
