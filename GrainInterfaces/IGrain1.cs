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
using System.Threading.Tasks;
using System.Text;
using Orleans;
using Globals; 

using Amazon;
using Amazon.S3;
using Amazon.S3.IO;


namespace GrainInterfaces
{
    /// <summary>
    /// Orleans grain communication interface IGrain1
    /// </summary>
    public interface IAWSGrain : Orleans.IGrain
    {
        Task<int> MoveFiles(AmazonS3Data s3data, List<string> files, AzureBlobData azblob, S3ToBlobTransferParameters parms);
    }

    public interface IAWSDispatcher: Orleans.IGrain
    {

        Task<int> CopyDispatcher(Guid guid, AmazonS3Data s3data, AzureBlobData azblob, S3ToBlobTransferParameters parms);
        Task<int> CopyDispatcherStub(Guid guid, AmazonS3Data s3data, AzureBlobData azblob, S3ToBlobTransferParameters parms, int waittime);
    }
}
