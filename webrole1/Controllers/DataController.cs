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
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web;           /* for HttpContext */ 
using System.Threading.Tasks;
using System.IO;
using Orleans;
using Orleans.Host;
using GrainInterfaces; 

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Blob;

using Globals; 

using Amazon;
using Amazon.S3;
using Amazon.S3.IO;

using Newtonsoft.Json;

namespace WebRole2.Controllers
{
    public class DataController : ApiController
    {
        // Get all data in the Azure Table; used by monitoring dashboard 
        public async Task<List<TblOperationStatusRecord>> Get(int tbldata)
        {

            if (tbldata >= 0 && tbldata <= 3)
            {
                List<TblOperationStatusRecord> recs = await Statics.GetAllStatus(tbldata);
                return recs;
            }
            else
                return null;
        }
        // GET api/Data?taskstatus=string 
        // deprecated and not used 
        public async Task<int> Get(string taskstatus)
        {
            return -1; 
        }

        // POST: api/Data
        async public Task<string> Post([FromBody]string value)
        {
            try
            {
                if (!OrleansAzureClient.IsInitialized)
                {
                    FileInfo f = new FileInfo(HttpContext.Current.Server.MapPath(@"~/ClientConfiguration.xml"));
                    OrleansAzureClient.Initialize(f);
                    OrleansClient.SetResponseTimeout(new TimeSpan(3, 0, 0)); /* 3 hours should really do it */
                }
            }
            catch (Exception exc)
            {
                Console.WriteLine("Could not init Orleans " + exc.Message);
                return "Error: could not initialize Orleans"; 
            }
            IAWSDispatcher dispatch = AWSDispatcherFactory.GetGrain(0);
            S3ToAz s3toaz = JsonConvert.DeserializeObject<S3ToAz>(value);
            try
            {
                /* do not await this call */ 
                dispatch.CopyDispatcher(s3toaz._guid, s3toaz._s3data, s3toaz._azdata, s3toaz._s3azparms);      
            }
            catch(Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                return e.Message; 
            }
            return s3toaz._guid.ToString();
        }

        // PUT: api/Data/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE: api/Data/5
        public void Delete(int id)
        {
        }
        /* cheesy way of deserializing json: handy for debugging */ 
        private static string JValFromKey(string text, string key)
        {
            if (string.IsNullOrEmpty(text)) 
                return null;
            string val = null;
            int idx = text.IndexOf(key);
            if (idx == -1)
                return null; 
            idx = text.IndexOf(':', idx);
            idx+=2; /* quotes */
            while(text[idx]!='\"')
                val+=text[idx++];
            return val; 

        }
    }
}
