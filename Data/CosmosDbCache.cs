// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

namespace SharingService.Data
{
 
    internal class AnchorCacheEntity : TableEntity
    {
        public AnchorCacheEntity() { }

        public AnchorCacheEntity(long anchorId, int partitionSize)
        {
            this.PartitionKey = (anchorId / partitionSize).ToString();
            this.RowKey = anchorId.ToString();
            
        }

        public string AnchorKey { get; set; }
        public string Authorable {  get; set; }
    }


    internal class CosmosDbCache : IAnchorKeyCache
    {
        /// <summary>
        /// Super basic partitioning scheme
        /// </summary>
        private const int partitionSize = 500;

        /// <summary>
        /// The database cache.
        /// </summary>
        private readonly CloudTable dbCache;

        /// <summary>
        /// The anchor numbering index.
        /// </summary>
        private long lastAnchorNumberIndex = -1;

        // To ensure our asynchronous initialization code is only ever invoked once, we employ two manualResetEvents
        ManualResetEventSlim initialized = new ManualResetEventSlim();
        ManualResetEventSlim initializing = new ManualResetEventSlim();

        private async Task InitializeAsync()
        {
            if (!this.initialized.Wait(0))
            {
                if (!this.initializing.Wait(0))
                {
                    this.initializing.Set();
                    await this.dbCache.CreateIfNotExistsAsync();
                    this.initialized.Set();
                }

                this.initialized.Wait();
            }
        }

        public CosmosDbCache(string storageConnectionString)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            this.dbCache = tableClient.GetTableReference("AnchorCache");
        }

        /// <summary>
        /// Determines whether the cache contains the specified anchor identifier.
        /// </summary>
        /// <param name="anchorId">The anchor identifier.</param>
        /// <returns>A <see cref="Task{System.Boolean}" /> containing true if the identifier is found; otherwise false.</returns>
        public async Task<bool> ContainsAsync(long anchorId)
        {
            await this.InitializeAsync();

            TableResult result = await this.dbCache.ExecuteAsync(TableOperation.Retrieve<AnchorCacheEntity>((anchorId / CosmosDbCache.partitionSize).ToString(), anchorId.ToString()));
            AnchorCacheEntity anchorEntity = result.Result as AnchorCacheEntity;
            return anchorEntity != null;
        }

        /// <summary>
        /// Gets the anchor key asynchronously.
        /// </summary>
        /// <param name="anchorId">The anchor identifier.</param>
        /// <exception cref="KeyNotFoundException"></exception>
        /// <returns>The anchor key.</returns>
        public async Task<string> GetAnchorKeyAsync(long anchorId)
        {
            await this.InitializeAsync();

            TableResult result = await this.dbCache.ExecuteAsync(TableOperation.Retrieve<AnchorCacheEntity>((anchorId / CosmosDbCache.partitionSize).ToString(), anchorId.ToString()));
            AnchorCacheEntity anchorEntity = result.Result as AnchorCacheEntity;
            if (anchorEntity != null)
            {
                return anchorEntity.AnchorKey;
            }

            throw new KeyNotFoundException($"The {nameof(anchorId)} {anchorId} could not be found.");
        }

        /// <summary>
        /// Gets the last anchor asynchronously.
        /// </summary>
        /// <returns>The anchor.</returns>
        public async Task<AnchorCacheEntity> GetLastAnchorAsync()
        {
            await this.InitializeAsync();

            List<AnchorCacheEntity> results = new List<AnchorCacheEntity>();
            TableQuery<AnchorCacheEntity> tableQuery = new TableQuery<AnchorCacheEntity>();
            TableQuerySegment<AnchorCacheEntity> previousSegment = null;
            while (previousSegment == null || previousSegment.ContinuationToken != null)
            {
                TableQuerySegment<AnchorCacheEntity> currentSegment = await this.dbCache.ExecuteQuerySegmentedAsync<AnchorCacheEntity>(tableQuery, previousSegment?.ContinuationToken);
                previousSegment = currentSegment;
                results.AddRange(previousSegment.Results);
            }

            return results.OrderByDescending(x => x.Timestamp).DefaultIfEmpty(null).First();
        }

        /// <summary>
        /// Gets the last anchor key asynchronously.
        /// </summary>
        /// <returns>The anchor key.</returns>
        public async Task<string> GetLastAnchorKeyAsync()
        {
            return (await this.GetLastAnchorAsync())?.AnchorKey;
        }

        public async Task<string> GetAllAnchorKeysAsStringAsync()
        {
            await this.InitializeAsync();

            List<AnchorCacheEntity> results = new List<AnchorCacheEntity>();
            TableQuery<AnchorCacheEntity> tableQuery = new TableQuery<AnchorCacheEntity>();
            TableQuerySegment<AnchorCacheEntity> previousSegment = null;

            while (previousSegment == null || previousSegment.ContinuationToken != null)
            {
                TableQuerySegment<AnchorCacheEntity> currentSegment = await this.dbCache.ExecuteQuerySegmentedAsync<AnchorCacheEntity>(tableQuery, previousSegment?.ContinuationToken);
                previousSegment = currentSegment;
                results.AddRange(previousSegment.Results);
            }

            string keys = "0";// new string[results.Count];
            int n = 0;
            foreach (var result in results)
            {
                keys = keys + "," + result.AnchorKey;
            }

            return keys;
        }

        public async Task<string[]> GetAllAnchorKeysAsync()
        {
            await this.InitializeAsync();

            List<AnchorCacheEntity> results = new List<AnchorCacheEntity>();
            TableQuery<AnchorCacheEntity> tableQuery = new TableQuery<AnchorCacheEntity>();
            TableQuerySegment<AnchorCacheEntity> previousSegment = null;

            while (previousSegment == null || previousSegment.ContinuationToken != null)
            {
                TableQuerySegment<AnchorCacheEntity> currentSegment = await this.dbCache.ExecuteQuerySegmentedAsync<AnchorCacheEntity>(tableQuery, previousSegment?.ContinuationToken);
                previousSegment = currentSegment;
                results.AddRange(previousSegment.Results);
            }

            string[] keys = new string[results.Count];
            int n = 0;
            foreach (var result in results)
            {
                keys[n++] = result.AnchorKey;
            }

            return keys;
        }

        /// <summary>
        /// Sets the anchor key asynchronously.
        /// </summary>
        /// <param name="anchorKey">The anchor key.</param>
        /// <returns>An <see cref="Task{System.Int64}" /> representing the anchor identifier.</returns>
        public async Task<long> SetAnchorKeyAsync(string anchorKey)
        {
            await this.InitializeAsync();

            if (lastAnchorNumberIndex == long.MaxValue)
            {
                // Reset the anchor number index.
                lastAnchorNumberIndex = -1;
            }

            if(lastAnchorNumberIndex < 0)
            {
                // Query last row key
                var rowKey = (await this.GetLastAnchorAsync())?.RowKey;
                long.TryParse(rowKey, out lastAnchorNumberIndex);
            }

            long newAnchorNumberIndex = ++lastAnchorNumberIndex;

            AnchorCacheEntity anchorEntity = new AnchorCacheEntity(newAnchorNumberIndex, CosmosDbCache.partitionSize)
            {
                AnchorKey = anchorKey,
                PartitionKey = "0:" + newAnchorNumberIndex
            };

            await this.dbCache.ExecuteAsync(TableOperation.Insert(anchorEntity));

            return newAnchorNumberIndex;
        }


        public async Task<long> SetAnchorKeyRegistrationAsync(string anchorKey, string objectName)
        {
            await this.InitializeAsync();

            if (lastAnchorNumberIndex == long.MaxValue)
            {
                // Reset the anchor number index.
                lastAnchorNumberIndex = -1;
            }

            if (lastAnchorNumberIndex < 0)
            {
                // Query last row key
                var rowKey = (await this.GetLastAnchorAsync())?.RowKey;
                long.TryParse(rowKey, out lastAnchorNumberIndex);
            }

            long newAnchorNumberIndex = ++lastAnchorNumberIndex;

            AnchorCacheEntity anchorEntity = new AnchorCacheEntity(newAnchorNumberIndex, CosmosDbCache.partitionSize)
            {
                AnchorKey = anchorKey,
                Authorable = objectName,
                PartitionKey = "1:" + newAnchorNumberIndex + ":" + objectName
            };

            await this.dbCache.ExecuteAsync(TableOperation.Insert(anchorEntity));

            return newAnchorNumberIndex;
        }
        public async Task<bool> DeleteAnchorKeyAsync(string anchorKey)
        {
            await this.InitializeAsync();


            bool success = false;
            var query = new TableQuery<AnchorCacheEntity>().Where(TableQuery.GenerateFilterCondition("AnchorKey", QueryComparisons.Equal, anchorKey));
            var segment = await dbCache.ExecuteQuerySegmentedAsync(query, null);
            var myEntities = segment.Results;

            if (myEntities.Count > 0)
            {
                foreach (var entity in myEntities)
                {
                    try
                    {
                        await dbCache.ExecuteAsync(TableOperation.Delete(entity));
                        success = true;
                    }catch(System.Exception ex)
                    {
                        success = false;
                       // throw (ex);   
                        
                    }
                }
            }

            return success;
        }

        public async Task<bool> DeleteAllAnchorKeysAsync()
        {
            await this.InitializeAsync();

            bool success = false;
            var query = new TableQuery<AnchorCacheEntity>().Where(TableQuery.GenerateFilterCondition("AnchorKey", QueryComparisons.NotEqual, "0"));
           
            var segment = await dbCache.ExecuteQuerySegmentedAsync(query, null);
            var myEntities = segment.Results;

            if (myEntities.Count > 0)
            {
                foreach (var entity in myEntities)
                {
                    try
                    {
                        await dbCache.ExecuteAsync(TableOperation.Delete(entity));
                        success = true;

                    }catch(System.Exception ex)
                    {
                        success= false;
                        break;
                    }
                }
            }


            return success;
        }

    }
}