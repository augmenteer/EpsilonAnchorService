// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
using Microsoft.AspNetCore.Mvc;
using SharingService.Data;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace SharingService.Controllers
{
    [Route("api/anchors")]
    [ApiController]
    public class AnchorsController : ControllerBase
    {
        private readonly IAnchorKeyCache anchorKeyCache;

        /// <summary>
        /// Initializes a new instance of the <see cref="AnchorsController"/> class.
        /// </summary>
        /// <param name="anchorKeyCache">The anchor key cache.</param>
        public AnchorsController(IAnchorKeyCache anchorKeyCache)
        {
            this.anchorKeyCache = anchorKeyCache;
        }

        // GET api/anchors/5
        [HttpGet("{anchorNumber}")]
        public async Task<ActionResult<string>> GetAsync(long anchorNumber)
        {
            // Get the key if present
            try
            {
                return await this.anchorKeyCache.GetAnchorKeyAsync(anchorNumber);
            }
            catch(KeyNotFoundException)
            {
                return this.NotFound();
            }
        }

        // GET api/anchors/last
        [HttpGet("last")]
        public async Task<ActionResult<string>> GetAsync()
        {
            // Get the last anchor
            string anchorKey = await this.anchorKeyCache.GetLastAnchorKeyAsync();

            if (anchorKey == null)
            {
                return "";
            }

            return anchorKey;
        }

        // GET api/anchors/all
        [HttpGet("all")]
        public async Task<ActionResult<string[]>> GetAllAsync()
        {
            // Get all keys
            string[] anchorKeys = await this.anchorKeyCache.GetAllAnchorKeysAsync();


            return anchorKeys;
        }

        // GET api/anchors/all_as_string
        [HttpGet("all_as_string")]
        public async Task<ActionResult<string>> GetAllAsStringAsync()
        {
            // Get all keys as a string, not json
            string anchorKeys = await this.anchorKeyCache.GetAllAnchorKeysAsStringAsync();


            return anchorKeys;
        }
        // GET api/anchors/delete_all
        [HttpGet("delete_all")]
        public async Task<ActionResult<bool>> DeleteAllAsync()
        {
            bool success = await this.anchorKeyCache.DeleteAllAnchorKeysAsync();

            return success;
        }

        // GET api/anchors/delete
        [HttpGet("delete")]
        public async Task<ActionResult<bool>> DeleteAsync(string anchorKey)
        {
            bool success = await this.anchorKeyCache.DeleteAnchorKeyAsync(anchorKey);

            return success;
        }

        // POST api/anchors
        [HttpPost]
        public async Task<ActionResult<long>> PostAsync()
        {
            string anchorKey;
            using (StreamReader reader = new StreamReader(this.Request.Body, Encoding.UTF8))
            {
                anchorKey = await reader.ReadToEndAsync();
            }

            if (string.IsNullOrWhiteSpace(anchorKey))
            {
                return this.BadRequest();
            }

            // Set the key and return the anchor number
            return await this.anchorKeyCache.SetAnchorKeyAsync(anchorKey);
        }

        // POST api/anchors/key
        [HttpPost("key")]
        public async Task<ActionResult<long>> PostKeyAsync(string anchorKey)
        {
            if (string.IsNullOrWhiteSpace(anchorKey))
            {
                return this.BadRequest();
            }

            // Set the key and return the anchor number
            return await this.anchorKeyCache.SetAnchorKeyAsync(anchorKey);
        }

        [HttpPost("registered_key")]
        public async Task<ActionResult<long>> PostRegisteredKeyAsync(string anchorKey, string objectName)
        {
            if (string.IsNullOrWhiteSpace(anchorKey))
            {
                return this.BadRequest();
            }

            return await this.anchorKeyCache.SetAnchorKeyRegistrationAsync(anchorKey, objectName);
        }
    }
}
