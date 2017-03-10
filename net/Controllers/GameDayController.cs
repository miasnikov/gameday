using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace dotnetcore.Controllers
{
    [Route("/")]
    public class GameDayController : Controller
    {
        public struct MessagePart {
            public string Id;
            public int PartNumber;
            public string Data;
        }

        private readonly ILogger _logger;
        private readonly string _API_BASE;
        private readonly string _API_TOKEN;

        static Dictionary<string, Nullable<MessagePart>[]> _messages = new Dictionary<string, Nullable<MessagePart>[]>();

        public GameDayController(ILogger<GameDayController> logger) {
            _logger = logger;
            _API_BASE = Environment.GetEnvironmentVariable("GD_API_BASE");
            if (_API_BASE == null)
                throw new Exception("Missing environment variable GD_API_BASE");
            _API_TOKEN = Environment.GetEnvironmentVariable("GD_API_TOKEN");
            if (_API_TOKEN == null)
                throw new Exception("Missing environment variable GD_API_TOKEN");
        }

        // GET /
        [HttpGet]
        public string Get()
        {
            // return number of messages in local dictonary
            return _messages.Keys.Count.ToString();
        }

        // POST /
        [HttpPost]
        public async Task<string> PostAsync([FromBody]MessagePart part)
        {
            // log
            _logger.LogInformation("Processing message for msg_id={msg_id} with part_number={part_number} and data={data}", part.Id, part.PartNumber, part.Data);
            // store
            if (!_messages.ContainsKey(part.Id))
                _messages.Add(part.Id, new Nullable<MessagePart>[2]);

            Dynamo
            _messages[part.Id][part.PartNumber] = part;
            // check if we have all parts
            if (_messages[part.Id][0].HasValue && _messages[part.Id][1].HasValue)
            {
                // log
                _logger.LogInformation("Have both parts for msg_id={msg_Id}", part.Id);
                // build final message
                string result = _messages[part.Id][0].Value.Data + _messages[part.Id][1].Value.Data;
                _logger.LogDebug("Assembled message: {result}", result);
                // build url
                Uri uri = new Uri(_API_BASE + "/" + part.Id);
                _logger.LogDebug("Making request to {} with payload {}", uri, result);

                // setup client
                using (var client = new HttpClient())
                {
                    // setup content
                    HttpContent content = new StringContent(result);
                    content.Headers.Add("x-gameday-token", _API_TOKEN);
                    // make call
                    var response = await client.PostAsync(uri, content);
                    // log
                    _logger.LogDebug("Response from server: {response}, response");
                }
            }
            return "OK";
        }
    }
}
