using System;
using System.Linq;
using System.Net;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Newtonsoft.Json.Linq;

namespace github_import_function
{
    public static class Import
    {
        [Function("Import")]
        public static async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Function, "POST")] HttpRequestData req,
            FunctionContext executionContext)
        {
            var githubEvent = new GithubEvent(req)
            {
                Payload = JObject.Parse(req.ReadAsString())
            };

            var cosmosClient = new CosmosClient(Environment.GetEnvironmentVariable("CosmosConnectionString"),
                new CosmosClientOptions {
                    SerializerOptions = new CosmosSerializationOptions{
                        PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase
                    }
            });

            await cosmosClient.CreateDatabaseIfNotExistsAsync("github-events");
            var database = cosmosClient.GetDatabase("github-events");
            await database.CreateContainerIfNotExistsAsync(new ContainerProperties {
                Id = "raw-events",
                PartitionKeyPath = "/partitionKey"
            });
            var rawEventsContainer = database.GetContainer("raw-events");
                
            await rawEventsContainer.UpsertItemAsync(githubEvent, new PartitionKey(githubEvent.PartitionKey));

            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Content-Type", "text/plain; charset=utf-8");

            response.WriteString("Welcome to Azure Functions!");

            return response;
        }
    }

    public class GithubEvent
    {
        public string PartitionKey { get; set; }

        [JsonPropertyName("id")]
        public string id { get; set; }
        public string DeliveryId { get; set; }
        public string Type { get; set; }
        public string HookId { get; set; }
        public string InstallationTargetId { get; set; }
        public string InstallationTargetType { get; set; }

        public GithubEvent() {}
        public GithubEvent(HttpRequestData req)
        {
            DeliveryId = req.Headers.GetSingleValue("X-GitHub-Delivery");
            Type = req.Headers.GetSingleValue("X-GitHub-Event");
            HookId = req.Headers.GetSingleValue("X-GitHub-Hook-ID");
            InstallationTargetId = req.Headers.GetSingleValue("X-GitHub-Hook-Installation-Target-ID");
            InstallationTargetType = req.Headers.GetSingleValue("X-GitHub-Hook-Installation-Target-Type");

            PartitionKey = $"{InstallationTargetType}_{InstallationTargetId}";
            id = DeliveryId;
        }

        public JObject Payload { get; set; }
    }

    public static class HeaderExtensions
    {
        public static string GetSingleValue(this HttpHeadersCollection headers, string name)
        {
            return headers.TryGetValues(name, out var vals) ? vals.First() : "";
        }
    }
}
