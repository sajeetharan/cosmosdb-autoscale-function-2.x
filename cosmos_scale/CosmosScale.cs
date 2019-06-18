using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.Documents;
using Newtonsoft.Json.Linq;
using System.Linq;
using System.Net.Http;
using System.Net;

namespace cosmos_scale
{
    public static class Cosmosscale
    {
        [FunctionName("Cosmosscale")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log,
            ExecutionContext context)
        {

            try
            {
                var config = new ConfigurationBuilder().SetBasePath(context.FunctionAppDirectory)
                    .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                    .AddEnvironmentVariables()
                    .Build();

                //1) initialize the cosmosdb client
                using (DocumentClient client = new DocumentClient(new Uri(config["CosmosDB_Uri"]), config["CosmosDB_appKey"]))
                {
                    //2) Get the database self link
                    string selfLink = client.CreateDocumentCollectionQuery(
                                        UriFactory.CreateDatabaseUri(config["CosmosDB_DatabaseId"]))
                                            .Where(c => c.Id == config["CosmosDB_ContainerId"])
                                            .AsEnumerable()
                                            .FirstOrDefault()
                                            .SelfLink;

                    //3) Get the current offer for the collection
                    Offer offer = client.CreateOfferQuery().Where(r => r.ResourceLink == selfLink).AsEnumerable().SingleOrDefault();

                    //4) Get the current throughput from the offer
                    int throughputCurrent = (int)offer.GetPropertyValue<JObject>("content").GetValue("offerThroughput");
                    log.LogInformation(string.Format("Current provisioned throughput is: {0} RU", throughputCurrent.ToString()));

                    //5) Get the RU increment from AppSettings and parse to an int
                    if (int.TryParse(config["CosmosDB_RU"], out int RUIncrement))
                    {
                        //5.a) create the new offer with the throughput increment added to the current throughput
                        int newThroughput = throughputCurrent + RUIncrement;
                        offer = new OfferV2(offer, newThroughput);

                        //5.b) persist the changes
                        await client.ReplaceOfferAsync(offer);
                        log.LogInformation(string.Format("New provisioned througput: {0} RU", newThroughput.ToString()));
                        return new OkObjectResult("The collection's throughput was changed...");
                    }
                    else
                    {
                        //5.c) if the throughputIncrement cannot be parsed return throughput not changed
                        return new BadRequestObjectResult("PARSE ERROR: The collection's throughput was not changed...");
                    }
                }
            }
            catch (Exception e)
            {
                log.LogInformation(e.Message);
                return new BadRequestObjectResult("ERROR: The collection's throughput was not changed...");
            }
        }
    }
}





