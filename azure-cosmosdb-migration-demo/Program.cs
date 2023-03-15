using Microsoft.Extensions.Configuration;
//using Microsoft.Extensions.Configuration.Json;
//using static System.Net.Mime.MediaTypeNames;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Cosmos;
using System.Collections;
using System.Diagnostics;
using System.ComponentModel;
using Newtonsoft.Json;
using System.Text;

namespace MigrationDemo
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Cosmos DB Migration demo");
            int ItemsToInsert = 0;
            String ApplicationName = String.Empty;
            String SourceCosmosDBAccountEndpoint = String.Empty;
            String SourceCosmosDBAccountKey = String.Empty;
            String SourceCosmosDBDatabase = String.Empty;
            String SourceCosmosDBContainer1 = String.Empty;
            String SourceCosmosDBContainer2 = String.Empty;
            String SourceCosmosDBContainerPartitionKey = String.Empty;
            String TargetCosmosDBAccountEndpoint = String.Empty;
            String TargetCosmosDBAccountKey = String.Empty;
            String TargetCosmosDBDatabase = String.Empty;
            String TargetCosmosDBContainer1 = String.Empty;
            String TargetCosmosDBContainer1PartitionKey = String.Empty;
            String TargetCosmosDBContainer2 = String.Empty;
            String TargetCosmosDBContainer2PartitionKey = String.Empty;
            int ThroughputValue = 400; //the minimum default value

            Stopwatch stopwatch = new Stopwatch();

            //Reading application settings
            try
            {
                IConfigurationRoot configuration = new ConfigurationBuilder()
                        .AddJsonFile("appSettings.json")
                        .Build();
                ItemsToInsert = Int32.Parse(TryGetAppSetting(configuration, "ItemsToInsert"));
                ApplicationName = TryGetAppSetting(configuration, "ApplicationName");
                SourceCosmosDBAccountEndpoint = TryGetAppSetting(configuration, "SourceCosmosDBAccountEndpoint");
                SourceCosmosDBAccountKey = TryGetAppSetting(configuration, "SourceCosmosDBAccountKey");
                SourceCosmosDBDatabase = TryGetAppSetting(configuration, "SourceCosmosDBDatabase");
                SourceCosmosDBContainer1 = TryGetAppSetting(configuration, "SourceCosmosDBContainer1");
                SourceCosmosDBContainer2 = TryGetAppSetting(configuration, "SourceCosmosDBContainer2");
                SourceCosmosDBContainerPartitionKey = TryGetAppSetting(configuration, "SourceCosmosDBContainerPartitionKey");
                TargetCosmosDBAccountEndpoint = TryGetAppSetting(configuration, "TargetCosmosDBAccountEndpoint");
                TargetCosmosDBAccountKey = TryGetAppSetting(configuration, "TargetCosmosDBAccountKey");
                TargetCosmosDBDatabase = TryGetAppSetting(configuration, "TargetCosmosDBDatabase");
                TargetCosmosDBContainer1 = TryGetAppSetting(configuration, "TargetCosmosDBContainer1");
                TargetCosmosDBContainer2 = TryGetAppSetting(configuration, "TargetCosmosDBContainer2");
                TargetCosmosDBContainer1PartitionKey = TryGetAppSetting(configuration, "TargetCosmosDBContainer1PartitionKey");
                TargetCosmosDBContainer2PartitionKey = TryGetAppSetting(configuration, "TargetCosmosDBContainer2PartitionKey");
                ThroughputValue = (Int32.Parse(TryGetAppSetting(configuration, "ThroughputValue")) < ThroughputValue ? ThroughputValue : Int32.Parse(TryGetAppSetting(configuration, "ThroughputValue")));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, reading application settings, failed: {ex.Message}");
            }

            //(SDKv2) Preparing connections
            ConnectionPolicy connectionPolicy = new ConnectionPolicy
            {
                UserAgentSuffix = ApplicationName,  //UserAgent = Windows/10.0.22621 documentdb-netcore-sdk/2.18.0 MigrationDemo
                ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Gateway //SDKv2 ConnectionMode.Gateway is the default value.
            };
            DocumentClient srcClientv2 = new DocumentClient(new Uri(SourceCosmosDBAccountEndpoint), SourceCosmosDBAccountKey, connectionPolicy);
            //DocumentClient dstClientv2 = new DocumentClient(new Uri(TargetCosmosDBAccountEndpoint), TargetCosmosDBAccountKey, connectionPolicy);

            //(SDKv3) Preparing connections
            CosmosClientOptions clientOptions = new CosmosClientOptions()
            {
                AllowBulkExecution = true,
                ConnectionMode = Microsoft.Azure.Cosmos.ConnectionMode.Direct,    //SDKv3 ConnectionMode.Direct is the default value, you can also choose ConnectionMode.Gateway for your environment.
                ApplicationName = ApplicationName, //UserAgent = cosmos-netstandard-sdk/3.31.2|1|X64|Microsoft Windows 10.0.22621|.NET 7.0.2|N|F 00000001| MigrationDemo
                MaxRetryAttemptsOnRateLimitedRequests = 300,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(15)
            };
            //CosmosClient srcClientv3 = new CosmosClient(SourceCosmosDBAccountEndpoint, SourceCosmosDBAccountKey, clientOptions);
            CosmosClient dstClientv3 = new CosmosClient(TargetCosmosDBAccountEndpoint, TargetCosmosDBAccountKey, clientOptions);

            //(SDKv2) Creating SOURCE Database and Containers
            try
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Connecting to source endpoint '{SourceCosmosDBAccountEndpoint}'");
                srcClientv2.CreateDatabaseIfNotExistsAsync(
                    new Microsoft.Azure.Documents.Database { Id = SourceCosmosDBDatabase }
                    ).Wait();
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Database '{SourceCosmosDBDatabase}' is created");

                DocumentCollection srcFixedCollection = new DocumentCollection();
                srcFixedCollection.Id = SourceCosmosDBContainer1;
                DocumentCollection srcPartitionedCollection = new DocumentCollection();
                srcPartitionedCollection.Id = SourceCosmosDBContainer2;
                srcPartitionedCollection.PartitionKey.Paths.Add(SourceCosmosDBContainerPartitionKey);

                srcFixedCollection = await srcClientv2.CreateDocumentCollectionIfNotExistsAsync(
                        UriFactory.CreateDatabaseUri(SourceCosmosDBDatabase),
                        srcFixedCollection,
                        new Microsoft.Azure.Documents.Client.RequestOptions { OfferThroughput = ThroughputValue }
                        );
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Container '{srcFixedCollection.Id}' is created in Database '{SourceCosmosDBDatabase}'");
                srcPartitionedCollection = await srcClientv2.CreateDocumentCollectionIfNotExistsAsync(
                        UriFactory.CreateDatabaseUri(SourceCosmosDBDatabase),
                        srcPartitionedCollection,
                        new Microsoft.Azure.Documents.Client.RequestOptions { OfferThroughput = ThroughputValue }
                        );
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Container '{srcPartitionedCollection.Id}' with PartitionKey = {SourceCosmosDBContainerPartitionKey}) is created in Database '{SourceCosmosDBDatabase}'");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, (SDKv2) Creating SOURCE Database/Container, failed: {ex.Message}");
            }

            //(SDKv3) Creating TARGET Databases and Containers
            try
            {              
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Connecting to target endpoint '{TargetCosmosDBAccountEndpoint}'");

                DatabaseResponse DatabaseResult = await dstClientv3.CreateDatabaseIfNotExistsAsync(TargetCosmosDBDatabase, ThroughputValue);
                Microsoft.Azure.Cosmos.Database database = dstClientv3.GetDatabase(TargetCosmosDBDatabase);
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Database '{TargetCosmosDBDatabase}' is created with shared throughput {ThroughputValue} RU/s");

                ContainerProperties dstCollection1prop = new ContainerProperties(id: TargetCosmosDBContainer1, partitionKeyPath: TargetCosmosDBContainer1PartitionKey);
                Microsoft.Azure.Cosmos.Container dstPartitionedCollection1 = await database.CreateContainerIfNotExistsAsync(containerProperties: dstCollection1prop);
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Container '{dstPartitionedCollection1.Id}' (with PartitionKey = {TargetCosmosDBContainer1PartitionKey}) is created in Database '{TargetCosmosDBDatabase}' and use database-level throughput {ThroughputValue} RU/s'");

                ContainerProperties dstCollection2prop = new ContainerProperties(id: TargetCosmosDBContainer2, partitionKeyPath: TargetCosmosDBContainer2PartitionKey);
                Microsoft.Azure.Cosmos.Container dstPartitionedCollection2 = await database.CreateContainerIfNotExistsAsync(containerProperties: dstCollection2prop, throughputProperties: ThroughputProperties.CreateManualThroughput(ThroughputValue));
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Container '{dstPartitionedCollection2.Id}' (with PartitionKey = {TargetCosmosDBContainer2PartitionKey}) is created in Database '{TargetCosmosDBDatabase}' with dedicated throughput {ThroughputValue} RU/s'");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, (SDKv3) Creating TARGET Database/Container, failed: {ex.Message}");
            }

            //Generating sample data
            Microsoft.Azure.Cosmos.Database dstDatabase = dstClientv3.GetDatabase(TargetCosmosDBDatabase);
            Microsoft.Azure.Cosmos.Container dstContainer1 = dstDatabase.GetContainer(TargetCosmosDBContainer1);
            Microsoft.Azure.Cosmos.Container dstContainer2 = dstDatabase.GetContainer(TargetCosmosDBContainer2);
            List<Task> tasksInsert = new List<Task>(5);
            List<Item> ItemLists = GetItemsToInsert(ItemsToInsert);
            try
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Demo 1 (SDKv2) - Writing sample data into fixed container");
                #region demo1
                stopwatch.Restart();

                Uri collectionUri1 = UriFactory.CreateDocumentCollectionUri(SourceCosmosDBDatabase, SourceCosmosDBContainer1);
                foreach (Item item in ItemLists)
                {
                    var result = await srcClientv2.CreateDocumentAsync(collectionUri1, item);
                }

                stopwatch.Stop();
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, {ItemsToInsert} items are created into /dbs/{SourceCosmosDBDatabase}/colls/{SourceCosmosDBContainer1} ... in {stopwatch.Elapsed}. " +
                    $"({(ItemsToInsert * 1000.0 / stopwatch.ElapsedMilliseconds).ToString("f2")} items/sec)");
                #endregion

                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Demo 2 (SDKv2) - Writing sample data into partitioned container");
                #region demo2
                stopwatch.Restart();

                Uri collectionUri2 = UriFactory.CreateDocumentCollectionUri(SourceCosmosDBDatabase, SourceCosmosDBContainer2);
                foreach (Item item in ItemLists)
                {
                    var result = await srcClientv2.CreateDocumentAsync(collectionUri2, item);
                }

                stopwatch.Stop();
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, {ItemsToInsert} items are created into /dbs/{SourceCosmosDBDatabase}/colls/{SourceCosmosDBContainer2} ... in {stopwatch.Elapsed}. " +
                    $"({(ItemsToInsert * 1000.0 / stopwatch.ElapsedMilliseconds).ToString("f2")} items/sec)");
                #endregion                

                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Demo 3 (SDKv3) - Writing sample data into partitioned container (with PartitionKey = {TargetCosmosDBContainer1PartitionKey})");
                #region demo3
                stopwatch.Restart();

                foreach (Item item in ItemLists)
                {
                    tasksInsert.Add(dstContainer1.CreateItemAsync<dynamic>(item));
                }
                await Task.WhenAll(tasksInsert);

                stopwatch.Stop();
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, {ItemsToInsert} items are created into /dbs/{TargetCosmosDBDatabase}/colls/{TargetCosmosDBContainer1} ... in {stopwatch.Elapsed}. " +
                    $"({(ItemsToInsert * 1000.0 / stopwatch.ElapsedMilliseconds).ToString("f2")} items/sec)");
                #endregion

                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Demo 4 (SDKv3) - Writing sample data into partitioned container (with PartitionKey = {TargetCosmosDBContainer2PartitionKey})");
                #region demo4
                stopwatch.Restart();

                foreach (Item item in ItemLists)
                {
                    tasksInsert.Add(dstContainer2.CreateItemAsync<dynamic>(item));
                }
                await Task.WhenAll(tasksInsert);

                stopwatch.Stop();
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, {ItemsToInsert} items are created into /dbs/{TargetCosmosDBDatabase}/colls/{TargetCosmosDBContainer2} ... in {stopwatch.Elapsed}. " +
                    $"({(ItemsToInsert * 1000.0 / stopwatch.ElapsedMilliseconds).ToString("f2")} items/sec)");
                #endregion

                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Demo 5 (SDKv3) - Writing sample data into partitioned container with sstream API");
                #region demo5
                stopwatch.Restart();
                foreach (Item item in ItemLists)
                {
                    String tmpstr = JsonConvert.SerializeObject(item);
                    byte[] byteArray = Encoding.UTF8.GetBytes(tmpstr);
                    MemoryStream stream = new MemoryStream(byteArray);

                    tasksInsert.Add(dstContainer1.UpsertItemStreamAsync(stream, new Microsoft.Azure.Cosmos.PartitionKey(item.pk))
                                    .ContinueWith((Task<ResponseMessage> task) =>
                                    {
                                        using (ResponseMessage response = task.Result)
                                        {
                                            if (!response.IsSuccessStatusCode)
                                            {
                                                switch (response.StatusCode.ToString())
                                                {
                                                    case "TooManyRequests":
                                                    case "BadRequest":
                                                    default:
                                                        Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Some operations failed, received status code: {response.StatusCode.ToString()}.");
                                                        break;
                                                }
                                            }
                                        }
                                    }));
                }
                stopwatch.Stop();
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, {ItemsToInsert} items are created into /dbs/{TargetCosmosDBDatabase}/colls/{TargetCosmosDBContainer1} ... in {stopwatch.Elapsed}. " +
                    $"({(ItemsToInsert * 1000.0 / stopwatch.ElapsedMilliseconds).ToString("f2")} items/sec)");
                #endregion
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Generating sample data, failed: {ex.Message}");
            }

            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Cosmos DB Migration demo ... end");

        }

        static String TryGetAppSetting(IConfigurationRoot configuration, String Key)
        {
            String Value = String.Empty;
            try
            {
                Value = configuration[Key] ?? "Key '" + Key + "' is missing";
            } 
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Parse AppSetting Error: {ex.Message}");
            }
            return Value;
        }

        static List<Item> GetItemsToInsert(int ItemCount)
        {
            var SampleTags = new Bogus.Faker<tags>()
                .StrictMode(true)
                .RuleFor(o => o.colors, f => f.Commerce.Color())
                .RuleFor(o => o.material, f => f.Commerce.ProductMaterial());

            var SampleItems = new Bogus.Faker<Item>()
                .StrictMode(false)
                .RuleFor(o => o.id, f => Guid.NewGuid().ToString())
                .RuleFor(o => o.pk, (f, o) => f.Commerce.Product())
                .RuleFor(o => o.product_id, f => f.Commerce.Ean13())
                .RuleFor(o => o.product_name, f => f.Commerce.Product())
                .RuleFor(o => o.product_category, f => f.Commerce.ProductName())
                .RuleFor(o => o.product_quantity, f => f.Random.Int(1, 500))
                .RuleFor(o => o.product_price, f => Math.Round(f.Random.Double(1, 100), 3))
                .RuleFor(o => o.product_tags, f => SampleTags.Generate(f.Random.Number(1, 5)))
                .RuleFor(o => o.sale_department, f => f.Commerce.Department())
                .RuleFor(o => o.user_mail, f => f.Internet.ExampleEmail())
                .RuleFor(o => o.user_name, f => f.Internet.UserName())
                .RuleFor(o => o.user_country, f => f.Address.CountryCode())
                .RuleFor(o => o.user_ip, f => f.Internet.Ip())
                .RuleFor(o => o.user_avatar, f => f.Internet.Avatar().Replace("cloudflare-ipfs.com/ipfs", "example.net"))
                .RuleFor(o => o.user_comments, f => f.Lorem.Text())
                .RuleFor(o => o.user_isvip, f => f.Random.Bool())
                .RuleFor(o => o.user_login_date, f => f.Date.Recent().ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'ffffff'Z'"))
                .RuleFor(o => o.timestamp, f => DateTime.UtcNow.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'ffffff'Z'"))
                .Generate(ItemCount);

            return SampleItems.ToList();
        }
    }
}