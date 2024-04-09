using System;
using Orleans;
using Utilities;
using System.Net;
using Orleans.Hosting;
using Orleans.Configuration;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Logging;
using Concurrency.Implementation.Logging;
using System.IO;
using Concurrency.Interface.GrainPlacement;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;
using NetMQ.Sockets;
using MessagePack;
using NetMQ;
using System.Diagnostics;
using Amazon.DynamoDBv2;
using Amazon;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace SnapperSiloHost
{
    class Program
    {
        static bool isLocalTest;
        static bool isGlobalSilo;
        static int numLocalSilo;
        static int siloID;
        static ImplementationType implementationType;
        static bool isLoggingEnabled;
        static string controller_PublicIPAddress;
        static string redis_ConnectionString;

        static int siloPort = 11111;      // silo-to-silo endpoint
        static int gatewayPort = 30000;   // client-to-silo endpoint

        static PushSocket pushSocket;
        static SubscriberSocket subscribeSocket;

        static int Main(string[] args)
        {
            if (args.Length == 0)
            {
                isLocalTest = true;
                isGlobalSilo = false;
                numLocalSilo = 1;
                siloID = 0;
                implementationType = ImplementationType.SNAPPER;
                isLoggingEnabled = true;
                controller_PublicIPAddress = "localhost";
                redis_ConnectionString = "localhost:6379";
            }
            else
            {
                isLocalTest = bool.Parse(args[0]);
                isGlobalSilo = bool.Parse(args[1]);
                numLocalSilo = int.Parse(args[2]);
                siloID = int.Parse(args[3]);
                implementationType = Enum.Parse<ImplementationType>(args[4]);
                isLoggingEnabled = bool.Parse(args[5]);
                controller_PublicIPAddress = args[6];
                redis_ConnectionString = args[7];
            }

            if (isLocalTest && isGlobalSilo == false)
            {
                siloPort += 1 + siloID;
                gatewayPort += 1 + siloID;
            }

            return RunMainAsync().Result;
        }

        static async Task<int> RunMainAsync()
        {
            ConnectController();

            // =========================================================================================================================
            var builder = new SiloHostBuilder();

            string ServiceRegion;
            string AccessKey;
            string SecretKey;

            using (var file = new StreamReader(Constants.credentialFile))
            {
                ServiceRegion = file.ReadLine();
                Debug.Assert(ServiceRegion == "eu-north-1");
                AccessKey = file.ReadLine();
                SecretKey = file.ReadLine();
            }

            // start the silo
            Action<DynamoDBClusteringOptions> dynamoDBOptions = options =>
            {
                options.AccessKey = AccessKey;
                options.SecretKey = SecretKey;
                options.TableName = Constants.SiloMembershipTable;
                options.Service = ServiceRegion;
                options.WriteCapacityUnits = 10;
                options.ReadCapacityUnits = 10;
            };

            builder
                .UseDynamoDBClustering(dynamoDBOptions)
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Parse(Helper.GetLocalIPAddress()));

            builder
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Constants.ClusterID;
                    options.ServiceId = Constants.ServiceID;
                })
                .Configure<EndpointOptions>(options =>
                {
                    options.SiloPort = siloPort;
                    options.GatewayPort = gatewayPort;
                })
                .ConfigureServices(ConfigureServices)
                .AddPlacementDirector<SnapperGrainPlacementStrategy, SnapperGrainPlacement>()
                .AddRedisGrainDirectory(Constants.GrainDirectoryName, options =>
                {
                    options.ConfigurationOptions = new ConfigurationOptions
                    {
                        EndPoints = { redis_ConnectionString },
                        DefaultDatabase = Constants.Redis_GrainDirectory
                    };
                })
                .UseDashboard(options => { });
                //.ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));

            if (implementationType == ImplementationType.ORLEANSTXN)
            {
                try
                {
                    builder.UseTransactions();

                    if (isLoggingEnabled == false)
                        builder.AddMemoryTransactionalStateStorageAsDefault(opts => { opts.InitStage = ServiceLifecycleStage.ApplicationServices; });
                    else
                    {
                        builder.AddFileTransactionalStateStorageAsDefault(opts => { opts.InitStage = ServiceLifecycleStage.ApplicationServices; opts.siloID = siloID; });

                        //builder
                        //.Configure<TransactionalStateOptions>(o => o.LockTimeout = TimeSpan.FromMilliseconds(200))
                        //.Configure<TransactionalStateOptions>(o => o.LockAcquireTimeout = TimeSpan.FromMilliseconds(200));
                        //.Configure<TransactionalStateOptions>(o => o.PrepareTimeout = TimeSpan.FromSeconds(20));
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"{e.Message} {e.StackTrace}");
                    throw;
                }
            }
            else builder.AddMemoryGrainStorageAsDefault();

            var siloHost = builder.Build();
            await siloHost.StartAsync();
            Console.WriteLine($"Silo: isGlobalSilo {isGlobalSilo}, is started, siloPort = {siloPort}, gatewayPort = {gatewayPort}");

            // =========================================================================================================================
            // get SiloAddress by reading the dynamoDB table
            var attributesToGet = new List<string> { "SiloIdentity", "SiloStatus" };
            var dynamoDBClient = new AmazonDynamoDBClient(AccessKey, SecretKey, RegionEndpoint.EUNorth1);
            var items = (await dynamoDBClient.ScanAsync(Constants.SiloMembershipTable, attributesToGet)).Items;
            dynamoDBClient.Dispose();
            Debug.Assert(items.Count != 0);

            Console.WriteLine($"Silo: isGlobalSilo {isGlobalSilo}, retrieve siloAddress from DynamoDB");
            string siloAddress = "";
            var myPrivateIP = Helper.GetLocalIPAddress();
            foreach (var item in items)
            {
                var silo = item["SiloIdentity"].S;
                var status = item["SiloStatus"].N;
                if (status != "3") continue;

                // RuntimeIdentity format: S127.0.0.1:11112:396016232
                // SiloIdentity format:     127.0.0.1-11112-396016232
                if (silo.Contains(myPrivateIP) && silo.Contains(siloPort.ToString()))
                {
                    siloAddress = "S" + silo.Replace('-', ':');
                    break;
                }
            }
            Debug.Assert(siloAddress.Length != 0);

            Console.WriteLine($"Silo: isGlobalSilo {isGlobalSilo}, write silo info to Redis");
            var redis = ConnectionMultiplexer.Connect(new ConfigurationOptions { EndPoints = { redis_ConnectionString }});
            var siloInfo_db = redis.GetDatabase(Constants.Redis_SiloInfo);
            if (isGlobalSilo) await siloInfo_db.StringSetAsync(Constants.GlobalSilo, siloAddress);
            else
            {
                siloInfo_db.HashSet(Constants.GeneralInfoPrefix + siloAddress, "isLocalConfigGrainActivated", false);
                siloInfo_db.ListRightPush("LocalSiloList", siloAddress);
            }

            // =========================================================================================================================
            // Set up processor affinity
            if (isLocalTest == false)
            {
                if (isGlobalSilo) Helper.SetCPU("SnapperSiloHost", Helper.GetNumCPUForGlobalSilo(numLocalSilo));
                else Helper.SetCPU("SnapperSiloHost", Constants.numCPUPerLocalSilo);
            }

            // =========================================================================================================================
            WaitToTerminate();
            await siloHost.StopAsync();

            Console.WriteLine($"Silo: isGlobalSilo {isGlobalSilo}, terminated, send ACK to controller");
            pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.ACK)));
            return 0;
        }

        static void ConnectController()
        {
            Console.WriteLine($"Silo: isGlobalSilo {isGlobalSilo}, try connect controller...");
            string prefix;
            if (isLocalTest) prefix = ">tcp://localhost:";
            else prefix = ">tcp://" + controller_PublicIPAddress + ":";

            var pushSocketAddress = prefix + Constants.controllerFromSilo_PullPort;
            var subscribeSocketAddress = prefix + Constants.controllerToSilo_PublishPort;
            pushSocket = new PushSocket(pushSocketAddress);
            subscribeSocket = new SubscriberSocket(subscribeSocketAddress);

            NetMsgType theMsg;
            if (isGlobalSilo) theMsg = NetMsgType.START_GLOBAL_SILO;
            else theMsg = NetMsgType.START_LOCAL_SILO;

            Console.WriteLine($"Silo: isGlobalSilo {isGlobalSilo}, try subscribe {theMsg} message");
            subscribeSocket.Subscribe($"{theMsg}");
            Console.WriteLine($"Silo: isGlobalSilo {isGlobalSilo}, connect to controller");
            pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.CONNECT)));

            Console.WriteLine($"Silo: isGlobalSilo {isGlobalSilo}, wait for controller's command");
            subscribeSocket.ReceiveFrameString();    // this is the topic name
            var msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
            Trace.Assert(msg.msgType == theMsg);
            subscribeSocket.Unsubscribe($"{theMsg}");
        }

        static void WaitToTerminate()
        {
            subscribeSocket.Subscribe("TERMINATE");
            Console.WriteLine($"Silo: isGlobalSilo {isGlobalSilo}, it is registered, send ACK to controller");
            pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.ACK)));

            Console.WriteLine($"Silo: isGlobalSilo {isGlobalSilo}, wait for terminate message");
            subscribeSocket.ReceiveFrameString();    // this is the topic name
            var msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
            Trace.Assert(msg.msgType == NetMsgType.TERMINATE);
        }

        static void ConfigureServices(IServiceCollection services)
        {
            // all the singletons have one instance per silo host
            // dependency injection <TService, TImplementation>
            services.AddSingleton<IGrainPlacementCache, GrainPlacementCache>();

            var redis = ConnectionMultiplexer.Connect(new ConfigurationOptions { EndPoints = { redis_ConnectionString } });
            services.AddSingleton<IConnectionMultiplexer>(redis);

            var redisServer = redis.GetServer(redis_ConnectionString);
            services.AddSingleton(redisServer);

            if (implementationType == ImplementationType.SNAPPER) services.AddSingleton<ILoggingProtocol, LoggingProtocol>();
            else if (implementationType == ImplementationType.ORLEANSTXN) services.AddSingleton<ISnapperLogger, SnapperLogger>();
        }
    }
}