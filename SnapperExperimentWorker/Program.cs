using NetMQ;
using System;
using Orleans;
using Utilities;
using NetMQ.Sockets;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using MessagePack;
using StackExchange.Redis;
using System.Linq;

namespace SnapperExperimentWorker
{
    class Program
    {
        static bool isLocalTest;
        static int numLocalSilo;
        static int workerID = -1;
        static ImplementationType implementationType;
        static string controller_PublicIPAddress;
        static string redis_ConnectionString;

        static PushSocket pushSocket;
        static SubscriberSocket subscribeSocket;
        static int maxNumExperiment;
        static CountdownEvent[] threadAcks;
        static WorkloadConfiguration workload;
        static CountdownEvent initializationDone;

        static Thread producerThread;
        static Thread[] consumerThreads;         // used to submit transaction requests
        static Thread grainMigrationThread;      // used to submit grain migration requests
        static bool[] isMigrationDone;
        static Barrier[] barriers;
        static IClusterClient[] clients;
        static IBenchmark[] benchmarks;
        static bool[] isEpochFinish;
        static bool[] isProducerFinish;
        static Dictionary<int, Queue<Tuple<bool, RequestData>>> shared_requests;                // <epoch, <isDet, grainIDs>>
        static Dictionary<int, Dictionary<int, ConcurrentQueue<RequestData>>> thread_requests;  // <epoch, <consumerID, grainIDs>>

        static WorkloadGenerator workloadGenerator;
        static WorkloadResult[] results;

        static List<string> registeredSilo;
        static Dictionary<string, List<Guid>> migrationWorkersPerSilo;

        static void Main(string[] args)
        {
            isLocalTest = bool.Parse(args[0]);
            numLocalSilo = int.Parse(args[1]);
            workerID = int.Parse(args[2]);
            implementationType = Enum.Parse<ImplementationType>(args[3]);
            controller_PublicIPAddress = args[4];
            redis_ConnectionString = args[5];

            // =========================================================================================================================
            // Set up processor affinity
            if (isLocalTest == false) Helper.SetCPU("SnapperExperimentWorker", Constants.numCPUPerLocalSilo);

            // =========================================================================================================================
            // build connection with Experiment Controller
            ConnectController();

            // =========================================================================================================================
            // run experiment one by one
            for (int i = 0; i <= maxNumExperiment; i++)
            {
                var isContinue = ProcessWork(i);
                if (isContinue == false) break;
            } 
        }

        static void ConnectController()
        {
            string prefix;
            if (isLocalTest) prefix = ">tcp://localhost:";
            else prefix = ">tcp://" + controller_PublicIPAddress + ":";

            var pushSocketAddress = prefix + Constants.controllerFromWorker_PullPort;
            var subscribeSocketAddress = prefix + Constants.controllerToWorker_PublishPort;

            pushSocket = new PushSocket(pushSocketAddress);
            subscribeSocket = new SubscriberSocket(subscribeSocketAddress);

            Console.WriteLine("ExpWorker connect to ExpController... ");
            subscribeSocket.Subscribe("WORKER_ID");
            var myPublicIP = Helper.GetPublicIPAddress();
            var msg = new NetworkMessage(NetMsgType.CONNECT, MessagePackSerializer.Serialize(myPublicIP));
            pushSocket.SendFrame(MessagePackSerializer.Serialize(msg));
            
            Console.WriteLine("ExpWorker: wait for controller's message");
            subscribeSocket.ReceiveFrameString();
            msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
            Trace.Assert(msg.msgType == NetMsgType.WORKER_ID);
            var content = MessagePackSerializer.Deserialize<Dictionary<string, int>>(msg.content);
            if (isLocalTest == false)
            {
                Debug.Assert(content.ContainsKey(myPublicIP));
                workerID = content[myPublicIP];
            } 
            else Debug.Assert(workerID != -1);
            Console.WriteLine($"ExpWorker get workerID = {workerID}");

            // send ACK to the controller
            subscribeSocket.Unsubscribe("WORKER_ID");
            subscribeSocket.Subscribe("WORKLOAD_INIT");
            msg = new NetworkMessage(NetMsgType.ACK);
            pushSocket.SendFrame(MessagePackSerializer.Serialize(msg));

            subscribeSocket.ReceiveFrameString();      // this is the topic name
            msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());  // this is the message content
            Trace.Assert(msg.msgType == NetMsgType.WORKLOAD_INIT);
            var numExperiment = MessagePackSerializer.Deserialize<int>(msg.content);
            Debug.Assert(workerID > -1 && numExperiment > 0);
            maxNumExperiment = Constants.maxNumReRun * numExperiment;
            Console.WriteLine($"ExpWorker {workerID} receive numExperiment = {numExperiment} from ExpController");

            subscribeSocket.Unsubscribe("WORKLOAD_INIT");
            subscribeSocket.Subscribe("TERMINATE");
            subscribeSocket.Subscribe("WORKLOAD_CONFIG");
            msg = new NetworkMessage(NetMsgType.ACK);
            pushSocket.SendFrame(MessagePackSerializer.Serialize(msg));
            Console.WriteLine($"ExpWorker {workerID} ACK to ExpController... ");
        }

        static bool ProcessWork(int experimentID)
        {
            subscribeSocket.ReceiveFrameString();
            var msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
            if (msg.msgType == NetMsgType.TERMINATE)
            {
                Console.WriteLine($"Worker {workerID}: receive terminate message. ");
                return false;
            }

            Console.WriteLine($"ExpWorker {workerID} run experiment {experimentID}...");
            Trace.Assert(msg.msgType == NetMsgType.WORKLOAD_CONFIG);
            workload = MessagePackSerializer.Deserialize<WorkloadConfiguration>(msg.content);
            Console.WriteLine($"ExpWorker {workerID} receive workload configuration");

            // Initialize threads and other data-structures for epoch runs
            initializationDone = new CountdownEvent(1);
            var isGrainMigrationExp = workload.distPercent == -1;
            try
            {
                Initialize(isGrainMigrationExp);
            }
            catch (Exception e)
            {
                Console.WriteLine($"ExpWorker {workerID} exception {e.Message} {e.StackTrace}");
                throw;
            }
            initializationDone.Wait();

            subscribeSocket.Subscribe("RUN_EPOCH");
            msg = new NetworkMessage(NetMsgType.ACK);
            pushSocket.SendFrame(MessagePackSerializer.Serialize(msg));
            Console.WriteLine($"ExpWorker {workerID} finish initialization, sending ACK to controller.");

            var numEpoch = isGrainMigrationExp ? 1 : Constants.numEpoch;
            for (int i = 0; i < numEpoch; i++)
            {
                subscribeSocket.ReceiveFrameString();
                msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
                Trace.Assert(msg.msgType == NetMsgType.RUN_EPOCH);
                Console.WriteLine($"ExpWorker {workerID} start running epoch {i}...");
                // Signal the barrier
                barriers[i].SignalAndWait();
                // Wait for all threads to finish the epoch
                threadAcks[i].Wait();
                var result = ExperimentResultAggregator.AggregateResultForEpoch(results);
                msg = new NetworkMessage(NetMsgType.ACK, MessagePackSerializer.Serialize(result));
                pushSocket.SendFrame(MessagePackSerializer.Serialize(msg));
            }
            subscribeSocket.Unsubscribe("RUN_EPOCH");
            Console.WriteLine($"ExpWorker {workerID} Finished running epochs, exiting");
            try
            {
                foreach (var thread in consumerThreads) thread.Join();
                if (isGrainMigrationExp) grainMigrationThread.Join();
                producerThread.Join();
            }
            catch (Exception e)
            {
                Console.WriteLine($"ExpWorker {workerID}: {e.Message} {e.StackTrace}");
                throw;
            }
            return true;
        }

        static void ProducerThreadWork(object obj)
        {
            var input = (Tuple<int, int, int>)obj;
            isEpochFinish = new bool[input.Item1];
            isProducerFinish = new bool[input.Item1];
            for (int e = 0; e < input.Item1; e++)
            {
                isEpochFinish[e] = false;  // when worker thread finishes an epoch, set true
                isProducerFinish[e] = false;
            }

            ProducerThread.Run(
                input.Item1,
                input.Item2,
                input.Item3,
                workload.pactPipeSize * 10,
                workload.actPipeSize * 10,
                isEpochFinish,
                isProducerFinish,
                shared_requests,
                thread_requests
                );
        }

        static async void ConsumerThreadWorkAsync(object obj)
        {
            var input = (Tuple<bool, int, int, bool>)obj;
            var isGrainMigrationExp = input.Item1;
            var numEpoch = input.Item2;
            var threadIndex = input.Item3;
            var isDet = input.Item4;

            var benchmark = benchmarks[threadIndex];
            var client = clients[threadIndex];
            var pipeSize = isDet ? workload.pactPipeSize : workload.actPipeSize;
            var globalWatch = new Stopwatch();
            
            Console.WriteLine($"thread = {threadIndex}, isDet = {isDet}, pipe = {pipeSize}");
            for (int eIndex = 0; eIndex < numEpoch; eIndex++)
            {
                var queue = thread_requests[eIndex][threadIndex];
                await Task.Delay(TimeSpan.FromMilliseconds(500));   // give some time for producer to populate the buffer

                // Wait for all threads to arrive at barrier point
                barriers[eIndex].SignalAndWait();
                
                var consumer = new ConsumerThread(isDet, workload, benchmark, client, queue, globalWatch, isEpochFinish, isProducerFinish, isGrainMigrationExp, isMigrationDone, workloadGenerator);
                globalWatch.Restart();
                results[threadIndex] = await consumer.RunEpoch(eIndex);
                Console.WriteLine($"Consumer thread signal");
                threadAcks[eIndex].Signal();  // Signal the completion of epoch
            }
        }

        static async void GrainMigrationThreadWorkAsync(object _)
        {
            var client = clients.Last();
            barriers[0].SignalAndWait();   // Wait for all threads to arrive at barrier point
            var thread = new GrainMigrationThread(workerID, workload.migrationPipeSize, registeredSilo, migrationWorkersPerSilo, client, isMigrationDone);
            try
            {
                await thread.Run();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message} {e.StackTrace}");
                throw;
            }
            Console.WriteLine($"Migration thread signal");
            threadAcks[0].Signal();        // Signal the completion of epoch
        }

        static async void Initialize(bool isGrainMigrationExp)
        {
            var numEpoch = isGrainMigrationExp ? 1 : Constants.numEpoch;
            var numGrainMigrationThread = isGrainMigrationExp ? 1 : 0;
            var numDetConsumer = Constants.numCPUPerLocalSilo / 4;
            var numNonDetConsumer = Constants.numCPUPerLocalSilo / 4;

            if (workload.pactPercent == 100) numNonDetConsumer = 0;
            else if (workload.pactPercent == 0) numDetConsumer = 0;

            switch (Constants.benchmark)
            {
                case BenchmarkType.SMALLBANK:
                    benchmarks = new SmallBankBenchmark[numDetConsumer + numNonDetConsumer];
                    for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++) benchmarks[i] = new SmallBankBenchmark();
                    break;
                case BenchmarkType.TPCC:
                    benchmarks = new TPCCBenchmark[numDetConsumer + numNonDetConsumer];
                    for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++) benchmarks[i] = new TPCCBenchmark();
                    break;
                default:
                    throw new Exception("Exception: SnapperExperimentWorker only support SmallBank and TPCC benchmarks");
            }

            results = new WorkloadResult[numDetConsumer + numNonDetConsumer];
            for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++)
            {
                if (i < numDetConsumer) benchmarks[i].GenerateBenchmark(implementationType, workload, true);
                else benchmarks[i].GenerateBenchmark(implementationType, workload, false);
            }

            // some initialization for generating workload
            shared_requests = new Dictionary<int, Queue<Tuple<bool, RequestData>>>();   // <epoch, <producerID, <isDet, grainIDs>>>
            for (int epoch = 0; epoch < numEpoch; epoch++) shared_requests.Add(epoch, new Queue<Tuple<bool, RequestData>>());

            workloadGenerator = new WorkloadGenerator(workerID, numLocalSilo, implementationType, workload, shared_requests);
            workloadGenerator.GenerateWorkload();

            isMigrationDone = new bool[numEpoch];
            for (int i = 0; i < numEpoch; i++) isMigrationDone[i] = false;

            InitializeProducerThread(numEpoch, numGrainMigrationThread, numDetConsumer, numNonDetConsumer);
            await InitializeClients(numGrainMigrationThread, numDetConsumer, numNonDetConsumer);
            InitializeConsumerThreads(isGrainMigrationExp, numEpoch, numDetConsumer, numNonDetConsumer);
            if (isGrainMigrationExp)
            {
                Debug.Assert(implementationType == ImplementationType.SNAPPER && numGrainMigrationThread == 1);
                InitializeGrainMigrationThread(numDetConsumer, numNonDetConsumer);
                LoadMigrationWorkerInfo();
            }
            initializationDone.Signal();
        }

        static void InitializeProducerThread(int numEpoch, int numGrainMigrationThread, int numDetConsumer, int numNonDetConsumer)
        {
            barriers = new Barrier[numEpoch];
            threadAcks = new CountdownEvent[numEpoch];
            for (int i = 0; i < numEpoch; i++)
            {
                barriers[i] = new Barrier(numDetConsumer + numNonDetConsumer + numGrainMigrationThread + 1);
                threadAcks[i] = new CountdownEvent(numDetConsumer + numNonDetConsumer + numGrainMigrationThread);
            }

            thread_requests = new Dictionary<int, Dictionary<int, ConcurrentQueue<RequestData>>>();
            for (int epoch = 0; epoch < numEpoch; epoch++)
            {
                thread_requests.Add(epoch, new Dictionary<int, ConcurrentQueue<RequestData>>());
                for (int t = 0; t < numDetConsumer + numNonDetConsumer; t++) thread_requests[epoch].Add(t, new ConcurrentQueue<RequestData>());
            }

            producerThread = new Thread(ProducerThreadWork);
            producerThread.Start(new Tuple<int, int, int>(numEpoch, numDetConsumer, numNonDetConsumer));
        }

        static async Task InitializeClients(int numGrainMigrationThread, int numDetConsumer, int numNonDetConsumer)
        {
            var manager = new OrleansClientManager();
            clients = new IClusterClient[numDetConsumer + numNonDetConsumer + numGrainMigrationThread];
            for (int i = 0; i < numDetConsumer + numNonDetConsumer + numGrainMigrationThread; i++)
                clients[i] = await manager.StartOrleansClient();
        }

        static void InitializeConsumerThreads(bool isGrainMigrationExp, int numEpoch, int numDetConsumer, int numNonDetConsumer)
        {
            consumerThreads = new Thread[numDetConsumer + numNonDetConsumer];
            for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++)
            {
                var thread = new Thread(ConsumerThreadWorkAsync);
                consumerThreads[i] = thread;
                if (i < numDetConsumer) thread.Start(new Tuple<bool, int, int, bool>(isGrainMigrationExp, numEpoch, i, true));
                else thread.Start(new Tuple<bool, int, int, bool>(isGrainMigrationExp, numEpoch, i, false));
            }
        }

        static void InitializeGrainMigrationThread(int numDetConsumer, int numNonDetConsumer)
        {
            grainMigrationThread = new Thread(GrainMigrationThreadWorkAsync);
            grainMigrationThread.Start(new Tuple<int, int>(numDetConsumer, numNonDetConsumer));
        }

        static void LoadMigrationWorkerInfo()
        {
            var redis = ConnectionMultiplexer.Connect(new ConfigurationOptions { EndPoints = { redis_ConnectionString } });
            var siloInfo_db = redis.GetDatabase(Constants.Redis_SiloInfo);

            // load silo info
            var index = 0;
            registeredSilo = new List<string>();
            var siloAddress = siloInfo_db.ListGetByIndex("LocalSiloList", index);
            while (siloAddress != RedisValue.Null)
            {
                registeredSilo.Add(siloAddress);
                index++;
                siloAddress = siloInfo_db.ListGetByIndex("LocalSiloList", index);
            }
            Debug.Assert(registeredSilo.Count == numLocalSilo && numLocalSilo == 4);

            // load migration worker info
            migrationWorkersPerSilo = new Dictionary<string, List<Guid>>();
            for (int i = 0; i < numLocalSilo / 2; i++)
            {
                var list = new List<Guid>();
                migrationWorkersPerSilo.Add(registeredSilo[i], list);

                index = 0;
                var guid = siloInfo_db.ListGetByIndex("MigrationWorkerList-" + registeredSilo[i], index);
                while (guid != RedisValue.Null)
                {
                    list.Add(Guid.Parse(guid.ToString()));
                    index++;
                    guid = siloInfo_db.ListGetByIndex("MigrationWorkerList-" + registeredSilo[i], index);
                }
                Debug.Assert(list.Count == Constants.numGrainMigrationWorkerPerSilo);
            }
        }
    }
}