using NetMQ;
using System;
using Utilities;
using NetMQ.Sockets;
using System.Threading;
using System.Diagnostics;
using SnapperExperimentWorker;
using MessagePack;
using System.Xml;
using System.Collections.Generic;

namespace SnapperExperimentController
{
    static class Program
    {
        static bool isLocalTest;
        static int numLocalSilo;
        static string redis_ConnectionString;
        // for communication between ExpController and ExpProcess
        static PullSocket pullFromWorkerSocket;
        static PublisherSocket publishToWorkerSocket;
        static CountdownEvent ackedWorkers;
        // for communication between ExpController and Silos
        static PullSocket pullFromSiloSocket;
        static PublisherSocket publishToSiloSocket;

        static List<WorkloadConfiguration> workloadGroup;

        static ServerConnector serverConnector;
        static ExperimentResultAggregator resultAggregator;

        static void Main(string[] args)
        {
            isLocalTest = bool.Parse(args[0]);
            var experimentID = args[1];
            numLocalSilo = int.Parse(args[2]);
            var implementationType = Enum.Parse<ImplementationType>(args[3]);
            var isLoggingEnabled = bool.Parse(args[4]);
            var hierarchicalCoord = bool.Parse(args[5]);
            var optimizeCommit = bool.Parse(args[6]);
            var optimizeBatching = bool.Parse(args[7]);
            redis_ConnectionString = args[8];
            if (Constants.benchmark == BenchmarkType.SMALLBANK) GenerateSmallBankWorkLoadFromXMLFile(experimentID, implementationType);
            else if (Constants.benchmark == BenchmarkType.TPCC) GenerateTPCCWorkLoadFromXMLFile(experimentID, implementationType);

            Console.WriteLine($"ExpController: experimentID = {experimentID}, " +
                $"numLocalSilo = {numLocalSilo}, implementationType = {implementationType}, isLoggingEnabled = {isLoggingEnabled} " +
                $"hierarchicalCoord = {hierarchicalCoord}, optimizeCommit = {optimizeCommit}, optimizeBatching = {optimizeBatching}");
            
            // =========================================================================================================================
            string pullFromWorkerSocketAddress;
            string publishToWorkerSocketAddress;
            string pullFromSiloSocketAddress;
            string publishToSiloSocketAddress;

            if (isLocalTest)
            {
                var prefix = "@tcp://localhost:";
                pullFromWorkerSocketAddress = prefix + Constants.controllerFromWorker_PullPort;
                publishToWorkerSocketAddress = prefix + Constants.controllerToWorker_PublishPort;
                pullFromSiloSocketAddress = prefix + Constants.controllerFromSilo_PullPort;
                publishToSiloSocketAddress = prefix + Constants.controllerToSilo_PublishPort;
            }
            else
            {
                var prefix1 = "@tcp://" + Helper.GetLocalIPAddress() + ":";
                var prefix2 = "@tcp://*:";
                pullFromWorkerSocketAddress = prefix1 + Constants.controllerFromWorker_PullPort;
                publishToWorkerSocketAddress = prefix2 + Constants.controllerToWorker_PublishPort;
                pullFromSiloSocketAddress = prefix1 + Constants.controllerFromSilo_PullPort;
                publishToSiloSocketAddress = prefix2 + Constants.controllerToSilo_PublishPort;
            }

            pullFromWorkerSocket = new PullSocket(pullFromWorkerSocketAddress);
            publishToWorkerSocket = new PublisherSocket(publishToWorkerSocketAddress);
            pullFromSiloSocket = new PullSocket(pullFromSiloSocketAddress);
            publishToSiloSocket = new PublisherSocket(publishToSiloSocketAddress);

            // =========================================================================================================================
            // build connection with silos
            serverConnector = new ServerConnector(numLocalSilo, implementationType, isLoggingEnabled, redis_ConnectionString, hierarchicalCoord, optimizeCommit);
            ConnectSilos();
            serverConnector.InitiateClientAndServer();
            var isGrainMigrationExp = experimentID[0] == Constants.grainMigrationExpID;
            var numEpoch = isGrainMigrationExp ? 1 : Constants.numEpoch;
            serverConnector.LoadGrains(isGrainMigrationExp);
            serverConnector.PrepareCache(isGrainMigrationExp);

            // =========================================================================================================================
            // build connection with workers
            ConnectWorkers(workloadGroup.Count);
            
            // start running experiments
            foreach (var workload in workloadGroup)
            {
                for (int i = 0; i < Constants.maxNumReRun; i++)
                {
                    Console.WriteLine($"Run experiment for {i}th time: pactPercent = {workload.pactPercent}%, distPercent = {workload.distPercent}%, grainSkewness = {workload.grainSkewness * 100.0}%");
                    var batchSizeInMSecsBasic = optimizeBatching ? workload.batchSizeInMSecsBasic : 0;
                    serverConnector.SetGlobalBatchSize(batchSizeInMSecsBasic);
                    resultAggregator = new ExperimentResultAggregator(experimentID, numLocalSilo, implementationType, isLoggingEnabled, batchSizeInMSecsBasic,
                        isGrainMigrationExp, numEpoch, workload);

                    //Start the controller thread
                    var outputThread = new Thread(PublishToWorkers);
                    outputThread.Start(new Tuple<bool, int, WorkloadConfiguration>(isGrainMigrationExp, numEpoch, workload));

                    //Start the sink thread
                    var inputThread = new Thread(PullFromWorkers);
                    inputThread.Start(numEpoch);

                    //Wait for the threads to exit
                    inputThread.Join();
                    outputThread.Join();

                    var success = resultAggregator.AggregateResultsAndPrint(i);
                    if (success) break;
                    Thread.Sleep(5000);
                }

                Console.WriteLine("Finished running experiment.");
                Thread.Sleep(5000);
            }

            // =========================================================================================================================
            TerminateSilos();
            TerminateWorkers();
        }

        static void GenerateSmallBankWorkLoadFromXMLFile(string experimentID, ImplementationType implementationType)
        {
            var path = Constants.dataPath + @$"XML\Exp{experimentID}-{implementationType}.xml";
            var xmlDoc = new XmlDocument();
            xmlDoc.Load(path);
            var rootNode = xmlDoc.DocumentElement;

            var txnSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("txnSize").FirstChild.Value.Split(","), x => int.Parse(x));

            var txnDistLevelGroup = Array.ConvertAll(rootNode.SelectSingleNode("txnDistLevel").FirstChild.Value.Split(","), x => int.Parse(x));
            var batchSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("batchSizeInMSecsBasic").FirstChild.Value.Split(","), x => int.Parse(x));

            var pactPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPercent").FirstChild.Value.Split(","), x => int.Parse(x));

            var distPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("distPercent").FirstChild.Value.Split(","), x => int.Parse(x));

            var grainSkewnessGroup = Array.ConvertAll(rootNode.SelectSingleNode("grainSkewness").FirstChild.Value.Split(","), x => double.Parse(x) / 100.0);
            var actPipeSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("actPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));
            var pactPipeSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));

            var migrationPipeSizeGroup = experimentID[0] == Constants.grainMigrationExpID ?
                Array.ConvertAll(rootNode.SelectSingleNode("migrationPipeSize").FirstChild.Value.Split(","), x => int.Parse(x)) : new int[] { 0 };

            workloadGroup = new List<WorkloadConfiguration>();
            for (int i = 0; i < txnSizeGroup.Length; i++)
            {
                var txnSize = txnSizeGroup[i];
                for (int j = 0; j < txnDistLevelGroup.Length; j++)
                {
                    var txnDistLevel = txnDistLevelGroup[j];
                    var batchSizeInMSecsBasic = batchSizeGroup[j];
                    for (int k = 0; k < pactPercentGroup.Length; k++)
                    {
                        var pactPercent = pactPercentGroup[k];
                        for (int m = 0; m < distPercentGroup.Length; m++)
                        {
                            var distPercent = distPercentGroup[m];
                            for (int n = 0; n < grainSkewnessGroup.Length; n++)
                            {
                                var grainSkewness = grainSkewnessGroup[n];
                                var actPipeSize = actPipeSizeGroup[n];
                                var pactPipeSize = pactPipeSizeGroup[n];
                                for (int p = 0; p < migrationPipeSizeGroup.Length; p++)
                                {
                                    var migrationPipeSize = migrationPipeSizeGroup[p];
                                    var workload = new WorkloadConfiguration(txnSize, txnDistLevel, batchSizeInMSecsBasic, pactPercent, distPercent, grainSkewness, actPipeSize, pactPipeSize, migrationPipeSize);
                                    workloadGroup.Add(workload);
                                }
                            }
                        }
                    }
                }
            }
        }

        static void GenerateTPCCWorkLoadFromXMLFile(string experimentID, ImplementationType implementationType)
        {
            var path = Constants.dataPath + @$"XML\Exp{experimentID}-{implementationType}.xml";
            var xmlDoc = new XmlDocument();
            xmlDoc.Load(path);
            var rootNode = xmlDoc.DocumentElement;

            var batchSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("batchSizeInMSecsBasic").FirstChild.Value.Split(","), x => int.Parse(x));

            var pactPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPercent").FirstChild.Value.Split(","), x => int.Parse(x));

            var actPipeSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("actPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));
            var pactPipeSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));

            workloadGroup = new List<WorkloadConfiguration>();
            for (var i = 0; i < batchSizeGroup.Length; i++)
            {
                var batchSizeInMSecsBasic = batchSizeGroup[i];
                for (var j = 0; j < pactPercentGroup.Length; j++)
                {
                    var pactPercent = pactPercentGroup[j];
                    for (var k = 0; k < actPipeSizeGroup.Length; k++)
                    {
                        var actPipeSize = actPipeSizeGroup[k];
                        var pactPipeSize = pactPipeSizeGroup[k];
                        var workload = new WorkloadConfiguration(-2, -2, batchSizeInMSecsBasic, pactPercent, -2, -2, actPipeSize, pactPipeSize, -2);
                        workloadGroup.Add(workload);
                    }
                }
            }
        }

        static void ConnectSilos()
        {
            NetworkMessage msg;
            Console.WriteLine($"ExpController: wait for {numLocalSilo + 1} silos to connect");

            for (int i = 0; i < numLocalSilo + 1; i++)
            {
                msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullFromSiloSocket.ReceiveFrameBytes());
                Trace.Assert(msg.msgType == NetMsgType.CONNECT);
                Console.WriteLine($"ExpController: get one connection by silo");
            }

            Console.WriteLine($"ExpController: publish {NetMsgType.START_GLOBAL_SILO} message");
            msg = new NetworkMessage(NetMsgType.START_GLOBAL_SILO);
            publishToSiloSocket.SendMoreFrame("START_GLOBAL_SILO").SendFrame(MessagePackSerializer.Serialize(msg));

            Console.WriteLine($"ExpController: wait for global silo's ACK");
            msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullFromSiloSocket.ReceiveFrameBytes());
            Trace.Assert(msg.msgType == NetMsgType.ACK);

            Console.WriteLine($"ExpController: publish {NetMsgType.START_LOCAL_SILO} message");
            msg = new NetworkMessage(NetMsgType.START_LOCAL_SILO);
            publishToSiloSocket.SendMoreFrame("START_LOCAL_SILO").SendFrame(MessagePackSerializer.Serialize(msg));

            Console.WriteLine($"ExpController: wait for all local silos' ACK");
            for (int i = 0; i < numLocalSilo; i++)
            {
                msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullFromSiloSocket.ReceiveFrameBytes());
                Trace.Assert(msg.msgType == NetMsgType.ACK);
            }
        }

        static void TerminateSilos()
        {
            Console.WriteLine($"ExpController: publish {NetMsgType.TERMINATE} to all silos");
            var msg = new NetworkMessage(NetMsgType.TERMINATE);
            publishToSiloSocket.SendMoreFrame("TERMINATE").SendFrame(MessagePackSerializer.Serialize(msg));

            Console.WriteLine($"ExpController: wait for all silos' ACK");
            for (int i = 0; i < numLocalSilo; i++)
            {
                msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullFromSiloSocket.ReceiveFrameBytes());
                Trace.Assert(msg.msgType == NetMsgType.ACK);
            }
        }

        static void ConnectWorkers(int numExperiment)
        {
            ackedWorkers = new CountdownEvent(numLocalSilo);

            Console.WriteLine("ExpController: wait for workers to connect");
            NetworkMessage msg;

            var msgContent = new Dictionary<string, int>();
            for (int i = 0; i < numLocalSilo; i++)
            {
                msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullFromWorkerSocket.ReceiveFrameBytes());
                Trace.Assert(msg.msgType == NetMsgType.CONNECT);
                var workerIP = MessagePackSerializer.Deserialize<string>(msg.content);
                if (isLocalTest == false) msgContent.Add(workerIP, i);
            }

            Console.WriteLine($"ExpController: publish worker IDs");
            msg = new NetworkMessage(NetMsgType.WORKER_ID, MessagePackSerializer.Serialize(msgContent));
            publishToWorkerSocket.SendMoreFrame("WORKER_ID").SendFrame(MessagePackSerializer.Serialize(msg));

            for (int i = 0; i < numLocalSilo; i++)
            {
                msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullFromWorkerSocket.ReceiveFrameBytes());
                Trace.Assert(msg.msgType == NetMsgType.ACK);
            }
            Console.WriteLine($"ExpController: receive ACKs");

            Console.WriteLine($"ExpController: publish numExperiment = {numExperiment} to all workers");
            msg = new NetworkMessage(NetMsgType.WORKLOAD_INIT, MessagePackSerializer.Serialize(numExperiment));
            publishToWorkerSocket.SendMoreFrame("WORKLOAD_INIT").SendFrame(MessagePackSerializer.Serialize(msg));

            // receive ACKs from all workers
            Console.WriteLine($"ExpController: wait for all workers' ACK");
            for (int i = 0; i < numLocalSilo; i++)
            {
                msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullFromWorkerSocket.ReceiveFrameBytes());
                Trace.Assert(msg.msgType == NetMsgType.ACK);
            }
        }

        static void TerminateWorkers()
        {
            Console.WriteLine("ExpController: terminate all workers");
            var msg = new NetworkMessage(NetMsgType.TERMINATE);
            publishToWorkerSocket.SendMoreFrame("TERMINATE").SendFrame(MessagePackSerializer.Serialize(msg));
        }

        static void WaitForWorkerAcksAndReset()
        {
            ackedWorkers.Wait();
            ackedWorkers.Reset(numLocalSilo); //Reset for next ack, not thread-safe but provides visibility, ok for us to use due to lock-stepped (distributed producer/consumer) usage pattern i.e., Reset will never called concurrently with other functions (Signal/Wait)            
        }

        static void PublishToWorkers(object obj)
        {
            Console.WriteLine($"ExpController: publish workload configuration to {numLocalSilo} workers");
            var tuple = (Tuple<bool, int, WorkloadConfiguration>)obj;
            var isGrainMigrationExp = tuple.Item1;
            var numEpoch = tuple.Item2;
            var workload = tuple.Item3;
            var msg = new NetworkMessage(NetMsgType.WORKLOAD_CONFIG, MessagePackSerializer.Serialize(workload));
            publishToWorkerSocket.SendMoreFrame("WORKLOAD_CONFIG").SendFrame(MessagePackSerializer.Serialize(msg));

            Console.WriteLine($"ExpController: wait for all workers' ACK");
            WaitForWorkerAcksAndReset();
            Console.WriteLine($"ExpController: receive ACK from all workers");

            for (int i = 0; i < numEpoch; i++)
            {
                // send the command to run an epoch
                Console.WriteLine($"ExpController: run epoch {i} on {numLocalSilo} workers");
                msg = new NetworkMessage(NetMsgType.RUN_EPOCH);
                publishToWorkerSocket.SendMoreFrame("RUN_EPOCH").SendFrame(MessagePackSerializer.Serialize(msg));
                WaitForWorkerAcksAndReset();
                Console.WriteLine($"ExpController: finish running epoch {i} on {numLocalSilo} worker nodes");
            }

            Console.WriteLine("ExpController: wait to check GC");
            Thread.Sleep(5000);
            serverConnector.CheckGC();
        }

        static void PullFromWorkers(object obj)
        {
            var numEpoch = (int)obj;

            for (int i = 0; i < numLocalSilo; i++)
            {
                var msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullFromWorkerSocket.ReceiveFrameBytes());
                Trace.Assert(msg.msgType == NetMsgType.ACK);
                ackedWorkers.Signal();
            }

            // Wait for epoch acks
            for (int i = 0; i < numEpoch; i++)
            {
                for (int j = 0; j < numLocalSilo; j++)
                {
                    var msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullFromWorkerSocket.ReceiveFrameBytes());
                    Trace.Assert(msg.msgType == NetMsgType.ACK);
                    var result = MessagePackSerializer.Deserialize<WorkloadResult>(msg.content);
                    resultAggregator.SetResult(i, j, result);
                    ackedWorkers.Signal();
                }
            }
        }
    }
}