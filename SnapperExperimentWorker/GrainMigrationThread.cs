using System.Diagnostics;
using System;
using System.Threading.Tasks;
using Utilities;
using System.Collections.Generic;
using Orleans;
using System.Linq;
using Concurrency.Interface.GrainPlacement;
using MathNet.Numerics.Distributions;

namespace SnapperExperimentWorker
{
    internal class GrainMigrationThread
    {
        readonly int numGrainToMigrate;
        readonly int workerID;
        readonly int pipeSize;
        Queue<Tuple<IGrainMigrationWorker, Guid, string>> queue;
        bool[] isMigrationDone;

        List<double> latencies = new List<double>();

        List<double> informGrainTime = new List<double>();
        List<double> freezeGrainTime = new List<double>();
        List<double> prepareDeactivateTime = new List<double>();
        List<double> waitTxnCommitTime = new List<double>();      // prepare 1
        List<double> getCommittedStateTime = new List<double>();  // prepare 2
        List<double> deserializeStateTime = new List<double>();   // prepare 3
        List<double> updateRedisTime = new List<double>();
        List<double> updateCacheTime = new List<double>();
        List<double> deactivateTime = new List<double>();
        List<double> activateTime = new List<double>();
        List<double> unfreezeTime = new List<double>();

        int numReq = 0;
        List<Task<GrainMigrationRequestResult>> tasks = new List<Task<GrainMigrationRequestResult>>();
        Dictionary<Task, DateTime> reqStartTime = new Dictionary<Task, DateTime>();

        public GrainMigrationThread(
            int workerID,
            int migrationPipeSize,
            List<string> registeredSilo,
            Dictionary<string, List<Guid>> migrationWorkersPerSilo,
            IClusterClient client,
            bool[] isMigrationDone)
        {
            this.workerID = workerID;
            this.pipeSize = migrationPipeSize;
            this.isMigrationDone = isMigrationDone;

            // generate list of grain IDs that will be migrated
            var worker_dist = new DiscreteUniform(0, Constants.numGrainMigrationWorkerPerSilo - 1, new Random());
            var requests = new List<Tuple<IGrainMigrationWorker, Guid, string>>();
            for (var siloID = 0; siloID < 2; siloID++)
            {
                var targetSilo = registeredSilo[siloID + 2];
                for (int i = 0; i < Constants.numGrainPerLocalSilo / 8; i++)
                {
                    var migrationWorkerID = migrationWorkersPerSilo[registeredSilo[siloID]][worker_dist.Sample()];
                    var worker = client.GetGrain<IGrainMigrationWorker>(migrationWorkerID);
                    var grainID = i + workerID * Constants.numGrainPerLocalSilo / 4 + siloID * Constants.numGrainPerLocalSilo;
                    requests.Add(new Tuple<IGrainMigrationWorker, Guid, string>(worker, Helper.ConvertIntToGuid(grainID), targetSilo));
                } 
            }

            // shuffle the list
            var newRequests = requests.OrderBy(a => Guid.NewGuid()).ToList();
            queue = new Queue<Tuple<IGrainMigrationWorker, Guid, string>>();
            foreach (var item in newRequests) queue.Enqueue(item);
            numGrainToMigrate = queue.Count;
            Console.WriteLine($"Worker {workerID}: need to migrate {numGrainToMigrate} grains. ");
        }

        public async Task Run()
        {
            Tuple<IGrainMigrationWorker, Guid, string> req;
            Console.WriteLine($"Worker {workerID}: Wait for {Constants.beforeAndAfterDurationMSecs / 1000}s ...");
            await Task.Delay(Constants.beforeAndAfterDurationMSecs);
            Console.WriteLine($"Worker {workerID}: Start grain migration ...");
            var start = DateTime.Now;
            do 
            {
                while (tasks.Count < pipeSize && queue.TryDequeue(out req))
                {
                    var worker = req.Item1;
                    var grainID = new GrainID(req.Item2, Constants.grainClassName);
                    var targetSilo = req.Item3;
                    var startTxnTime = DateTime.Now;
                    var t = worker.MigrateGrain(grainID, targetSilo);
                    tasks.Add(t);
                    reqStartTime.Add(t, startTxnTime);
                }
                if (tasks.Count != 0) await WaitForTaskAsync();
            }
            while (pipeSize != 0 && queue.Count != 0);
            while (tasks.Count != 0) await WaitForTaskAsync();
            isMigrationDone[0] = true;
            var time = (DateTime.Now - start).TotalMilliseconds;
            var average = latencies.Count != 0 ? latencies.Average() : 0;
            Console.WriteLine($"GrainMigrationThread: numReq = {numReq}, tp = {(int)(numReq * 1000 / time)}, average latency = {Helper.ChangeFormat(average, 0)}ms");
            Console.WriteLine($"GrainMigrationThread: informGrainTime {Helper.ChangeFormat(informGrainTime.Average(), 2)} " +
                                                    $"freezeGrain {Helper.ChangeFormat(freezeGrainTime.Average(), 2)} " +
                                                    $"prepareDeactivate {Helper.ChangeFormat(prepareDeactivateTime.Average(), 2)} " +
                                                      $"({Helper.ChangeFormat(waitTxnCommitTime.Average(), 2)} + " +
                                                       $"{Helper.ChangeFormat(getCommittedStateTime.Average(), 2)} + " +
                                                       $"{Helper.ChangeFormat(deserializeStateTime.Average(), 2)}) " +
                                                    $"updateRedis {Helper.ChangeFormat(updateRedisTime.Average(), 2)} " +
                                                    $"updateCache {Helper.ChangeFormat(updateCacheTime.Average(), 2)} " +
                                                    $"deactivate {Helper.ChangeFormat(deactivateTime.Average(), 2)} " +
                                                    $"activate {Helper.ChangeFormat(activateTime.Average(), 2)} " +
                                                    $"unfreeze {Helper.ChangeFormat(unfreezeTime.Average(), 2)}");
        }

        async Task WaitForTaskAsync()
        {
            var task = await Task.WhenAny(tasks);
            var endTxnTime = DateTime.Now;
            var succeed = true;
            try
            {
                await task;
            }
            catch (SnapperGrainMigrationException e)
            {
                succeed = false;
                Debug.Assert(e.Message.Contains("is already in the target silo"));
            }

            if (succeed)
            {
                latencies.Add((endTxnTime - reqStartTime[task]).TotalMilliseconds);

                informGrainTime.Add(task.Result.informGrainTime);
                freezeGrainTime.Add(task.Result.freezeGrainTime);
                prepareDeactivateTime.Add(task.Result.prepareDeactivateTime);
                waitTxnCommitTime.Add(task.Result.waitTxnCommitTime);               // prepare 1
                getCommittedStateTime.Add(task.Result.getCommittedStateTime);       // prepare 2
                deserializeStateTime.Add(task.Result.deserializeStateTime);         // prepare 3
                updateRedisTime.Add(task.Result.updateRedisTime);
                updateCacheTime.Add(task.Result.updateRedisTime);
                deactivateTime.Add(task.Result.deactivateTime);
                activateTime.Add(task.Result.activateTime);
                unfreezeTime.Add(task.Result.unfreezeTime);
            }
            tasks.Remove(task);
            reqStartTime.Remove(task);
            numReq++;
            if (numReq % (numGrainToMigrate / 10) == 0) Console.WriteLine($"Finish migrating {numReq} grains");
        }
    }
}