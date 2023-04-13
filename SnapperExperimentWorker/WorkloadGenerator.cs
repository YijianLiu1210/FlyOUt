using Utilities;
using System;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;
using System.Diagnostics;
using Orleans.Runtime;

namespace SnapperExperimentWorker
{
    public class WorkloadGenerator
    {
        readonly int workerID;
        readonly int numLocalSilo;
        readonly ImplementationType implementationType;
        readonly WorkloadConfiguration workload;
        readonly IDiscreteDistribution distDistribution = new DiscreteUniform(0, 99, new Random());
        readonly IDiscreteDistribution detDistribution = new DiscreteUniform(0, 99, new Random());

        Dictionary<int, Queue<Tuple<bool, RequestData>>> shared_requests;  // <epoch, <isDet, grainIDs>>

        readonly IDiscreteDistribution gm_dist_dist = new DiscreteUniform(0, 1, new Random());
        readonly IDiscreteDistribution gm_grain_dist = new DiscreteUniform(0, Constants.numGrainPerLocalSilo / 8 - 1, new Random());   // [0, 2500)

        public WorkloadGenerator(
            int workerID,
            int numLocalSilo,
            ImplementationType implementationType,
            WorkloadConfiguration workload,
            Dictionary<int, Queue<Tuple<bool, RequestData>>> shared_requests)
        {
            this.workerID = workerID;
            this.numLocalSilo = numLocalSilo;
            this.workload = workload;
            this.shared_requests = shared_requests;
            this.implementationType = implementationType;
        }

        public void GenerateWorkload()
        {
            if (Constants.benchmark == BenchmarkType.SMALLBANK)
            {
                if (workload.distPercent == -1)
                {
                    Debug.Assert(workload.txnSize == 4 && workload.txnDistLevel == 2);
                    Debug.Assert(workload.distPercent == -1);
                    Debug.Assert(numLocalSilo == 4);
                    return;
                }
                else if (workload.txnSize == 4) InitializeSmallBankWorkloadForSize4();
                else if (workload.txnSize == 8 || workload.txnSize == 16) InitializeSmallBankWorkloadForSize8Or16();
                else throw new Exception($"Worker {workerID}: Unsupported workload");
                Console.WriteLine($"Worker {workerID}: finish generating workload");
            } 
        }

        bool isDet()
        {
            if (workload.pactPercent == 0) return false;
            else if (workload.pactPercent == 100) return true;

            var sample = detDistribution.Sample();
            if (sample < workload.pactPercent) return true;
            else return false;
        }

        bool isDist()
        {
            if (workload.distPercent == 0) return false;
            else if (workload.distPercent == 100) return true;
            
            var sample = distDistribution.Sample();
            if (sample < workload.distPercent) return true;
            else return false;
        }

        void InitializeSmallBankWorkloadForSize4()
        {
            Debug.Assert(workload.txnDistLevel == 2);
            Debug.Assert(workload.grainSkewness > 0 && workload.grainSkewness <= 1);
            if (workload.grainSkewness == 1) GenerateUniformSmallBankWorkload();
            else GenerateSkewSmallBankWorkload();
        }

        void GenerateUniformSmallBankWorkload()
        {
            var numTxnPerEpoch = Constants.BASE_NUM_MULTITRANSFER;
            if (implementationType == ImplementationType.NONTXN) numTxnPerEpoch *= 2;

            var firstGrainInSilo1 = workerID * Constants.numGrainPerLocalSilo;
            var firstGrainInSilo2 = ((workerID + 1) % numLocalSilo) * Constants.numGrainPerLocalSilo;
            var grain_dist_silo1 = new DiscreteUniform(firstGrainInSilo1, firstGrainInSilo1 + Constants.numGrainPerLocalSilo - 1, new Random());
            var grain_dist_silo2 = new DiscreteUniform(firstGrainInSilo2, firstGrainInSilo2 + Constants.numGrainPerLocalSilo - 1, new Random());

            for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
            {
                for (int txn = 0; txn < numTxnPerEpoch; txn++)
                {
                    var isDistTxn = isDist();
                    var grainsPerTxn = new List<int>();

                    if (isDistTxn)
                    {
                        // 1st grain ID
                        var id = grain_dist_silo1.Sample();
                        grainsPerTxn.Add(id);

                        // 2nd grain ID
                        id = grain_dist_silo1.Sample();
                        while (grainsPerTxn.Contains(id)) id = grain_dist_silo1.Sample();
                        grainsPerTxn.Add(id);

                        // 3rd grain ID
                        id = grain_dist_silo2.Sample();
                        grainsPerTxn.Add(id);

                        // 4th grain ID
                        id = grain_dist_silo2.Sample();
                        while (grainsPerTxn.Contains(id)) id = grain_dist_silo2.Sample();
                        grainsPerTxn.Add(id);
                    }
                    else
                    {
                        // 1st grain ID
                        var id = grain_dist_silo1.Sample();
                        grainsPerTxn.Add(id);

                        // 2nd, 3rd, 4th grain ID
                        for (int i = 0; i < workload.txnSize - 1; i++)
                        {
                            id = grain_dist_silo1.Sample();
                            while (grainsPerTxn.Contains(id)) id = grain_dist_silo1.Sample();
                            grainsPerTxn.Add(id);
                        }
                    }
                    var set = new HashSet<int>(grainsPerTxn);
                    Debug.Assert(set.Count == workload.txnSize);

                    var guids = new List<Guid>();
                    foreach (var id in grainsPerTxn) guids.Add(Helper.ConvertIntToGuid(id));
                    shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(isDistTxn, guids)));
                }
            }
        }

        void GenerateSkewSmallBankWorkload()
        {
            var numTxnPerEpoch = Constants.BASE_NUM_MULTITRANSFER;
            if (implementationType == ImplementationType.NONTXN) numTxnPerEpoch *= 2;

            var firstGrainInSilo1 = workerID * Constants.numGrainPerLocalSilo;
            var firstGrainInSilo2 = ((workerID + 1) % numLocalSilo) * Constants.numGrainPerLocalSilo;
            var numHotGrainPerSilo = (int)(Constants.numGrainPerLocalSilo * workload.grainSkewness);
            var hot_dist_silo1 = new DiscreteUniform(firstGrainInSilo1, firstGrainInSilo1 + numHotGrainPerSilo - 1, new Random());
            var normal_dist_silo1 = new DiscreteUniform(firstGrainInSilo1 + numHotGrainPerSilo, firstGrainInSilo1 + Constants.numGrainPerLocalSilo - 1, new Random());
            var hot_dist_silo2 = new DiscreteUniform(firstGrainInSilo2, firstGrainInSilo2 + numHotGrainPerSilo - 1, new Random());
            var normal_dist_silo2 = new DiscreteUniform(firstGrainInSilo2 + numHotGrainPerSilo, firstGrainInSilo2 + Constants.numGrainPerLocalSilo - 1, new Random());

            for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
            {
                for (int txn = 0; txn < numTxnPerEpoch; txn++)
                {
                    var isDistTxn = isDist();
                    var grainsPerTxn = new List<int>();

                    if (isDistTxn)
                    {
                        grainsPerTxn.Add(hot_dist_silo1.Sample());
                        grainsPerTxn.Add(normal_dist_silo1.Sample());
                        grainsPerTxn.Add(hot_dist_silo2.Sample());
                        grainsPerTxn.Add(normal_dist_silo2.Sample());
                    }
                    else
                    {
                        grainsPerTxn.Add(hot_dist_silo1.Sample());
                        grainsPerTxn.Add(normal_dist_silo1.Sample());

                        var id = hot_dist_silo1.Sample();
                        while(grainsPerTxn.Contains(id)) id = hot_dist_silo1.Sample();
                        grainsPerTxn.Add(id);

                        id = normal_dist_silo1.Sample();
                        while (grainsPerTxn.Contains(id)) id = normal_dist_silo1.Sample();
                        grainsPerTxn.Add(id);
                    }
                    var set = new HashSet<int>(grainsPerTxn);
                    Debug.Assert(set.Count == workload.txnSize);

                    var guids = new List<Guid>();
                    foreach (var id in grainsPerTxn) guids.Add(Helper.ConvertIntToGuid(id));
                    shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(isDistTxn, guids)));
                }
            }
        }

        void InitializeSmallBankWorkloadForSize8Or16()
        {
            Debug.Assert(workload.txnSize >= workload.txnDistLevel && workload.txnSize % workload.txnDistLevel == 0);
            
            var numTxnPerEpoch = Constants.BASE_NUM_MULTITRANSFER;
            if (implementationType == ImplementationType.NONTXN) numTxnPerEpoch *= 2;

            var grain_dist = new DiscreteUniform(0, Constants.numGrainPerLocalSilo - 1, new Random());
            
            for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
            {
                for (int txn = 0; txn < numTxnPerEpoch; txn++)
                {
                    var grainsPerTxn = new List<int>();
                    var firstSiloID = workerID;

                    var isDistTxn = workload.txnDistLevel == 1 ? false : isDist();
                    if (isDistTxn)
                    {
                        for (int i = 0; i < workload.txnDistLevel; i++)
                        {
                            var siloID = (firstSiloID + i) % numLocalSilo;
                            for (int j = 0; j < workload.txnSize / workload.txnDistLevel; j++)
                            {
                                var id = grain_dist.Sample() + siloID * Constants.numGrainPerLocalSilo;
                                while (grainsPerTxn.Contains(id)) id = grain_dist.Sample() + siloID * Constants.numGrainPerLocalSilo;
                                grainsPerTxn.Add(id);
                            }
                        }
                    }
                    else
                    {
                        var siloID = firstSiloID;
                        for (int i = 0; i < workload.txnSize; i++)
                        {
                            var id = grain_dist.Sample() + siloID * Constants.numGrainPerLocalSilo;
                            while (grainsPerTxn.Contains(id)) id = grain_dist.Sample() + siloID * Constants.numGrainPerLocalSilo;
                            grainsPerTxn.Add(id);
                        }
                    }
                    var set = new HashSet<int>(grainsPerTxn);
                    Debug.Assert(set.Count == workload.txnSize);

                    var guids = new List<Guid>();
                    foreach (var id in grainsPerTxn) guids.Add(Helper.ConvertIntToGuid(id));
                    shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(isDistTxn, guids)));
                }
            }
        }

        public RequestData GenerateSmallBankTransactionForGrainMigrationExperiment()
        {
            var grainsPerTxn = new List<int>();

            // STEP 1: generate 4 different IDs
            for (int i = 0; i < workload.txnSize; i++)
            {
                var id = gm_grain_dist.Sample();
                while (grainsPerTxn.Contains(id)) id = gm_grain_dist.Sample();
                grainsPerTxn.Add(id);
            }
            Debug.Assert(grainsPerTxn.Count == workload.txnSize);

            // STEP 2: select between [0, 1250) and [1250, 2.5k)
            var whichHalf = gm_dist_dist.Sample();
            for (int i = 0; i < workload.txnSize; i++) grainsPerTxn[i] += whichHalf * Constants.numGrainPerLocalSilo / 8;

            // STEP 3: select between [0, 2.5k), [2.5k, 5k), [5k, 7.5k), [7.5k, 10k)
            for (int i = 0; i < workload.txnSize; i++) grainsPerTxn[i] += workerID * Constants.numGrainPerLocalSilo / 4;

            // STEP 3: select which is the first accessed grain
            if (workerID / 2 == 0)   // worker 0 and 1 should select the last 2 grains from the second silo
            {
                grainsPerTxn[2] += Constants.numGrainPerLocalSilo;
                grainsPerTxn[3] += Constants.numGrainPerLocalSilo;
            }
            else                     // worker 2 and 3 should select the first 2 grains from the second silo
            {
                grainsPerTxn[0] += Constants.numGrainPerLocalSilo;
                grainsPerTxn[1] += Constants.numGrainPerLocalSilo;
            }

            var set = new HashSet<int>(grainsPerTxn);
            Debug.Assert(set.Count == workload.txnSize);

            var guids = new List<Guid>();
            foreach (var id in grainsPerTxn) guids.Add(Helper.ConvertIntToGuid(id));
            return new RequestData(workload.txnDistLevel > 1, guids);
        }
    }
}