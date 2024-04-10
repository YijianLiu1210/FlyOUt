using Utilities;
using System;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;
using System.Diagnostics;
using System.Linq;
using SmallBank.Grains;
using TPCC.Grains;

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

        // for SmallBank only
        readonly string grainName;

        // for TPCC only
        readonly string itemGrainName;
        readonly string warehouseGrainName;
        readonly string districtGrainName;
        readonly string customerGrainName;
        readonly string stockGrainName;
        readonly string orderGrainName;

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

            // ==================================================================================================================
            var eventual = implementationType == ImplementationType.NONTXN;
            if (Constants.benchmark == BenchmarkType.TPCC)
            {
                itemGrainName = eventual ? typeof(EventualItemGrain).FullName : typeof(ItemGrain).FullName;
                warehouseGrainName = eventual ? typeof(EventualWarehouseGrain).FullName : typeof(WarehouseGrain).FullName;
                districtGrainName = eventual ? typeof(EventualDistrictGrain).FullName : typeof(DistrictGrain).FullName;
                customerGrainName = eventual ? typeof(EventualCustomerGrain).FullName : typeof(CustomerGrain).FullName;
                stockGrainName = eventual ? typeof(EventualStockGrain).FullName : typeof(StockGrain).FullName;
                orderGrainName = eventual ? typeof(EventualOrderGrain).FullName : typeof(OrderGrain).FullName;
            }
            else if (Constants.benchmark == BenchmarkType.SMALLBANK)
            {
                switch (implementationType)
                {
                    case ImplementationType.SNAPPER:
                        grainName = typeof(SnapperTransactionalAccountGrain).FullName;
                        break;
                    case ImplementationType.NONTXN:
                        grainName = typeof(NonTransactionalAccountGrain).FullName;
                        break;
                    case ImplementationType.ORLEANSTXN:
                        grainName = typeof(OrleansTransactionalAccountGrain).FullName;
                        break;
                }
            }
            // ==================================================================================================================

            Debug.Assert(workerID < numLocalSilo);
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
                
            }
            else if (Constants.benchmark == BenchmarkType.TPCC) InitializeTPCCWorkload();
            
            Console.WriteLine($"Worker {workerID}: finish generating workload");
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

                    var guids = grainsPerTxn.Select(id => new GrainID(Helper.ConvertIntToGuid(id), grainName)).ToList();
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

                    var guids = grainsPerTxn.Select(id => new GrainID(Helper.ConvertIntToGuid(id), grainName)).ToList();
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

                    var guids = grainsPerTxn.Select(id => new GrainID(Helper.ConvertIntToGuid(id), grainName)).ToList();
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

            var guids = grainsPerTxn.Select(id => new GrainID(Helper.ConvertIntToGuid(id), grainName)).ToList();
            return new RequestData(workload.txnDistLevel > 1, guids);
        }

        void InitializeTPCCWorkload()
        {
            var firstWarehouseInThisSilo = workerID * Constants.NUM_W_PER_SILO;
            for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
            {
                var numRound = implementationType == ImplementationType.NONTXN ? 3 : 1;

                var remote_count = 0;
                var txn_size = new List<int>();
                Console.WriteLine($"Generate TPCC workload for epoch {epoch}, numRound = {numRound}");
                for (int round = 0; round < numRound; round++)
                {
                    var wh_dist = new DiscreteUniform(0, Constants.NUM_W_PER_SILO - 1, new Random());
                    var district_dist = new DiscreteUniform(0, Constants.NUM_D_PER_W - 1, new Random());

                    var ol_cnt_dist_uni = new DiscreteUniform(5, 15, new Random());
                    var rbk_dist_uni = new DiscreteUniform(1, 100, new Random());
                    var local_dist_uni = new DiscreteUniform(1, 100, new Random());
                    var quantity_dist_uni = new DiscreteUniform(1, 10, new Random());

                    for (int txn = 0; txn < Constants.BASE_NUM_NEWORDER; txn++)
                    {
                        var W_ID = firstWarehouseInThisSilo + wh_dist.Sample();
                        var warehouses = new HashSet<int> { W_ID };
                        var D_ID = district_dist.Sample();
                        var C_ID = Helper.NURand(1023, 1, Constants.NUM_C_PER_D, 0) - 1;
                        var firstGrainID = TPCCManager.GetCustomerGrain(W_ID, D_ID);
                        var grains = new Dictionary<int, string>
                        {
                            { TPCCManager.GetItemGrain(W_ID), itemGrainName },
                            { TPCCManager.GetWarehouseGrain(W_ID), warehouseGrainName },
                            { firstGrainID, customerGrainName },
                            { TPCCManager.GetDistrictGrain(W_ID, D_ID), districtGrainName },
                            { TPCCManager.GetOrderGrain(W_ID, D_ID, C_ID), orderGrainName }
                        };
                        var ol_cnt = ol_cnt_dist_uni.Sample();
                        var rbk = rbk_dist_uni.Sample();
                        //rbk = 0;
                        var itemsToBuy = new Dictionary<int, Tuple<int, int>>();  // <I_ID, <supply_warehouse, quantity>>

                        var remote_flag = false;

                        for (int i = 0; i < ol_cnt; i++)
                        {
                            int I_ID;

                            if (i == ol_cnt - 1 && rbk == 1) I_ID = -1;
                            else
                            {
                                do I_ID = Helper.NURand(8191, 1, Constants.NUM_I, 0) - 1;
                                while (itemsToBuy.ContainsKey(I_ID));
                            }

                            int supply_wh;
                            var local = local_dist_uni.Sample() > 1;
                            if (local) supply_wh = W_ID;    // supply by home warehouse
                            else   // supply by remote warehouse
                            {
                                remote_flag = true;
                                var nextSiloID = (workerID + 1) % numLocalSilo;
                                supply_wh = nextSiloID * Constants.NUM_W_PER_SILO + wh_dist.Sample();
                                Debug.Assert(supply_wh != W_ID);
                            }
                            var quantity = quantity_dist_uni.Sample();
                            itemsToBuy.Add(I_ID, new Tuple<int, int>(supply_wh, quantity));

                            if (I_ID != -1)
                            {
                                Debug.Assert(I_ID < Constants.NUM_I);
                                var grainID = TPCCManager.GetStockGrain(supply_wh, I_ID);
                                if (!grains.ContainsKey(grainID)) grains.Add(grainID, stockGrainName);

                                warehouses.Add(supply_wh);
                            }
                        }
                        if (remote_flag) remote_count++;
                        txn_size.Add(grains.Count);

                        var isDistTxn = !TPCCManager.IsInSameSilo(warehouses);
                        var grainIDs = grains.Select(item => new GrainID(Helper.ConvertIntToGuid(item.Key), item.Value)).ToList();
                        Debug.Assert(grainIDs.Count == grains.Count);
                        var input = new NewOrderInput(C_ID, itemsToBuy);
                        var firstGrain = new GrainID(Helper.ConvertIntToGuid(firstGrainID), customerGrainName);
                        var req = new RequestData(isDistTxn, firstGrain, grainIDs, input);
                        shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), req));
                    }
                }
                var numTxn = Constants.BASE_NUM_NEWORDER * numRound;
                Console.WriteLine($"epoch = {epoch}, remote wh rate = {remote_count * 100.0 / numTxn}%, txn_size_ave = {txn_size.Average()}");
            }
        }
    }
}