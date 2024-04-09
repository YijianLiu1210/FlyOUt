using System;
using Orleans;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;
using System.Linq;

namespace SnapperExperimentWorker
{
    public class SmallBankBenchmark : IBenchmark
    {
        bool isDet;
        ImplementationType implementationType;
        IDiscreteDistribution transferAmountDistribution;

        public void GenerateBenchmark(ImplementationType implementationType, WorkloadConfiguration _, bool isDet)
        {
            this.isDet = isDet;
            this.implementationType = implementationType;
            transferAmountDistribution = new DiscreteUniform(0, 10, new Random());
        }

        Task<TransactionResult> Execute(IClusterClient client, GrainID grainId, string startFunc, object funcInput, List<GrainID> grainIDList)
        {
            switch (implementationType)
            {
                case ImplementationType.SNAPPER:
                    var grain = client.GetGrain<ISnapperTransactionalAccountGrain>(grainId.id);
                    if (isDet) return grain.StartTransaction(startFunc, funcInput, grainIDList);
                    else return grain.StartTransaction(startFunc, funcInput);
                case ImplementationType.NONTXN:
                    var eventuallyConsistentGrain = client.GetGrain<INonTransactionalAccountGrain>(grainId.id);
                    return eventuallyConsistentGrain.StartTransaction(startFunc, funcInput);
                case ImplementationType.ORLEANSTXN:
                    var txnGrain = client.GetGrain<IOrleansTransactionalAccountGrain>(grainId.id);
                    return txnGrain.StartTransaction(startFunc, funcInput);
                default:
                    return null;
            }
        }

        public Task<TransactionResult> NewTransaction(IClusterClient client, RequestData data)
        {
            var accountGrains = data.grains;
            //accountGrains.Sort();

            var grainIDList = new List<GrainID>(accountGrains);

            var firstGrainID = accountGrains.First();
            accountGrains.RemoveAt(0);

            var money = transferAmountDistribution.Sample();
            var args = new Tuple<int, List<GrainID>>(money, accountGrains);
            var task = Execute(client, firstGrainID, "MultiTransfer", args, grainIDList);
            return task;
        }
    }
}