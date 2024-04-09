using Orleans;
using Utilities;
using TPCC.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace SnapperExperimentWorker
{
    public class TPCCBenchmark : IBenchmark
    {
        bool isDet;
        ImplementationType implementationType;

        public void GenerateBenchmark(ImplementationType implementationType, WorkloadConfiguration _, bool isDet)
        {
            this.isDet = isDet;
            this.implementationType = implementationType;
        }

        Task<TransactionResult> Execute(IClusterClient client, GrainID grainId, string startFunc, object funcInput, List<GrainID> grainIDList)
        {
            switch (implementationType)
            {
                case ImplementationType.SNAPPER:
                    var grain = client.GetGrain<ICustomerGrain>(grainId.id);
                    if (isDet) return grain.StartTransaction(startFunc, funcInput, grainIDList);
                    else return grain.StartTransaction(startFunc, funcInput);
                case ImplementationType.NONTXN:
                    var eventuallyConsistentGrain = client.GetGrain<IEventualCustomerGrain>(Helper.ConvertGuidToInt(grainId.id));
                    return eventuallyConsistentGrain.StartTransaction(startFunc, funcInput);
                default:
                    return null;
            }
        }

        public Task<TransactionResult> NewTransaction(IClusterClient client, RequestData data) 
            => Execute(client, data.firstGrainID, "NewOrder", data.input, data.grains);
    }
}