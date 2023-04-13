using Orleans;
using Utilities;
using System.Threading.Tasks;

namespace SnapperExperimentWorker
{
    public interface IBenchmark
    {
        void GenerateBenchmark(ImplementationType implementationType, WorkloadConfiguration workloadConfig, bool isDet);
        Task<TransactionResult> NewTransaction(IClusterClient client, RequestData data);
    }
}
