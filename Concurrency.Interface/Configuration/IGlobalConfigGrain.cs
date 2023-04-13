using Orleans;
using System;
using System.Threading.Tasks;

namespace Concurrency.Interface.Configuration
{
    public interface IGlobalConfigGrain : IGrainWithStringKey
    {
        Task ConfigGlobalEnv(int numLocalSilo, bool isLoggingEnabled, bool hierarchicalCoord, bool optimizeCommit);
        Task ConfigGlobalBatchSize(int batchSizeInMSecsBasic);

        Task PrepareCache(bool isGrainMigrationExp);

        Task<int> getNumLocalSilo();

        Task CheckGC();

        Task UpdateUserGrainInfoInCache(Guid grainID, string siloAddress);
    }
}