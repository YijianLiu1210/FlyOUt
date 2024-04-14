using Orleans;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Concurrency.Interface.Configuration
{
    public interface IGlobalConfigGrain : IGrainWithStringKey
    {
        Task ConfigGlobalEnv(int numLocalSilo, bool isLoggingEnabled, bool hierarchicalCoord, bool optimizeCommit, Dictionary<string, string> tpccGrainNames);
        Task ConfigGlobalBatchSize(int batchSizeInMSecsBasic);

        Task PrepareCache(bool isGrainMigrationExp);

        Task<int> getNumLocalSilo();

        Task CheckGC();

        Task UpdateUserGrainInfoInCache(Guid grainID, string siloAddress);
    }
}