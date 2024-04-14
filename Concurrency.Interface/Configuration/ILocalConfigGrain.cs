using Orleans;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Concurrency.Interface.Configuration
{
    public interface ILocalConfigGrain : IGrainWithGuidKey
    {
        // configuration
        Task Init(bool hierarchicalCoord, Dictionary<string, string> tpccGrainNames);
        Task ConfigLogging(bool isLoggingEnabled, int siloID, int numLocalSilo);
        Task ConfigLocalCoordinator(int siloID, bool optimizeCommit);
        Task ConfigPlacementManager(int numLocalSilo, int siloID);
        Task ConfigMigrationWorker(int siloID);

        // load grain info into memory
        Task PrepareCache(bool isGrainMigrationExp);

        // for grain migration
        Task UpdateUserGrainInfoInCache(Guid grainID, string siloAddress);

        Task CheckGC();
    }
}