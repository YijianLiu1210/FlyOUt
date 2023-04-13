using Orleans;
using System;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Interface.GrainPlacement
{
    public interface IGrainMigrationWorker : IGrainWithGuidKey
    {
        Task Init();
        Task<GrainMigrationRequestResult> MigrateGrain(Guid grainID, string targetSilo);
        Task<GrainMigrationRequestResult> DoMigration(Guid grainID, string targetSilo);
        Task CheckGC();
    }
}
