using Orleans;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Interface.GrainPlacement
{
    public interface IGrainMigrationWorker : IGrainWithGuidKey
    {
        Task Init();
        Task<GrainMigrationRequestResult> MigrateGrain(GrainID grainID, string targetSilo);
        Task<GrainMigrationRequestResult> DoMigration(GrainID grainID, string targetSilo);
        Task CheckGC();
    }
}
