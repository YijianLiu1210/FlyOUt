using System;
using System.Collections.Generic;

namespace Concurrency.Interface.GrainPlacement
{
    public interface IGrainPlacementCache
    {
        void SetHierarchicalCoord(bool hierarchicalCoord);
        bool GetHierarchicalCoord();
        void PrepareCache(bool isGrainMigrationExp, string siloAddress);
        void UpdateUserGrainInfo(Guid grainID, string siloAddress);
        void AddUserGrain(Guid grainID, string silo);

        string GetSilo(Guid grainID);
        string GetGlobalSiloAddress();
        Guid GetCoordNeighborID(string siloAddress, Guid coordID);
        List<Guid> GetAllCoordsInSilo(string siloAddress);
        Guid GetOneRandomCoordInSilo(string siloAddress);
        Guid GetOneCoordInSiloByID(int numLocalSilo, string siloAddress, Guid placementManagerID);

        // for grain migration
        List<Guid> GetAllPMInSilo(string siloAddress);
        Guid GetOneRandomPMInSilo(string siloAddress);
        Guid GetOneRandomMWInSilo(string siloAddress);
        List<Guid> GetAllPM();
        Guid GetMigrationWorker(Guid grainID);

        // for ACT hybrid processing
        Dictionary<string, Guid> GetOneRandomLocalCoordPerSilo();
    }
}