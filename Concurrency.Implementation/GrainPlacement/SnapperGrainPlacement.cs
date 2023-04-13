using System;
using System.Linq;
using Orleans.Runtime;
using Orleans.Placement;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;
using Utilities;
using StackExchange.Redis;
using System.Collections.Generic;
using Concurrency.Interface.GrainPlacement;

namespace Concurrency.Implementation.GrainPlacement
{
    public class SnapperGrainPlacement : IPlacementDirector
    {
        SiloAddress globalSiloAddress;
        IDatabase siloInfo_db;
        IDatabase grainPlacement_db;
        IGrainPlacementCache grainPlacementCache;
        readonly Comparer<int> duplicateComparer;

        //SemaphoreSlim instanceLock = new SemaphoreSlim(1);

        public SnapperGrainPlacement(IGrainPlacementCache grainPlacementCache, IConnectionMultiplexer redis)
        {
            this.grainPlacementCache = grainPlacementCache;
            siloInfo_db = redis.GetDatabase(Constants.Redis_SiloInfo);
            grainPlacement_db = redis.GetDatabase(Constants.Redis_GrainPlacementMap);
            duplicateComparer = Comparer<int>.Create((x, y) =>
            {
                var res = y.CompareTo(x);
                return res == 0 ? 1 : res;
            });
        }

        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var silos = context.GetCompatibleSilos(target).ToList();
            if (silos.Count == 1) return Task.FromResult(silos[0]);

            var myStrategy = (SnapperGrainPlacementStrategy)strategy;
            var grainType = myStrategy.grainType;
            switch (grainType)
            {
                case GrainType.GlobalConfig:
                    return Task.FromResult(silos[0]);
                case GrainType.LocalConfig:
                    return GetSiloOfLocalConfigGrain(silos);
                case GrainType.GlobalCoord:
                    return GetGlobalSilo(silos);
                case GrainType.LocalCoord:
                    var coordID = target.GrainIdentity.PrimaryKey;
                    return GetSilo(silos, coordID, Constants.CoordIDPrefix);
                case GrainType.UserGrain:
                    var grainID = target.GrainIdentity.PrimaryKey;
                    var silo = grainPlacementCache.GetSilo(grainID);
                    return Task.FromResult(GetSiloByString(silos, silo));
                case GrainType.MigrationWorker:
                    var workerID = target.GrainIdentity.PrimaryKey;
                    return GetSilo(silos, workerID, "MigrationWorker-");
                case GrainType.PlacementManager:
                    var managerID = target.GrainIdentity.PrimaryKey;
                    return GetSilo(silos, managerID, "PlacementManager-");
                default:
                    throw new Exception($"Unknown grain type {grainType}");
            }
        }

        SiloAddress GetSiloByString(List<SiloAddress> silos, string addr)
        {
            foreach (var silo in silos)
            {
                var siloString = Helper.SiloStringToRuntimeID(silo.ToParsableString());
                if (siloString == addr) return silo;
            }
            throw new SnapperStorageException($"Fail to find silo {addr}");
        }

        async Task<SiloAddress> GetSilo(List<SiloAddress> silos, Guid grainID, string grainType)
        {
            var addr = (await grainPlacement_db.StringGetAsync(grainType + grainID.ToString())).ToString();
            if (addr == RedisValue.Null) throw new Exception($"GetSilo: addr is null");
            return GetSiloByString(silos, addr);
        }

        async Task<SiloAddress> GetGlobalSilo(List<SiloAddress> silos)
        {
            if (globalSiloAddress != null)
            {
                if (silos.Contains(globalSiloAddress) == false) throw new Exception($"GetGlobalSilo: globalSiloAddress {globalSiloAddress} is not in silos");
                return globalSiloAddress;
            }

            var addr = (await siloInfo_db.StringGetAsync(Constants.GlobalSilo)).ToString();

            foreach (var silo in silos)
            {
                var siloString = Helper.SiloStringToRuntimeID(silo.ToParsableString());
                if (siloString == addr)
                {
                    globalSiloAddress = silo;
                    return silo;
                }
            }
            throw new SnapperStorageException("No silo is registered as global silo");
        }

        async Task<SiloAddress> GetSiloOfLocalConfigGrain(List<SiloAddress> silos)
        {
            await GetGlobalSilo(silos);   // make sure globalSiloAddress has been set

            foreach (var silo in silos)
            {
                if (silo == globalSiloAddress) continue;
                var siloString = Constants.GeneralInfoPrefix + Helper.SiloStringToRuntimeID(silo.ToParsableString());
                var isLocalConfigGrainActivated = await siloInfo_db.HashGetAsync(siloString, "isLocalConfigGrainActivated");
                if (isLocalConfigGrainActivated == true) continue;

                await siloInfo_db.HashSetAsync(siloString, "isLocalConfigGrainActivated", true);
                return silo;
            }
            throw new SnapperStorageException("Cannot find a silo that has no LocalConfigGrain activated");
        }
    }

    [Serializable]
    public class SnapperGrainPlacementStrategy : PlacementStrategy
    {
        public readonly GrainType grainType;
        public SnapperGrainPlacementStrategy(GrainType grainType)
        {
            this.grainType = grainType;
        }
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class SnapperGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public SnapperGrainPlacementStrategyAttribute(GrainType grainType) : base(new SnapperGrainPlacementStrategy(grainType))
        {
        }
    }
}