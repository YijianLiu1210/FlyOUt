using Orleans;
using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Utilities;
using Concurrency.Interface.GrainPlacement;
using Concurrency.Interface.Coordinator;

namespace Concurrency.Implementation.GrainPlacement
{
    [Reentrant]
    [SnapperGrainPlacementStrategy(GrainType.PlacementManager)]
    public class GrainPlacementManager : Grain, IGrainPlacementManager
    {
        int myID;
        ILocalCoordGrain myLocalCoord;
        IGlobalCoordGrain myGlobalCoord;
        readonly IGrainPlacementCache grainPlacementCache;
        bool hierarchicalCoord;

        Dictionary<GrainID, TaskCompletionSource> freezedGrains;                       // <grain ID, when the grain is unfreezed>
        Dictionary<GrainID, MyCounter> numToBeRegisteredTxnPerGrain;                   // <grain ID, number of transactions that will be registered but haven't got txn context>
        Dictionary<GrainID, TaskCompletionSource> allTxnRegisteredPerGrain;            // <grain ID, when all transactions have either got local tid or global tid>
        Dictionary<GrainID, long> maxUnCommittedLocalBidPerGrain;                      // <grain ID, max local tid>, only for local transactions
        Dictionary<GrainID, long> maxUnCommittedGlobalBidPerGrain;

        public GrainPlacementManager(IGrainPlacementCache grainPlacementCache)
        {
            this.grainPlacementCache = grainPlacementCache;
        }

        public Task Init(int numLocalSilo)
        {
            // set up local and global coordinator info
            var localCoordID = grainPlacementCache.GetOneRandomCoordInSilo(RuntimeIdentity);
            myLocalCoord = GrainFactory.GetGrain<ILocalCoordGrain>(localCoordID);
            hierarchicalCoord = grainPlacementCache.GetHierarchicalCoord();
            if (hierarchicalCoord)
            {
                var globalSiloAddress = grainPlacementCache.GetGlobalSiloAddress();
                var globalCoordID = grainPlacementCache.GetOneCoordInSiloByID(numLocalSilo, globalSiloAddress, this.GetPrimaryKey());
                myGlobalCoord = GrainFactory.GetGrain<IGlobalCoordGrain>(globalCoordID);
            }
            return Task.CompletedTask;
        }

        public Task CheckGC()
        {
            if (freezedGrains.Count != 0) Console.WriteLine($"PM: freezedGrains.Count = {freezedGrains.Count}");
            if (numToBeRegisteredTxnPerGrain.Count != 0) Console.WriteLine($"PM: numToBeRegisteredTxnPerGrain.Count = {numToBeRegisteredTxnPerGrain.Count}");
            if (allTxnRegisteredPerGrain.Count != 0) Console.WriteLine($"PM: allTxnRegisteredPerGrain.Count = {allTxnRegisteredPerGrain.Count}");
            if (maxUnCommittedLocalBidPerGrain.Count != 0) Console.WriteLine($"PM: maxUnCommittedLocalBidPerGrain.Count = {maxUnCommittedLocalBidPerGrain.Count}");
            if (maxUnCommittedGlobalBidPerGrain.Count != 0) Console.WriteLine($"PM: maxUnCommittedGlobalBidPerGrain.Count = {maxUnCommittedGlobalBidPerGrain.Count}");

            return Task.CompletedTask;
        }

        public override Task OnActivateAsync()
        {
            var guid = this.GetPrimaryKey();
            myID = Helper.ConvertGuidToInt(guid);

            freezedGrains = new Dictionary<GrainID, TaskCompletionSource>();
            numToBeRegisteredTxnPerGrain = new Dictionary<GrainID, MyCounter>();
            allTxnRegisteredPerGrain = new Dictionary<GrainID, TaskCompletionSource>();
            maxUnCommittedLocalBidPerGrain = new Dictionary<GrainID, long>();
            maxUnCommittedGlobalBidPerGrain = new Dictionary<GrainID, long>();
            return Task.CompletedTask;
        }

        public async Task<Tuple<long, long>> FreezeGrain(GrainID grainID)
        {
            //Console.WriteLine($"PM {myID}: try to freeze grain {grainID}");
            // add the grain, so no new transactions (that will access this grain) will be generated 
            if (freezedGrains.ContainsKey(grainID)) throw new Exception($"FreezeGrain: grainID {grainID} is already in freezedGrains");
            freezedGrains.Add(grainID, new TaskCompletionSource());

            // make sure no more messages will send to this grain
            // STEP 1: check if there are transactions that will be registered
            if (numToBeRegisteredTxnPerGrain.ContainsKey(grainID))
            {
                if (numToBeRegisteredTxnPerGrain[grainID].GetCount() == 0)
                    throw new Exception($"FreezeGrain: numToBeRegisteredTxnPerGrain[{grainID}] should not be 0");

                if (allTxnRegisteredPerGrain.ContainsKey(grainID) == false)
                    allTxnRegisteredPerGrain[grainID] = new TaskCompletionSource();

                await allTxnRegisteredPerGrain[grainID].Task;
            }

            // STEP 2: get the max emitted local and global bid to this grain
            // notice: if the grain is not in this silo, the localBid we get must be -1
            long localBid = -1;
            long globalBid = -1;
            if (maxUnCommittedLocalBidPerGrain.ContainsKey(grainID))
            {
                localBid = maxUnCommittedLocalBidPerGrain[grainID];
                maxUnCommittedLocalBidPerGrain.Remove(grainID);
            }
            if (maxUnCommittedGlobalBidPerGrain.ContainsKey(grainID))
            {
                globalBid = maxUnCommittedGlobalBidPerGrain[grainID];
                maxUnCommittedGlobalBidPerGrain.Remove(grainID);
            }
            //Console.WriteLine($"max_local_bid = {localBid}, max_global_bid = {globalBid}");
            return new Tuple<long, long>(localBid, globalBid);
        }

        public Task UnFreezeGrain(GrainID grainID)
        {
            if (freezedGrains.ContainsKey(grainID) == false) throw new Exception($"UnFreezeGrain: grainID {grainID} is not in freezedGrains");
            var task = freezedGrains[grainID];
            freezedGrains.Remove(grainID);
            task.SetResult();
            return Task.CompletedTask;
        }

        HashSet<GrainID> GetIntersectionWithFreezedGrains(HashSet<GrainID> grainSet)
        {
            var grains = new HashSet<GrainID>(freezedGrains.Keys);
            grains.IntersectWith(grainSet);
            return grains;
        }

        async Task WaitForGrainUnfreeze(List<GrainID> grainList)
        {
            var grainSet = new HashSet<GrainID>(grainList);

            var intersection = GetIntersectionWithFreezedGrains(grainSet);
            
            while (intersection.Count != 0)
            {
                // wait for all freezed grains to unfreeze
                var tasks = new List<Task>();
                foreach (var grain in intersection) tasks.Add(freezedGrains[grain].Task);
                await Task.WhenAll(tasks);

                // check again in case more grains are freezed while waiting
                intersection = GetIntersectionWithFreezedGrains(grainSet);
            }

            // if no grains are freezed
            foreach (var grain in grainList)
            {
                if (numToBeRegisteredTxnPerGrain.ContainsKey(grain) == false)
                    numToBeRegisteredTxnPerGrain.Add(grain, new MyCounter());
                numToBeRegisteredTxnPerGrain[grain].Increment();
            }
        }

        void RegisterATransaction(GrainID grain)
        {
            if (numToBeRegisteredTxnPerGrain.ContainsKey(grain) == false)
                throw new Exception($"RegisterATransaction: grain {grain} is not in numToBeRegisteredTxnPerGrain");
            var isZero = numToBeRegisteredTxnPerGrain[grain].Decrement();
            if (isZero)
            {
                numToBeRegisteredTxnPerGrain.Remove(grain);
                if (allTxnRegisteredPerGrain.ContainsKey(grain))
                {
                    allTxnRegisteredPerGrain[grain].SetResult();
                    allTxnRegisteredPerGrain.Remove(grain);
                }
            }
        }

        void CleanUp(long highestCommittedLocalBidOnGrain, long highestCommittedGlobalBidOnGrain)
        {
            // clean up local info
            var itemsToRemove = new List<GrainID>();
            foreach (var item in maxUnCommittedLocalBidPerGrain)
                if (item.Value <= highestCommittedLocalBidOnGrain) itemsToRemove.Add(item.Key);

            foreach (var grain in itemsToRemove) maxUnCommittedLocalBidPerGrain.Remove(grain);

            // clean up global info
            itemsToRemove = new List<GrainID>();
            foreach (var item in maxUnCommittedGlobalBidPerGrain)
                if (item.Value <= highestCommittedGlobalBidOnGrain) itemsToRemove.Add(item.Key);

            foreach (var grain in itemsToRemove) maxUnCommittedGlobalBidPerGrain.Remove(grain);
        }

        public async Task<Tuple<MyTransactionContext, long, long>> NewTransaction(List<GrainID> grainList)
        {
            await WaitForGrainUnfreeze(grainList);

            if (hierarchicalCoord)
            {
                // check if the transaction will access multiple silos
                var siloList = new List<string>();
                var grainListPerSilo = new Dictionary<string, List<string>>();
                for (int i = 0; i < grainList.Count; i++)
                {
                    var grainID = grainList[i];
                    var siloAddress = grainPlacementCache.GetSilo(grainID.id);
                    if (grainListPerSilo.ContainsKey(siloAddress) == false)
                    {
                        siloList.Add(siloAddress);
                        grainListPerSilo.Add(siloAddress, new List<string>());
                    }

                    grainListPerSilo[siloAddress].Add(grainID.ToString());
                }

                if (siloList.Count != 1)
                {
                    // get global tid from global coordinator
                    var globalInfo = await myGlobalCoord.NewGlobalPACT(siloList);
                    var globalBid = globalInfo.Item1;
                    var globalTid = globalInfo.Item2;
                    var siloIDToLocalCoordID = globalInfo.Item3;

                    // this is for grain migration
                    foreach (var grain in grainList)
                    {
                        if (maxUnCommittedGlobalBidPerGrain.ContainsKey(grain) == false ||
                            maxUnCommittedGlobalBidPerGrain[grain] < globalBid)
                            maxUnCommittedGlobalBidPerGrain[grain] = globalBid;
                         
                        RegisterATransaction(grain);
                    }

                    // send corresponding grainAccessInfo to local coordinators in different silos
                    if (grainListPerSilo.ContainsKey(RuntimeIdentity) == false)
                        throw new Exception($"NewTransaction: RuntimeIdentity {RuntimeIdentity} is not in grainListPerSilo");
                    Task<Tuple<long, long, long, long, long, long>> task = null;
                    for (int i = 0; i < siloList.Count; i++)
                    {
                        var siloAddress = siloList[i];
                        var coordID = siloIDToLocalCoordID[siloAddress];

                        // get local tid, bid from local coordinator
                        var localCoord = GrainFactory.GetGrain<ILocalCoordGrain>(coordID);
                        if (siloAddress == RuntimeIdentity) task = localCoord.NewTemporaryLocalPACT(globalBid, globalTid, grainListPerSilo[siloAddress]);
                        else _ = localCoord.NewTemporaryLocalPACT(globalBid, globalTid, grainListPerSilo[siloAddress]);
                    }

                    if (task == null) throw new Exception($"NewTransaction: task is null");
                    
                    var localInfo = await task;
                    CleanUp(localInfo.Item3, localInfo.Item4);
                    var cxt = new MyTransactionContext(localInfo.Item1, localInfo.Item2, globalBid, globalTid);
                    return new Tuple<MyTransactionContext, long, long>(cxt, localInfo.Item5, localInfo.Item6);
                }
            }
            
            var grainStringList = new List<string>();
            foreach (var grain in grainList) grainStringList.Add(grain.ToString());
            var info = await myLocalCoord.NewRegularLocalPACT(grainStringList);
            
            // this is for grain migration
            foreach (var grain in grainList)
            {
                if (maxUnCommittedLocalBidPerGrain.ContainsKey(grain) == false ||
                    maxUnCommittedLocalBidPerGrain[grain] < info.Item1)
                    maxUnCommittedLocalBidPerGrain[grain] = info.Item1;
                
                RegisterATransaction(grain);
            }

            CleanUp(info.Item3, info.Item4);
            var cxt1 = new MyTransactionContext(info.Item1, info.Item2);
            return new Tuple<MyTransactionContext, long, long>(cxt1, info.Item5, info.Item6);
        }
    }
}