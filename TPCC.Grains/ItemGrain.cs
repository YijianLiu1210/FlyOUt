using Utilities;
using TPCC.Interfaces;
using System.Diagnostics;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Interface.GrainPlacement;
using Concurrency.Interface.Logging;
using StackExchange.Redis;
using Concurrency.Implementation.TransactionExecution;
using Orleans;
using MessagePack;

namespace TPCC.Grains
{
    [MessagePackObject]
    [Serializable]
    public class ItemTable : ICloneable, IPrintable
    {
        [Key(0)]
        public Dictionary<int, Item> items;  // key: I_ID

        public ItemTable()
        {
            items = new Dictionary<int, Item>();
        }

        public ItemTable(ItemTable warehouse)
        {
            items = warehouse.items;
        }

        public string PrintState()
        {
            throw new NotImplementedException();
        }

        object ICloneable.Clone()
        {
            return new ItemTable(this);
        }
    }

    public class ItemGrain : TransactionExecutionGrain<ItemTable>, IItemGrain
    {
        public ItemGrain(ILoggingProtocol log, IGrainPlacementCache grainPlacementInfo, IConnectionMultiplexer redis) : base(log, grainPlacementInfo, redis)
        {
        }

        // input, output: null
        public async Task<TransactionResult> Init(MyTransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var myState = await GetState(context, AccessMode.ReadWrite);
                if (myState.items.Count == 0) myState.items = InMemoryDataGenerator.GenerateItemTable();
                else Debug.Assert(myState.items.Count == Constants.NUM_I);
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        // input: List<int> (item IDs)
        // output: Dictionary<int, float> (I_ID, item price)
        public async Task<TransactionResult> GetItemsPrice(MyTransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var item_ids = (List<int>)funcInput;
                var item_prices = new Dictionary<int, float>();  // <I_ID, price>
                var myState = await GetState(context, AccessMode.Read);
                
                foreach (var id in item_ids)
                {
                    if (myState.items.ContainsKey(id)) item_prices.Add(id, myState.items[id].I_PRICE);
                    else throw new Exception("Exception: invalid I_ID");
                }
                res.resultObj = item_prices;
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }
    }
}