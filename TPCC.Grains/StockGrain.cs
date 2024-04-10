using Utilities;
using TPCC.Interfaces;
using System.Diagnostics;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Interface.GrainPlacement;
using Concurrency.Interface.Logging;
using StackExchange.Redis;
using Concurrency.Implementation.TransactionExecution;
using MessagePack;

namespace TPCC.Grains
{
    [MessagePackObject]
    [Serializable]
    public class UpdateStockInput
    {
        [Key(0)]
        public int W_ID;
        [Key(1)]
        public int D_ID;
        [Key(2)]
        public bool isRemote;
        [Key(3)]
        public Dictionary<int, int> itemsToBuy;   // <I_ID, I_QUANTITY>

        public UpdateStockInput(int W_ID, int D_ID, bool isRemote, Dictionary<int, int> itemsToBuy)
        {
            this.W_ID = W_ID;
            this.D_ID = D_ID;
            this.isRemote = isRemote;
            this.itemsToBuy = itemsToBuy;
        }
    }

    [MessagePackObject]
    [Serializable]
    public class StockTable : ICloneable, IPrintable
    {
        [Key(0)]
        public int W_ID;
        [Key(1)]
        public Dictionary<int, Stock> stock;  // key: I_ID

        public StockTable()
        {
            stock = new Dictionary<int, Stock>();
        }

        public StockTable(StockTable stock_table)
        {
            W_ID = stock_table.W_ID;
            stock = stock_table.stock;
        }

        public string PrintState()
        {
            throw new NotImplementedException();
        }

        object ICloneable.Clone()
        {
            return new StockTable(this);
        }
    }

    public class StockGrain : TransactionExecutionGrain<StockTable>, IStockGrain
    {
        public StockGrain(ILoggingProtocol log, IGrainPlacementCache grainPlacementInfo, IConnectionMultiplexer redis) : base(log, grainPlacementInfo, redis)
        {
        }

        // input: Tuple<int, int>     W_ID, StockGrain index within the warehouse
        // output: null
        public async Task<TransactionResult> Init(MyTransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var input = (Tuple<int, int>)funcInput;    // W_ID, StockGrain index within the warehouse
                var myState = await GetState(context, AccessMode.ReadWrite);
                myState.W_ID = input.Item1;
                myState.stock = InMemoryDataGenerator.GenerateStockTable(input.Item2);
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        // input: UpdateStockInput   W_ID, D_ID, isRemote, <I_ID, I_QUANTITY>
        // output: Dictionary<int, string>    <I_ID, S_DIST_xx info>
        public async Task<TransactionResult> UpdateStock(MyTransactionContext context, object funcInput)
        {
            var ret = new TransactionResult();
            var result = new Dictionary<int, string>();
            try
            {
                var input = (UpdateStockInput)funcInput;   // W_ID, D_ID, isRemote, <I_ID, I_QUANTITY>
                var W_ID = input.W_ID;
                var D_ID = input.D_ID;
                var remoteFlag = input.isRemote ? 1 : 0;
                var items = input.itemsToBuy;
                if (items.Count == 0) throw new Exception("Exception: no items to buy");
                var myState = await GetState(context, AccessMode.ReadWrite);
                
                if (remoteFlag == 1) Debug.Assert(W_ID != myState.W_ID);
                else Debug.Assert(W_ID == myState.W_ID);
                foreach (var item in items)
                {
                    var I_ID = item.Key;
                    var quantity = item.Value;

                    Debug.Assert(myState.stock.ContainsKey(I_ID));
                    var the_stock = myState.stock[I_ID];
                    var S_QUANTITY = the_stock.S_QUANTITY;
                    if (S_QUANTITY - quantity >= 10) S_QUANTITY -= quantity;
                    else S_QUANTITY += 91 - quantity;

                    the_stock.S_YTD += quantity;
                    the_stock.S_ORDER_CNT++;
                    the_stock.S_REMOTE_CNT += remoteFlag;

                    var S_DIST = the_stock.S_DIST[D_ID];
                    result.Add(I_ID, S_DIST);
                }
                ret.resultObj = result;
            }
            catch (Exception)
            {
                ret.exception = true;
                if (context.localBid != -1) await GetState(context, AccessMode.ReadWrite);
            }
            return ret;
        }
    }
}