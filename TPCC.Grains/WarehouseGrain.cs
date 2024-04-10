using Utilities;
using TPCC.Interfaces;
using Concurrency.Interface.GrainPlacement;
using Concurrency.Interface.Logging;
using StackExchange.Redis;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface.TransactionExecution;
using MessagePack;

namespace TPCC.Grains
{
    [MessagePackObject]
    [Serializable]
    public class WarehouseInfo : ICloneable, IPrintable
    {
        [Key(0)]
        public Warehouse warehouse;

        public WarehouseInfo()
        {
        }

        public WarehouseInfo(WarehouseInfo warehouse_info)
        {
            warehouse = warehouse_info.warehouse;
        }

        public string PrintState()
        {
            throw new NotImplementedException();
        }

        object ICloneable.Clone()
        {
            return new WarehouseInfo(this);
        }
    }

    public class WarehouseGrain : TransactionExecutionGrain<WarehouseInfo>, IWarehouseGrain
    {
        public WarehouseGrain(ILoggingProtocol log, IGrainPlacementCache grainPlacementInfo, IConnectionMultiplexer redis) : base(log, grainPlacementInfo, redis)
        {
        }

        // input: int     W_ID
        // output: null
        public async Task<TransactionResult> Init(MyTransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var W_ID = (int)funcInput;   // W_ID
                var myState = await GetState(context, AccessMode.ReadWrite);
                myState.warehouse = InMemoryDataGenerator.GenerateWarehouseInfo(W_ID);
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        // input: null
        // output: float    D_TAX
        public async Task<TransactionResult> GetWTax(MyTransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var myState = await GetState(context, AccessMode.Read);
                //Console.WriteLine($"RO grain: {this.myID.className} || {Helper.ConvertGuidToInt(this.myID.id)}");
                res.resultObj = myState.warehouse.W_TAX;
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }
    }
}