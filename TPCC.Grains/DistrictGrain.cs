using Utilities;
using TPCC.Interfaces;
using Concurrency.Interface.GrainPlacement;
using Concurrency.Interface.Logging;
using StackExchange.Redis;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Implementation.TransactionExecution;
using MessagePack;

namespace TPCC.Grains
{
    [MessagePackObject]
    [Serializable]
    public class DistrictInfo : ICloneable, IPrintable
    {
        [Key(0)]
        public int W_ID;
        [Key(1)]
        public District district;  // key: I_ID

        public DistrictInfo()
        {
        }

        public DistrictInfo(DistrictInfo district_info)
        {
            W_ID = district_info.W_ID;
            district = district_info.district;
        }

        public string PrintState()
        {
            throw new NotImplementedException();
        }

        object ICloneable.Clone()
        {
            return new DistrictInfo(this);
        }
    }

    // each DistrictGrain only represents one district in an warehouse
    public class DistrictGrain : TransactionExecutionGrain<DistrictInfo>, IDistrictGrain
    {
        public DistrictGrain(ILoggingProtocol log, IGrainPlacementCache grainPlacementInfo, IConnectionMultiplexer redis) : base(log, grainPlacementInfo, redis)
        {
        }

        // input: W_ID, D_ID
        // output: null
        public async Task<TransactionResult> Init(MyTransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var input = (Tuple<int, int>)funcInput;   // W_ID, D_ID
                var myState = await GetState(context, AccessMode.ReadWrite);
                myState.district = InMemoryDataGenerator.GenerateDistrictInfo(input.Item2);
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        // input: null
        // output: Tuple<float, long>    D_TAX, O_ID
        public async Task<TransactionResult> GetDTax(MyTransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var myState = await GetState(context, AccessMode.Read);
                var O_ID = myState.district.D_NEXT_O_ID++;
                res.resultObj = new Tuple<float, long>(myState.district.D_TAX, O_ID);
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }
    }
}