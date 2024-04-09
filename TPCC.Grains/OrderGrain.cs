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
    public class OrderInfo
    {
        [Key(0)]
        public Order order;
        [Key(1)]
        public List<OrderLine> orderlines;

        public OrderInfo(Order order, List<OrderLine> orderlines)
        {
            this.order = order;
            this.orderlines = orderlines;
        }
    }

    [MessagePackObject]
    [Serializable]
    public class OrderData : ICloneable, IPrintable
    {
        [Key(0)]
        public int W_ID;
        [Key(1)]
        public int D_ID;
        [Key(2)]
        public int OrderGrainID;
        [Key(3)]
        public List<long> neworder;
        [Key(4)]
        public Dictionary<long, Order> order_table;                      // key: O_ID
        [Key(5)]
        public Dictionary<Tuple<long, int>, OrderLine> orderline_table;  // key: <O_ID, number of items in the order>

        public OrderData()
        {
            neworder = new List<long>();
            order_table = new Dictionary<long, Order>();
            orderline_table = new Dictionary<Tuple<long, int>, OrderLine>();
        }

        public OrderData(OrderData orderdata)
        {
            W_ID = orderdata.W_ID;
            D_ID = orderdata.D_ID;
            OrderGrainID = orderdata.OrderGrainID;
            neworder = new List<long>(orderdata.neworder);
            order_table = new Dictionary<long, Order>(orderdata.order_table);
            orderline_table = new Dictionary<Tuple<long, int>, OrderLine>(orderdata.orderline_table);
        }

        public string PrintState()
        {
            throw new NotImplementedException();
        }

        object ICloneable.Clone()
        {
            return new OrderData(this);
        }
    }

    public class OrderGrain : TransactionExecutionGrain<OrderData>, IOrderGrain
    {
        public OrderGrain(ILoggingProtocol log, IGrainPlacementCache grainPlacementInfo, IConnectionMultiplexer redis) : base(log, grainPlacementInfo, redis)
        {
        }

        // input: Tuple<int, int, int>    W_ID, D_ID, OrderGrain index within the district
        // output: null
        public async Task<TransactionResult> Init(MyTransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var input = (Tuple<int, int, int>)funcInput;    // W_ID, D_ID, OrderGrain index within the district
                var myState = await GetState(context, AccessMode.ReadWrite);
                myState.W_ID = input.Item1;
                myState.D_ID = input.Item2;
                myState.OrderGrainID = input.Item3;
                myState.neworder = new List<long>();
                myState.order_table = new Dictionary<long, Order>();
                myState.orderline_table = new Dictionary<Tuple<long, int>, OrderLine>();
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        public async Task<TransactionResult> AddNewOrder(MyTransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                if (funcInput == null) throw new Exception("Exception: input data is null. ");
                var input = (OrderInfo)funcInput;
                var O_ID = input.order.O_ID;
                var myState = await GetState(context, AccessMode.ReadWrite);
                Debug.Assert(myState.neworder.Contains(O_ID) == false);

                // only do it for local test
                if (Constants.isLocalTest)
                {
                    myState.neworder.Clear();
                    myState.orderline_table.Clear();
                    myState.orderline_table.Clear();
                }

                myState.neworder.Add(O_ID);
                myState.order_table.Add(O_ID, input.order);
                foreach (var orderline in input.orderlines)
                {
                    var num = orderline.OL_NUMBER;
                    myState.orderline_table.Add(new Tuple<long, int>(O_ID, num), orderline);
                }
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }
    }
}