using System.Collections.Generic;
using System.Linq;

namespace Utilities
{
    public static class TPCCManager
    {
        public static int NUM_OrderGrain_PER_D = 1;
        public static int NUM_GRAIN_PER_W = 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + Constants.NUM_D_PER_W * NUM_OrderGrain_PER_D;

        // 1 ItemGrain + 1 WarehouseGrain + 10 DistrictGrain + 10 CustomerGrain + xx StockGrain + yy OrderGrain

        public static int GetItemGrain(int W_ID)
        {
            return W_ID * NUM_GRAIN_PER_W;
        }

        public static int GetWarehouseGrain(int W_ID)
        {
            return W_ID * NUM_GRAIN_PER_W + 1;
        }

        public static int GetDistrictGrain(int W_ID, int D_ID)
        {
            return W_ID * NUM_GRAIN_PER_W + 1 + 1 + D_ID;
        }

        public static int GetCustomerGrain(int W_ID, int D_ID)
        {
            return W_ID * NUM_GRAIN_PER_W + 1 + 1 + 10 + D_ID;
        }

        public static int GetStockGrain(int W_ID, int I_ID)
        {
            return W_ID * NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + I_ID / (Constants.NUM_I / Constants.NUM_StockGrain_PER_W);
        }

        public static int GetOrderGrain(int W_ID, int D_ID, int C_ID)
        {
            return W_ID * NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + D_ID * NUM_OrderGrain_PER_D + C_ID / (Constants.NUM_C_PER_D / NUM_OrderGrain_PER_D);
        }

        /// <summary> check if these warehouses locate in the same silo </summary>
        public static bool IsInSameSilo(HashSet<int> W_IDs) => W_IDs.Select(id => id / Constants.NUM_W_PER_SILO).ToHashSet().Count == 1; 
    }
}