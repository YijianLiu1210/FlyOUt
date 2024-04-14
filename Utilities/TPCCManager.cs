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

        public static Dictionary<string, Dictionary<string, HashSet<int>>> CalculateGrainPlacement(int numLocalSilo, List<string> registeredSilo, Dictionary<string, string> grainNames)
        {
            var itemGrainName = grainNames["itemGrainName"];
            var warehouseGrainName = grainNames["warehouseGrainName"];
            var districtGrainName = grainNames["districtGrainName"];
            var customerGrainName = grainNames["customerGrainName"];
            var stockGrainName = grainNames["stockGrainName"];
            var orderGrainName = grainNames["orderGrainName"];

            var grainsPerSilo = new Dictionary<string, Dictionary<string, HashSet<int>>>();

            for (var siloID = 0; siloID < numLocalSilo; siloID++)
            {
                var silo = registeredSilo[siloID];
                grainsPerSilo.Add(silo, new Dictionary<string, HashSet<int>>
                {
                    { itemGrainName, new HashSet<int>() },
                    { warehouseGrainName, new HashSet<int>() },
                    { districtGrainName, new HashSet<int>() },
                    { customerGrainName, new HashSet<int>() },
                    { stockGrainName, new HashSet<int>() },
                    { orderGrainName, new HashSet<int>() },
                });

                // STEP 1: calculate IDs of all grains in the target silo
                var minWarehouseID = siloID * Constants.NUM_W_PER_SILO;
                for (var W_ID = minWarehouseID; W_ID < minWarehouseID + Constants.NUM_W_PER_SILO; W_ID++)
                {
                    // ItemGrain
                    grainsPerSilo[silo][itemGrainName].Add(GetItemGrain(W_ID));

                    // WarehouseGrain
                    grainsPerSilo[silo][warehouseGrainName].Add(GetWarehouseGrain(W_ID));

                    // DistrictGrain and CustomerGrain
                    for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                    {
                        grainsPerSilo[silo][districtGrainName].Add(GetDistrictGrain(W_ID, D_ID));
                        grainsPerSilo[silo][customerGrainName].Add(GetCustomerGrain(W_ID, D_ID));
                    }

                    // StockGrain
                    for (int i = 0; i < Constants.NUM_StockGrain_PER_W; i++)
                    {
                        var stockGrainID = W_ID * NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + i;
                        grainsPerSilo[silo][stockGrainName].Add(stockGrainID);
                    }

                    // OrderGrain
                    for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                    {
                        for (int i = 0; i < NUM_OrderGrain_PER_D; i++)
                        {
                            var id = W_ID * NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + D_ID * NUM_OrderGrain_PER_D + i;
                            grainsPerSilo[silo][orderGrainName].Add(id);
                        }
                    }
                }
            }

            return grainsPerSilo;
        }
    }
}