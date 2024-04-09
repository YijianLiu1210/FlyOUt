using MessagePack;

namespace TPCC.Grains
{
    public enum AllTxnTypes { Init, NewOrder, GetDTax, GetItemsPrice, AddNewOrder, UpdateStock, GetWTax };

    [MessagePackObject]
    [Serializable]
    public class Warehouse
    {
        [Key(0)]
        public int W_ID;        // primary key
        [Key(1)]
        public string W_NAME;
        [Key(2)]
        public string W_STREET_1;
        [Key(3)]
        public string W_STREET_2;
        [Key(4)]
        public string W_CITY;
        [Key(5)]
        public string W_STATE;
        [Key(6)]
        public string W_ZIP;
        [Key(7)]
        public float W_TAX;
        [Key(8)]
        public float W_YTD;

        public Warehouse(int W_ID, string W_NAME, string W_STREET_1, string W_STREET_2, string W_CITY, string W_STATE, string W_ZIP, float W_TAX, float W_YTD)
        {
            this.W_ID = W_ID;
            this.W_NAME = W_NAME;
            this.W_STREET_1 = W_STREET_1;
            this.W_STREET_2 = W_STREET_2;
            this.W_CITY = W_CITY;
            this.W_STATE = W_STATE;
            this.W_ZIP = W_ZIP;
            this.W_TAX = W_TAX;
            this.W_YTD = W_YTD;
        }
    }

    [MessagePackObject]
    [Serializable]
    public class District
    {
        [Key(0)]
        public int D_ID;  // primary key

        [Key(1)]
        public string D_NAME;
        [Key(2)]
        public string D_STREET_1;
        [Key(3)]
        public string D_STREET_2;
        [Key(4)]
        public string D_CITY;
        [Key(5)]
        public string D_STATE;
        [Key(6)]
        public string D_ZIP;
        [Key(7)]
        public float D_TAX;
        [Key(8)]
        public float D_YTD;
        [Key(9)]
        public long D_NEXT_O_ID;

        public District(int D_ID, string D_NAME, string D_STREET_1, string D_STREET_2, string D_CITY, string D_STATE, string D_ZIP, float D_TAX, float D_YTD, long D_NEXT_O_ID)
        {
            this.D_ID = D_ID;
            this.D_NAME = D_NAME;
            this.D_STREET_1 = D_STREET_1;
            this.D_STREET_2 = D_STREET_2;
            this.D_CITY = D_CITY;
            this.D_STATE = D_STATE;
            this.D_ZIP = D_ZIP;
            this.D_TAX = D_TAX;
            this.D_YTD = D_YTD;
            this.D_NEXT_O_ID = D_NEXT_O_ID;
        }
    }

    [MessagePackObject]
    [Serializable]
    public class Customer
    {
        [Key(0)]
        public int C_ID;  // primary key

        [Key(1)]
        public string C_FIRST;
        [Key(2)]
        public string C_MIDDLE;
        [Key(3)]
        public string C_LAST;
        [Key(4)]
        public string C_STREET_1;
        [Key(5)]
        public string C_STREET_2;
        [Key(6)]
        public string C_CITY;
        [Key(7)]
        public string C_STATE;
        [Key(8)]
        public string C_ZIP;
        [Key(9)]
        public string C_PHONE;
        [Key(10)]
        public DateTime C_SINCE;
        [Key(11)]
        public string C_CREDIT;
        [Key(12)]
        public float C_CREDIT_LIM;
        [Key(13)]
        public float C_DISCOUNT;
        [Key(14)]
        public float C_BALANCE;
        [Key(15)]
        public float C_YTD_PAYMENT;
        [Key(16)]
        public int C_PAYMENT_CNT;
        [Key(17)]
        public int C_DELIVERY_CNT;
        [Key(18)]
        public string C_DATA;

        public Customer(int C_ID, string C_FIRST, string C_MIDDLE, string C_LAST, string C_STREET_1, string C_STREET_2, string C_CITY, string C_STATE, string C_ZIP, string C_PHONE, DateTime C_SINCE, string C_CREDIT, float C_CREDIT_LIM, float C_DISCOUNT, float C_BALANCE, float C_YTD_PAYMENT, int C_PAYMENT_CNT, int C_DELIVERY_CNT, string C_DATA)
        {
            this.C_ID = C_ID;
            this.C_FIRST = C_FIRST;
            this.C_MIDDLE = C_MIDDLE;
            this.C_LAST = C_LAST;
            this.C_STREET_1 = C_STREET_1;
            this.C_STREET_2 = C_STREET_2;
            this.C_CITY = C_CITY;
            this.C_STATE = C_STATE;
            this.C_ZIP = C_ZIP;
            this.C_PHONE = C_PHONE;
            this.C_SINCE = C_SINCE;
            this.C_CREDIT = C_CREDIT;
            this.C_CREDIT_LIM = C_CREDIT_LIM;
            this.C_DISCOUNT = C_DISCOUNT;
            this.C_BALANCE = C_BALANCE;
            this.C_YTD_PAYMENT = C_YTD_PAYMENT;
            this.C_PAYMENT_CNT = C_PAYMENT_CNT;
            this.C_DELIVERY_CNT = C_DELIVERY_CNT;
            this.C_DATA = C_DATA;
        }
    }

    [MessagePackObject]
    [Serializable]
    public class Item
    {
        [Key(0)]
        public int I_ID;   // primary key
        [Key(1)]
        public int I_IM_ID;
        [Key(2)]
        public string I_NAME;
        [Key(3)]
        public float I_PRICE;
        [Key(4)]
        public string I_DATA;

        public Item(int I_ID, int I_IM_ID, string I_NAME, float I_PRICE, string I_DATA)
        {
            this.I_ID = I_ID;
            this.I_IM_ID = I_IM_ID;
            this.I_NAME = I_NAME;
            this.I_PRICE = I_PRICE;
            this.I_DATA = I_DATA;
        }
    }

    [MessagePackObject]
    [Serializable]
    public class History
    {
        // no primary key
        [Key(0)]
        public int H_C_ID;
        [Key(1)]
        public int H_C_D_ID;
        [Key(2)]
        public int H_C_W_ID;
        [Key(3)]
        public int H_D_ID;
        [Key(4)]
        public int H_W_ID;
        [Key(5)]
        public DateTime H_DATE;
        [Key(6)]
        public float H_AMOUNT;
        [Key(7)]
        public string H_DATA;

        public History(int H_C_ID, int H_C_D_ID, int H_C_W_ID, int H_D_ID, int H_W_ID, DateTime H_DATE, float H_AMOUNT, string H_DATA)
        {
            this.H_C_ID = H_C_ID;
            this.H_C_D_ID = H_C_D_ID;
            this.H_C_W_ID = H_C_W_ID;
            this.H_D_ID = H_D_ID;
            this.H_W_ID = H_W_ID;
            this.H_DATE = H_DATE;
            this.H_AMOUNT = H_AMOUNT;
            this.H_DATA = H_DATA;
        }
    }

    [MessagePackObject]
    [Serializable]
    public class NewOrder
    {
        [Key(0)]
        public long NO_O_ID;  // primary key

        public NewOrder(long NO_O_ID)
        {
            this.NO_O_ID = NO_O_ID;
        }
    }

    [MessagePackObject]
    [Serializable]
    public class Order
    {
        [Key(0)]
        public long O_ID;  // primary key

        [Key(1)]
        public int O_C_ID;
        [Key(2)]
        public DateTime O_ENTRY_D;
        [Key(3)]
        public int O_CARRIER_ID;
        [Key(4)]
        public int O_OL_CNT;
        [Key(5)]
        public bool O_ALL_LOCAL;

        public Order(long O_ID, int O_C_ID, DateTime O_ENTRY_D, object O_CARRIER_ID, int O_OL_CNT, bool O_ALL_LOCAL)
        {
            this.O_ID = O_ID;
            this.O_C_ID = O_C_ID;
            this.O_ENTRY_D = O_ENTRY_D;
            if (O_CARRIER_ID != null) this.O_CARRIER_ID = (int)O_CARRIER_ID;
            this.O_OL_CNT = O_OL_CNT;
            this.O_ALL_LOCAL = O_ALL_LOCAL;
        }
    }

    [MessagePackObject]
    [Serializable]
    public class OrderLine
    {
        // primary key
        [Key(0)]
        public long OL_O_ID;
        [Key(1)]
        public int OL_NUMBER;

        [Key(2)]
        public int OL_I_ID;
        [Key(3)]
        public int OL_SUPPLY_W_ID;
        [Key(4)]
        public DateTime OL_DELIVERY_D;
        [Key(5)]
        public int OL_QUANTITY;
        [Key(6)]
        public float OL_AMOUNT;
        [Key(7)]
        public string OL_DIST_INFO;

        public OrderLine(long OL_O_ID, int OL_NUMBER, int OL_I_ID, int OL_SUPPLY_W_ID, object OL_DELIVERY_D, int OL_QUANTITY, float OL_AMOUNT, string OL_DIST_INFO)
        {
            this.OL_O_ID = OL_O_ID;
            this.OL_NUMBER = OL_NUMBER;
            this.OL_I_ID = OL_I_ID;
            this.OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
            if (OL_DELIVERY_D != null) this.OL_DELIVERY_D = (DateTime)OL_DELIVERY_D;
            this.OL_QUANTITY = OL_QUANTITY;
            this.OL_AMOUNT = OL_AMOUNT;
            this.OL_DIST_INFO = OL_DIST_INFO;
        }
    }

    [MessagePackObject]
    [Serializable]
    public class Stock
    {
        [Key(0)]
        public int S_I_ID;  // primary key

        [Key(1)]
        public int S_QUANTITY;
        [Key(2)]
        public Dictionary<int, string> S_DIST;
        [Key(3)]
        public int S_YTD;
        [Key(4)]
        public int S_ORDER_CNT;
        [Key(5)]
        public int S_REMOTE_CNT;
        [Key(6)]
        public string S_DATA;

        public Stock(int S_I_ID, int S_QUANTITY, Dictionary<int, string> S_DIST, int S_YTD, int S_ORDER_CNT, int S_REMOTE_CNT, string S_DATA)
        {
            this.S_I_ID = S_I_ID;
            this.S_QUANTITY = S_QUANTITY;
            this.S_DIST = S_DIST;
            this.S_YTD = S_YTD;
            this.S_ORDER_CNT = S_ORDER_CNT;
            this.S_REMOTE_CNT = S_REMOTE_CNT;
            this.S_DATA = S_DATA;
        }
    }
}