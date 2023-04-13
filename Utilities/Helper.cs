using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace Utilities
{
    public static class Helper
    {
        public static int GetNumCPUForGlobalSilo(int numLocalSilo)
        {
            return numLocalSilo;
            var cpu = 2;
            for (int i = numLocalSilo; i > 2; i /= 2) cpu += 2;
            return cpu;
        }

        public static List<string> GetLocalSiloList(IDatabase siloInfo_db)
        {
            // get the list of all registered local silo
            var index = 0;
            var registeredSilo = new List<string>();
            var siloAddress = siloInfo_db.ListGetByIndex("LocalSiloList", index);
            while (siloAddress != RedisValue.Null)
            {
                registeredSilo.Add(siloAddress);
                index++;
                siloAddress = siloInfo_db.ListGetByIndex("LocalSiloList", index);
            }
            return registeredSilo;
        }

        // the ID is the index in the LocalSiloList
        // the grain list is the initial list when start the whole system
        public static List<Guid> GetGrainsOfSilo(int siloID) 
        {
            var grains = new List<Guid>();
            var firstGrainID = siloID * Constants.numGrainPerLocalSilo;
            for (int i = 0; i < Constants.numGrainPerLocalSilo; i++)
                grains.Add(ConvertIntToGuid(firstGrainID + i));
            return grains;
        }

        public static int MapGuidToServiceID(Guid guid, int numService)
        {
            try
            {
                var intID = Helper.ConvertGuidToInt(guid);
                return intID % numService;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}, {e.StackTrace}");
                throw;
            }
        }

        public static void SetCPU(string processName, int numCPU)
        {
            if (Constants.isLocalTest) return;
            Console.WriteLine($"Set processor affinity for {processName}, numCPU = {numCPU}");
            var processes = Process.GetProcessesByName(processName);
            Debug.Assert(processes.Length == 1);

            var str = GetProcessorAffinityString(numCPU);
            var serverProcessorAffinity = Convert.ToInt64(str, 2);     // server uses the highest n bits

            processes[0].ProcessorAffinity = (IntPtr)serverProcessorAffinity;
        }

        static string GetProcessorAffinityString(int numCPU)
        {
            var str = "";
            Debug.Assert(numCPU <= Environment.ProcessorCount);

            for (int i = 0; i < Environment.ProcessorCount; i++)
            {
                if (i < numCPU) str += "1";
                else str += "0";
            }
            return str;
        }

        public static string GetLocalIPAddress()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
                if (ip.AddressFamily == AddressFamily.InterNetwork) return ip.ToString();

            throw new Exception("No network adapters with an IPv4 address in the system!");
        }

        public static string GetPublicIPAddress() => new WebClient().DownloadString("https://ipv4.icanhazip.com/").TrimEnd();
        
        public static string ChangeFormat(double n, int num)
        {
            if (double.IsNaN(n)) return "NaN";
            return Math.Round(n, num).ToString().Replace(',', '.');
        }

        public static Guid ConvertIntToGuid(int id) => new Guid(id.ToString().PadLeft(32, '0'));
        
        public static int ConvertGuidToInt(Guid id)
        {
            var str = id.ToString("N");
            var index = 0;
            while (index < str.Length && str[index] == '0') index++;
            if (index == str.Length) return 0;
            else return int.Parse(str.Substring(index));
        }

        public static string SiloStringToRuntimeID(string silo) => "S" + silo.Replace("@", ":");
    }
}