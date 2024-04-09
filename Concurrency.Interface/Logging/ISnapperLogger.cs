using System;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Interface.Logging
{
    public interface ISnapperLogger
    {
        Task ClearLogFile();
        Task Write(byte[] value);
        Task<Tuple<DateTime, byte[]>> ReadGrainState(GrainID grainID);
        Task<Tuple<DateTime, byte[]>> ReadGrainState(GrainID grainID, long lastCommittedLocalBid);
    }
}