using System;
using System.Threading.Tasks;

namespace Concurrency.Interface.Logging
{
    public interface ISnapperLogger
    {
        Task ClearLogFile();
        Task Write(byte[] value);
        Task<Tuple<DateTime, byte[]>> ReadGrainState(Guid grainID);
        Task<Tuple<DateTime, byte[]>> ReadGrainState(Guid grainID, long lastCommittedLocalBid);
    }
}