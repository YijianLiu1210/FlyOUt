using System;
using Utilities;
using System.IO;
using System.Threading;
using System.Diagnostics;
using Orleans.Concurrency;
using System.Threading.Tasks;
using Concurrency.Interface.Logging;
using MessagePack;

namespace Concurrency.Implementation.Logging
{
    [Reentrant]
    public class SnapperLogger : ISnapperLogger
    {
        int index;
        byte[] buffer;
        int maxBufferSize;
        int numWaitLog;
        TaskCompletionSource waitFlush;

        FileStream fileWriter;
        string fileName;
        SemaphoreSlim fileLock;

        public SnapperLogger(string name)
        {
            if (Directory.Exists(Constants.logPath) == false) Directory.CreateDirectory(Constants.logPath);
            Init(name);
        }
        
        public SnapperLogger(int siloID, LogContentType type, int id)
        {
            Init($"Silo-{siloID}-{type}-{id}");
        }

        void Init(string name)
        {
            if (Constants.loggingBatching)
            {
                index = 0;
                maxBufferSize = 15000;
                buffer = new byte[maxBufferSize];
                waitFlush = new TaskCompletionSource();
            }

            fileLock = new SemaphoreSlim(1);
            fileName = Constants.logPath + name;
            fileWriter = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.Read);
        }

        public async Task ClearLogFile()
        {
            await fileLock.WaitAsync();
            File.Delete(fileName);
            fileWriter = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.Read);
            fileLock.Release();
        }

        public async Task Write(byte[] value)
        {
            if (!Constants.loggingBatching)
            {
                await fileLock.WaitAsync();
                var sizeBytes = BitConverter.GetBytes(value.Length);
                await fileWriter.WriteAsync(sizeBytes, 0, sizeBytes.Length);
                await fileWriter.WriteAsync(value, 0, value.Length);
                await fileWriter.FlushAsync();
                fileLock.Release();
            }
            else
            {
                await fileLock.WaitAsync();

                // STEP 1: add log to buffer
                var sizeBytes = BitConverter.GetBytes(value.Length);
                Debug.Assert(index + sizeBytes.Length + value.Length <= maxBufferSize);
                Buffer.BlockCopy(sizeBytes, 0, buffer, index, sizeBytes.Length);
                index += sizeBytes.Length;
                Buffer.BlockCopy(value, 0, buffer, index, value.Length);
                index += value.Length;
                numWaitLog++;

                // STEP 2: check if need to flush
                if (numWaitLog == Constants.loggingBatchSize)
                {
                    await Flush();
                    fileLock.Release();
                }
                else
                {
                    fileLock.Release();
                    await waitFlush.Task;
                }
            }
        }

        async Task Flush()
        {
            await fileWriter.WriteAsync(buffer, 0, index);
            await fileWriter.FlushAsync();

            index = 0;
            numWaitLog = 0;
            buffer = new byte[maxBufferSize];

            waitFlush.SetResult();
            waitFlush = new TaskCompletionSource();
        }

        public async Task<Tuple<DateTime, byte[]>> ReadGrainState(GrainID grainID, long lastCommittedLocalBid)
        {
            Debug.Assert(fileName.Contains($"{LogContentType.LocalBatchComplete}"));
            await fileLock.WaitAsync();

            var timestamp = DateTime.MinValue;
            byte[] state = null;
            var fileReader = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Write);
            while (fileReader.Position < fileReader.Length)
            {
                var logBytes = await ReadOneLog(fileReader);
                var log = MessagePackSerializer.Deserialize<LogFormat>(logBytes);
                Debug.Assert(log.logType == LogContentType.LocalBatchComplete);
                var logContent = MessagePackSerializer.Deserialize<LocalBatchCompleteLog>(log.logContent);
                if (logContent.grainID.Equals(grainID) && logContent.localBid == lastCommittedLocalBid)
                {
                    timestamp = logContent.timestamp;
                    state = logContent.state;
                    break;
                }
            }
            fileReader.Close();

            fileLock.Release();

            return new Tuple<DateTime, byte[]>(timestamp , state);
        }

        public async Task<Tuple<DateTime, byte[]>> ReadGrainState(GrainID grainID)
        {
            Debug.Assert(fileName.Contains($"{LogContentType.Prepare}"));
            await fileLock.WaitAsync();

            var timestamp = DateTime.MinValue;
            byte[] state = null;
            var fileReader = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Write);
            while (fileReader.Position < fileReader.Length)
            {
                var logBytes = await ReadOneLog(fileReader);
                var log = MessagePackSerializer.Deserialize<LogFormat>(logBytes);
                Debug.Assert(log.logType == LogContentType.Prepare);
                var logContent = MessagePackSerializer.Deserialize<PrepareLog>(log.logContent);
                if (logContent.grainID.Equals(grainID))
                {
                    if (logContent.timestamp > timestamp)     // find the record that has the max timestamp
                    {
                        timestamp = logContent.timestamp;
                        state = logContent.state;
                    }
                }
            }
            fileReader.Close();

            fileLock.Release();

            return new Tuple<DateTime, byte[]>(timestamp, state);
        }

        async Task<byte[]> ReadOneLog(FileStream reader)
        {
            var sizeBytes = new byte[sizeof(int)];
            await reader.ReadAsync(sizeBytes, 0, sizeof(int));
            var size = BitConverter.ToInt32(sizeBytes);
            var logBytes = new byte[size];
            await reader.ReadAsync(logBytes, 0, size);

            return logBytes;
        }
    }
}