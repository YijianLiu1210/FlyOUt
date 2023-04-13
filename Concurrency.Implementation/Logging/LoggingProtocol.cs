using Concurrency.Interface.Logging;
using MessagePack;
using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation.Logging
{
    [Reentrant]
    public class LoggingProtocol : ILoggingProtocol
    {
        bool isLoggingEnabled;
        bool isInitiated = false;
        bool hierarchicalCoord;

        // loggers for ACT
        ISnapperLogger[] loggerOfCoordPrepare;
        ISnapperLogger[] loggerOfPrepare;
        ISnapperLogger[] loggerOfCoordCommit;
        ISnapperLogger[] loggerOfCommit;

        // loggers for PACT
        ISnapperLogger[] loggerOfGlobalBatchInfo;
        ISnapperLogger[] loggerOfLocalBatchInfo;
        ISnapperLogger[] loggerOfLocalBatchComplete;
        ISnapperLogger[] loggerOfLocalBatchCommit;

        public async Task Init(bool isLoggingEnabled, bool isGlobalSilo, int siloID, int numLocalSilo, bool hierarchicalCoord)
        {
            if (Directory.Exists(Constants.logPath) == false) Directory.CreateDirectory(Constants.logPath);

            this.isLoggingEnabled = isLoggingEnabled;
            this.hierarchicalCoord = hierarchicalCoord;
            if (isLoggingEnabled)
            {
                if (isInitiated) await ClearLogFiles();
                else
                {
                    InitiateLoggers(isGlobalSilo, siloID, numLocalSilo);
                    isInitiated = true;
                }
            }
        }

        async Task ClearLogFiles()
        {
            var tasks = new List<Task>();
            if (loggerOfCoordPrepare != null) foreach (var logger in loggerOfCoordPrepare) tasks.Add(logger.ClearLogFile());
            if (loggerOfPrepare != null) foreach (var logger in loggerOfPrepare) tasks.Add(logger.ClearLogFile());
            if (loggerOfCoordCommit != null) foreach (var logger in loggerOfCoordCommit) tasks.Add(logger.ClearLogFile());
            if (loggerOfCommit != null) foreach (var logger in loggerOfCommit) tasks.Add(logger.ClearLogFile());

            if (loggerOfGlobalBatchInfo != null) foreach (var logger in loggerOfGlobalBatchInfo) tasks.Add(logger.ClearLogFile());
            if (loggerOfLocalBatchInfo != null) foreach (var logger in loggerOfLocalBatchInfo) tasks.Add(logger.ClearLogFile());
            if (loggerOfLocalBatchComplete != null) foreach (var logger in loggerOfLocalBatchComplete) tasks.Add(logger.ClearLogFile());
            if (loggerOfLocalBatchCommit != null) foreach (var logger in loggerOfLocalBatchCommit) tasks.Add(logger.ClearLogFile());

            await Task.WhenAll(tasks);
        }

        public bool IsLoggingEnabled() => isLoggingEnabled;

        public async Task<byte[]> GetLastCommittedGrainStateFromLog(Guid grainID, long lastCommittedLocalBid)
        {
            Debug.Assert(isLoggingEnabled);
            Debug.Assert(loggerOfLocalBatchComplete.Length == loggerOfPrepare.Length);
            var id = Helper.MapGuidToServiceID(grainID, loggerOfLocalBatchComplete.Length);
            var t1 = loggerOfPrepare[id].ReadGrainState(grainID);
            var t2 = loggerOfLocalBatchComplete[id].ReadGrainState(grainID, lastCommittedLocalBid);
            await Task.WhenAll(t1, t2);
            
            if (t1.Result.Item1 == DateTime.MinValue) return t2.Result.Item2;    // did not find any state committed by ACTs
            if (lastCommittedLocalBid == -1) return t1.Result.Item2;             // did not find any state committed by newly happened PACT batches
            return t2.Result.Item1 > t1.Result.Item1 ? t2.Result.Item2 : t1.Result.Item2;
        }

        void InitiateLoggers(bool isGlobalSilo, int siloID, int numLocalSilo)
        {
            if (hierarchicalCoord)
            {
                if (isGlobalSilo)
                {
                    var numPartitionPerGlobalLogFile = numLocalSilo * 2;
                    loggerOfGlobalBatchInfo = new ISnapperLogger[numPartitionPerGlobalLogFile];

                    for (int i = 0; i < numPartitionPerGlobalLogFile; i++)
                        loggerOfGlobalBatchInfo[i] = new SnapperLogger(siloID, LogContentType.GlobalBatchInfo, i);
                }
                else
                {
                    // loggers used by local coordinators
                    loggerOfLocalBatchInfo = new ISnapperLogger[Constants.numPartitionPerLocalLogFile];
                    loggerOfLocalBatchCommit = new ISnapperLogger[Constants.numPartitionPerLocalLogFile];
                    // loggers used by grains
                    loggerOfCoordPrepare = new ISnapperLogger[Constants.numPartitionPerLocalLogFile];
                    loggerOfPrepare = new ISnapperLogger[Constants.numPartitionPerLocalLogFile];
                    loggerOfCoordCommit = new ISnapperLogger[Constants.numPartitionPerLocalLogFile];
                    loggerOfCommit = new ISnapperLogger[Constants.numPartitionPerLocalLogFile];
                    loggerOfLocalBatchComplete = new ISnapperLogger[Constants.numPartitionPerLocalLogFile];

                    for (int i = 0; i < Constants.numPartitionPerLocalLogFile; i++)
                    {
                        loggerOfLocalBatchInfo[i] = new SnapperLogger(siloID, LogContentType.LocalBatchInfo, i);
                        loggerOfLocalBatchCommit[i] = new SnapperLogger(siloID, LogContentType.LocalBatchCommit, i);

                        loggerOfCoordPrepare[i] = new SnapperLogger(siloID, LogContentType.CoordPrepare, i);
                        loggerOfPrepare[i] = new SnapperLogger(siloID, LogContentType.Prepare, i);
                        loggerOfCoordCommit[i] = new SnapperLogger(siloID, LogContentType.CoordCommit, i);
                        loggerOfCommit[i] = new SnapperLogger(siloID, LogContentType.Commit, i);
                        loggerOfLocalBatchComplete[i] = new SnapperLogger(siloID, LogContentType.LocalBatchComplete, i);
                    }
                }
            }
            else
            {
                if (isGlobalSilo)
                {
                    var numPartitionPerGlobalLogFile = numLocalSilo * 2;

                    // loggers used by local coordinators
                    loggerOfLocalBatchInfo = new ISnapperLogger[numPartitionPerGlobalLogFile];
                    loggerOfLocalBatchCommit = new ISnapperLogger[numPartitionPerGlobalLogFile];

                    for (int i = 0; i < numPartitionPerGlobalLogFile; i++)
                    {
                        loggerOfLocalBatchInfo[i] = new SnapperLogger(siloID, LogContentType.LocalBatchInfo, i);
                        loggerOfLocalBatchCommit[i] = new SnapperLogger(siloID, LogContentType.LocalBatchCommit, i);
                    }
                }
                else
                {
                    // loggers used by grains
                    loggerOfCoordPrepare = new ISnapperLogger[Constants.numPartitionPerLocalLogFile];
                    loggerOfPrepare = new ISnapperLogger[Constants.numPartitionPerLocalLogFile];
                    loggerOfCoordCommit = new ISnapperLogger[Constants.numPartitionPerLocalLogFile];
                    loggerOfCommit = new ISnapperLogger[Constants.numPartitionPerLocalLogFile];
                    loggerOfLocalBatchComplete = new ISnapperLogger[Constants.numPartitionPerLocalLogFile];

                    for (int i = 0; i < Constants.numPartitionPerLocalLogFile; i++)
                    {
                        loggerOfCoordPrepare[i] = new SnapperLogger(siloID, LogContentType.CoordPrepare, i);
                        loggerOfPrepare[i] = new SnapperLogger(siloID, LogContentType.Prepare, i);
                        loggerOfCoordCommit[i] = new SnapperLogger(siloID, LogContentType.CoordCommit, i);
                        loggerOfCommit[i] = new SnapperLogger(siloID, LogContentType.Commit, i);
                        loggerOfLocalBatchComplete[i] = new SnapperLogger(siloID, LogContentType.LocalBatchComplete, i);
                    }
                }
            }
        }

        public async Task CoordPrepare(Guid coordID, long tid, HashSet<Guid> participateGrains)
        {
            var id = Helper.MapGuidToServiceID(coordID, loggerOfCoordPrepare.Length);
            var logContent = MessagePackSerializer.Serialize(new CoordPrepareLog(coordID, tid, participateGrains));
            await loggerOfCoordPrepare[id].Write(MessagePackSerializer.Serialize(new LogFormat(LogContentType.CoordPrepare, logContent)));
        }

        public async Task Prepare(Guid grainID, long tid, Guid coordID, byte[] state, DateTime timestamp)
        {
            var id = Helper.MapGuidToServiceID(grainID, loggerOfPrepare.Length);
            var logContent = MessagePackSerializer.Serialize(new PrepareLog(timestamp, grainID, tid, coordID, state));
            await loggerOfPrepare[id].Write(MessagePackSerializer.Serialize(new LogFormat(LogContentType.Prepare, logContent)));
        }

        public async Task CoordCommit(Guid coordID, long tid)
        {
            var id = Helper.MapGuidToServiceID(coordID, loggerOfCoordCommit.Length);
            var logContent = MessagePackSerializer.Serialize(new CoordCommitLog(coordID, tid));
            await loggerOfCoordCommit[id].Write(MessagePackSerializer.Serialize(new LogFormat(LogContentType.CoordCommit, logContent)));
        }

        public async Task Commit(Guid grainID, long tid)
        {
            var id = Helper.MapGuidToServiceID(grainID, loggerOfCommit.Length);
            var logContent = MessagePackSerializer.Serialize(new CommitLog(grainID, tid));
            await loggerOfCommit[id].Write(MessagePackSerializer.Serialize(new LogFormat(LogContentType.Commit, logContent)));
        }

        public async Task GlobalBatchInfo(Guid globalCoordID, long globalBid, HashSet<Guid> participateLocalCoords)
        {
            var id = Helper.MapGuidToServiceID(globalCoordID, loggerOfGlobalBatchInfo.Length);
            var logContent = MessagePackSerializer.Serialize(new GlobalBatchInfoLog(globalCoordID, globalBid, participateLocalCoords));
            await loggerOfGlobalBatchInfo[id].Write(MessagePackSerializer.Serialize(new LogFormat(LogContentType.GlobalBatchInfo, logContent)));
        }

        public async Task LocalBatchInfo(Guid localCoordID, long localBid, long globalBid, Guid globalCoordID, HashSet<Guid> participateGrains)
        {
            var id = Helper.MapGuidToServiceID(localCoordID, loggerOfLocalBatchInfo.Length);
            var logContent = MessagePackSerializer.Serialize(new LocalBatchInfoLog(localCoordID, localBid, globalBid, globalCoordID, participateGrains));
            await loggerOfLocalBatchInfo[id].Write(MessagePackSerializer.Serialize(new LogFormat(LogContentType.LocalBatchInfo, logContent)));
        }

        public async Task LocalBatchComplete(Guid grainID, long localBid, Guid localCoordID, byte[] state, DateTime timestamp)
        {
            var id = Helper.MapGuidToServiceID(grainID, loggerOfLocalBatchComplete.Length);
            var logContent = MessagePackSerializer.Serialize(new LocalBatchCompleteLog(timestamp, grainID, localBid, localCoordID, state));
            await loggerOfLocalBatchComplete[id].Write(MessagePackSerializer.Serialize(new LogFormat(LogContentType.LocalBatchComplete, logContent)));
        }

        public async Task LocalBatchCommit(Guid localCoordID, long localBid)
        {
            var id = Helper.MapGuidToServiceID(localCoordID, loggerOfLocalBatchCommit.Length);
            var logContent = MessagePackSerializer.Serialize(new LocalBatchCommitLog(localCoordID, localBid));
            await loggerOfLocalBatchCommit[id].Write(MessagePackSerializer.Serialize(new LogFormat(LogContentType.LocalBatchCommit, logContent)));
        }
    }
}