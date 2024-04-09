using System;

namespace Utilities
{
    public enum AccessMode { Read, ReadWrite };
    public enum BenchmarkType { SMALLBANK, TPCC };
    public enum ImplementationType { SNAPPER, NONTXN, ORLEANSTXN };
    public enum TxnType { Init, MultiTransfer, Deposit };
    public enum GrainType { UserGrain, LocalCoord, GlobalCoord, LocalConfig, GlobalConfig, MigrationWorker, PlacementManager }
    public enum LogContentType { CoordPrepare, Prepare, CoordCommit, Commit, GlobalBatchInfo, LocalBatchInfo, LocalBatchComplete, LocalBatchCommit };

    [Serializable]
    public enum NetMsgType { CONNECT, START_GLOBAL_SILO, START_LOCAL_SILO, WORKER_ID, WORKLOAD_INIT, ACK, WORKLOAD_CONFIG, RUN_EPOCH, TERMINATE }

    public class Constants
    {
        public const bool isLocalTest = true;
        public const char grainMigrationExpID = '5';

        public const string ClusterID = "SnapperCluster";
        public const string ServiceID = "Snapper";
        public const string SiloMembershipTable = "SnapperMembershipTable";
        public const string GrainDirectoryName = "SnapperGrainDirectory";

        // architecture 1: single silo
        //                 local coordinators (num = numLocalCoordPerSilo)
        //                 1 global config grain
        // architecture 2: multi silo, non-hierarchical
        //                 all local coordinators locate in a separate silo (num = numGlobalCoord)
        //                 1 global config grain
        //                 1 local config grain per silo
        // architecture 3: multi silo, hierarchical
        //                 in each silo, local coordinators (num = numLocalCoordPerSilo)
        //                 all global coordinators locate in a separate silo (num = numGlobalCoord)
        //                 1 global config grain
        //                 1 local config grain per silo
        
        // general silo config
        public const int loggingBatchSize = 1;
        public const bool loggingBatching = false;
        public static TimeSpan deadlockTimeout = TimeSpan.FromMilliseconds(20);
        public static TimeSpan lostMsgTimeout = TimeSpan.FromSeconds(20);
        // local silo config
        public const int numCPUPerLocalSilo = 4;
        public const int numGlobalCoordPerLocalSilo = 1;
        public const int numLocalCoordPerSilo = numCPUPerLocalSilo * 2;
        public const int numGrainMigrationWorkerPerSilo = numCPUPerLocalSilo * 2;
        public const int numGrainPlacementManagerPerSilo = numCPUPerLocalSilo * 2;
        public const int numPartitionPerLocalLogFile = 2;
        public const int numLoggerPerSilo = numCPUPerLocalSilo * 2;    // this is only for OrleansTxn
        // global silo config
        public const double scaleSpeedForGlobalBatchSize = 1.1;

        // benchmark config
        public const int numEpoch = 6;
        public const int numWarmupEpoch = 2;
        public const int epochDurationMSecs = 10000;
        public const BenchmarkType benchmark = BenchmarkType.TPCC;
        // for SmallBank
        public const int numGrainPerLocalSilo = 10000;
        public const string grainClassName = "SmallBank.Grains.SnapperTransactionalAccountGrain";
        // for TPCC
        public const int NUM_W_PER_SILO = 2;
        public const int NUM_D_PER_W = 10;
        public const int NUM_C_PER_D = 3000;
        public const int NUM_I = 100000;
        public const int NUM_StockGrain_PER_W = 10000;

        public const string userName = "yijia"; // isLocalTest ? "yijia" : "Administrator";
        public const string workDir = @$"C:\Users\{userName}\Desktop\DistributedSnapper";
        public const string dataPath = workDir + @"\data\";
        public const string logPath = dataPath + @"log\";
        public const string resultPath = dataPath + "result.txt";
        public const string credentialFile = dataPath + "AWS_credential.txt";
        // for grain migration experiment
        public const int beforeAndAfterDurationMSecs = 30000;
        public const long MeasurementDurationMSecs = 1000;     // calculate txn tp in every 1000 ms

        // controller <==> worker
        public const string controllerToWorker_PushPort = "5554";         // send workerID
        public const string controllerToWorker_PublishPort = "5555";
        public const string controllerFromWorker_PullPort = "5556";
        // controller <==> silo
        public const string controllerToSilo_PublishPort = "5557";
        public const string controllerFromSilo_PullPort = "5558";

        // Redis
        public const string GlobalSilo = "GlobalSiloAddress";
        public const int Redis_GrainDirectory = 0;

        public const int Redis_GrainStorage = isLocalTest ? 1 : 0;
        public const string GrainStatePrefix = "GrainState-";

        public const int Redis_GrainPlacementMap = isLocalTest ? 2 : 0;   // grainID => siloID
        public const string GrainIDPrefix = "GrainID+";
        public const string CoordIDPrefix = "CoordID+";

        public const int Redis_SiloInfo = isLocalTest ? 3 : 0;
        public const string GeneralInfoPrefix = "General-";
        public const string CoordInfoPrefix = "CoordList-";

        // for workload generation
        public const int BASE_NUM_MULTITRANSFER = 150000;
        public const int BASE_NUM_NEWORDER = 20000;

        public const int maxNumReRun = 3;
        public const double sdSafeRange = 0.1;   // standard deviation should within the range of 5% * mean
    }
}