using Renci.SshNet;

namespace ExperimentAccelerator
{
    internal class SSHManager
    {
        readonly string processName;
        readonly SnapperInstanceType type;
        readonly SshClient sshClient;
        readonly ScpClient scpClient;
        CountdownEvent[] barriers;

        public SSHManager(SnapperInstanceType type, string host, string password)
        {
            this.type = type;
            switch (type)
            {
                case SnapperInstanceType.Controller:
                    processName = "SnapperExperimentController";
                    break;
                case SnapperInstanceType.Worker:
                    processName = "SnapperExperimentWorker";
                    break;
                default:
                    processName = "SnapperSiloHost";
                    break;
            }
            sshClient = new SshClient(host, Utilities.Constants.userName, password);
            scpClient = new ScpClient(host, Utilities.Constants.userName, password);
        }

        public void SetBarriers(CountdownEvent[] barriers) => this.barriers = barriers;
        
        public void ThreadWork(object obj)
        {
            Connect();
            Console.WriteLine($"{type}: connected to instance, now transfer codes...");
            TerminateProcess();
            //if (type == SnapperInstanceType.Controller) 
            DeployCode();

            var tuple = (Tuple<string, string, List<ExperimentSetting>, int>)obj;
            var controller_public_ip = tuple.Item1;
            var redis_connectionString = tuple.Item2;
            var experiments = tuple.Item3;
            var instanceIndex = tuple.Item4;

            for (int i = 0; i < experiments.Count; i++)
            {
                Connect();
                barriers[i].Signal();
                barriers[i].Wait();           // all threads start at the same time

                var exp = experiments[i];
                var numLocalSilo = exp.numLocalSilo;
                if (instanceIndex < numLocalSilo)
                {
                    Console.WriteLine($"{type}: wait for 5s...");
                    Thread.Sleep(TimeSpan.FromSeconds(5));

                    Console.WriteLine($"{type}: run experiment: expID = {exp.experimentID}, numSilo = {exp.numLocalSilo}, imp = {exp.implementation}, log = {exp.isLoggingEnabled}");
                    RunCode(controller_public_ip, redis_connectionString, exp);
                }

                if (type == SnapperInstanceType.Controller)
                {
                    Console.WriteLine($"{type}: downloading experiment result");
                    Connect();
                    GetResult();
                }

                if (instanceIndex < numLocalSilo) TerminateProcess();
            }

            Disconnect();
            Console.WriteLine($"{type}: disconnect the instance");
        }

        void Connect()
        {
            var succeed = false;
            while (succeed == false)
            {
                try
                {
                    if (sshClient.IsConnected == false) sshClient.Connect();
                    if (scpClient.IsConnected == false) scpClient.Connect();
                    succeed = true;
                }
                catch (Exception e)
                {
                    //Console.WriteLine($"{e.Message} {e.StackTrace}");
                    Console.WriteLine("Fail to connect, wait for 5s, and try again...");
                    Thread.Sleep(TimeSpan.FromSeconds(5));   // try it again after 5s
                }
            }
        }

        void TerminateProcess()
        {
            // force to kill the running process which may be using the source code
            var res = sshClient.RunCommand($"taskkill.exe /F /IM {processName}.exe");
            //if (res.Error.Length != 0) Console.WriteLine($"Kill the process for {type}, {res.Error}");
        }

        void DeployCode()
        {
            // remove the exisiting Snapper directory
            var res = sshClient.RunCommand($"rmdir /Q /S {Utilities.Constants.workDir}");
            //if (res.Error.Length != 0) Console.WriteLine($"Remove the directory for {type}, {res.Error}");

            // create a new empty directory
            res = sshClient.RunCommand($"mkdir {Utilities.Constants.workDir}");
            //if (res.Error.Length != 0) Console.WriteLine($"Create the directory for {type}, {res.Error}");

            // transfer all files from local to remote
            var directoryInfo = new DirectoryInfo(Constants.localWorkDir);
            scpClient.Upload(directoryInfo, Utilities.Constants.workDir);
        }

        void RunCode(string controller_public_ip, string redis_connectionString, ExperimentSetting exp)
        {
            string cmd;
            switch (type)
            {
                case SnapperInstanceType.GlobalSilo:
                    cmd = @$"dotnet run --project {Utilities.Constants.workDir}\SnapperSiloHost false true {exp.numLocalSilo} -1 {exp.implementation} {exp.isLoggingEnabled} {controller_public_ip} {redis_connectionString}";
                    break;
                case SnapperInstanceType.Controller:
                    cmd = @$"dotnet run --project {Utilities.Constants.workDir}\SnapperExperimentController false {exp.experimentID} {exp.numLocalSilo} {exp.implementation} {exp.isLoggingEnabled} {exp.hierarchicalCoord} {exp.optimizeCommit} {exp.optimizeBatching} {redis_connectionString}";
                    break;
                case SnapperInstanceType.LocalSilo:
                    cmd = @$"dotnet run --project {Utilities.Constants.workDir}\SnapperSiloHost false false {exp.numLocalSilo} -1 {exp.implementation} {exp.isLoggingEnabled} {controller_public_ip} {redis_connectionString}";
                    break;
                case SnapperInstanceType.Worker:
                    cmd = @$"dotnet run --project {Utilities.Constants.workDir}\SnapperExperimentWorker false {exp.numLocalSilo} -1 {exp.implementation} {controller_public_ip} {redis_connectionString}";
                    break;
                default:
                    throw new Exception($"Unsupported instance type {type}");
            }

            
            var command = sshClient.CreateCommand(cmd);
            var asyncExe = command.BeginExecute();

            //if (type == SnapperInstanceType.GlobalSilo) PrintConsole(command, asyncExe);
            if (type == SnapperInstanceType.Controller) PrintConsole(command, asyncExe);
            //if (type == SnapperInstanceType.LocalSilo) PrintConsole(command, asyncExe);
            if (type == SnapperInstanceType.Worker) PrintConsole(command, asyncExe);

            command.EndExecute(asyncExe);     // wait until the command is completed
            Console.WriteLine($"Finish experiment on {type}");
        }

        void PrintConsole(SshCommand command, IAsyncResult asyncExe)
        {
            // read console output data
            string line;
            var flag = false;
            var reader = new StreamReader(command.OutputStream);
            while (asyncExe.IsCompleted == false)
            {
                line = reader.ReadToEnd();
                if (flag == false && line.Contains(GetString(type))) flag = true;
                if (flag && line.Length != 0) Console.Write(line);
            }
            line = reader.ReadToEnd();
            if (flag && line.Length != 0) Console.Write(line);
        }

        string GetString(SnapperInstanceType type)
        {
            switch (type)
            {
                case SnapperInstanceType.GlobalSilo:
                    return "Silo: isGlobalSilo";
                case SnapperInstanceType.Controller:
                    return "ExpController: experimentID = ";
                case SnapperInstanceType.LocalSilo:
                    return "Silo: isGlobalSilo";
                case SnapperInstanceType.Worker:
                    return "ExpWorker connect to ExpController";
                default:
                    throw new Exception($"Unsupported instance type {type}");
            }
        }

        void GetResult()
        {
            var outputFile1 = new FileInfo($@"{Constants.resultPath}");
            scpClient.Download($@"{Utilities.Constants.dataPath}result.txt", outputFile1);

            var fileName = "tp-GrainMigration-100%PACT.txt";
            var outputFile2 = new FileInfo(Constants.localDataPath + fileName);
            scpClient.Download(Utilities.Constants.dataPath + fileName, outputFile2);

            fileName = "tp-GrainMigration-50%PACT.txt";
            var outputFile3 = new FileInfo(Constants.localDataPath + fileName);
            scpClient.Download(Utilities.Constants.dataPath + fileName, outputFile3);

            fileName = "tp-GrainMigration-0%PACT.txt";
            var outputFile4 = new FileInfo(Constants.localDataPath + fileName);
            scpClient.Download(Utilities.Constants.dataPath + fileName, outputFile4);
        }

        void Disconnect()
        {
            sshClient.Disconnect();
            scpClient.Disconnect();
        }
    }
}