using ExperimentAccelerator;
using System.Diagnostics;

// STEP 1: manually launch an instance
//         (1) use image Windows Server 2022 Base
//         (2) create and use the key-pair "snapper-ami"
//         (3) create and use the security group "snapper"
//             - inbound: (TCP 22) allow ssh connection from my local pc
//             - inbound: (TCP 3389) allow RDP connection from my local pc
//             - inbound: (TCP 6379) allow connection to redis cluster
//             - inbound: (TCP 5554 - 5558) allow connection between instances
//             - inbound: (TCP 11111 - 11131) allow traffic between silo ports
//             - inbound: (TCP 30000 - 30020) allow traffic between silo gateway ports
//             - outbound: all allowed
//         (4) create and use the (cluster) placement group "snapper"
//             - cluster placement
//         (5) 40GB gp2 storage
// STEP 2: install .NET SDK on the instance, install OpenSSH Server and enable it on the instance
// STEP 3: create an image "snapper-ami" of this instance
// STEP 4: create a launch template
//         (1) it uses the "snapper-ami" as image
//         (2) it uses the key-pair "snapper-ami"
//         (3) it uses "snapper" security group
//         (4) it uses "snapper" placement group
// STEP 5: in the code, all instances are launched by using the created template
//         (1) need to specify the instance type (large or xlarge) in the code

//var numLocalSilo = int.Parse(args[0]);
//var reInvokeAllResources = bool.Parse(args[1]);
var numLocalSilo = 2;
var reInvokeAllResources = false;

// ========================================================================================================
// read credential info from file and create a EC2 client
string AccessKey, SecretKey, Password;
using (var file = new StreamReader(Constants.credentialFile))
{
    var ServiceRegion = file.ReadLine();
    Debug.Assert(ServiceRegion == "eu-north-1");
    AccessKey = file.ReadLine();
    SecretKey = file.ReadLine();
    Password = file.ReadLine();
}

Console.WriteLine($"Start setup resources, numLocalSilo = {numLocalSilo}");
var ec2Manager = new EC2Manager(AccessKey, SecretKey);
var redisManager = new RedisManager(AccessKey, SecretKey);
var dynamoManager = new DynamoManager(AccessKey, SecretKey);

// ========================================================================================================
if (reInvokeAllResources)
{
    await ec2Manager.CleanUp();

    await ec2Manager.SpawnNew(numLocalSilo);
    var securityGroupID = await ec2Manager.GetSecurityGroupID();
    await redisManager.SpawnNew(securityGroupID);

    Console.WriteLine("All instances are re-invoked, wait for 5m so the instances are ready");
    Thread.Sleep(TimeSpan.FromMinutes(5));
}

await dynamoManager.CleanUp();
await redisManager.CleanUp();

// ========================================================================================================
var snapper = await ec2Manager.StartAllInstances(Password, numLocalSilo);
Debug.Assert(snapper.localSilos.Count == numLocalSilo);
Debug.Assert(snapper.workers.Count == numLocalSilo);

// ========================================================================================================
// clean the local files
var file1 = File.Create(Constants.localWorkDir + @"data\result.txt");
file1.Close();
var file2 = File.Create(Constants.localWorkDir + @"data\tp-GrainMigration-100%PACT.txt");
file2.Close();
var file3 = File.Create(Constants.localWorkDir + @"data\tp-GrainMigration-50%PACT.txt");
file3.Close();
var file4 = File.Create(Constants.localWorkDir + @"data\tp-GrainMigration-0%PACT.txt");
file4.Close();
if (Directory.Exists(Constants.localWorkDir + @"data\log")) Directory.Delete(Constants.localWorkDir + @"data\log", true);

// ========================================================================================================
var controller_public_ip = snapper.controller.publicIP;
var experiments = XMLParser.LoadExperimentSettingsFromXMLFile();
var tuple = new Tuple<string, string, List<ExperimentSetting>, int>(controller_public_ip, redisManager.connectionString, experiments, 0);

var barriers = new CountdownEvent[experiments.Count];
for (int i = 0; i < barriers.Length; i++) barriers[i] = new CountdownEvent(2 + numLocalSilo * 2);

Console.WriteLine($"Start globalSilo thread");
snapper.globalSilo.sshManager.SetBarriers(barriers);
var globalSiloThread = new Thread(snapper.globalSilo.sshManager.ThreadWork);
globalSiloThread.Start(tuple);

Console.WriteLine($"Start controller thread");
snapper.controller.sshManager.SetBarriers(barriers);
var controllerThread = new Thread(snapper.controller.sshManager.ThreadWork);
controllerThread.Start(tuple);

var index = 0;
var localSiloThreads = new List<Thread>();
foreach (var localSilo in snapper.localSilos)
{
    Console.WriteLine($"Start localSilo thread");
    localSilo.sshManager.SetBarriers(barriers);
    var localSiloThread = new Thread(localSilo.sshManager.ThreadWork);
    var tuple1 = new Tuple<string, string, List<ExperimentSetting>, int>(controller_public_ip, redisManager.connectionString, experiments, index);
    localSiloThread.Start(tuple1);
    localSiloThreads.Add(localSiloThread);
    index++;
}

index = 0;
var workerThreads = new List<Thread>();
foreach (var worker in snapper.workers)
{
    Console.WriteLine($"Start worker thread");
    worker.sshManager.SetBarriers(barriers);
    var workerThread = new Thread(worker.sshManager.ThreadWork);
    var tuple1 = new Tuple<string, string, List<ExperimentSetting>, int>(controller_public_ip, redisManager.connectionString, experiments, index);
    workerThread.Start(tuple1);
    workerThreads.Add(workerThread);
    index++;
}

globalSiloThread.Join();
foreach (var localSiloThread in localSiloThreads) localSiloThread.Join();
controllerThread.Join();
foreach (var workerThread in workerThreads) workerThread.Join();
Console.WriteLine($"All threads are done");

// ========================================================================================================
// need to wait until the experiments are done
//await ec2Manager.StopAllInstances();
//Console.WriteLine("all insatnces are stopped");