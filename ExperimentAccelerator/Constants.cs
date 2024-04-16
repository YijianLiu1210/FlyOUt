namespace ExperimentAccelerator
{
    internal enum SnapperInstanceType { GlobalSilo, LocalSilo, Controller, Worker };

    internal class Constants
    {
        public const string localWorkDir = @"C:\Users\jhs316\Desktop\DistributedSnapper\";
        public const string localDataPath = localWorkDir + @"data\";
        public const string snapperAmiInstanceID = "i-02fce13908698bcd8";
        //public const string amiPrivateKeyFile = localDataPath + "snapper-ami.pem";
        //public const string privateKeyFile = localDataPath + "snapper.pem";
        public const string instanceInfo = localDataPath + "instance_info.txt";
        public const string credentialFile = localDataPath + "AWS_credential.txt";
        public const string resultPath = localDataPath + "result.txt";
    }
}
