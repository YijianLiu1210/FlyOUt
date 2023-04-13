namespace ExperimentAccelerator
{
    internal class SnapperServices
    {
        public SnapperInstance globalSilo;
        public SnapperInstance controller;
        public List<SnapperInstance> localSilos = new List<SnapperInstance>();
        public List<SnapperInstance> workers = new List<SnapperInstance>();
    }

    internal class SnapperInstance
    {
        public readonly string instanceID;
        public readonly string publicIP;
        public SSHManager sshManager { get; set; }

        public SnapperInstance(string instanceID, string publicIP)
        {
            this.instanceID = instanceID;
            this.publicIP = publicIP;
        }
    }
}
