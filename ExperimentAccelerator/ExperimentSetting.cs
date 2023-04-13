namespace ExperimentAccelerator
{
    internal class ExperimentSetting
    {
        public readonly string experimentID;
        public readonly int numLocalSilo;
        public readonly Utilities.ImplementationType implementation;
        public readonly bool isLoggingEnabled;
        public readonly bool hierarchicalCoord;
        public readonly bool optimizeCommit;
        public readonly bool optimizeBatching;

        public ExperimentSetting(string experimentID, int numLocalSilo, Utilities.ImplementationType implementation, bool isLoggingEnabled,
            bool hierarchicalCoord, bool optimizeCommit, bool optimizeBatching)
        { 
            this.experimentID = experimentID;
            this.numLocalSilo = numLocalSilo;
            this.implementation = implementation;
            this.isLoggingEnabled = isLoggingEnabled;
            this.hierarchicalCoord = hierarchicalCoord;
            this.optimizeCommit = optimizeCommit;
            this.optimizeBatching = optimizeBatching;
        }
    }
}
