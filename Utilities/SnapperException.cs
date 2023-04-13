using System;

namespace Utilities
{
    public class SnapperDeadlockException : Exception
    {
        public SnapperDeadlockException(string message) : base(message) { }
    }

    public class SnapperStorageException : Exception
    {
        public SnapperStorageException(string message) : base(message) { }
    }

    public class SnapperGrainMigrationException : Exception
    {
        public SnapperGrainMigrationException(string message) : base(message) { }
    }
}
