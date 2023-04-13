using System;
using System.Collections.Generic;

namespace SnapperExperimentWorker
{
    [Serializable]
    public class RequestData
    {
        // for SmallBank
        public bool isDistTxn;
        public List<Guid> grains;

        public RequestData(bool isDistTxn, List<Guid> grains)
        {
            this.isDistTxn = isDistTxn;
            this.grains = grains;
        }
    }
}
