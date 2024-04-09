using System;
using System.Collections.Generic;
using TPCC.Grains;
using Utilities;

namespace SnapperExperimentWorker
{
    [Serializable]
    public class RequestData
    {
        public bool isDistTxn;
        public List<GrainID> grains;

        // only for TPCC
        public GrainID firstGrainID;
        public NewOrderInput input;

        public RequestData(bool isDistTxn, List<GrainID> grains)
        {
            this.isDistTxn = isDistTxn;
            this.grains = grains;
        }

        public RequestData(bool isDistTxn, GrainID firstGrainID, List<GrainID> grains, NewOrderInput input)
        {
            this.isDistTxn = isDistTxn;
            this.grains = grains;
            this.firstGrainID = firstGrainID;
            this.input = input;
        }
    }
}