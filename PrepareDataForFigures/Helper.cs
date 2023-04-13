namespace PrepareDataForFigures
{
    public enum ColumnName
    {
        // experiment setting
        Fig,
        numLocalSilo,
        implementation,
        loggingEnabled,
        txnSize,
        txnDistLevel,
        pactPercent,
        distPercent,
        grainSkewness,
        actPipeSize,
        pactPipeSize,
        migrationPipeSize,
        // throughput
        pact_dist_tp,
        pact_non_dist_tp,
        act_dist_tp,
        act_non_dist_tp,
        // abort reasons
        act_total_abort,
        act_dist_abort,
        act_dist_abort_rw,
        act_dist_abort_deadlock,
        act_dist_abort_notSerializable,
        act_dist_abort_notSureSerializable,
        act_non_dist_abort,
        act_non_dist_abort_rw,
        act_non_dist_abort_deadlock,
        act_non_dist_abort_notSerializable,
        act_non_dist_abort_notSureSerializable,
        // PACT-dist: percentile latencies + breakdone latencies
        pact_dist_50th_latency_ms,
        pact_dist_90th_latency_ms,
        pact_dist_99th_latency_ms,
        pact_dist_reg_ms,
        pact_dist_pre_ms,
        pact_dist_exe_ms,
        pact_dist_com_ms,
        pact_non_dist_50th_latency_ms,
        pact_non_dist_90th_latency_ms,
        pact_non_dist_99th_latency_ms,
        pact_non_dist_reg_ms,
        pact_non_dist_pre_ms,
        pact_non_dist_exe_ms,
        pact_non_dist_com_ms,
        // ACT-dist: percentile latencies + breakdone latencies
        act_dist_50th_latency_ms,
        act_dist_90th_latency_ms,
        act_dist_99th_latency_ms,
        act_dist_reg_ms,
        act_dist_pre_ms,
        act_dist_exe_ms,
        act_dist_com_ms,
        act_non_dist_50th_latency_ms,
        act_non_dist_90th_latency_ms,
        act_non_dist_99th_latency_ms,
        act_non_dist_reg_ms,
        act_non_dist_pre_ms,
        act_non_dist_exe_ms,
        act_non_dist_com_ms,
    };

    public static class Helper
    {
        public static int GetIndexOftxnDistLevelFig1(int n)
        {
            switch (n)
            {
                case 1: return 0;
                case 2: return 1;
                case 4: return 2;
                case 8: return 3;
                default:
                    throw new Exception($"Exception: Unsupported txnDistLevel {n} for Fig1");
            }
        }

        public static int GetIndexOfDistPercentFig2(string distPercent)
        {
            switch (distPercent)
            {
                case "0%": return 0;
                case "1%": return 1;
                case "10%": return 2;
                case "25%": return 3;
                case "50%": return 4;
                case "75%": return 5;
                case "100%": return 6;
                default:
                    throw new Exception($"Exception: Unsupported distPercent {distPercent}  for Fig2");
            }
        }

        public static int GetIndexOfSkewnessFig31(string grainSkewness)
        {
            switch (grainSkewness)
            {
                case "100%": return 0;
                case "2%": return 1;
                case "1%": return 2;
                case "0.5%": return 3;
                case "0.2%": return 4;
                case "0.1%": return 5;
                default:
                    throw new Exception($"Exception: Unsupported grainSkewness {grainSkewness} for Fig31");
            }
        }

        public static int GetIndexOfDistPercentFig31(string distPercent)
        {
            switch (distPercent)
            {
                case "0%": return 0;
                case "50%": return 1;
                case "100%": return 2;
                default:
                    throw new Exception($"Exception: Unsupported distPercent {distPercent} for Fig31");
            }
        }

        public static int GetIndexOfPactPercentFig3(string pactPercent)
        {
            switch (pactPercent)
            {
                case "100": return 0;
                case "99%": return 1;
                case "90%": return 2;
                case "75%": return 3;
                case "50%": return 4;
                case "25%": return 5;
                case "0%": return 6;
                default:
                    throw new Exception($"Exception: Unsupported pactPercent {pactPercent} for Fig3");
            }
        }

        public static int GetIndexOfNumSiloFig4(string numLocalSilo)
        {
            switch (numLocalSilo)
            {
                case "2": return 0;
                case "4": return 1;
                case "8": return 2;
                case "16": return 3;
                default:
                    throw new Exception($"Exception: Unsupported numLocalSilo {numLocalSilo} for Fig4");
            }
        }
    }
}