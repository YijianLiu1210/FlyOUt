using PrepareDataForFigures;
using System.Diagnostics;

int[] f1_nt_tp = new int[4];
double[] f1_pact_logging_tp = new double[4];
double[] f1_pact_delta = new double[4];
double[] f1_act_logging_tp = new double[4];
double[] f1_act_delta = new double[4];
double[] f1_abort = new double[4];
double[] f1_logging_abort = new double[4];

int[] f2_nt_tp = new int[7];
int[] f2_pact_tp = new int[7];
int[] f2_pact_logging_tp = new int[7];
int[] f2_act_tp = new int[7];
int[] f2_act_logging_tp = new int[7];
double[] f2_abort = new double[7];
double[] f2_logging_abort = new double[7];

int[] f31_pact_logging_tp = new int[18];
int[] f31_act_logging_tp = new int[18];
double[] f31_dist_abort = new double[20];
double[] f31_non_dist_abort = new double[20];

int[] f3_pact_logging_tp = new int[42];
int[] f3_act_logging_tp = new int[42];
double[] f3_dist_abort = new double[47];
double[] f3_non_dist_abort = new double[47];

int[] f4_100pact_0dist_uniform = new int[4];
int[] f4_100pact_0dist_skew = new int[4];
int[] f4_100pact_50dist_uniform = new int[4];
int[] f4_100pact_50dist_skew = new int[4];
int[] f4_100pact_100dist_uniform = new int[4];
int[] f4_100pact_100dist_skew = new int[4];

int[] f4_50pact_0dist_uniform = new int[4];
int[] f4_50pact_0dist_skew = new int[4];
int[] f4_50pact_50dist_uniform = new int[4];
int[] f4_50pact_50dist_skew = new int[4];
int[] f4_50pact_100dist_uniform = new int[4];
int[] f4_50pact_100dist_skew = new int[4];

int[] f4_0pact_0dist_uniform = new int[4];
int[] f4_0pact_0dist_skew = new int[4];
int[] f4_0pact_50dist_uniform = new int[4];
int[] f4_0pact_50dist_skew = new int[4];
int[] f4_0pact_100dist_uniform = new int[4];
int[] f4_0pact_100dist_skew = new int[4];

List<int> f5_timeSlots = new List<int>();
List<int> f5_tp = new List<int>();
List<double> f5_latency = new List<double>();

try
{
    using (var file = new StreamReader(Constants.resultPath))
    {
        string line;
        while ((line = file.ReadLine()) != null)
        {
            var strs = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            switch (strs[0])
            {
                case "Fig.1":
                    PreProcessFig1Data(strs);
                    break;
                case "Fig.2":
                    PreProcessFig2Data(strs);
                    break;
                case "Fig.31":
                    PreProcessFig3Data(strs);
                    break;
                case "Fig.32":
                    PreProcessFig3Data(strs);
                    break;
                case "Fig.33":
                    PreProcessFig3Data(strs);
                    break;
                case "Fig.4":
                    PreProcessFig4Data(strs);
                    break;
                default:
                    throw new Exception($"Exception: Unknown data {line}");
            }
        }
    }
}
catch (Exception e)
{
    Console.WriteLine($"{e.Message} {e.StackTrace}");
    throw;
}

void PreProcessFig1Data(string[] strs)
{
    var index = Helper.GetIndexOftxnDistLevelFig1(int.Parse(strs[(int)ColumnName.txnDistLevel]));
    var implementation = Enum.Parse<Utilities.ImplementationType>(strs[(int)ColumnName.implementation]);
    var loggingEnabled = bool.Parse(strs[(int)ColumnName.loggingEnabled]);
    var pactPercent = strs[(int)ColumnName.pactPercent];

    switch (implementation)
    {
        case Utilities.ImplementationType.NONTXN:
            f1_nt_tp[index] = int.Parse(strs[(int)ColumnName.act_dist_tp]) + int.Parse(strs[(int)ColumnName.act_non_dist_tp]);
            break;
        case Utilities.ImplementationType.SNAPPER:
            if (loggingEnabled)
            {
                if (pactPercent == "100%")
                {
                    var dist_tp = double.Parse(strs[(int)ColumnName.pact_dist_tp]);
                    var non_dist_tp = double.Parse(strs[(int)ColumnName.pact_non_dist_tp]);
                    Debug.Assert(dist_tp == 0 || non_dist_tp == 0);
                    f1_pact_logging_tp[index] = dist_tp + non_dist_tp;
                }
                else if (pactPercent == "0%")
                {
                    var dist_tp = double.Parse(strs[(int)ColumnName.act_dist_tp]);
                    var non_dist_tp = double.Parse(strs[(int)ColumnName.act_non_dist_tp]);
                    Debug.Assert(dist_tp == 0 || non_dist_tp == 0);
                    f1_act_logging_tp[index] = dist_tp + non_dist_tp;

                    var dist_abort = double.Parse(strs[(int)ColumnName.act_dist_abort]);
                    var non_dist_abort = double.Parse(strs[(int)ColumnName.act_non_dist_abort]);
                    Debug.Assert(dist_abort == 0 || non_dist_abort == 0);
                    f1_logging_abort[index] = dist_abort + non_dist_abort;
                }
            }
            else
            {
                if (pactPercent == "100%")
                {
                    var dist_tp = double.Parse(strs[(int)ColumnName.pact_dist_tp]);
                    var non_dist_tp = double.Parse(strs[(int)ColumnName.pact_non_dist_tp]);
                    Debug.Assert(dist_tp == 0 || non_dist_tp == 0);
                    f1_pact_delta[index] = dist_tp + non_dist_tp;
                }
                else if (pactPercent == "0%")
                {
                    var dist_tp = double.Parse(strs[(int)ColumnName.act_dist_tp]);
                    var non_dist_tp = double.Parse(strs[(int)ColumnName.act_non_dist_tp]);
                    Debug.Assert(dist_tp == 0 || non_dist_tp == 0);
                    f1_act_delta[index] = dist_tp + non_dist_tp;

                    var dist_abort = double.Parse(strs[(int)ColumnName.act_dist_abort]);
                    var non_dist_abort = double.Parse(strs[(int)ColumnName.act_non_dist_abort]);
                    Debug.Assert(dist_abort == 0 || non_dist_abort == 0);
                    f1_abort[index] = dist_abort + non_dist_abort;
                }
            }
            break;
        default:
            throw new Exception($"Exception: Unsupported ImplementationType {implementation}");
    }
}

void PostProcessFig1Data()
{
    for (int i = 0; i < 4; i++)
    {
        f1_pact_delta[i] -= f1_pact_logging_tp[i];
        f1_pact_delta[i] *= 1.0 / f1_nt_tp[i];
        Debug.Assert(f1_pact_delta[i] != 0);
        f1_pact_logging_tp[i] *= 1.0 / f1_nt_tp[i];
        Debug.Assert(f1_pact_logging_tp[i] != 0);

        f1_act_delta[i] -= f1_act_logging_tp[i];
        f1_act_delta[i] *= 1.0 / f1_nt_tp[i];
        Debug.Assert(f1_act_delta[i] != 0);
        f1_act_logging_tp[i] *= 1.0 / f1_nt_tp[i];
        Debug.Assert(f1_act_logging_tp[i] != 0);
    }
}

void PreProcessFig2Data(string[] strs)
{
    var index = Helper.GetIndexOfDistPercentFig2(strs[(int)ColumnName.distPercent]);
    var implementation = Enum.Parse<Utilities.ImplementationType>(strs[(int)ColumnName.implementation]);
    var loggingEnabled = bool.Parse(strs[(int)ColumnName.loggingEnabled]);
    var pactPercent = strs[(int)ColumnName.pactPercent];

    switch (implementation)
    {
        case Utilities.ImplementationType.NONTXN:
            f2_nt_tp[index] = int.Parse(strs[(int)ColumnName.act_dist_tp]) + int.Parse(strs[(int)ColumnName.act_non_dist_tp]);
            break;
        case Utilities.ImplementationType.SNAPPER:
            if (loggingEnabled)
            {
                if (pactPercent == "100%")
                    f2_pact_logging_tp[index] = int.Parse(strs[(int)ColumnName.pact_dist_tp]) + int.Parse(strs[(int)ColumnName.pact_non_dist_tp]);
                else if (pactPercent == "0%")
                {
                    f2_act_logging_tp[index] = int.Parse(strs[(int)ColumnName.act_dist_tp]) + int.Parse(strs[(int)ColumnName.act_non_dist_tp]);
                    f2_logging_abort[index] = double.Parse(strs[(int)ColumnName.act_total_abort]);
                }
            }
            else
            {
                if (pactPercent == "100%")
                    f2_pact_tp[index] = int.Parse(strs[(int)ColumnName.pact_dist_tp]) + int.Parse(strs[(int)ColumnName.pact_non_dist_tp]);
                else if (pactPercent == "0%")
                {
                    f2_act_tp[index] = int.Parse(strs[(int)ColumnName.act_dist_tp]) + int.Parse(strs[(int)ColumnName.act_non_dist_tp]);
                    f2_abort[index] = double.Parse(strs[(int)ColumnName.act_total_abort]);
                }
            }
            break;
        default:
            throw new Exception($"Exception: Unsupported ImplementationType {implementation}");
    }
}

void PreProcessFig3Data(string[] strs)
{
    // get data for Fig3.1
    var group_index = Helper.GetIndexOfDistPercentFig31(strs[(int)ColumnName.distPercent]);
    var stack_index = Helper.GetIndexOfSkewnessFig31(strs[(int)ColumnName.grainSkewness]);
    var index = group_index * 6 + stack_index;
    var pactPercent = strs[(int)ColumnName.pactPercent];
    switch (pactPercent)
    {
        case "100%":
            f31_pact_logging_tp[index] = int.Parse(strs[(int)ColumnName.pact_dist_tp]) + int.Parse(strs[(int)ColumnName.pact_non_dist_tp]);
            break;
        case "0%":
            f31_act_logging_tp[index] = int.Parse(strs[(int)ColumnName.act_dist_tp]) + int.Parse(strs[(int)ColumnName.act_non_dist_tp]);
            f31_dist_abort[group_index * 7 + stack_index] = double.Parse(strs[(int)ColumnName.act_dist_abort]);
            f31_non_dist_abort[group_index * 7 + stack_index] = double.Parse(strs[(int)ColumnName.act_non_dist_abort]);
            break;
    }

    // get data for Fig3
    group_index = Helper.GetIndexOfSkewnessFig31(strs[(int)ColumnName.grainSkewness]);
    stack_index = Helper.GetIndexOfPactPercentFig3(strs[(int)ColumnName.pactPercent]);
    index = group_index * 7 + stack_index;
    var distPercent = strs[(int)ColumnName.distPercent];
    if (distPercent == "50%")
    {
        f3_pact_logging_tp[index] = int.Parse(strs[(int)ColumnName.pact_dist_tp]) + int.Parse(strs[(int)ColumnName.pact_non_dist_tp]);
        f3_act_logging_tp[index] = int.Parse(strs[(int)ColumnName.act_dist_tp]) + int.Parse(strs[(int)ColumnName.act_non_dist_tp]);
        f31_dist_abort[group_index * 8 + stack_index] = double.Parse(strs[(int)ColumnName.act_dist_abort]);
        f31_non_dist_abort[group_index * 8 + stack_index] = double.Parse(strs[(int)ColumnName.act_non_dist_abort]);
    }
}

void PreProcessFig4Data(string[] strs)
{
    var index = Helper.GetIndexOfNumSiloFig4(strs[(int)ColumnName.numLocalSilo]);
    var distPercent = strs[(int)ColumnName.distPercent];
    var pactPercent = strs[(int)ColumnName.pactPercent];
    switch (pactPercent)
    {
        case "100%":
            switch (distPercent)
            {
                case "0%":
                    if (strs[(int)ColumnName.grainSkewness] == "100%") f4_100pact_0dist_uniform[index] = int.Parse(strs[(int)ColumnName.pact_dist_tp]) + int.Parse(strs[(int)ColumnName.pact_non_dist_tp]);
                    else f4_100pact_0dist_skew[index] = int.Parse(strs[(int)ColumnName.pact_dist_tp]) + int.Parse(strs[(int)ColumnName.pact_non_dist_tp]);
                    break;
                case "50%":
                    if (strs[(int)ColumnName.grainSkewness] == "100%") f4_100pact_50dist_uniform[index] = int.Parse(strs[(int)ColumnName.pact_dist_tp]) + int.Parse(strs[(int)ColumnName.pact_non_dist_tp]);
                    else f4_100pact_50dist_skew[index] = int.Parse(strs[(int)ColumnName.pact_dist_tp]) + int.Parse(strs[(int)ColumnName.pact_non_dist_tp]);
                    break;
                case "100%":
                    if (strs[(int)ColumnName.grainSkewness] == "100%") f4_100pact_100dist_uniform[index] = int.Parse(strs[(int)ColumnName.pact_dist_tp]) + int.Parse(strs[(int)ColumnName.pact_non_dist_tp]);
                    else f4_100pact_100dist_skew[index] = int.Parse(strs[(int)ColumnName.pact_dist_tp]) + int.Parse(strs[(int)ColumnName.pact_non_dist_tp]);
                    break;
                default:
                    throw new Exception($"Exception: Unsupported distPercent {distPercent} for Fig4");
            }
            break;
        case "50%":
            switch (distPercent)
            {
                case "0%":
                    break;
                case "50%":
                    break;
                case "100%":
                    break;
                default:
                    throw new Exception($"Exception: Unsupported distPercent {distPercent} for Fig4");
            }
            break;
        case "0%":
            switch (distPercent)
            {
                case "0%":
                    break;
                case "50%":
                    break;
                case "100%":
                    break;
                default:
                    throw new Exception($"Exception: Unsupported distPercent {distPercent} for Fig4");
            }
            break;
        default:
            throw new Exception($"Exception: Unsupported pactPercent {pactPercent} for Fig4");
    }
}