using System.Xml;
using Utilities;

namespace ExperimentAccelerator
{
    internal static class XMLParser
    {
        public static List<ExperimentSetting> LoadExperimentSettingsFromXMLFile()
        {
            var path = Constants.localDataPath + @"XML\ExperimentSettings.xml";
            var xmlDoc = new XmlDocument();
            xmlDoc.Load(path);
            var rootNode = xmlDoc.DocumentElement;

            var experiments = new List<ExperimentSetting>();

            var runExperiment = Array.ConvertAll(rootNode.SelectSingleNode("runExperiment").FirstChild.Value.Split(","), x => x.Trim());
            foreach (var experimentID in runExperiment)
            {
                var numNodes = rootNode.ChildNodes.Count;
                for (int n = 1; n < numNodes; n++)
                {
                    var node = rootNode.ChildNodes.Item(n);
                    if (node.Attributes.Item(0).Value != experimentID) continue;

                    var numLocalSilos = Array.ConvertAll(node.SelectSingleNode("numLocalSilo").FirstChild.Value.Split(","), x => int.Parse(x));
                    var implementations = Array.ConvertAll(node.SelectSingleNode("implementation").FirstChild.Value.Split(","), x => Enum.Parse<ImplementationType>(x));
                    var isLoggingEnableds = Array.ConvertAll(node.SelectSingleNode("isLoggingEnabled").FirstChild.Value.Split(","), x => bool.Parse(x));
                    
                    var hierarchicalCoords = new bool[numLocalSilos.Length];
                    var optimizeCommits = new bool[numLocalSilos.Length];
                    var optimizeBatchings = new bool[numLocalSilos.Length];
                    for (int i = 0; i < numLocalSilos.Length; i++)
                    {
                        hierarchicalCoords[i] = true;
                        optimizeCommits[i] = true;
                        optimizeBatchings[i] = true;
                    }

                    if (node.SelectSingleNode("hierarchicalCoord") != null) hierarchicalCoords = Array.ConvertAll(node.SelectSingleNode("hierarchicalCoord").FirstChild.Value.Split(","), x => bool.Parse(x));
                    if (node.SelectSingleNode("optimizeCommit") != null) optimizeCommits = Array.ConvertAll(node.SelectSingleNode("optimizeCommit").FirstChild.Value.Split(","), x => bool.Parse(x));
                    if (node.SelectSingleNode("optimizeBatching") != null) optimizeBatchings = Array.ConvertAll(node.SelectSingleNode("optimizeBatching").FirstChild.Value.Split(","), x => bool.Parse(x));

                    for (int i = 0; i < numLocalSilos.Length; i++)
                        experiments.Add(new ExperimentSetting(experimentID, numLocalSilos[i], implementations[i], isLoggingEnableds[i], 
                            hierarchicalCoords[i], optimizeCommits[i], optimizeBatchings[i]));
                }
            }

            return experiments;
        }
    }
}