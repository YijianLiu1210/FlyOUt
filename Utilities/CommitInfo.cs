using System;
using System.Collections.Generic;

namespace Utilities
{
    public class CommitInfo
    {
        public long highestCommittedGlobalBid;
        public Dictionary<string, long> highestCommittedLocalBidPerSilo;

        public CommitInfo()
        {
            highestCommittedGlobalBid = -1;
            highestCommittedLocalBidPerSilo = new Dictionary<string, long>();
        }

        public void MergeCommitInfo(CommitInfo commitInfo)
        {
            MergeCommitInfoOfSilo("", commitInfo.highestCommittedGlobalBid);
            foreach (var item in commitInfo.highestCommittedLocalBidPerSilo)
                MergeCommitInfoOfSilo(item.Key, item.Value);
        }

        public void MergeCommitInfoOfSilo(string siloAddress, long hiestCommittedBid)
        {
            if (siloAddress == "")
            {
                highestCommittedGlobalBid = Math.Max(highestCommittedGlobalBid, hiestCommittedBid);
                return;
            } 
            if (highestCommittedLocalBidPerSilo.ContainsKey(siloAddress))
                highestCommittedLocalBidPerSilo[siloAddress] = Math.Max(highestCommittedLocalBidPerSilo[siloAddress], hiestCommittedBid);
            else highestCommittedLocalBidPerSilo[siloAddress] = hiestCommittedBid;
        }
    }
}