
using System;
using System.Collections.Generic;
using System.Linq;

namespace SnapperGeoRegionalIntegration.Tests
{
    public class TestDataGenerator
    {
        public static List<string> GetAccessInfoClassNames(int n)
        {
            Type snapperTransactionalAccountGrainType = typeof(SmallBank.Grains.SnapperTransactionalAccountGrain);
            string snapperTransactionalAccountGrainTypeName = snapperTransactionalAccountGrainType.ToString();
            return Enumerable.Repeat(snapperTransactionalAccountGrainTypeName, n).ToList<string>();
        }

        public static List<Tuple<int, string>> GetAccountsFromRegion(int n, int startAccountId, string deployedRegion, string homeRegion, int serverIndex)
        {
            List<Tuple<int, string>> accountIds = new List<Tuple<int, string>>();
            var regionAndServer = $"{deployedRegion}-{homeRegion}-{serverIndex}";
            for (int accountId = startAccountId; accountId < n+startAccountId; accountId++)
            {
                accountIds.Add(new Tuple<int, string>(accountId, regionAndServer));
            }
            return accountIds;
        }
    }
}