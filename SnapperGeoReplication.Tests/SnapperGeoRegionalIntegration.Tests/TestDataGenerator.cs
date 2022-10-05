
using System;
using System.Collections.Generic;
using System.Linq;
using Concurrency.Interface.Models;

namespace SnapperGeoRegionalIntegration.Tests
{
    public class TestDataGenerator
    {
        public static List<string> GetAccessInfoClassNames(int n)
        {
            Type snapperTransactionalAccountGrainType = typeof(SmallBank.Grains.SnapperTransactionalAccountGrain);
            // string snapperTransactionalAccountGrainTypeName = snapperTransactionalAccountGrainType.ToString();
            string snapperTransactionalAccountGrainTypeName = "SmallBank.Grains.SnapperTransactionalAccountGrain";
            // Consol
            return Enumerable.Repeat(snapperTransactionalAccountGrainTypeName, n).ToList<string>();
        }

        public static List<GrainAccessInfo> GetAccountsFromRegion(int n, int startAccountId, string deployedRegion, string homeRegion, int serverIndex, string grainClassName)
        {
            List<GrainAccessInfo> accountIds = new List<GrainAccessInfo>();
            var regionAndServer = $"{deployedRegion}-{homeRegion}-{serverIndex}";
            for (int accountId = startAccountId; accountId < n+startAccountId; accountId++)
            {
                accountIds.Add(
                new GrainAccessInfo()
                {
                    Id = accountId,
                    Region = regionAndServer,
                    GrainClassName = grainClassName
                });
            }
            return accountIds;
        }

        public static List<Tuple<int, string>> GetAccountsFromRegion(List<GrainAccessInfo> grainAccessInfos)
        {
            List<Tuple<int, string>> accountIds = new List<Tuple<int, string>>();
            foreach (var grainAccessInfo in grainAccessInfos)
            {
                accountIds.Add(new Tuple<int, string>(grainAccessInfo.Id, grainAccessInfo.Region));
            }

            return accountIds;
        }
    }
}