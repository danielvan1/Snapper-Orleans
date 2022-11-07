
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
            string snapperTransactionalAccountGrainTypeName = "SmallBank.Grains.SnapperTransactionalAccountGrain";

            return Enumerable.Repeat(snapperTransactionalAccountGrainTypeName, n).ToList<string>();
        }

        public static List<GrainAccessInfo> CreateAccountIds(int n, int startAccountId, string deployedRegion, string homeRegion, int serverIndex, string grainClassName)
        {
            List<GrainAccessInfo> accountIds = new List<GrainAccessInfo>();

            var regionAndServer = $"{deployedRegion}-{homeRegion}-{serverIndex}";

            for (int accountId = startAccountId; accountId < n+startAccountId; accountId++)
            {
                accountIds.Add(
                new GrainAccessInfo()
                {
                    Id = accountId,
                    SiloId = regionAndServer,
                    GrainClassNamespace = grainClassName
                });
            }

            return accountIds;
        }

        public static FunctionInput CreateFunctionInput(List<GrainAccessInfo> grainAccessInfos)
        {
            FunctionInput functionInput = new FunctionInput()
            {
                DestinationGrains = new List<TransactionInfo>()
            };

            foreach (var grainAccessInfo in grainAccessInfos)
            {
                functionInput.DestinationGrains.Add(new TransactionInfo()
                {
                    DestinationGrain = new Tuple<int, string>(grainAccessInfo.Id, grainAccessInfo.SiloId),
                    Value = 1
                });
            }

            return functionInput;
        }
    }
}