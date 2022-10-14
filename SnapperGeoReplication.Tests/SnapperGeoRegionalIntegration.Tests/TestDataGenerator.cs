
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

        public static FunctionInput CreateMultiTransferFunctionInput(int value, List<Tuple<int, string>> grainAccessInfos)
        {
            FunctionInput functionInput = new FunctionInput()
            {
                DestinationGrains = new List<TransactionInfo>()
            };

            foreach (var grainAccessInfo in grainAccessInfos)
            {
                functionInput.DestinationGrains.Add(new TransactionInfo()
                {
                    DestinationGrain = grainAccessInfo,
                    Value = value
                });
            }

            return functionInput;
        }
    }
}