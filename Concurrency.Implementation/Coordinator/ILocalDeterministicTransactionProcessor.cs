using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Utilities;

namespace Concurrency.Implementation.Coordinator
{
    public interface ILocalDeterministicTransactionProcessor
    {
        Task<TransactionRegisterInfo> NewLocalTransaction(List<GrainAccessInfo> grainAccessInfos);

        Task<TransactionRegisterInfo> NewRegionalTransaction(long regionalBid, long regionalTid, List<GrainAccessInfo> grainAccessInfo);

        IList<long> GenerateRegionalBatch(LocalToken token);

        long GenerateLocalBatch(LocalToken token);

        Task EmitBatch(long bid);

        Task WaitForBatchToCommit(long bid);

        Task AckBatchCompletion(long bid);

        Task AckRegionalBatchCommit(long regionalBid);

        Task ReceiveBatchSchedule(SubBatch batch);
    }
}