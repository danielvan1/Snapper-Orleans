using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Interface.Models;

namespace Concurrency.Implementation.Coordinator
{
    public class RegionalBatchProcessInfo
    {
        public long RegionalCoordinatorId { get; set; }

        public bool IsPreviousBatchRegional { get; set; }

        public SubBatch RegionalSubBatch { get; set; }

        public TaskCompletionSource<bool> BatchCommitPromise { get; set; } = new TaskCompletionSource<bool>(false);

        public Dictionary<long, List<GrainAccessInfo>> RegionalTransactionInfos { get; } = new Dictionary<long, List<GrainAccessInfo>>();
    }

}