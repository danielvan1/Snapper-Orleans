using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Models;

namespace Concurrency.Implementation.Coordinator
{
    public class LocalBatchProcessInfo
    {
        public long RegionalBid { get; set; } = -1;

        public bool IsRegional {get { return this.RegionalBid != -1; } }

        public long PreviousBid { get; set; }

        public long PreviousCoordinatorId { get; set; }

        public IDictionary<GrainAccessInfo, SubBatch> SchedulePerGrain { get; } = new Dictionary<GrainAccessInfo, SubBatch>();

        public Dictionary<long, long> RegionalToLocalTidMapping { get; } = new Dictionary<long, long>();

        public TaskCompletionSource<bool> BatchCommitPromise { get; set; } = null;
    }
}