using System;

namespace Utilities
{
    [Serializable]
    public class NonDetScheduleInfo
    {
        public long maxBeforeBid;
        public long minAfterBid;
        public bool isAfterComplete;

        public NonDetScheduleInfo()
        {
            maxBeforeBid = -1;
            minAfterBid = long.MaxValue;
            isAfterComplete = true;
        }

        public NonDetScheduleInfo(long maxBeforeBid, long minAfterBid, bool isAfterComplete)
        {
            this.maxBeforeBid = maxBeforeBid;
            this.minAfterBid = minAfterBid;
            this.isAfterComplete = isAfterComplete;
        }
    }
}