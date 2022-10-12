using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Utilities
{
    [Serializable]
    public class NonDetFuncResult : BasicFuncResult
    {
        public bool exception = false;
        public bool Exp_Deadlock = false;

        // this info is used to check global serializability
        public NonDetScheduleInfo globalScheduleInfo;
        public Dictionary<int, NonDetScheduleInfo> scheduleInfoPerSilo;     // <silo ID, scheule info>

        // <silo ID, grain ID, grain name>
        // this info is used to commit an ACT after its dependent batch has committed
        public Dictionary<int, Tuple<int, string>> grainWithMaxBeforeLocalBidPerSilo;

        public NonDetFuncResult() : base()
        {
            globalScheduleInfo = new NonDetScheduleInfo();
            scheduleInfoPerSilo = new Dictionary<int, NonDetScheduleInfo>();
            grainWithMaxBeforeLocalBidPerSilo = new Dictionary<int, Tuple<int, string>>();
        }

        public void MergeFuncResult(NonDetFuncResult res)
        {
            exception |= res.exception;
            Exp_Deadlock |= res.Exp_Deadlock;

            foreach (var info in res.scheduleInfoPerSilo)
                MergeBeforeAfterLocalInfo(info.Value, res.grainWithMaxBeforeLocalBidPerSilo[info.Key], info.Key);

            MergeBeforeAfterGlobalInfo(res.globalScheduleInfo);

            MergeGrainOpInfo(res);
        }

        public void MergeBeforeAfterLocalInfo(
            NonDetScheduleInfo newLocalInfo, 
            Tuple<int, string> grainFullID, 
            int mySiloID)
        {
            if (scheduleInfoPerSilo.ContainsKey(mySiloID) == false)
            {
                scheduleInfoPerSilo.Add(mySiloID, newLocalInfo);
                grainWithMaxBeforeLocalBidPerSilo.Add(mySiloID, grainFullID);
            }
            else
            {
                var myLocalInfo = scheduleInfoPerSilo[mySiloID];
                
                if (myLocalInfo.maxBeforeBid < newLocalInfo.maxBeforeBid)
                {
                    myLocalInfo.maxBeforeBid = newLocalInfo.maxBeforeBid;
                    Debug.Assert(grainWithMaxBeforeLocalBidPerSilo.ContainsKey(mySiloID));
                    grainWithMaxBeforeLocalBidPerSilo[mySiloID] = grainFullID;
                }

                myLocalInfo.minAfterBid = Math.Min(myLocalInfo.minAfterBid, newLocalInfo.minAfterBid);
                myLocalInfo.isAfterComplete &= newLocalInfo.isAfterComplete;
            }
        }

        public void MergeBeforeAfterGlobalInfo(NonDetScheduleInfo newGlobalInfo)
        {
            globalScheduleInfo.maxBeforeBid = Math.Max(globalScheduleInfo.maxBeforeBid, newGlobalInfo.maxBeforeBid);
            globalScheduleInfo.minAfterBid = Math.Min(globalScheduleInfo.minAfterBid, newGlobalInfo.minAfterBid);
            globalScheduleInfo.isAfterComplete &= newGlobalInfo.isAfterComplete;
        }
    }
}