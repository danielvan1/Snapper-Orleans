using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class BasicFuncResult
    {
        public object resultObj;
        public bool isNoOpOnGrain = true;
        public bool isReadOnlyOnGrain = true;
        public Dictionary<int, OpOnGrain> grainOpInfo = new Dictionary<int, OpOnGrain>();   // <grainID, operations performed on the grains>

        public void SetResultObj(object resultObj)
        {
            this.resultObj = resultObj;
        }

        public void MergeGrainOpInfo(BasicFuncResult res)
        {
            foreach (var item in res.grainOpInfo)
            {
                if (grainOpInfo.ContainsKey(item.Key) == false)
                    grainOpInfo.Add(item.Key, item.Value);
                else
                {
                    var grainClassName = item.Value.grainClassName;
                    var isReadOnly = grainOpInfo[item.Key].isReadonly && item.Value.isReadonly;
                    var isNoOp = grainOpInfo[item.Key].isNoOp && item.Value.isNoOp;
                    grainOpInfo[item.Key] = new OpOnGrain(grainClassName, isNoOp, isReadOnly);
                }
            }
        }
    }
}