using System;

namespace Utilities
{
    [Serializable]
    public class OpOnGrain
    {
        public bool isNoOp;
        public bool isReadonly;
        public readonly string grainClassName;

        public OpOnGrain(string grainClassName, bool isNoOp, bool isReadonly)
        {
            this.grainClassName = grainClassName;
            this.isNoOp = isNoOp;
            this.isReadonly = isReadonly;
        }
    }
}