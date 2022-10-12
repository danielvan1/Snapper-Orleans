using System;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public class FunctionCall
    {
        public readonly string funcName;
        public readonly FunctionInput funcInput;
        public readonly Type grainClassName;

        public FunctionCall(string funcName, FunctionInput funcInput, Type grainClassName)
        {
            this.funcName = funcName;
            this.funcInput = funcInput;
            this.grainClassName = grainClassName;
        }

        public override string ToString()
        {
            return $"FunctionName: {funcName}, FunctionInput: {funcInput}, GrainClassName: {grainClassName}";
        }
    }
}