using System;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public class FunctionCall
    {
        public readonly string funcName;
        public readonly FunctionInput functionInput;
        public readonly Type grainClassName;

        public FunctionCall(string funcName, FunctionInput funcInput, Type grainClassName)
        {
            this.funcName = funcName;
            this.functionInput = funcInput;
            this.grainClassName = grainClassName;
        }

        public override string ToString()
        {
            return $"FunctionName: {funcName}, FunctionInput: {functionInput}, GrainClassName: {grainClassName}";
        }
    }
}