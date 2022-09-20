using System;

namespace Concurrency.Implementation.Exceptions
{
    public class GrainPlacementException : Exception
    {
        public GrainPlacementException(string message) : base(message)
        {
        }
    }
}