using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;

namespace Concurrency.Implementation.Coordinator
{
    public interface ILocalDeterministicTransactionProcessorFactory
    {
        ILocalDeterministicTransactionProcessor Create(IGrainFactory grainFactory, GrainReference grainReference);
    }
}