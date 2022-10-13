using System;
using Utilities;
using System.Threading.Tasks;
using Concurrency.Interface.TransactionExecution;

namespace Concurrency.Implementation.TransactionExecution
{
    public class DeterministicState<TState> : ITransactionalState<TState> where TState : ICloneable, new()
    {
        private TState committedState;

        // when execution grain is initialized, its hybrid state is initialized
        public DeterministicState() : this(new TState())
        {
        }

        public DeterministicState(TState state)
        {
            committedState = state;
        }

        public TState DetOp()
        {
            return committedState;
        }
    }
}