using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Concurrency.Interface.TransactionExecution;
using Utilities;

namespace SmallBank.Interfaces
{
    public interface ISnapperTransactionalAccountGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(TransactionContext context, FunctionInput funcInput);

        Task<TransactionResult> Balance(TransactionContext context, FunctionInput funcInput);

        Task<TransactionResult> MultiTransfer(TransactionContext context, FunctionInput funcInput);

        Task<TransactionResult> Deposit(TransactionContext context, FunctionInput funcInput);

        Task<BankAccount> GetState();
    }
}