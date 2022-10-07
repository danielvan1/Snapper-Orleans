using Orleans;

namespace Concurrency.Implementation.TransactionBroadcasting
{
    public interface ITransactionBroadCasterFactory
    {
        ITransactionBroadCaster Create(IGrainFactory grainFactory);
    }
}