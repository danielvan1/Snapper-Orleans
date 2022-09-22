using Orleans;

namespace Concurrency.Interface.Coordinator
{
    public interface ICoordMap
    {
        void Init(IGrainFactory myGrainFactory);

        ILocalCoordinatorGrain GetLocalCoord(long localCoordID);

        IGlobalCoordinatorGrain GetGlobalCoord(long globalCoordID);
    }
}
