using Orleans;

namespace Concurrency.Interface.Coordinator
{
    public interface ICoordMap
    {
        void Init(IGrainFactory myGrainFactory);

        ILocalCoordinatorGrain GetLocalCoord(int localCoordID);

        IGlobalCoordinatorGrain GetGlobalCoord(int globalCoordID);
    }
}
