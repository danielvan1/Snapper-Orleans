using Utilities;
using Concurrency.Interface.Coordinator;
using System.Collections.Generic;
using Orleans;
using System.Diagnostics;

namespace Concurrency.Implementation.Coordinator
{
    public class CoordMap : ICoordMap
    {
        private Dictionary<int, ILocalCoordGrain> localCoordMap;
        private Dictionary<int, IGlobalCoordGrain> globalCoordMap;

        public void Init(IGrainFactory myGrainFactory)
        {
            var totalNumLocalCoord = 0;
            var totalNumGlobalCoord = 0;

            if (Constants.multiSilo)
            {
                if (Constants.hierarchicalCoord)
                {
                    totalNumLocalCoord = Constants.numSilo * Constants.numLocalCoordPerSilo;
                    totalNumGlobalCoord = Constants.numGlobalCoord;
                }
                else
                {
                    totalNumLocalCoord = Constants.numGlobalCoord;
                }             
            }
            else
            {
                totalNumLocalCoord = Constants.numLocalCoordPerSilo;
            }

            this.localCoordMap = new Dictionary<int, ILocalCoordGrain>();
            this.globalCoordMap = new Dictionary<int, IGlobalCoordGrain>();

            for (int i = 0; i < totalNumLocalCoord; i++)
            {
                var localCoord = myGrainFactory.GetGrain<ILocalCoordGrain>(i);
                this.localCoordMap.Add(i, localCoord);
            }

            // for (int i = 0; i < totalNumGlobalCoord; i++)
            // {
            //     var globalCoord = myGrainFactory.GetGrain<IGlobalCoordGrain>(i);
            //     this.globalCoordMap.Add(i, globalCoord);
            // }
        }

        public IGlobalCoordGrain GetGlobalCoord(int globalCoordID)
        {
            Debug.Assert(this.globalCoordMap.ContainsKey(globalCoordID));

            return this.globalCoordMap[globalCoordID];
        }

        public ILocalCoordGrain GetLocalCoord(int localCoordID)
        {
            Debug.Assert(this.localCoordMap.ContainsKey(localCoordID));

            return localCoordMap[localCoordID];
        }
    }
}
