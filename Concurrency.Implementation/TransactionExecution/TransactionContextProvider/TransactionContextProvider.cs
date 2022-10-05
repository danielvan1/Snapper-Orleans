using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.LoadBalancing;
using Concurrency.Implementation.TransactionExecution.TransactionPlacement;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution.TransactionContextProvider
{
    public class TransactionContextProvider : ITransactionContextProvider
    {
        private readonly ILogger<TransactionContextProvider> logger;
        private readonly ICoordinatorProvider coordinatorProvider;
        private readonly IPlacementManager placementManager;
        private readonly IGrainFactory grainFactory;
        private readonly GrainReference grainReference;
        private readonly GrainId grainId;
        private readonly string mySiloId;

        private readonly ILocalCoordinatorGrain localCoordinator;
        private readonly IRegionalCoordinatorGrain regionalCoordinator;                                // use this coord to get tid for global transactions

        public TransactionContextProvider(ILogger<TransactionContextProvider> logger,
                                          ICoordinatorProvider coordinatorProvider,
                                          IPlacementManager placementManager,
                                          IGrainFactory grainFactory,
                                          GrainReference grainReference,
                                          GrainId grainId)
        {
            this.logger = logger ?? throw new System.ArgumentNullException(nameof(logger));
            this.coordinatorProvider = coordinatorProvider ?? throw new ArgumentNullException(nameof(coordinatorProvider));
            this.placementManager = placementManager ?? throw new ArgumentNullException(nameof(placementManager));
            this.grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
            this.grainReference = grainReference ?? throw new ArgumentNullException(nameof(grainReference));
            this.grainId = grainId ?? throw new ArgumentNullException(nameof(grainId));

            this.mySiloId = grainId.StringId;

            this.localCoordinator = coordinatorProvider.GetLocalCoordinatorGrain(grainId.IntId, grainId.StringId, grainFactory);
            this.regionalCoordinator = coordinatorProvider.GetRegionalCoordinator(grainId.IntId, this.mySiloId.Substring(0, 2), grainFactory);
        }

        // int: the highestCommittedBid get from local coordinator
        /// <summary>
        /// This returns the Bid and TransactionContext. Also start the process of starting to create the (sub)batches
        /// in the RegionalCoordinator (if it is a regional transaction) and also in the LocalCoordinator.
        /// We also figure out whether the transaction is a multi-server or single server transacation.
        /// </summary>
        /// <param name="grainAccessInfos"></param>
        /// <param name="grainClassNames"></param>
        /// <returns></returns>
        public async Task<Tuple<long, TransactionContext>> GetDeterministicContext(List<GrainAccessInfo> grainAccessInfos)
        {
            this.logger.LogInformation("Getting context for grainList: [{grainList}] and grainClassNames: [{grainClassNames}]", this.grainReference, string.Join(", ", grainAccessInfos));

            var grainListPerSilo = this.GroupGrainsPerSilo(grainAccessInfos);

            TransactionType transactionType = this.placementManager.GetTransactionType(grainAccessInfos);

            return TransactionType.SingleHomeMultiServer.Equals(transactionType) ? await this.GetRegionalContext(grainListPerSilo)
                                                                                 : await this.GetLocalContext(grainAccessInfos);
        }

        private Dictionary<string, List<GrainAccessInfo>> GroupGrainsPerSilo(List<GrainAccessInfo> grainAccessInfos)
        {
            // check if the transaction will access multiple silos
            var grainsPerSilo = new Dictionary<string, List<GrainAccessInfo>>();

            // This is the placement manager(PM) code described in the paper
            foreach (GrainAccessInfo grainAccessInfo in grainAccessInfos)
            {
                var siloId = grainAccessInfo.Region;
                this.logger.LogInformation("SiloId: {siloId}", siloId);

                if (!grainsPerSilo.ContainsKey(siloId))
                {
                    grainsPerSilo.Add(siloId, new List<GrainAccessInfo>());
                }


                grainsPerSilo[siloId].Add(grainAccessInfo);
            }

            return grainsPerSilo;
        }

        private async Task<Tuple<long, TransactionContext>> GetLocalContext(List<GrainAccessInfo> grainAccessInfos)
        {
            TransactionRegisterInfo info = await this.localCoordinator.NewLocalTransaction(grainAccessInfos);
            this.logger.LogInformation("Received TransactionRegisterInfo {info} from localCoordinator: {coordinator}", this.grainReference, info, this.localCoordinator);

            var cxt2 = new TransactionContext(info.Tid, info.Bid);
            var localContext = new Tuple<long, TransactionContext>(info.HighestCommittedBid, cxt2);


            return localContext;
        }

        private async Task<Tuple<long, TransactionContext>> GetRegionalContext(Dictionary<string, List<GrainAccessInfo>> grainsPerSilo)
        {
            var siloIds = grainsPerSilo.Keys.ToList();
            // get regional tid from regional coordinator
            // Note the Dictionary<string, Tuple<int, string>> part of the
            // return type of NewTransaction(..) is a map between the region
            // and which local coordinators
            Tuple<TransactionRegisterInfo, Dictionary<string, Tuple<int, string>>> regionalInfo =
                await this.regionalCoordinator.NewRegionalTransaction(siloIds);

            var regionalTid = regionalInfo.Item1.Tid;
            var regionalBid = regionalInfo.Item1.Bid;
            Dictionary<string, Tuple<int, string>> siloIDToLocalCoordID = regionalInfo.Item2;

            // send corresponding grainAccessInfo to local coordinators in different silos
            Debug.Assert(grainsPerSilo.ContainsKey(mySiloId));
            Task<TransactionRegisterInfo> task = null;

            for (int i = 0; i < siloIds.Count; i++)
            {
                var siloId = siloIds[i];
                Debug.Assert(siloIDToLocalCoordID.ContainsKey(siloId));

                // TODO: Need a map from coordinator to local coordinator
                var coordId = siloIDToLocalCoordID[siloId];
                var localCoordinator = this.grainFactory.GetGrain<ILocalCoordinatorGrain>(coordId.Item1, coordId.Item2);

                // get local tid, bid from local coordinator
                this.logger.LogInformation("Chosen LocalCoordinator: {id}-{regionl}, siloId: {siloId}", this.grainReference, coordId.Item1, coordId.Item2, this.mySiloId);
                Console.WriteLine($"{this.grainReference}: Chosen LocalCoordinator: {coordId.Item1}-{coordId.Item2}, siloId: {this.mySiloId}");
                this.logger.LogInformation("Herp: {x}, {grainReference}", 1, this.grainReference);
                if (coordId.Item2 == this.mySiloId)
                {
                    this.logger.LogInformation($"Is calling NewRegionalTransaction w/ task", this.grainReference);
                    task = localCoordinator.NewRegionalTransaction(regionalBid, regionalTid, grainsPerSilo[siloId]);
                }
                else
                {
                    this.logger.LogInformation($"Is calling NewRegionalTransaction w/o task", this.grainReference);

                    _ = localCoordinator.NewRegionalTransaction(regionalBid, regionalTid, grainsPerSilo[siloId]);
                }
            }

            Debug.Assert(task != null);
            this.logger.LogInformation($"Waiting for task in GetDetContext", this.grainReference);
            TransactionRegisterInfo localInfo = await task;
            this.logger.LogInformation($"Is DONE waiting for task in GetDetContext, going to return tx context", this.grainReference);
            var regionalContext = new TransactionContext(localInfo.Bid, localInfo.Tid, regionalBid, regionalTid);

            // TODO: What is this -1??
            return new Tuple<long, TransactionContext>(-1, regionalContext);
        }
    }
}