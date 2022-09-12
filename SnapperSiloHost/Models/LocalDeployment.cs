using System.Collections.Generic;

namespace SnapperSiloHost.Models
{
    public sealed class LocalDeployment
    {
        public string ClusterId {get; set;}

        public string ServiceId {get; set;}

        public int PrimarySiloEndpoint {get; set;}

        public string IsMultiSiloDeployment {get; set;}

        public string RealScaleOut {get; set;}

        public string LocalCluster {get; set;}

        public string LoggingType {get; set;}

        public string NumberOfCPUPerSilo {get; set;}

        public string NumberOfSilos {get; set;}

        public string ImplementationType {get; set;}

        public List<SiloInfo> Silos {get; set;}
    }
}