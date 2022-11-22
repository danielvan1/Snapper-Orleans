using System;
using System.Net;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public class SiloInfo
    {
        public IPEndPoint IPEndPoint {get; init;}

        public string ClusterId {get; set;}

        public string ServiceId {get; set;}

        public int ServerIndex { get; set; }

        public int SiloId {get; init;}

        public int SiloPort {get; init;}

        public int GatewayPort {get; init;}

        public string Region {get; set;}

        public string HomeRegion {get; set;}

        public bool IsReplica {get; set;}
    }
}