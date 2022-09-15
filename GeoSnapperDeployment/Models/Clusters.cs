using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace GeoSnapperDeployment
{
    public record Clusters
    {
        public string ClusterId {get; init;} 

        public string ServiceId {get; init;}
    }
}