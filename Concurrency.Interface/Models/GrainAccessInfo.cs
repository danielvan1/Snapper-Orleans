using System;
using System.Diagnostics.Contracts;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public record GrainAccessInfo
    {
        public int Id { get; init; }

        /// <summary>
        /// If the silo is a LocalSilo then it consists of the string deploymentRegion-homeRegion-x where x is some integer.
        /// If it is a RegionalSilo then the silo id consist of the string region-x.
        /// </summary>
        public string SiloId { get; init; }

        public string GrainClassNamespace { get; init; }

        public string ReplaceDeploymentRegion(string newDeploymentRegion)
        {
            return $"{newDeploymentRegion}-{this.SiloId.Substring(3)}";
        }

        public override string ToString()
        {
            return $"Id: {this.Id}, SiloId {this.SiloId}, GrainClassName: {this.GrainClassNamespace}";
        }
    }
}