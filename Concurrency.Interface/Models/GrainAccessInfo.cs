using System;
using System.Diagnostics.Contracts;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public record GrainAccessInfo
    {
        public int Id { get; init; }

        public string Region { get; init; }

        public string GrainClassName { get; init; }

        public string GetDeploymentRegion()
        {
            return this.Region.Substring(0, 2);
        }

        public string GetHomeRegion()
        {
            return this.Region.Substring(3, 5);
        }

        public string ReplaceDeploymentRegion(string newRegion)
        {
            return $"{newRegion}-{this.Region.Substring(3)}";
        }

        public override string ToString()
        {
            return $"Id: {this.Id}, Region {this.Region}, GrainClassName: {this.GrainClassName}";
        }
    }
}