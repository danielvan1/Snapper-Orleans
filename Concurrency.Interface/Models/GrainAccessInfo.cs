using System.Diagnostics.Contracts;

namespace Concurrency.Interface.Models
{
    public class GrainAccessInfo
    {
        public int Id { get; init; }

        public string Region { get; init; }

        public string GrainClassName { get; init; }

        public override string ToString()
        {
            return $"Id: {this.Id}, Region {this.Region}, GrainClassName: {this.GrainClassName}";
        }

    }
}