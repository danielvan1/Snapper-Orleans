using System;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public record GrainAccessInfo
    {
        public int Id { get; init; }

        public string Region { get; init; }

        public string GrainClassName { get; init; }
    }
}