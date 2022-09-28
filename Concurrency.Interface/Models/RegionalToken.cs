using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Concurrency.Interface.Models
{
    public class RegionalToken : TokenBase
    {
        public Dictionary<string, long> PreviousBidPerSilo { get; }

        public RegionalToken() : base()
        {
            this.PreviousBidPerSilo = new Dictionary<string, long>();
        }
    }
}