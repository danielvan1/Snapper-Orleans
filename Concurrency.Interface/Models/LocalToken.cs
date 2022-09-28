using System;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public class LocalToken : TokenBase
    {
        // for global info
        public long lastEmitGlobalBid;

        public LocalToken() : base()
        {
            lastEmitGlobalBid = -1;
        }
    }
}