using System;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public class LocalToken : BasicToken
    {
        // for global info
        public long lastEmitGlobalBid;

        public LocalToken() : base()
        {
            lastEmitGlobalBid = -1;
        }
    }
}