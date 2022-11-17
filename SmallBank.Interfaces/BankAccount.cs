using System;
using System.Runtime.Serialization;
using MessagePack;

namespace SmallBank.Interfaces
{
    [MessagePackObject]
    public class BankAccount : ICloneable, ISerializable
    {
        [Key(0)]
        public Tuple<int, string> accountID = null;
        [Key(1)]
        public float Balance = 0;

        public BankAccount()
        {
        }

        /// <summary> This constructor is used to deserialize values. </summary>
        public BankAccount(SerializationInfo info, StreamingContext context)
        {
            accountID = (Tuple<int, string>) info.GetValue("ID", typeof(Tuple<int, string>));
            Balance = (float)info.GetValue("balance", typeof(float));
        }

        /// <summary> This method is used to serialize values. </summary>
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("ID", accountID, typeof(int));
            info.AddValue("balance", Balance, typeof(float));
        }

        public object Clone()
        {
            var account = new BankAccount();
            account.accountID = accountID;
            account.Balance = Balance;
            return account;
        }
    }
}