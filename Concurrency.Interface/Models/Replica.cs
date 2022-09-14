namespace Concurrency.Interface.Models
{
    public record Replica
    {
        public int Id {get; init;}

        public string Region {get; init;}

        public string IPAddress {get; init;}

        public int Port {get; init;}
    }
}