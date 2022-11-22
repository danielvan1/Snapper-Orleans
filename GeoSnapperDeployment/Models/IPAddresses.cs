namespace GeoSnapperDeployment.Models
{
    public record IPConfig
    {
        public string Region { get; init; }

        public string IPAddress { get; init; }

        public int ServerIndex { get; init; }
    }
}