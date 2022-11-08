using System.IO;

namespace Utilities
{
    public class Constants
    {
        public const int NumberOfLocalCoordinatorsPerSilo = 4;
        public const int NumberOfRegionalCoordinators = 2;

        // @"..\Snapper-Orleans\data\"
        public static readonly string LogPath = Path.Combine(Directory.GetCurrentDirectory(), "Logs");
    }
}