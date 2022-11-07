using System.IO;

namespace Utilities
{
    public class Constants
    {
        public const int NumberOfLocalCoordinatorsPerSilo = 2;
        public const int NumberOfRegionalCoordinators = 2;

        public const int numSilo = 2;
        // global silo config
        public const double scaleSpeed = 1.75;
        //public const int batchSizeInMSecsBasic = RealScaleOut ? 30 : 20;
        public const int batchSizeInMSecsBasic = 20;

        // @"..\Snapper-Orleans\data\"
        public static readonly string LogPath = Path.Combine(Directory.GetCurrentDirectory(), "Logs");
    }
}