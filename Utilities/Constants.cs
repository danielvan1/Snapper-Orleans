using System.IO;

namespace Utilities
{
    public class Constants
    {
        public const int NumberOfLocalCoordinatorsPerSilo = 1;
        public const int NumberOfRegionalCoordinators = 1;

        public const double scaleSpeed = 1.75;
        public const int batchSizeInMSecsBasic = RealScaleOut ? 30 : 20;
        public const bool RealScaleOut = false;
        public const int numSilo = 4;


        // @"..\Snapper-Orleans\data\"
        public static readonly string LogPath = Path.Combine(Directory.GetCurrentDirectory(), "Logs");
    }
}