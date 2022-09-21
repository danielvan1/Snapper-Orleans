using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Concurrency.Implementation.Logging
{
    public static class LoggerExtension
    {
        public static void LogInformation(this ILogger logger, string message, GrainReference grainReference)
        {
            var grainIdentity = grainReference.GrainIdentity;

            long id = grainIdentity.GetPrimaryKeyLong(out string region);
            string identityString = grainIdentity.IdentityString;

            logger.LogInformation($"[{id}-{region}-{GetClassName(identityString)}]: {message}");
        }

        public static void LogError(this ILogger logger, string message, GrainReference grainReference)
        {
            var grainIdentity = grainReference.GrainIdentity;

            long id = grainIdentity.GetPrimaryKeyLong(out string region);
            string identityString = grainIdentity.IdentityString;

            logger.LogError($"[{id}-{region}-{GetClassName(identityString)}]: {message}");
        }

        private static string GetClassName(string identityString)
        {
            // IdentityString is on the form xxx/my.namespace.class/yyyyy
            var split = identityString.Split("/")[1].Split(".");

            return split[split.Length - 1];
        }
    }
}