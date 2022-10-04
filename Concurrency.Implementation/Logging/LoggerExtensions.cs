using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;

namespace Concurrency.Implementation.Logging
{
    public static class LoggerExtension
    {
        public static void LogInformation(this ILogger logger, string message, GrainReference grainReference, object o1, object o2, object o3, object o4)
        {
            var grainIdentity = grainReference.GrainIdentity;

            long id = grainIdentity.GetPrimaryKeyLong(out string region);
            string identityString = grainIdentity.IdentityString;

            logger.LogInformation($"[{{id}}-{{region}}-{{ClassName}}]: {message}", id, region, GetClassName(identityString), o1, o2, o3, o4);
        }

        public static void LogInformation(this ILogger logger, string message, GrainReference grainReference, object o1, object o2, object o3)
        {
            var grainIdentity = grainReference.GrainIdentity;

            long id = grainIdentity.GetPrimaryKeyLong(out string region);
            string identityString = grainIdentity.IdentityString;

            logger.LogInformation($"[{{id}}-{{region}}-{{ClassName}}]: {message}", id, region, GetClassName(identityString), o1, o2, o3);
        }

        public static void LogInformation(this ILogger logger, string message, GrainReference grainReference, object o1, object o2)
        {
            var grainIdentity = grainReference.GrainIdentity;

            long id = grainIdentity.GetPrimaryKeyLong(out string region);
            string identityString = grainIdentity.IdentityString;

            logger.LogInformation($"[{{id}}-{{region}}-{{ClassName}}]: {message}", id, region, GetClassName(identityString), o1, o2);
        }

        public static void LogInformation(this ILogger logger, string message, GrainReference grainReference, object o1)
        {
            var grainIdentity = grainReference.GrainIdentity;

            long id = grainIdentity.GetPrimaryKeyLong(out string region);
            string identityString = grainIdentity.IdentityString;

            logger.LogInformation($"[{{id}}-{{region}}-{{ClassName}}]: {message}", id, region, GetClassName(identityString), o1);
        }

        public static void LogInformation(this ILogger logger, string message, GrainReference grainReference)
        {
            var grainIdentity = grainReference.GrainIdentity;

            long id = grainIdentity.GetPrimaryKeyLong(out string region);
            string identityString = grainIdentity.IdentityString;

            logger.LogInformation($"[{{id}}-{{region}}-{{ClassName}}]: {message}", id, region, GetClassName(identityString));
        }

        public static void LogError(this ILogger logger, string message, GrainReference grainReference)
        {
            var grainIdentity = grainReference.GrainIdentity;
            long id = grainReference.GetPrimaryKeyLong(out string region);
            string identityString = grainIdentity.IdentityString;

            logger.LogError($"[{{id}}-{{region}}-{{ClassName}}]: {message}", id, region, GetClassName(identityString));
        }

        private static string GetClassName(string identityString)
        {
            // IdentityString is on the form xxx/my.namespace.class/yyyyy
            var split = identityString.Split("/")[1].Split(".");

            return split.LastOrDefault(string.Empty);
        }
    }
}