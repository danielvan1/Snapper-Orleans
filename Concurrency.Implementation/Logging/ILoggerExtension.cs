using Orleans.Runtime;

namespace Concurrency.Implementation.Logging
{
    public interface ILoggerExtension
    {
        public void LogInformation(string message, GrainReference grainReference, object o1, object o2, object o3, object o4);

        public void LogInformation(string message, GrainReference grainReference, object o1, object o2, object o3);

        public void LogInformation(string message, GrainReference grainReference, object o1, object o2);

        public void LogInformation(string message, GrainReference grainReference, object o1);

        public void LogInformation(string message, GrainReference grainReference);

        public void LogError(string message, GrainReference grainReference);
    }
}