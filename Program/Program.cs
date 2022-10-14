namespace Program
{
    public class Program
    {
        public static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }

        private static async Task MainAsync(string[] args)
        {
            ExperimentRunner experimentRunner = new ExperimentRunner();
            await experimentRunner.ManyMultiTransferTransactions();
        }
    }
}