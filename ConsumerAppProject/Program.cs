using Testing;

namespace ConsumerAppProject
{
    internal class Program
    {
        static void Main(string[] args)
        {
            try
            {
                TestClass.SolveTest();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
