using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Helper
{
    public static class VerifierHelper
    {
        public static async Task<bool> CheckAsync<T>(T value,
            Func<T, bool> condition,
            string filePath, string sidecarPath, string reason,
            Func<string,string, string, Task> errorHandler
            )
        {
            if(!condition(value))
            {
                await errorHandler(filePath, sidecarPath, reason);
                return false;
            }

            return true;
        }
    }
}
