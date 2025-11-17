using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Interfaces
{
    public interface IFileHandler
    {
        Task HandleAsync(string filePath, CancellationToken ct = default);
    }
}
