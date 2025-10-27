using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Objects
{
    public sealed record ConsumerConfig(
        string Inbox,
        string Archive,
        string Error,
        int BufferSize,
        int MaxRetries,
        int DebounceMs
    );
}
