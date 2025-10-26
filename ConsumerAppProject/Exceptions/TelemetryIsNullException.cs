using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exceptions
{
    public class TelemetryIsNullException:Exception
    {
        public TelemetryIsNullException():base("Content inside the telemetry is null.") { }
    }
}
