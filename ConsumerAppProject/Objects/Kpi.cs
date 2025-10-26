using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Objects
{
    public class Kpi
    {
        public float avgSpeed { get; set; }
        public float minFuel { get; set; }
        public float maxFuel { get; set; }
        public float TempViolation { get; set; }

        public readonly float TEMPLIMIT = 90;
    }
}
