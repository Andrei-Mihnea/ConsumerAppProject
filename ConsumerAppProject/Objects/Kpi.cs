using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Objects
{
    public class Kpi
    {
        public int Count;
        public float SpeedSum;
        public float AvgSpeed => Count == 0 ? 0 : SpeedSum / Count;
        public float MinFuel = -999;
        public float MaxFuel = -999;
        public float TempViolation;

        public readonly float TEMPLIMIT = 90;
    }
}
