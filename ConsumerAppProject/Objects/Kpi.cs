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
        public double SpeedSum;
        public double AvgSpeed => Count == 0 ? 0 : SpeedSum / Count;
        public double MinFuel = double.PositiveInfinity;
        public double MaxFuel = double.NegativeInfinity;
        public double TempViolation;

        public const double TEMPLIMIT = 90;
    }
}
