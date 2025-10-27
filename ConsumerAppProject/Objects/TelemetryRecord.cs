using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Objects
{
    public class TelemetryRecord
    {
        public string vehicleId { get; set; }
        public DateTime tsUtc { get; set; }
        public float speedKmh { get; set; }
        public float fuelPct { get; set; }
        public float coolantTempC {  get; set; }

        public TelemetryRecord(string vehicleId, DateTime tsUtc, float speedKmh, float fuelPct, float coolantTempC)
        {
            this.vehicleId = vehicleId;
            this.tsUtc = tsUtc;
            this.speedKmh = speedKmh;
            this.fuelPct = fuelPct;
            this.coolantTempC = coolantTempC;
        }
    }
}
