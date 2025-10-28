//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;
//using Exceptions;

//namespace Objects
//{
//    public class FinalKpi
//    {
//        public IEnumerable<TelemetryRecord> telemetryRecords;
//        public Kpi kpi = new();

//        public FinalKpi(IEnumerable<TelemetryRecord> telemetryRecords)
//        {
//            if(telemetryRecords == null) throw new TelemetryIsNullException();
//            this.telemetryRecords = telemetryRecords;
//        }

//        public string CalculateKpisAndReturnAsString()
//        {
//            kpi.avgSpeed = telemetryRecords.Average(record => record.speedKmh);//calculate avg of speed
//            kpi.minFuel = telemetryRecords.Min(record => record.fuelPct);//calculate min usage of fuel
//            kpi.maxFuel = telemetryRecords.Max(record => record.fuelPct);//calculate max usage of fuel
//            kpi.TempViolation = telemetryRecords.Count(record => record.coolantTempC > kpi.TEMPLIMIT);//count of temp violations

//            return $"average speed: {kpi.avgSpeed}\n min fuel: {kpi.minFuel}\n max fuel: {kpi.maxFuel}\n Number of temperature violations: {kpi.TempViolation}";

//        }
//    }
//}
