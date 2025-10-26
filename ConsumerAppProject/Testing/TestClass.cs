using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Objects;
using Exceptions;

namespace Testing
{
    public static class TestClass
    {
        private static IEnumerable<TelemetryRecord> CreateTelemetryRecordTestTest()
        {
            return new List<TelemetryRecord>
            {
                new (vehicleId:"V-001", tsUtc:"2025-10-26T12:00:00Z", speedKmh:72.3f, fuelPct:58.2f, coolantTempC:91.4f),
                new (vehicleId:"V-002", tsUtc:"2025-10-26T12:00:01Z", speedKmh:65.8f, fuelPct:42.9f, coolantTempC:87.6f),
                new (vehicleId:"V-003", tsUtc:"2025-10-26T12:00:02Z", speedKmh:80.5f, fuelPct:60.1f, coolantTempC:95.2f),
                new (vehicleId:"V-004", tsUtc:"2025-10-26T12:00:03Z", speedKmh:50.2f, fuelPct:30.7f, coolantTempC:82.9f),
                new (vehicleId:"V-005", tsUtc:"2025-10-26T12:00:04Z", speedKmh:90.1f, fuelPct:76.3f, coolantTempC:99.8f),
                new (vehicleId:"V-006", tsUtc:"2025-10-26T12:00:05Z", speedKmh:43.9f, fuelPct:25.4f, coolantTempC:85.1f),
                new (vehicleId : "V-007", tsUtc:"2025-10-26T12:00:06Z", speedKmh:77.7f, fuelPct:52.0f, coolantTempC:89.0f),
                new (vehicleId : "V-008", tsUtc:"2025-10-26T12:00:07Z", speedKmh:68.4f, fuelPct:48.5f, coolantTempC:88.3f),
                new (vehicleId : "V-009", tsUtc:"2025-10-26T12:00:08Z", speedKmh:55.3f, fuelPct:33.2f, coolantTempC:84.7f),
                new (vehicleId : "V-010", tsUtc:"2025-10-26T12:00:09Z", speedKmh:83.9f, fuelPct:70.4f, coolantTempC:93.6f)
            };
        }

        public static void SolveTest()
        {
            IEnumerable<TelemetryRecord> telemetryRecords = CreateTelemetryRecordTestTest();
            FinalKpi kpi = new (telemetryRecords);

            Console.WriteLine($"Final kpi is:\n{kpi.CalculateKpisAndReturnAsString()}");
        }
    }
}
