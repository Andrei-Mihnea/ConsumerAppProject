using Extensions;
using Objects;
using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Helper;

public class FileProcessor
{
    private readonly ConsumerConfig config;
    private static readonly JsonSerializerOptions jsonOptions = new() { PropertyNameCaseInsensitive = true };
    private readonly string supportedSidecarVersion = "1.";

    public FileProcessor(ConsumerConfig config) => this.config = config;

    public async Task ProcessFileAsync(string filePath)
    {
        var sidecarPath = filePath + ".meta.json";
        var perMinute = new Dictionary<(string vehicle, DateTime minute), Kpi>();
        var totals = new Dictionary<string, Kpi>(StringComparer.OrdinalIgnoreCase);
        var prevByVehicle = new Dictionary<string, TelemetryRecord>(StringComparer.OrdinalIgnoreCase);

        if (!await VerifierHelper.CheckAsync(
                sidecarPath,
                File.Exists,
                filePath, sidecarPath,
                "Missing sidecar metadata file.",
                MoveToErrorWithReportAsync)) 
            return;


        SidecarMetadata? metadata = null;
        try
        {
            await using var metaFs = new FileStream(sidecarPath, FileMode.Open, FileAccess.Read, FileShare.Read, config.BufferSize, useAsync: true);
            metadata = await JsonSerializer.DeserializeAsync<SidecarMetadata>(metaFs, jsonOptions);
        }
        catch (Exception ex)
        {
            await MoveToErrorWithReportAsync(filePath, sidecarPath, $"Failed to read/parse sidecar: {ex.Message}");
            return;
        }

        if (!await VerifierHelper.CheckAsync(
                metadata,
                mtdata => mtdata is not null,
                filePath,
                sidecarPath,
                "Sidecar metadata is null.",
                MoveToErrorWithReportAsync)) 
            return;


#pragma warning disable CS8602 // Dereference of a possibly null reference. I know metadata is not null here.

        if (!await VerifierHelper.CheckAsync(
                metadata.version,
                mtdVer => mtdVer.StartsWith(supportedSidecarVersion, StringComparison.Ordinal),
                filePath,
                sidecarPath,
                $"Unsupported sidecar Version: {metadata.version}",
                MoveToErrorWithReportAsync)) 
            return;

#pragma warning restore CS8602 // Dereference of a possibly null reference.

        string computedHash;
        try
        {
            computedHash = await ComputeSha256HexAsync(filePath, config.BufferSize);
        }
        catch (Exception ex)
        {
            await MoveToErrorWithReportAsync(filePath, sidecarPath, $"Failed to compute SHA-256: {ex.Message}");
            return;
        }

        if (!await VerifierHelper.CheckAsync(
                (expected: metadata!.sha256, actual: computedHash),
                pair => string.Equals(pair.expected, pair.actual, StringComparison.OrdinalIgnoreCase),
                filePath, sidecarPath,
                $"Checksum mismatch. expected={metadata!.sha256} actual={computedHash}",
                MoveToErrorWithReportAsync)) 
            return;


        var swTotal = System.Diagnostics.Stopwatch.StartNew();
        int records = 0;

        try
        {
            await foreach (var rec in ReadTelemetryAsync(filePath, metadata).SlidingPairs())
            {
                Aggregate(rec.first, rec.second, perMinute, totals);
            }
        }
        catch (Exception ex)
        {
            await MoveToErrorWithReportAsync(filePath, sidecarPath, $"Parse error: {ex.Message}");
            return;
        }
        finally
        {
            swTotal.Stop();
        }

        try
        {
            var archiveDir = Path.Combine(
                config.Archive,
                DateTime.UtcNow.Year.ToString("0000"),
                DateTime.UtcNow.Month.ToString("00"),
                DateTime.UtcNow.Day.ToString("00"));
            Directory.CreateDirectory(archiveDir);

            await WriteKpisJsonAsync(archiveDir, filePath, perMinute, totals);

            var newData = Path.Combine(archiveDir, Path.GetFileName(filePath));
            var newMeta = Path.Combine(archiveDir, Path.GetFileName(sidecarPath));

            File.Move(filePath, newData, overwrite: true);
            File.Move(sidecarPath, newMeta, overwrite: true);

            Console.WriteLine($"Processed '{Path.GetFileName(filePath)}' | {records} records | {swTotal.ElapsedMilliseconds} ms");
        }
        catch (Exception ex)
        {
            await MoveToErrorWithReportAsync(filePath, sidecarPath, $"Archival failed: {ex.Message}");
        }
    }

    private void Aggregate(TelemetryRecord firstRec, TelemetryRecord secondRec,
                          Dictionary<(string, DateTime), Kpi> perMinute,
                          Dictionary<string, Kpi> totals)
    {
        var minute = new DateTime(firstRec.tsUtc.Year, firstRec.tsUtc.Month, firstRec.tsUtc.Day,
                                  firstRec.tsUtc.Hour, firstRec.tsUtc.Minute, 0, DateTimeKind.Utc);
        var pairAvgSpeed = (firstRec.speedKmh + secondRec.speedKmh) / 2.0;
        var pairMinFuel = Math.Min(firstRec.fuelPct, secondRec.fuelPct);
        var pairMaxFuel = Math.Max(firstRec.fuelPct, secondRec.fuelPct);
        var pairTempViolation = (firstRec.coolantTempC > Kpi.TEMPLIMIT ? 1 : 0);

        var key = (firstRec.vehicleId, minute);

        if (!perMinute.TryGetValue(key, out var minuteKpi))
            minuteKpi = perMinute[key] = new Kpi();

        minuteKpi.Count++;
        minuteKpi.SpeedSum += pairAvgSpeed;
        minuteKpi.MinFuel = Math.Min(minuteKpi.MinFuel, pairMinFuel);
        minuteKpi.MaxFuel = Math.Max(minuteKpi.MaxFuel, pairMaxFuel);
        minuteKpi.TempViolation += pairTempViolation;
    }

    private static string GetBaseName(string path)
    {
        var name = Path.GetFileName(path);
        if (name.EndsWith(".jsonl.gz", StringComparison.OrdinalIgnoreCase))
            return name.Substring(0, name.Length - ".jsonl.gz".Length);
        if (name.EndsWith(".jsonl", StringComparison.OrdinalIgnoreCase))
            return name.Substring(0, name.Length - ".jsonl".Length);
        return Path.GetFileNameWithoutExtension(name);
    }

    private async Task WriteKpisJsonAsync(
       string archiveDir,
       string dataFilePath,
       Dictionary<(string vehicle, DateTime minute), Kpi> perMinute,
       Dictionary<string, Kpi> totals)
    {
        var baseName = GetBaseName(dataFilePath);
        var kpiPath = Path.Combine(archiveDir, baseName + ".kpis.json");

        var totalsByVehicle = perMinute
            .OrderBy(k => k.Key)
            .ToDictionary(
                k => k.Key.minute.ToString("o"),
                k => new {
                    avgSpeedKmh = k.Value.AvgSpeed,
                    minFuelPct = double.IsPositiveInfinity(k.Value.MinFuel) ? 0 : k.Value.MinFuel,
                    maxFuelPct = double.IsNegativeInfinity(k.Value.MaxFuel) ? 0 : k.Value.MaxFuel,
                    highTempCount = k.Value.TempViolation
                });


        var payload = new
        {
            file = baseName,
            generatedUtc = DateTime.UtcNow,
            vehicles = totalsByVehicle
        };

        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true
        };

        Directory.CreateDirectory(archiveDir);
        await File.WriteAllTextAsync(
            kpiPath,
            JsonSerializer.Serialize(payload, options),
            new UTF8Encoding(false));
    }



    private async IAsyncEnumerable<TelemetryRecord> ReadTelemetryAsync(string filePath, SidecarMetadata metadata)
    {
        await using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, config.BufferSize, useAsync: true);

        Stream dataStream = fs;
        if (string.Equals(metadata.compression, "gzip", StringComparison.OrdinalIgnoreCase))
        {
            dataStream = new GZipStream(fs, CompressionMode.Decompress, leaveOpen: false);
        }

        using var reader = new StreamReader(
            dataStream,
            encoding: Encoding.GetEncoding(metadata.encoding ?? "utf-8"),
            detectEncodingFromByteOrderMarks: false,
            bufferSize: config.BufferSize,
            leaveOpen: false);

        while (true)
        {
            var line = await reader.ReadLineAsync();
            if (line is null) yield break;
            if (line.Length == 0) continue;

            TelemetryRecord? rec;
            try
            {
                rec = JsonSerializer.Deserialize<TelemetryRecord>(line, jsonOptions);
            }
            catch
            {
                throw new InvalidDataException("Invalid JSON line.");
            }

            if (rec is not null) yield return rec;
        }
    }

    private async Task<string> ComputeSha256HexAsync(string filePath, int bufferSize)
    {
        using var sha256 = SHA256.Create();
        await using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, useAsync: true);

        var buffer = new byte[bufferSize];
        int read;
        while ((read = await fs.ReadAsync(buffer.AsMemory(0, buffer.Length))) > 0)
        {
            sha256.TransformBlock(buffer, 0, read, null, 0);
        }
        sha256.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        return Convert.ToHexString(sha256.Hash!).ToLowerInvariant();
    }

    private async Task MoveToErrorWithReportAsync(string filePath, string sidecarPath, string reason)
    {
        try
        {
            Directory.CreateDirectory(config.Error);

            var dataName = Path.GetFileName(filePath);
            if (File.Exists(filePath))
            {
                var errData = Path.Combine(config.Error, dataName);
                File.Move(filePath, errData, overwrite: true);
            }

            if (File.Exists(sidecarPath))
            {
                var errMeta = Path.Combine(config.Error, Path.GetFileName(sidecarPath));
                File.Move(sidecarPath, errMeta, overwrite: true);
            }

            var report = new
            {
                file = dataName,
                utc = DateTime.UtcNow,
                reason
            };
            var repPath = Path.Combine(config.Error, Path.GetFileNameWithoutExtension(dataName) + ".error.json");
            await File.WriteAllTextAsync(repPath, JsonSerializer.Serialize(report, jsonOptions));

            Console.WriteLine($"ERR {dataName} | {reason}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ERR (while moving to error): {ex.Message}");
        }
    }
}
