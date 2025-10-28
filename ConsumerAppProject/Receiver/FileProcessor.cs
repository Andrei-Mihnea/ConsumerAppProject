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

public class FileProcessor
{
    private readonly ConsumerConfig config;
    private static readonly JsonSerializerOptions jsonOptions = new() { PropertyNameCaseInsensitive = true };
    private readonly string supportedSidecarVersion = "1.";
    private readonly Dictionary<(string vehicle, DateTime minute), Kpi> vehicleKpis = new();

    public FileProcessor(ConsumerConfig config) => this.config = config;

    public async Task ProcessFileAsync(string filePath)
    {
        // Use .meta.json to match the contract
        var sidecarPath = filePath + ".meta.json";

        if (!File.Exists(sidecarPath))
        {
            await MoveToErrorWithReportAsync(filePath, sidecarPath, "Missing sidecar metadata file.");
            return;
        }

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

        if (metadata is null)
        {
            await MoveToErrorWithReportAsync(filePath, sidecarPath, "Sidecar is null/empty.");
            return;
        }

        if (!metadata.version.StartsWith(supportedSidecarVersion, StringComparison.Ordinal))
        {
            await MoveToErrorWithReportAsync(filePath, sidecarPath, $"Unsupported sidecar version: {metadata.version}");
            return;
        }

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

        if (!string.Equals(metadata.sha256, computedHash, StringComparison.OrdinalIgnoreCase))
        {
            await MoveToErrorWithReportAsync(filePath, sidecarPath, $"Checksum mismatch. expected={metadata.sha256} actual={computedHash}");
            return;
        }

        var swTotal = System.Diagnostics.Stopwatch.StartNew();
        int records = 0;

        try
        {
            await foreach (var rec in ReadTelemetryAsync(filePath, metadata))
            {
                Aggregate(rec);
                records++;
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

            await WriteKpisCsvAsync(archiveDir, filePath);

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

    private void Aggregate(TelemetryRecord rec)
    {
        var minute = new DateTime(rec.tsUtc.Year, rec.tsUtc.Month, rec.tsUtc.Day, rec.tsUtc.Hour, rec.tsUtc.Minute, 0, DateTimeKind.Utc);
        var key = (rec.vehicleId, minute);

        if (!vehicleKpis.TryGetValue(key, out var vehicleKpi))
        {
            vehicleKpi = new Kpi();
            vehicleKpis[key] = vehicleKpi;
        }

        vehicleKpi.Count++;
        vehicleKpi.SpeedSum += rec.speedKmh;
        vehicleKpi.MinFuel = Math.Min(vehicleKpi.MinFuel, rec.fuelPct);
        vehicleKpi.MaxFuel = Math.Max(vehicleKpi.MaxFuel, rec.fuelPct);
        if (rec.coolantTempC > vehicleKpi.TEMPLIMIT) vehicleKpi.TempViolation++;
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

    private async Task WriteKpisCsvAsync(string archiveDir, string dataFilePath)
    {
        var baseName = GetBaseName(dataFilePath);
        var kpiCsvPathTmp = Path.Combine(archiveDir, baseName + ".kpi.csv.tmp");
        var kpiCsvPath = Path.Combine(archiveDir, baseName + ".kpi.csv");

        var sb = new StringBuilder();
        sb.AppendLine("file,vehicleId,minuteUtc,avgSpeedKmh,minFuelPct,maxFuelPct,highTempCount,count");

        foreach (var kvp in vehicleKpis.OrderBy(k => k.Key.vehicle).ThenBy(k => k.Key.minute))
        {
            var (vehicle, minute) = kvp.Key;
            var b = kvp.Value;

            sb.Append(baseName).Append(',')
              .Append(vehicle).Append(',')
              .Append(minute.ToString("o")).Append(',')
              .Append(b.AvgSpeed.ToString(System.Globalization.CultureInfo.InvariantCulture)).Append(',')
              .Append(b.MinFuel.ToString(System.Globalization.CultureInfo.InvariantCulture)).Append(',')
              .Append(b.MaxFuel.ToString(System.Globalization.CultureInfo.InvariantCulture)).Append(',')
              .Append(b.TempViolation).Append(',')
              .Append(b.Count)
              .AppendLine();
        }

        var utf8NoBom = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        await File.WriteAllTextAsync(kpiCsvPathTmp, sb.ToString(), utf8NoBom);
        File.Move(kpiCsvPathTmp, kpiCsvPath, overwrite: true);

        vehicleKpis.Clear();
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
                reason // include the reason!
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
