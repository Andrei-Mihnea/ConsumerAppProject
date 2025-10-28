using Objects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Receiver
{
    public class FileProcessor
    {
        private readonly ConsumerConfig config;
        private static readonly JsonSerializerOptions jsonOptions = new() { PropertyNameCaseInsensitive = true};
        private readonly string supportedSidecarVersion = "1.";
        public FileProcessor(ConsumerConfig config) => 
            this.config = config;

        public async Task ProcessFileAsync(string filePath)
        {
            var sidecarPath = filePath + ".metadata.json";
            
            if(!File.Exists(sidecarPath))
            {
                await MoveToErrorWithReportAsync(filePath, sidecarPath, "Missing sidecar metadata file.");
                return;
            }

            SidecarMetadata? metadata = null;

            try
            {
                await using var metaFs = new FileStream(sidecarPath, FileMode.Open, FileAccess.Read, FileShare.Read, config.BufferSize, useAsync:true);
                metadata = await JsonSerializer.DeserializeAsync<SidecarMetadata>(metaFs, jsonOptions);
            }
            catch(Exception ex)
            {
                await MoveToErrorWithReportAsync(filePath, sidecarPath, $"Failed to read or parse sidecar metadata file");
            }

            if(metadata is null)
            {
                await MoveToErrorWithReportAsync(filePath, sidecarPath, $"Sidecar metadata file is empty or invalid.");
                return;
            }

            if(!metadata.version.StartsWith(supportedSidecarVersion, StringComparison.Ordinal))
            {
                await MoveToErrorWithReportAsync(filePath, sidecarPath, $"Unsupported sidecar metadata version: {metadata.version}");
                return;
            }

            string computedHash = string.Empty;

            try
            {
                computedHash = await ComputeSha256HexAsync(filePath, config.BufferSize);
            }
            catch(Exception ex)
            {
                await MoveToErrorWithReportAsync(filePath, sidecarPath, $"Failed to compute hash of data file: {ex.Message}");
                return;
            }

            if (!string.Equals(metadata.sha256, computedHash, StringComparison.OrdinalIgnoreCase))
            {
                await MoveToErrorWithReportAsync(filePath, sidecarPath, $"Hash mismatch. Expected: {metadata.sha256}, Computed: {computedHash}");
                return;
            }

            var swTotal = System.Diagnostics.Stopwatch.StartNew();
            int records = 0;

            try
            {
                await foreach(var rec in ReadTelemtryAsync(filePath, metadata))
                {
                    Aggregate(rec);
                    ++records;
                }
            }
            catch(Exception ex)
            {
                await MoveToErrorWithReportAsync(filePath, sidecarPath, $"Failed to process data file: {ex.Message}");
                return;
            }
            finally
            {
                swTotal.Stop();
            }

            try
            {
                var archiveFilePath = Path.Combine(config.Archive,
                    DateTime.UtcNow.Year.ToString("0000"),
                    DateTime.UtcNow.Month.ToString("00"),
                    DateTime.UtcNow.Day.ToString("00")
                    );
                Directory.CreateDirectory(archiveFilePath);

                var newDataFileName = Path.Combine(archiveFilePath, Path.GetFileName(filePath));
                var newSidecarFileName = Path.Combine(archiveFilePath, Path.GetFileName(sidecarPath));

                File.Move(filePath, newDataFileName, overwrite:true);
                File.Move(sidecarPath, newSidecarFileName, overwrite:true);

                Console.WriteLine($"Processed file '{filePath}' with {records} records in {swTotal.ElapsedMilliseconds} ms.");
            }
            catch(Exception ex)
            {
                await MoveToErrorWithReportAsync(filePath, sidecarPath, $"Failed to archive processed files: {ex.Message}");
                return;
            }
        }

        private void Aggregate(object rec)
        {
            throw new NotImplementedException();
        }

        private IAsyncEnumerable<object> ReadTelemtryAsync(string filePath, SidecarMetadata metadata)
        {
            throw new NotImplementedException();
        }

        private async Task<string> ComputeSha256HexAsync(string filePath, int bufferSize)
        {
            using var sha256 = SHA256.Create();

            await using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, useAsync:true);
            var buffer = new byte[bufferSize];
            int bytesRead;

            while((bytesRead = await fs.ReadAsync(buffer.AsMemory(0, buffer.Length))) > 0)
            {
                sha256.TransformBlock(buffer, 0, bytesRead, null, 0);
            }

        }

        private async Task MoveToErrorWithReportAsync(string filePath, string sidecarPath, string v)
        {
            throw new NotImplementedException();
        }
    }
}
