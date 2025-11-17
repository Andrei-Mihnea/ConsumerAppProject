using Interfaces;
using Objects;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Receiver
{
    public class FileReceiver
    {
        private readonly ConsumerConfig config;
        private readonly IFileHandler handler;
        private readonly ConcurrentDictionary<string, byte> inProgress =
            new(StringComparer.OrdinalIgnoreCase);

        public FileReceiver(ConsumerConfig config, IFileHandler handler)
        {
            this.config = config;
            this.handler = handler;
        }

        public void CreateOnStartDirectories()
        {
            if (Directory.Exists(config.Inbox) &&
                Directory.Exists(config.Archive) &&
                Directory.Exists(config.Error))
            {
                Console.WriteLine("[SYSTEM]: Directories already exist.");
                return;
            }
            Directory.CreateDirectory(config.Inbox);
            Directory.CreateDirectory(config.Archive);
            Directory.CreateDirectory(config.Error);
            Console.WriteLine("[SYSTEM]: Directories created on start-up.");
        }

        public async Task RunAsync(CancellationToken ct)
        {
            using var fileWatcher = new FileSystemWatcher(config.Inbox)
            {
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.CreationTime | NotifyFilters.Size,
                IncludeSubdirectories = false,
                Filter = "*.*",
                EnableRaisingEvents = true
            };

            Console.WriteLine($"[CFG] Inbox:   {Path.GetFullPath(config.Inbox)}");
            Console.WriteLine($"[CFG] Archive: {Path.GetFullPath(config.Archive)}");
            Console.WriteLine($"[CFG] Error:   {Path.GetFullPath(config.Error)}");

            fileWatcher.Created += async (_, e) => await TryQueue(e.FullPath, ct);
            fileWatcher.Renamed += async (_, e) => await TryQueue(e.FullPath, ct);

            var sweepTask = Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    string[] files;
                    try { files = Directory.GetFiles(config.Inbox); }
                    catch { await Task.Delay(3000, ct); continue; }

                    foreach (var path in files)
                    {
                        try { await TryQueue(path, ct); }
                        catch (Exception ex) { Console.WriteLine($"[SWEEP ERROR] {ex.Message}"); }
                    }
                    await Task.Delay(3000, ct);
                }
            }, ct);

            Console.WriteLine("Consumer running. Press Ctrl+C to exit.");
            await sweepTask;
        }

        private async Task TryQueue(string dataPath, CancellationToken ct)
        {
            if (!IsDataFile(dataPath)) return;
            if (!inProgress.TryAdd(dataPath, 0)) return;

            await Task.Delay(3000, ct);
            _ = ProcessWithRetries(dataPath, ct);
        }

        private async Task ProcessWithRetries(string dataPath, CancellationToken ct)
        {
            try
            {
                var sidecar = dataPath + ".meta.json";
                for (int attempt = 1; attempt <= config.MaxRetries; attempt++)
                {
                    if (File.Exists(sidecar))
                    {
                        long len1; try { len1 = new FileInfo(dataPath).Length; } catch { return; }
                        await Task.Delay(config.DebounceMs, ct);
                        long len2; try { len2 = new FileInfo(dataPath).Length; } catch { return; }
                        if (len1 != len2)
                        {
                            Console.WriteLine($"[WAIT] Growing: {Path.GetFileName(dataPath)}");
                            attempt--;
                            continue;
                        }

                        Console.WriteLine($"[QUEUE] {Path.GetFileName(dataPath)} (sidecar present)");
                        await handler.HandleAsync(dataPath, ct);   // façade-injected processor
                        return;
                    }
                    Console.WriteLine($"[WAIT] Sidecar missing for {Path.GetFileName(dataPath)} (attempt {attempt}/{config.MaxRetries})");
                    await Task.Delay(3000, ct);
                }
                await MoveToErrorMissingSidecar(dataPath);
            }
            catch (Exception ex) { Console.WriteLine($"[RETRY WORKER ERROR] {ex.Message}"); }
            finally { inProgress.TryRemove(dataPath, out _); }
        }

        private async Task MoveToErrorMissingSidecar(string dataPath)
        {
            try
            {
                Directory.CreateDirectory(config.Error);

                var sidecarPath = dataPath + ".meta.json";
                var dataName = Path.GetFileName(dataPath);

                if (File.Exists(dataPath))
                {
                    var errData = Path.Combine(config.Error, dataName);
                    File.Move(dataPath, errData, overwrite: true);
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
                    reason = $"Missing sidecar after {config.MaxRetries} attempts"
                };
                var repPath = Path.Combine(config.Error, Path.GetFileNameWithoutExtension(dataName) + ".error.json");
                await File.WriteAllTextAsync(repPath, JsonSerializer.Serialize(report));

                Console.WriteLine($"ERR {dataName} | Missing sidecar after {config.MaxRetries} attempts");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERR (while moving to error for missing sidecar): {ex.Message}");
            }
        }

        private static bool IsDataFile(string p) =>
            p.EndsWith(".jsonl", StringComparison.OrdinalIgnoreCase) ||
            p.EndsWith(".jsonl.gz", StringComparison.OrdinalIgnoreCase);
    }
}
