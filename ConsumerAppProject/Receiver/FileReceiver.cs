using Objects;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;

namespace Receiver
{
    public class FileReceiver
    {
        private readonly ConsumerConfig config = new
        (
            Inbox: @"C:\Producer\inbox",//for later testing will use  @"\\ENDAUTOQ1FDTPOF\inbox"
            Archive: @"C:\Producer\archive",//for later testing will use  @"\\ENDAUTOQ1FDTPOF\archive"
            Error: @"C:\Producer\error",//for later testing will use  @"\\ENDAUTOQ1FDTPOF\error"
            BufferSize: 128 * 1024,
            MaxRetries: 3,
            DebounceMs: 200
        );

        private readonly FileProcessor processor;
        private readonly ConcurrentDictionary<string, byte> inProgress = new(StringComparer.OrdinalIgnoreCase);


        public FileReceiver()
        {
            processor = new FileProcessor(config);
        }

        public void CreateOnStartDirectories()
        {
            if(Directory.Exists(config.Inbox) &&
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

        public async Task FileWatcher()
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

            fileWatcher.Created += async (_, e) => await TryQueue(e.FullPath);
            fileWatcher.Renamed += async (_, e) => await TryQueue(e.FullPath);

            var sweepTask = Task.Run(async () =>
            {
                while (true)
                {
                    string[] files;
                    try 
                    { 
                        files = Directory.GetFiles(config.Inbox); 
                    }
                    catch 
                    { 
                        await Task.Delay(3000); 
                        continue; 
                    }

                    foreach (var path in files)
                    {
                        try 
                        { 
                            await TryQueue(path); 
                        }
                        catch (Exception ex) 
                        { 
                            Console.WriteLine($"[SWEEP ERROR] {ex.Message}"); 
                        }
                    }
                    await Task.Delay(3000);
                }
            });

            Console.WriteLine("Consumer running. Press Ctrl+C to exit.");
            await sweepTask;
        }

        private async Task TryQueue(string dataPath)
        {
            
            if (!IsDataFile(dataPath)) return;
            if (!inProgress.TryAdd(dataPath, 0)) return;

            await Task.Delay(3000);
            _ = ProcessWithRetries(dataPath);

        }

        private async Task ProcessWithRetries(string dataPath)
        {
            try
            {
                var sidecar = dataPath + ".meta.json";

                for (int attempt = 1; attempt <= config.MaxRetries; attempt++)
                {
                    if (File.Exists(sidecar))
                    {
                       
                        long len1;
                        try { len1 = new FileInfo(dataPath).Length; } catch { return; }
                        await Task.Delay(config.DebounceMs);
                        long len2;
                        try { len2 = new FileInfo(dataPath).Length; } catch { return; }
                        if (len1 != len2)
                        {
                            Console.WriteLine($"[WAIT] Growing: {Path.GetFileName(dataPath)}");
                            attempt--;
                            continue;
                        }

                        Console.WriteLine($"[QUEUE] {Path.GetFileName(dataPath)} (sidecar present)");
                        await processor.ProcessFileAsync(dataPath);
                        return;
                    }

                    Console.WriteLine($"[WAIT] Sidecar missing for {Path.GetFileName(dataPath)} (attempt {attempt}/{config.MaxRetries})");
                    await Task.Delay(3000);
                }

                await MoveToErrorMissingSidecar(dataPath);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RETRY WORKER ERROR] {ex.Message}");
            }
            finally
            {
                inProgress.TryRemove(dataPath, out _);
            }
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
