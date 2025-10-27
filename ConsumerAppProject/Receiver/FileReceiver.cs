using Objects;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Receiver
{
    public class FileReceiver
    {
        private readonly ConsumerConfig config = new
        (
            Inbox: "inbox",
            Archive: "archive",
            Error: "error",
            BufferSize: 128 * 1024,
            MaxRetries: 3,
            DebounceMs: 200
        );

        private readonly FileProcessor processor;

        public FileReceiver()
        {
            processor = new FileProcessor(config);
        }

        public void CreateOnStartDirectories()
        {
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
            if (dataPath.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase)) return;
            if (!IsDataFile(dataPath)) return;

            long len1, len2;
            try 
            { 
                len1 = new FileInfo(dataPath).Length; 
            }
            catch 
            { 
                return; 
            }

            await Task.Delay(config.DebounceMs);

            try 
            { 
                len2 = new FileInfo(dataPath).Length; 
            }
            catch 
            { 
                return; 
            
            }
            if (len1 != len2) return;

            await processor.ProcessFileAsync(dataPath);
        }

        private static bool IsDataFile(string p) =>
            p.EndsWith(".jsonl", StringComparison.OrdinalIgnoreCase) ||
            p.EndsWith(".jsonl.gz", StringComparison.OrdinalIgnoreCase);
    }
}
