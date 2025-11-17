using Interfaces;
using Objects;
using Receiver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Facade
{
    public sealed class ConsumerFacade: IAsyncDisposable
    {
        private readonly ConsumerConfig _config;
        private readonly FileReceiver _receiver;
        private readonly IFileHandler _processor;
        private CancellationTokenSource? _cts;

        public ConsumerFacade(ConsumerConfig? cfg = null)
        {
            _config = cfg ?? new ConsumerConfig(
                Inbox: @"C:\Producer\inbox",
                Archive: @"C:\Producer\archive",
                Error: @"C:\Producer\error",
                BufferSize: 128 * 1024,
                MaxRetries: 3,
                DebounceMs: 200);

            _processor = new FileProcessor(_config);
            _receiver = new FileReceiver(_config, _processor);
        }

        public void CreateDirectories() => _receiver.CreateOnStartDirectories();

        public Task StartAsync(CancellationToken ct = default)
        {
            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _ = _receiver.RunAsync(_cts.Token);
            return Task.CompletedTask;
        }

        public async ValueTask DisposeAsync()
        {
            try { _cts?.Cancel(); } 
            catch(ObjectDisposedException)
            {
                Console.WriteLine("Attempted an operation on a disposed object inside ConsumerFacade");
            }
            catch(AggregateException)
            {
                Console.WriteLine("One or more errors occured when executing inside ConsumerFacade");
            }
            await Task.CompletedTask;
        }
    }
}
