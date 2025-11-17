using Facade;

await using var app = new ConsumerFacade();
app.CreateDirectories();
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
await app.StartAsync(cts.Token);
await Task.Delay(Timeout.Infinite, cts.Token);
