var receiver = new Receiver.FileReceiver();
receiver.CreateOnStartDirectories();
await receiver.FileWatcher();