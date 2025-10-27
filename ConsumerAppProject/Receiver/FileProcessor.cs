using Objects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ConsumerAppProject.Receiver
{
    public class FileProcessor
    {
        private readonly ConsumerConfig config;
        private static readonly JsonSerializerOptions jsonOptions = new() { PropertyNameCaseInsensitive = true};

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



        }

        private async Task MoveToErrorWithReportAsync(string filePath, string sidecarPath, string v)
        {
            throw new NotImplementedException();
        }
    }
}
