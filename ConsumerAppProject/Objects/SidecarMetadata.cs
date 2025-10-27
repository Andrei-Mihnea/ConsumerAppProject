using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Objects
{
    public sealed class SidecarMetadata
    {
        public string version { get; set; } = "1.0";
        public DateTime createdUtc { get; set; }
        public int recordCount { get; set; }
        public string sha256 { get; set; }
        public string encoding { get; set; } = "utf-8";
        public string compression { get; set; } = "none";
    }
}
