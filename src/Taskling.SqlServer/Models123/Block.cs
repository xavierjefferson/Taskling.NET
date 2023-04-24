using System;
using System.Collections.Generic;

namespace Taskling.Models123
{
    public partial class Block
    {
        public Block()
        {
            ForceBlockQueues = new HashSet<ForceBlockQueue>();
            ListBlockItems = new HashSet<ListBlockItem>();
        }

        public long BlockId { get; set; }
        public int TaskDefinitionId { get; set; }
        public DateTime? FromDate { get; set; }
        public DateTime? ToDate { get; set; }
        public long? FromNumber { get; set; }
        public long? ToNumber { get; set; }
        public string ObjectData { get; set; }
        public byte[] CompressedObjectData { get; set; }
        public int BlockType { get; set; }
        public bool IsPhantom { get; set; }
        public DateTime CreatedDate { get; set; } = DateTime.UtcNow;

        public virtual TaskDefinition TaskDefinition { get; set; }
        public virtual ICollection<ForceBlockQueue> ForceBlockQueues { get; set; }
        public virtual ICollection<ListBlockItem> ListBlockItems { get; set; }
    }
}
