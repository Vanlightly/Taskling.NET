using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks
{
    public class ListBlockItem
    {
        public string ListBlockItemId { get; set; }
        public string Value { get; set; }
        public ListBlockItemStatus Status { get; set; }
    }
}
