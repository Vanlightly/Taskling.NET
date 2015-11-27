using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks
{
    public class ListBlock 
    {
        public ListBlock()
        {
            Items = new List<ListBlockItem>();
        }

        public string ListBlockId { get; set; }
        public List<ListBlockItem> Items;
    }
}
