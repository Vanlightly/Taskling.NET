using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Configuration
{
    public class ConfigElement<T>
    {
        public ConfigElement()
        {
            Exists = false;
        }

        public ConfigElement(T value)
        {
            Value = value;
            Exists = true;
        }

        public T Value { get; set; }
        public bool Exists { get; set; }
    }
}
