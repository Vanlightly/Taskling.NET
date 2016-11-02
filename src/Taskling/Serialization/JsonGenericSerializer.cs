using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Exceptions;

namespace Taskling.Serialization
{
    public class JsonGenericSerializer
    {
        public static string Serialize<T>(T data)
        {
            if (data == null)
                throw new ExecutionException("The object being serialized is null");

            return JsonConvert.SerializeObject(data);
        }

        public static T Deserialize<T>(string input, bool allowNullValues = false)
        {
            if (input == null)
            {
                if (allowNullValues)
                    return default(T);

                throw new ExecutionException("The object being deserialized is null");
            }

            try
            {
                return JsonConvert.DeserializeObject<T>(input);
            }
            catch (Exception ex)
            {
                throw new ExecutionException("The object type being deserialized is not compatible with the specified type. This could happen if you change the type of an existing process for a different type, or if you make non retro compatible changes to the exsiting type, for example by removing properties.", ex);
            }
        }
    }
}
