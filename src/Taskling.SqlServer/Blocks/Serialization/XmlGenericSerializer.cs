using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using Taskling.Exceptions;

namespace Taskling.SqlServer.Blocks.Serialization
{
    public class XmlGenericSerializer
    {
        public static string Serialize<T>(T data)
        {
            if (data == null)
                throw new ExecutionException("The object being serialized is null");

            var xmlSerializer = new XmlSerializer(typeof(T));
            using (var sr = new StringWriter())
            {
                xmlSerializer.Serialize(sr, data);
                return sr.ToString();
            }
        }

        public static T Deserialize<T>(string input)
        {
            if (input == null)
                throw new ExecutionException("The object being deserialized is null");

            try
            {
                var xmlSerializer = new XmlSerializer(typeof(T));

                using (var sr = new StringReader(input))
                {
                    return (T)xmlSerializer.Deserialize(sr);
                }
            }
            catch (Exception ex)
            {
                throw new ExecutionException("The object type being deserialized is not compatible with the specified type. This could happen if you change the type of an existing process for a different type, or if you make non retro compatible changes to the exsiting type, for example by removing properties.", ex);
            }
        }
    }
}
