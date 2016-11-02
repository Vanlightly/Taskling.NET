using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.Exceptions;
using Taskling.Serialization;

namespace Taskling.SqlServer.Blocks.Serialization
{
    public class SerializedValueReader
    {
        public static T ReadValue<T>(SqlDataReader reader, string valueColumn, string compressedColum)
        {
            if (reader[valueColumn] == DBNull.Value && reader[compressedColum] == DBNull.Value)
            {
                return default(T);
            }
            else if (reader[valueColumn] != DBNull.Value)
            {
                return JsonGenericSerializer.Deserialize<T>(reader[valueColumn].ToString());
            }
            else if (reader[compressedColum] != DBNull.Value)
            {
                var compressedBytes = (byte[])reader[compressedColum];
                var uncompressedText = LargeValueCompressor.Unzip(compressedBytes);
                return JsonGenericSerializer.Deserialize<T>(uncompressedText);
            }

            throw new ExecutionException("The stored value is null which is not a valid state");
        }

        public static string ReadValueAsString(SqlDataReader reader, string valueColumn, string compressedColumn)
        {
            if (reader[valueColumn] == DBNull.Value && reader[compressedColumn] == DBNull.Value)
            {
                return string.Empty;
            }
            else if (reader[valueColumn] != DBNull.Value)
            {
                return reader[valueColumn].ToString();
            }
            else if (reader[compressedColumn] != DBNull.Value)
            {
                var compressedBytes = (byte[])reader[compressedColumn];
                var uncompressedText = LargeValueCompressor.Unzip(compressedBytes);
                return uncompressedText;
            }

            throw new ExecutionException("The stored value is null which is not a valid state");
        }
    }
}
