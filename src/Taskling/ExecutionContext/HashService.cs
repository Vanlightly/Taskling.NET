using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Taskling.ExecutionContext
{
    internal class HashService
    {
        public static byte[] GetHashAsByteArray(string value)
        {
            return ComputeMD5(value);
        }

        public static string GetHashAsText(string value)
        {
            var bytes = ComputeMD5(value);

            var sb = new StringBuilder();
            for (int i = 0; i < bytes.Length; i++)
            {
                sb.Append(bytes[i].ToString("X2"));
            }

            return sb.ToString();
        }

        private static byte[] ComputeMD5(string value)
        {
            Encoder enc = System.Text.Encoding.Unicode.GetEncoder();
            byte[] unicodeText = new byte[value.Length * 2];
            enc.GetBytes(value.ToCharArray(), 0, value.Length, unicodeText, 0, true);

            MD5 md5 = new MD5CryptoServiceProvider();
            return md5.ComputeHash(unicodeText);
        }
    }
}
