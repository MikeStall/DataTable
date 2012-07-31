using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.StorageClient;
using System.Data.Services.Client;
using Xunit;
using System.IO;
using DataAccess;
using Microsoft.WindowsAzure;

namespace CsvReader.Azure.Tests
{
    internal static class Utility
    {
        public static T Lookup<T>(CloudTableClient tableClient, string tableName, string partitionKey, string rowKey) where T : TableServiceEntity
        {
            TableServiceContext ctx = tableClient.GetDataServiceContext();

            // Azure will special case this lookup pattern for a single entity. 
            // See http://blogs.msdn.com/b/windowsazurestorage/archive/2010/11/06/how-to-get-most-out-of-windows-azure-tables.aspx 
            try
            {
                // This will throw DataServiceQueryException if not found. (as opposed to return an empty query)
                var x = from row in ctx.CreateQuery<T>(tableName)
                        where row.PartitionKey == partitionKey && row.RowKey == rowKey
                        select row;
                var x2 = x.AsTableServiceQuery<T>();

                return x2.First();
            }
            catch (DataServiceQueryException)
            {
                // Not found. 
                return null;
            }
        }

        // Table will come back sorted by (parition, row key)
        // Integers don't sort nicely as strings.
        public static T[] ReadTable<T>(CloudStorageAccount account, string tableName) where T : TableServiceEntity
        {
            CloudTableClient tableClient = account.CreateCloudTableClient();
            TableServiceContext ctx = tableClient.GetDataServiceContext();


            var query = from row in ctx.CreateQuery<T>(tableName) select row;
            var query2 = query.AsTableServiceQuery<T>();

            // Verify table matches source           
            T[] result = query2.ToArray();

            return result;
        }

        // Do string comparison, ignoring ignoring newline (\r\n vs just \n)
        public static void AssertEquals(string content, string actual)
        {
            content = content.Replace("\r\n", "\n");
            actual = actual.Replace("\r\n", "\n");
            Assert.Equal(content, actual);
        }

        public static void AssertEquals(string content, DataTable dt)
        {
            string actual = ToString(dt);
            AssertEquals(content, actual);
        }

        public static void AssertEquals(DataTable tableExpected, DataTable tableActual)
        {
            string actual = ToString(tableActual);
            string content = ToString(tableExpected);
            AssertEquals(content, actual);
        }

        private static string ToString(DataTable dt)
        {
            StringWriter sw = new StringWriter();
            dt.SaveToStream(sw);
            string actual = sw.ToString();
            return actual;
        }
    }
}
