using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;
using System.IO;
using System.Diagnostics;

namespace DataAccess
{
    // Azure Extensions for DataTable instance.
    // These generally support saving a datatable up to Azure as a blob of Azure Table.
    public static class DataTableAzureExtensions
    {
        /// <summary>
        /// Save the data table to the given azure blob. This will overwrite an existing blob.
        /// </summary>
        /// <param name="table">instance of table to save</param>
        /// <param name="account">azure acount</param>
        /// <param name="containerName">conatiner name</param>
        /// <param name="blobName">blob name</param>
        public static void SaveToAzureBlob(this DataTable table, CloudStorageAccount account, string containerName, string blobName)
        {
            CloudBlobContainer container = DataTableBuilderAzureExtensions.GetContainer(account, containerName);
            SaveToAzureBlob(table, container, blobName);
        }

        /// <summary>
        /// Save the data table to the given azure blob. This will overwrite an existing blob.
        /// </summary>
        /// <param name="table">instance of table to save</param>
        /// <param name="container">conatiner</param>
        /// <param name="blobName">blob name</param>
        public static void SaveToAzureBlob(this DataTable table, CloudBlobContainer container, string blobName)
        {
            var blob = container.GetBlobReference(blobName);
            using (BlobStream stream = blob.OpenWrite())
            using (TextWriter writer = new StreamWriter(stream))
            {
                table.SaveToStream(writer);
            }
        }

        // Untyped. Save all types as strings. 
        public static void SaveToAzureTable(this DataTable table, CloudStorageAccount account, string tableName)
        {
            // When no types are provided, just assume they're all strings.
            int len = table.ColumnNames.Count();
            Type[] columnTypes = new Type[len];
            for (int i = 0; i < len; i++)
            {
                columnTypes[i] = typeof(string);
            }

            table.SaveToAzureTable(account, tableName, columnTypes);
        }

        // Each row from the table is typed as a T. 
        // Matches each property type on T to a column name (using same rules as the strongly typed binder)
        public static void SaveToAzureTable<T>(this DataTable table, CloudStorageAccount account, string tableName)
        {
            throw new NotImplementedException();
            // Type[] columnTypes = StrongTypeBinder $$$ Internal
        }
                
        public static void SaveToAzureTable(this DataTable table, CloudStorageAccount account, string tableName, Type[] columnTypes)
        {
            table.SaveToAzureTable(account, tableName, columnTypes, funcComputeKeys : null);
        }

        // ColumnTypes is a parallel array to table.ColumnNames.
        // columnTypes should be types that can be normalized to OData (string,byte,sbyte,i16,i32,i64,double,single,boolean,decimal, datetime, guid).
        // if columnTypes[i] is null, then that column is skipped.
        public static void SaveToAzureTable(this DataTable table, CloudStorageAccount account, string tableName, Type[] columnTypes, Func<int, Row, ParitionRowKey> funcComputeKeys)
        {
            GenericTableWriter.SaveToAzureTable(table, account, tableName, columnTypes, funcComputeKeys);
        }        
    }

    /// <summary>
    /// Class to encapsulate a partition and row key. This is similar to Tuple[string,string], but less ambiguous. 
    /// </summary>
    public class ParitionRowKey
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        public ParitionRowKey()
        { }

        public ParitionRowKey(string partitionKey, string rowKey)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
        }

        // pad rowkey with 0s so that it sorts nicely. 
        public ParitionRowKey(string partitionKey, int rowKey)            
        {
            PartitionKey = partitionKey;
            RowKey = rowKey.ToString("D8");
        }
    }
}