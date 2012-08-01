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
    /// <summary>
    /// Azure Extensions for DataTable instance.
    /// These generally support saving a datatable up to Azure as a blob or Azure Table.
    /// </summary>
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

        /// <summary>
        /// Save to azure table, typing all columns as strings. 
        /// Overwrite if the table already exists
        /// Fabricate a partition and row key is they're not provided in the table.
        /// </summary>
        /// <param name="table">datatable to save</param>
        /// <param name="account">cloud account to write to</param>
        /// <param name="tableName">azure table name to save as. </param>
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

        public static void SaveToAzureTable(this DataTable table, CloudStorageAccount account, string tableName, Type[] columnTypes)
        {
            table.SaveToAzureTable(account, tableName, columnTypes, funcComputeKeys : null);
        }

        /// <summary>
        /// Save the datatable up to an AzureTable. Overwrite if the azure table already exists.
        /// </summary>
        /// <param name="table">source table to save</param>
        /// <param name="account">azure storage account</param>
        /// <param name="tableName">name of azure table to write to. </param>
        /// <param name="columnTypes">parallel array to table.ColumnNames. 
        /// Strong typing for the columns in the azure table. Column i is skipped if columnTypes[i] is null.
        /// ColumnTypes should be types that can be normalized to OData (string,byte,sbyte,i16,i32,i64,double,single,boolean,decimal, datetime, guid).
        /// </param>
        /// <param name="funcComputeKeys">function to compute the partion and row keys for each row. </param>
        public static void SaveToAzureTable(this DataTable table, CloudStorageAccount account, string tableName, Type[] columnTypes, Func<int, Row, ParitionRowKey> funcComputeKeys)
        {
            GenericTableWriter.SaveToAzureTable(table, account, tableName, columnTypes, funcComputeKeys);
        }        
    }

    /// <summary>
    /// Class to encapsulate a partition and row key. This is similar to Tuple[string,string], but less ambiguous. 
    /// Partition plus Row key must be unique. 
    /// </summary>
    public class ParitionRowKey
    {
        /// <summary>
        /// Partition key to use for Azure Table. 
        /// </summary>
        public string PartitionKey { get; set; }
        
        /// <summary>
        /// Row key to use for azure table.
        /// </summary>
        public string RowKey { get; set; }

        /// <summary>
        /// Empty constructor. Set the partition and row key via the properties.
        /// </summary>
        public ParitionRowKey()
        { }


        /// <summary>
        /// initialize a container for an parition key and row key pair.
        /// </summary>
        /// <param name="partitionKey">partition key for azure table row</param>
        /// <param name="rowKey">Row key for azure table row.</param>
        public ParitionRowKey(string partitionKey, string rowKey)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
        }

        /// <summary>
        /// initialize a container for an parition key and row key pair.
        /// Overload to pad rowkey with 0s so that rows sort nicely as strings.
        /// </summary>
        /// <param name="partitionKey">partition key for azure table row</param>
        /// <param name="rowKey">Row key for azure table row. pad rowkey with 0s so that it sorts nicely. </param>
        public ParitionRowKey(string partitionKey, int rowKey)            
        {
            PartitionKey = partitionKey;
            RowKey = rowKey.ToString("D8");
        }
    }
}