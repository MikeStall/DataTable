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
    public static class DataTableBuilderAzureExtensions
    {
#if false
        public static MutableDataTable ReadFromAzureBlobLazy(this DataTableBuilder builder, CloudStorageAccount account, string containerName, string blobName)
        {
            CloudBlobContainer container = GetContainer(account, containerName);
            return ReadFromAzureBlobLazy(builder, container, blobName);
        }

        
        public static MutableDataTable ReadFromAzureBlobLazy(this DataTableBuilder builder, CloudBlobContainer container, string blobName)
        {

        }
#endif

        /// <summary>
        /// Read a data table from azure blob. This will read the entire blob into memory and return a mutable data table.
        /// </summary>
        /// <param name="builder">builder</param>
        /// <param name="account">azure acount</param>
        /// <param name="containerName">conatiner name</param>
        /// <param name="blobName">blob name</param>
        /// <returns>in-memory mutable datatable from blob</returns>
        public static MutableDataTable ReadFromAzureBlob(this DataTableBuilder builder, CloudStorageAccount account, string containerName, string blobName)
        {
            CloudBlobContainer container = GetContainer(account, containerName);
            return ReadFromAzureBlob(builder, container, blobName);
        }

        /// <summary>
        /// Read a data table from azure blob. This will read the entire blob into memory and return a mutable data table.
        /// </summary>
        /// <param name="builder">builder</param>
        /// <param name="container">conatiner</param>
        /// <param name="blobName">blob name</param>
        /// <returns>in-memory mutable datatable from blob</returns>
        public static MutableDataTable ReadFromAzureBlob(this DataTableBuilder builder, CloudBlobContainer container, string blobName)
        {            
            CloudBlob blob = container.GetBlobReference(blobName);
            if (!Exists(blob))
            {
                string containerName = container.Name;
                string accountName = container.ServiceClient.Credentials.AccountName;
                throw new FileNotFoundException(string.Format("container.blob {0}.{0} does not exist on the storage account '{2}'", containerName, blobName, accountName));
            }

            // We're returning a MutableDataTable (which is in-memory) anyways, so fine to download into an in-memory buffer.
            // Avoid downloading to a file because Azure nodes may not have a local file resource.
            string content = blob.DownloadText();
            
            var stream = new StringReader(content);
            var dt = DataTable.New.Read(stream);
            dt.Name = container.Name + "." + blobName;
            return dt;
        }

        internal static CloudBlobContainer GetContainer(CloudStorageAccount account, string containerName)
        {
            var client = account.CreateCloudBlobClient();

            var container = client.GetContainerReference(containerName);
            container.CreateIfNotExist();
            return container;
        }

        // Super lame that you need to check for exceptions. Seriously?
        // http://blog.smarx.com/posts/testing-existence-of-a-windows-azure-blob
        [DebuggerNonUserCode]
        private static bool Exists(CloudBlob blob)
        {
            try
            {
                blob.FetchAttributes();
                return true;
            }
            catch (StorageClientException e)
            {
                if (e.ErrorCode == StorageErrorCode.ResourceNotFound)
                {
                    return false;
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Read an Azure Table as a CSV. Returned CSV includes columns for the ParitionKey and RowKey.
        /// The row order is the same as Azure's natural row ordering (sorted by partition key, rowkey)
        /// This is a lazy function, so it reads the table rows at a time and does not read the entire table into memory. 
        /// </summary>
        /// <param name="builder">builder</param>
        /// <param name="account">azure storage account</param>
        /// <param name="tableName">name of table within account</param>
        /// <returns></returns>
        public static DataTable ReadAzureTableLazy(this DataTableBuilder builder, CloudStorageAccount account, string tableName)
        {
            CloudTableClient tableClient = account.CreateCloudTableClient();

            return new AzureStreamingTable { _tableName = tableName, _tableClient = tableClient, Name = tableName };
        }
    }
}
