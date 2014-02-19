using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;

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
        public static MutableDataTable ReadAzureBlob(this DataTableBuilder builder, CloudStorageAccount account, string containerName, string blobName)
        {
            CloudBlobContainer container = GetContainer(account, containerName);
            return ReadAzureBlob(builder, container, blobName);
        }

        /// <summary>
        /// Read a data table from azure blob. This will read the entire blob into memory and return a mutable data table.
        /// </summary>
        /// <param name="builder">builder</param>
        /// <param name="container">conatiner</param>
        /// <param name="blobName">blob name</param>
        /// <returns>in-memory mutable datatable from blob</returns>
        public static MutableDataTable ReadAzureBlob(this DataTableBuilder builder, CloudBlobContainer container, string blobName)
        {
            var blob = GetBlobAndVerify(container, blobName);

            // We're returning a MutableDataTable (which is in-memory) anyways, so fine to download into an in-memory buffer.
            // Avoid downloading to a file because Azure nodes may not have a local file resource.
            var stream = new MemoryStream();
            blob.DownloadToStream(stream);
            stream.Seek(0, SeekOrigin.Begin);

            TextReader textReader = new StreamReader(stream);
            var dt = DataTable.New.Read(textReader);
            dt.Name = container.Name + "." + blobName;
            return dt;
        }

        /// <summary>
        /// Read a data table from azure blob. This reads streaming. 
        /// </summary>
        /// <param name="builder">builder</param>
        /// <param name="account">azure acount</param>
        /// <param name="containerName">conatiner name</param>
        /// <param name="blobName">blob name</param>
        /// <returns>in-memory mutable datatable from blob</returns>
        public static DataTable ReadAzureBlobLazy(this DataTableBuilder builder, CloudStorageAccount account, string containerName, string blobName)
        {
            CloudBlobContainer container = GetContainer(account, containerName);
            return ReadAzureBlobLazy(builder, container, blobName);
        }

        /// <summary>
        /// Read a data table from azure blob. This reads streaming. 
        /// </summary>
        /// <param name="builder">builder</param>
        /// <param name="container">conatiner</param>
        /// <param name="blobName">blob name</param>
        /// <returns>in-memory mutable datatable from blob</returns>
        public static DataTable ReadAzureBlobLazy(this DataTableBuilder builder, CloudBlobContainer container, string blobName)
        {
            var blob = GetBlobAndVerify(container, blobName);            
            var stream = blob.OpenRead();
            var dt = DataTable.New.ReadLazy(stream);
            
            dt.Name = container.Name + "." + blobName;
            return dt;
        }


        private static ICloudBlob GetBlobAndVerify(CloudBlobContainer container, string blobName)
        {
            var blob = container.GetBlockBlobReference(blobName);
            if (!blob.Exists())
            {
                string containerName = container.Name;
                string accountName = container.ServiceClient.Credentials.AccountName;
                throw new FileNotFoundException(string.Format("container.blob {0}.{0} does not exist on the storage account '{2}'", containerName, blobName, accountName));
            }
            return blob;
        }


        internal static CloudBlobContainer GetContainer(CloudStorageAccount account, string containerName)
        {
            var client = account.CreateCloudBlobClient();

            var container = client.GetContainerReference(containerName);
            container.CreateIfNotExists();
            return container;
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
            var tableClient = account.CreateCloudTableClient();

            return new AzureStreamingTable { _tableName = tableName, _tableClient = tableClient, Name = tableName };
        }
    }
}
