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
    public static class DataTableAzureExtensions
    {
        public static MutableDataTable ReadFromAzureBlob(this DataTableBuilder builder, CloudStorageAccount account, string containerName, string blobName)
        {
            CloudBlobContainer container = GetContainer(account, containerName);
            return ReadFromAzureBlob(builder, container, blobName);
        }

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
            return DataTable.New.Read(stream);
        }

        /// <summary>
        /// Save the data table to the given azure blob
        /// </summary>
        /// <param name="table"></param>
        /// <param name="account"></param>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        public static void SaveToAzureBlob(this DataTable table, CloudStorageAccount account, string containerName, string blobName)
        {
            CloudBlobContainer container = GetContainer(account, containerName);
            SaveToAzureBlob(table, container, blobName);
        }

        public static void SaveToAzureBlob(this DataTable table, CloudBlobContainer container, string blobName)
        {
            var blob = container.GetBlobReference(blobName);
            using (BlobStream stream = blob.OpenWrite())
            using (TextWriter writer = new StreamWriter(stream))
            {
                table.SaveToStream(writer);
            }
        }

        private static CloudBlobContainer GetContainer(CloudStorageAccount account, string containerName)
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
    }
}
