using System;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table.DataServices;
using Xunit;
using DataAccess;
using System.IO;
using System.Diagnostics;

namespace CsvReader.Azure.Tests
{
    public class Tests
    {
        static private CloudStorageAccount GetStorage()
        {
            return CloudStorageAccount.DevelopmentStorageAccount;
        }

        [Fact]
        public void DownloadNonExistentBlob()
        {
            var account = GetStorage();
            var client = account.CreateCloudBlobClient();

            string containerName = "csvtestcontainer";
            string blobName = "csvtestblobnoexist";

            // Test when container doesn't exist.
            DeleteContainer(account, containerName);
            Assert.Throws<FileNotFoundException>(() => DataTable.New.ReadAzureBlob(account, containerName, blobName));

            // Container exists, blob name doesn't.
            var container = client.GetContainerReference(containerName);
            container.CreateIfNotExists();
            Assert.Throws<FileNotFoundException>(() => DataTable.New.ReadAzureBlob(account, containerName, blobName));
        }

        [Fact]
        public void UploadUnsupportedTypeFails()
        {
            // Save a table to azure, and then use traditional techniques to query it back. 
            var account = GetStorage();
            
            var source = from x in Enumerable.Range(1, 10) select new { N = x, N2 = x * x };
            DataTable dtSource = DataTable.New.FromEnumerable(source);

            Assert.Throws<InvalidOperationException>(() => dtSource.SaveToAzureTable(account, "table", new Type[] { typeof(object), typeof(object) }));
        }

        [Fact]
        public void UploadCsvToBlob()
        {
            // Save a table to azure, and then use traditional techniques to query it back. 
            var account = GetStorage();
            var client = account.CreateCloudBlobClient();

            var source = from x in Enumerable.Range(1, 10) select new { N = x, N2 = x * x };
            DataTable dtSource = DataTable.New.FromEnumerable(source);
            string originalContent = TableToString(dtSource);

            string containerName = "csvtestcontainer";
            string blobName = "csvtestblob";

            // Will create container if it doesn't exist. So delete
            DeleteContainer(account, containerName);

            dtSource.SaveToAzureBlob(account, containerName, blobName);

            // Verify existence.
            {
                var container = client.GetContainerReference(containerName);
                var blob = container.GetBlockBlobReference(blobName);
                string contents = blob.DownloadText();
                Assert.Equal(originalContent, contents);
            }

            // Now download with helpers.
            DataTable dtDownload = DataTable.New.ReadAzureBlob(account, containerName, blobName);
            string downloadContent = TableToString(dtDownload);
            Assert.Equal(containerName + "." + blobName, dtDownload.Name); // verify name
            Assert.Equal(originalContent, downloadContent); // verify contents
        }

        private static string TableToString(DataTable dt)
        {
            StringWriter sw = new StringWriter();
            dt.SaveToStream(sw);
            return sw.ToString();
        }

        [DebuggerNonUserCode]
        private static void DeleteContainer(CloudStorageAccount account, string containerName)
        {
            var client = account.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(containerName);
            try
            {
                container.Delete();
            }
            catch
            {
                // Throws if container doesn't exist. That's ok. 
            }
        }

        [Fact]
        public void UploadInvalidTableName()
        {
            var account = GetStorage();

            var source = from x in Enumerable.Range(1, 10) select new { N = x, N2 = x * x };
            DataTable dtSource = DataTable.New.FromEnumerable(source);

            string tableName = "csvtesttable%%"; // illegal name

            // Illegal name should get graceful error.
            Assert.Throws < InvalidOperationException>(() => dtSource.SaveToAzureTable(account, tableName));
        }

        [Fact]
        public void SaveToTableCausesDelete()
        {
            // Save a table to azure, and then use traditional techniques to query it back. 
            var account = GetStorage();

            var source = from x in Enumerable.Range(1, 10) select new { N = x, N2 = x * x };
            DataTable dtSource = DataTable.New.FromEnumerable(source);

            string tableName = "csvtesttable";

            dtSource.SaveToAzureTable(account, tableName);

            // Now save a different table. 
            var source2 = from x in Enumerable.Range(1, 5) select new { abc = x };
            DataTable dtSource2 = DataTable.New.FromEnumerable(source2);

            dtSource2.SaveToAzureTable(account, tableName);

            // Now look at table, and make sure it's the latest one. 
            DataTable dtFromAzure = DataTable.New.ReadAzureTableLazy(account, tableName);            
            Assert.False(dtFromAzure.HasColumnName("N"));
            Assert.False(dtFromAzure.HasColumnName("N2"));
            Assert.True(dtFromAzure.HasColumnName("abc"));

            int totalRows = dtFromAzure.Rows.Count();
            Assert.Equal(source2.Count(), totalRows);
        }

        [Fact]
        public void UploadWithRowKeyNoPartition()
        {
            var account = GetStorage();

            var source = from x in Enumerable.Range(1, 3) select new { RowKey = x * 10, N2 = x * x };
            DataTable dtSource = DataTable.New.FromEnumerable(source);

            string tableName = "csvtesttable";

            dtSource.SaveToAzureTable(account, tableName);

            DataTable dtFromAzure = DataTable.New.ReadAzureTableLazy(account, tableName);

            TestRecord[] result = Utility.ReadTable<TestRecord>(account, tableName);
            Assert.Equal(3, result.Length);

            int idx = 0;
            for (int i = 1; i <= result.Length; i++)
            {
                Assert.Equal((i * 10).ToString(), result[idx].RowKey);
                Assert.Equal("1", result[idx].PartitionKey);
                Assert.Equal(i * i, result[idx].N2);
                idx++;
            }
        }

        [Fact]
        public void UploadWithRowKeyAndPartition()
        {
            var account = GetStorage();

            // This is also a good test of each row having a different partition key
            var source = from x in Enumerable.Range(1, 3) select new { RowKey = x * 10, PartitionKey = x * 100, N2 = x * x };
            DataTable dtSource = DataTable.New.FromEnumerable(source);

            string tableName = "csvtesttable";

            dtSource.SaveToAzureTable(account, tableName);

            DataTable dtFromAzure = DataTable.New.ReadAzureTableLazy(account, tableName);

            TestRecord[] result = Utility.ReadTable<TestRecord>(account, tableName);
            Assert.Equal(3, result.Length);

            int idx = 0;
            for (int i = 1; i <= result.Length; i++)
            {
                Assert.Equal((i * 10).ToString(), result[idx].RowKey);
                Assert.Equal((i*100).ToString(), result[idx].PartitionKey);
                Assert.Equal(i * i, result[idx].N2);
                idx++;
            }
        }

        [Fact]
        public void UploadWithNoRowKeyAndPartition()
        {
            var account = GetStorage();

            // This is also a good test of each row having a different partition key
            var source = from x in Enumerable.Range(1, 3) select new { PartitionKey = x * 100, N2 = x * x };
            DataTable dtSource = DataTable.New.FromEnumerable(source);

            string tableName = "csvtesttable";

            dtSource.SaveToAzureTable(account, tableName);

            DataTable dtFromAzure = DataTable.New.ReadAzureTableLazy(account, tableName);

            TestRecord[] result = Utility.ReadTable<TestRecord>(account, tableName);
            Assert.Equal(3, result.Length);

            int idx = 0;
            for (int i = 1; i <= result.Length; i++)
            {
                Assert.Equal((i * 100).ToString(), result[idx].PartitionKey);
                Assert.Equal(i * i, result[idx].N2);
                idx++;
            }
        }

        [Fact]
        public void UploadWithExplicitRowAndPartitionFunc()
        {
            var account = GetStorage();

            // This is also a good test of each row having a different partition key
            var source = from x in Enumerable.Range(1, 3) select new { N = x * 10 };
            DataTable dtSource = DataTable.New.FromEnumerable(source);

            string tableName = "csvtesttable";

            Func<int, Row, PartitionRowKey> explicitFunc = (rowIdx, row) => new PartitionRowKey((rowIdx * 10).ToString(), rowIdx);
            dtSource.SaveToAzureTable(account, tableName, new Type[] { typeof(int) }, explicitFunc);

            DataTable dtFromAzure = DataTable.New.ReadAzureTableLazy(account, tableName);
            Assert.Equal(tableName, dtFromAzure.Name); // names should match.

            TestRecord[] result = Utility.ReadTable<TestRecord>(account, tableName);
            Assert.Equal(3, result.Length);

            int idx = 0;
            for (int i = 1; i <= result.Length; i++)
            {
                Assert.Equal(idx, int.Parse(result[idx].RowKey));
                Assert.Equal((idx*10).ToString(), result[idx].PartitionKey);
                Assert.Equal(i*10, result[idx].N);
                idx++;
            }
        }

        [Fact]
        public void RoundtripTable()
        {
            // Test that if we download and then reupload, it's ok. 
            var account = GetStorage();

            var source = from x in Enumerable.Range(1, 200) select new { N = x, N2 = x * x };
            DataTable dtSource = DataTable.New.FromEnumerable(source);

            string tableName = "csvtesttable";

            dtSource.SaveToAzureTable(account, tableName); // original upload

            DataTable dtDownload1 = DataTable.New.ReadAzureTableLazy(account, tableName);
            MutableDataTable dtA = DataTable.New.GetMutableCopy(dtDownload1);            

            // this writes back to the source that dtDownload1 was streaming from. 
            // But we already copied to dtA, so safe to overwrite.
            dtA.SaveToAzureTable(account, tableName); // 2nd upload

            DataTable dtDownload2 = DataTable.New.ReadAzureTableLazy(account, tableName); 
            MutableDataTable dtB = DataTable.New.GetMutableCopy(dtDownload2);

            // Everything except timestamps should match.
            dtA.DeleteColumns("TimeStamp");
            dtB.DeleteColumns("TimeStamp");
            Utility.AssertEquals(dtA, dtB);          
        }

        [Fact]
        public void UploadCsvToAzureTables()
        {
            // Save a table to azure, and then use traditional techniques to query it back. 
            // Use a large enough value to forcve batching and spilling.
            var account = GetStorage();

            var source = from x in Enumerable.Range(1, 200) select new { N = x, N2 = x * x };
            DataTable dtSource = DataTable.New.FromEnumerable(source);

            string tableName = "csvtesttable";

            dtSource.SaveToAzureTable(account, tableName);
            
            // Use traditional Azure table read to verify the newly uploaded table matches source.
            TestRecord[] result = Utility.ReadTable<TestRecord>(account, tableName);
            int i = 0;
            foreach(var item in source)
            {
                Assert.Equal(item.N, result[i].N);
                Assert.Equal(item.N2, result[i].N2);
                i++;
            }
            Assert.Equal(i, result.Length);            

            // Read back as datatable
            DataTable dtFromAzure = DataTable.New.ReadAzureTableLazy(account, tableName);

            // When reading from Azure, we should get back row, parition, and timestamp keys.
            Assert.True(dtFromAzure.HasColumnName("RowKey"));
            Assert.True(dtFromAzure.HasColumnName("PartitionKey"));
            Assert.True(dtFromAzure.HasColumnName("TimeStamp"));

            // Compare contents with original table that was uploaded. Easy way to do this is to just keep the original columns.
            MutableDataTable dt5 = DataTable.New.GetMutableCopy(dtFromAzure);
            dt5.KeepColumns("N", "N2");
            Utility.AssertEquals(dtSource, dt5);          
        }
    }

    class TestRecord : TableServiceEntity
    {
        public int N { get; set; }
        public int N2 { get; set; }        
    }
}
