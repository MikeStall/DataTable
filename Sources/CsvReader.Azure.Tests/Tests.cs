using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using Microsoft.WindowsAzure;
using DataAccess;
using Microsoft.WindowsAzure.StorageClient;

namespace CsvReader.Azure.Tests
{
#if false
    Azure blob

    Other tests:
    - Read from Azure

    Unit test for other overloads. 

    Round-trip

    Custom RowKey func

    Read is lazy 

#endif
    public class Tests
    {
        static private CloudStorageAccount GetStorage()
        {
            return CloudStorageAccount.DevelopmentStorageAccount;
        }

        [Fact]
        public void Save()
        {
            // Save a table to azure, and then use traditional techniques to query it back. 
            var account = GetStorage();

            var source = from x in Enumerable.Range(1, 10) select new { N = x, N2 = x * x };
            DataTable dtSource = DataTable.New.FromEnumerable(source);

            string tableName = "csvtesttable";

            // $$$ Test case: use <T> overload for saving, skip explicitly providing types
            dtSource.SaveToAzureTable(account, tableName, new Type[] { typeof(int), typeof(int) });
            
            // Verify table matches source
            TestRecord[] result = Utility.ReadTable<TestRecord>(account, tableName);
            int i = 0;
            foreach(var item in source)
            {
                Assert.Equal(item.N, result[i].N);
                Assert.Equal(item.N2, result[i].N2);
                i++;
            }
            Assert.Equal(i, result.Length);            

            // Read back as table
            DataTable dt4 = DataTable.New.ReadAzureTableLazy(account, tableName);

            MutableDataTable dt5 = DataTable.New.GetMutableCopy(dt4);
            dt5.KeepColumns("N", "N2");
            
            // This will azure's extra fields, like row and parition key and timestamp.
            Utility.AssertEquals(dtSource, dt5);            
        }


    }

    class TestRecord : TableServiceEntity
    {
        public int N { get; set; }
        public int N2 { get; set; }
    }
}
