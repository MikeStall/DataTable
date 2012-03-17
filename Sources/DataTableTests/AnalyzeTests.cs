using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using DataAccess;
using System.IO;

namespace DataTableTests
{
    public class AnalyzeTests
    {
        // Use a small table here because any table update here will likely require a test update.
        DataTableReference GetTable()
        {
            string content =
@"first,last,age
Bob, Smith, 12
Bob, Jones, 34
Ed,  Smith, 12
John, Smith, 34";

            string temp = Path.GetTempFileName();
            File.WriteAllText(temp, content);

            return new DataTableStream(temp);
        }
                
        DataTableReference GetInMemoryTable()
        {
            string content =
@"first,last,age
Bob, Smith, 12
Bob, Jones, 34
Ed,  Smith, 12
John, Smith, 34";

            TextReader tr = new StringReader(content);
            DataTable dt = Reader.ReadCSV(tr);
            return dt;
        }

        void AssertEquals(string content, DataTable dt)
        {
            StringWriter sw = new StringWriter();
            dt.SaveToStream(sw);
            Assert.Equal(content, sw.ToString());
        }

        [Fact]
        public void ColumnCountsStreaming()
        {
            DataTableReference dtOriginal = GetTable();
            GetColumnValueCounts(dtOriginal);
        }

        [Fact]
        public void ColumnCountsInMemory()
        {
            DataTableReference dtOriginal = GetInMemoryTable();
            GetColumnValueCounts(dtOriginal);
        }

        void GetColumnValueCounts(DataTableReference dtOriginal)
        {
            DataTable result = Analyze.GetColumnValueCounts(dtOriginal, 1);

            AssertEquals(
@"column name,count,Top Value 0,Top Occurrence 0
first,3,Bob,2
last,2,Smith,3
age,2,12,2
", result);
        }

        [Fact]
        public void DupStreaming()
        {
            DataTableReference dtOriginal = GetTable();
            DuplicatTests(dtOriginal);
        }

        [Fact]
        public void DupInMemory()
        {
            DataTableReference dtOriginal = GetInMemoryTable();
            DuplicatTests(dtOriginal);
        }

        public void DuplicatTests(DataTableReference dtOriginal)
        {
            // Select first colyumn
            DataTable dt1 = Analyze.SelectDuplicates(dtOriginal, "first");

            AssertEquals(
@"first,last,age
Bob,Smith,12
Bob,Jones,34
", dt1);

            // Select two columns
            DataTable dt2 = Analyze.SelectDuplicates(dtOriginal, "last", "age");

            AssertEquals(
@"first,last,age
Bob,Smith,12
Ed,Smith,12
", dt2);


            // Select two columns, empty
            DataTable dt3 = Analyze.SelectDuplicates(dtOriginal, "first", "age");

            AssertEquals(
@"first,last,age
", dt3);

        }
    }
}