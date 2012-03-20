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
        DataTable GetTable()
        {
            // Note irregular spacing should get parsed out
            string content =
@"first,last,age
Bob, Smith,  12
Bob, Jones, 34
Ed,  Smith, 12
John, Smith, 34";

            string temp = Path.GetTempFileName();
            File.WriteAllText(temp, content);

            return DataTable.New.ReadLazy(temp);
        }
               
        MutableDataTable GetInMemoryTable()
        {
            string content =
@"first,last,age
Bob, Smith, 12
Bob, Jones, 34
Ed,  Smith,  12
John, Smith, 34";

            TextReader tr = new StringReader(content);
            MutableDataTable dt = DataTable.New.ReadAll(tr);
            return dt;
        }

        public static void AssertEquals(string content, DataTable dt)
        {
            StringWriter sw = new StringWriter();
            dt.SaveToStream(sw);
            Assert.Equal(content, sw.ToString());
        }

        [Fact]
        public void ColumnCountsStreaming()
        {
            DataTable dtOriginal = GetTable();
            GetColumnValueCounts(dtOriginal);
        }

        [Fact]
        public void SampleTest()
        {
            DataTable dtOriginal = GetTable();
            MutableDataTable result = Analyze.SampleTopN(dtOriginal, 2);

            AssertEquals(
@"first,last,age
Bob,Smith,12
Bob,Jones,34
", result);
        }


        [Fact]
        public void ColumnCountsInMemory()
        {
            DataTable dtOriginal = GetInMemoryTable();
            GetColumnValueCounts(dtOriginal);
        }

        void GetColumnValueCounts(DataTable dtOriginal)
        {
            MutableDataTable result = Analyze.GetColumnValueCounts(dtOriginal, 1);

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
            DataTable dtOriginal = GetTable();
            DuplicatTests(dtOriginal);
        }

        [Fact]
        public void DupInMemory()
        {
            DataTable dtOriginal = GetInMemoryTable();
            DuplicatTests(dtOriginal);
        }

        public void DuplicatTests(DataTable dtOriginal)
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

        [Fact]
        public void Join()
        {            
            MutableDataTable d1 = DataTable.New.ReadAll(new StringReader(
@"first, last
Bob, Jones
Alfred, Smith
Ed, Edson
"));

            MutableDataTable d2 = DataTable.New.ReadAll(new StringReader(
@"last, country
smith, English
Piere, French
Jones, American
"));

            MutableDataTable merge = Analyze.Join(d1, d2, "last");



            AssertEquals(
@"first,last,country
Bob,JONES,American
Alfred,SMITH,English
Ed,EDSON,
,PIERE,French
", merge);

        }
    }
}