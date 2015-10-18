using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using DataAccess;
using System.IO;

namespace DataTableTests
{
    public class DataTableTests
    {
        [Fact]
        public void SaveToCSV()
        {
            var dt = DataTable.New.FromEnumerable(new int[] { 10, 20, 30, 40, 50 });

            string temp = Path.GetTempFileName() +".csv";
            dt.SaveCSV(temp);

            string content = File.ReadAllText(temp);
            AnalyzeTests.AssertEquals(
@"value
10
20
30
40
50
", content);
            File.Delete(temp);            
        }

        [Fact]
        public void HasColumnName()
        {
            MutableDataTable dt = Scenarios.GetTable();

            // Whitespace has been stripped
            Assert.True(dt.HasColumnName("name"));
            Assert.True(dt.HasColumnName("age"));
            Assert.True(dt.HasColumnName("AGE")); // Case insensitive
            Assert.True(dt.HasColumnName("favorite fruit"));

            Assert.False(dt.HasColumnName("boo")); // missing
            Assert.False(dt.HasColumnName(null));
        }

        [Fact]
        public void SetName()
        {
            MutableDataTable dt = new MutableDataTable();
            dt.Name = "test";

            Assert.Equal("test", dt.Name);
        }


        [Fact]
        public void StreamReaderNoTrailingNewline()
        {
            // Arrange
            Stream s = new MemoryStream();
            TextWriter tw = new StreamWriter(s);
            tw.Write(
@"value
10
20
30"); // No newline after last row
            tw.Flush();
            s.Position = 0;


            // Act

            DataTable dt = DataTable.New.ReadLazy(s);

            var x = dt.Rows.ToArray();

            // assert
            Assert.Equal(3, x.Length);
            Assert.Equal("10", x[0]["value"]);
            Assert.Equal("20", x[1]["value"]);
            Assert.Equal("30", x[2]["value"]);

            s.Position = 0; // verify stream is not disposed            
        }

        [Fact]
        public void StreamReaderTrailingNewline()
        {
            // Arrange
            Stream s = new MemoryStream();
            TextWriter tw = new StreamWriter(s);
            tw.Write(
@"value
10
20
30
"); // Newline after the last row.
            tw.Flush();
            s.Position = 0;


            // Act

            DataTable dt = DataTable.New.ReadLazy(s);

            var x = dt.Rows.ToArray();
            
            // assert
            Assert.Equal(3, x.Length);
            Assert.Equal("10", x[0]["value"]);
            Assert.Equal("20", x[1]["value"]);
            Assert.Equal("30", x[2]["value"]);

            s.Position = 0; // verify stream is not disposed            
        }

        private class test
        {
            public decimal a { get; set; }
            public string b { get; set; }
        }

        [Fact]
        public void FromEnumerable_WithDecimalField_CheckWithRowAs()
        {
            var items = new List<test> {new test {a = 1.23m, b = "abc"}, new test {a = 2.21m, b = "def"}};
            var table = DataTable.New.FromEnumerable(items);
            Assert.NotNull(table);
            Assert.Equal(2, table.NumRows);
            Assert.Equal("a", table.ColumnNames.First());

            // test that the value made it in properly
            var firstRow = table.Rows.First();
            Assert.Equal(1.23m, Convert.ToDecimal(firstRow.Values.First()));

            // test that RowsAs handles decimals gracefully as well
            var i = 0;
            foreach (var newRow in table.RowsAs<test>())
            {
                var existingRow = items[i++];
                Assert.Equal(existingRow.a, newRow.a);
                Assert.Equal(existingRow.b, newRow.b);
            }
        }

        [Fact]
        public void ReadFromString_ProperInputWithDefaultLineEndings_EvaluatesProperly()
        {
            string input = "ID,Name,Age\r\n1,John Smith,23\r\n2,Jane Jones,32";
            var dt = DataTable.New.ReadFromString(input);
            Assert.Equal(2, dt.Rows.Count());
            Assert.Equal(3, dt.Columns.Count());
            Assert.Equal("Name", dt.ColumnNames.ToList()[1]);
        }

        [Fact]
        public void ReadFromString_ProperInputUnixLineEndings_EvaluatesProperly()
        {
            string input = "ID,Name,Age\n1,John Smith,23\n2,Jane Jones,32";
            var dt = DataTable.New.ReadFromString(input, "\n");
            Assert.Equal(2, dt.Rows.Count());
            Assert.Equal(3, dt.Columns.Count());
            Assert.Equal("Name", dt.ColumnNames.ToList()[1]);
        }

        // Reported by:
        // https://github.com/MikeStall/DataTable/issues/44
        [Fact]
        public void StreamReaderRowEndingWithEmptyValue()
        {
            // Arrange
            Stream s = new MemoryStream();
            TextWriter tw = new StreamWriter(s);
            tw.Write(
@"value1,value2
10,");
            tw.Flush();
            s.Position = 0;


            // Act

            DataTable dt = DataTable.New.ReadLazy(s);

            var x = dt.Rows.ToArray();

            // assert
            Assert.Equal(1, x.Length);
            Assert.Equal("10", x[0]["value1"]);
            Assert.Equal("", x[0]["value2"]);

            s.Position = 0; // verify stream is not disposed            
        }

    }
}
