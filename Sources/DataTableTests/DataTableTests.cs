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
        public void StreamReader()
        {
            // Arrange
            Stream s = new MemoryStream();
            TextWriter tw = new StreamWriter(s);
            tw.Write(
@"value
10
20
30
");
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
    }
}
