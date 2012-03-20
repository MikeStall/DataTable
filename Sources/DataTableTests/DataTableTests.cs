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
            Assert.Equal(
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
    }
}
