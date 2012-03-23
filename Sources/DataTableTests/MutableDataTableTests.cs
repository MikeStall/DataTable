using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using DataAccess;
using System.IO;

namespace DataTableTests
{
    public class MutableDataTableTests
    {
        MutableDataTable GetTable()
        {
            string content =
@"first,last
Bob, Smith
Fred, Jones";

            TextReader tr = new StringReader(content);
            MutableDataTable dt = DataTable.New.Read(tr);
            return dt;
        }

        [Fact]
        public void NumRows()
        {
            MutableDataTable dt = GetTable();

            Assert.Equal(2, dt.NumRows);
        }

        [Fact]
        public void MutateRow()
        {
            MutableDataTable dt = GetTable();
            Row row = dt.GetRow(0);
            
            row["first"] = "Ed"; // modify by row

            Assert.Equal("Ed", row["first"]);
            Assert.Equal("Ed", row.Values[0]);
            Assert.Equal(dt.GetColumn("first").Values[0], "Ed"); // Verify lookup by column
        }

        [Fact]
        public void KeepColumnsReorder()
        {
            MutableDataTable dt = GetTable();

            dt.KeepColumns("last", "first");

            AnalyzeTests.AssertEquals(
@"last,first
Smith,Bob
Jones,Fred
", dt);
        }

        [Fact]
        public void KeepColumnsRemove()
        {
            MutableDataTable dt = GetTable();

            dt.KeepColumns("last");

            AnalyzeTests.AssertEquals(
@"last
Smith
Jones
", dt);
        }

        [Fact]
        public void KeepColumnsBadNameRemove()
        {
            MutableDataTable dt = GetTable();

            Assert.Throws<InvalidOperationException>(() => dt.KeepColumns("missing"));

        }

        [Fact]
        public void RemoveColumns()
        {
            MutableDataTable dt = GetTable();
            dt.RemoveColumn(1);

            AnalyzeTests.AssertEquals(
@"first
Bob
Fred
", dt);
        }

        [Fact]
        public void RemoveColumnIllegalIndex()
        {
            MutableDataTable dt = GetTable();

            Assert.Equal(2, dt.Columns.Length); // initial value
            Assert.Throws<ArgumentOutOfRangeException>(()=> { dt.RemoveColumn(-1); });
            Assert.Throws<ArgumentOutOfRangeException>(()=> { dt.RemoveColumn(5); });
            Assert.Equal(2, dt.Columns.Length); // no change
        }


        [Fact]
        public void GetColumn()
        {
            MutableDataTable dt = GetTable();

            Column c = dt.GetColumn("first");

            Assert.Equal("first", c.Name);
            Assert.Equal(new string[] { "Bob", "Fred" }, c.Values);
        }

        [Fact]
        public void GetMissingColumnIsNull()
        {
            MutableDataTable dt = GetTable();

            Column c = dt.GetColumn("missing");
            Assert.Null(c);
        }

        [Fact]
        public void AssertType()
        {
            Type t = typeof(MutableDataTable);

            Assert.True(t.IsPublic);
            Assert.True(typeof(DataTable).IsAssignableFrom(t));
        }
    }
}
