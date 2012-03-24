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
        public void DeleteColumnsEmptyArray()
        {
            MutableDataTable dt = GetTable();

            dt.DeleteColumns();
        }

        [Fact]
        public void DeleteColumns()
        {
            MutableDataTable dt = GetTable();

            dt.DeleteColumns("first");

            AnalyzeTests.AssertEquals(
@"last
Smith
Jones
", dt);
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
        public void KeepRows()
        {
            MutableDataTable dt = GetTable();

            dt.KeepRows(row => row["last"] == "Jones");
            
            AnalyzeTests.AssertEquals(
@"first,last
Fred,Jones
", dt);
        }

        [Fact]
        public void KeepRowsThrowsOnNull()
        {
            MutableDataTable dt = GetTable();

            Assert.Throws<ArgumentNullException>(() => dt.KeepRows(null));
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
            dt.DeleteColumn(1);

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
            Assert.Throws<ArgumentOutOfRangeException>(()=> { dt.DeleteColumn(-1); });
            Assert.Throws<ArgumentOutOfRangeException>(()=> { dt.DeleteColumn(5); });
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
        public void GetColumns()
        {
            MutableDataTable dt = GetTable();

            Column[] cs = dt.GetColumns("last", "first");

            Assert.Equal(2, cs.Length);
            Assert.Equal("last", cs[0].Name);
            Assert.Equal("first", cs[1].Name);            
        }

        [Fact]
        public void CreateColumn()
        {
            MutableDataTable dt = GetTable();

            dt.CreateColumn("fullname", row => row["first"] + "_" + row["last"]);

            AnalyzeTests.AssertEquals(
@"first,last,fullname
Bob,Smith,Bob_Smith
Fred,Jones,Fred_Jones
", dt);
        }

        [Fact]
        public void CreateColumnFromMerging()
        {
            MutableDataTable dt = GetTable();

            Column c = dt.CreateColumnFromMerging("fullname", "first", "last");

            Assert.Equal("fullname", c.Name);

            AnalyzeTests.AssertEquals(
@"first,last,fullname
Bob,Smith,Bob Smith
Fred,Jones,Fred Jones
", dt);
        }

        [Fact]
        public void Rename()
        {
            MutableDataTable dt = GetTable();

            dt.RenameColumn("first", "FName");

            AnalyzeTests.AssertEquals(
@"FName,last
Bob,Smith
Fred,Jones
", dt);
        }

        [Fact]
        public void RenameSame()
        {
            MutableDataTable dt = GetTable();

            dt.RenameColumn("first", "first");

            AnalyzeTests.AssertEquals(
@"first,last
Bob,Smith
Fred,Jones
", dt);
        }

        [Fact]
        public void RenameBadOldName()
        {
            MutableDataTable dt = GetTable();

            // Fail when old name does not exist
            Assert.Throws<InvalidOperationException>(() => dt.RenameColumn("illegal", "FName"));
        }

        [Fact]
        public void RenameBadNewName()
        {
            MutableDataTable dt = GetTable();

            // Fail when new name already exists.
            Assert.Throws<InvalidOperationException>(() => dt.RenameColumn("first", "last"));
        }



        [Fact]
        public void GetMissingColumnIsNull()
        {
            MutableDataTable dt = GetTable();

            Column c = dt.GetColumn("missing");
            Assert.Null(c);
        }

        [Fact]
        public void ApplyToColumn()
        {
            MutableDataTable dt = GetTable();

            dt.ApplyToColumn("first", value => value.ToUpper());

            AnalyzeTests.AssertEquals(
@"first,last
BOB,Smith
FRED,Jones
", dt);
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
