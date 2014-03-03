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
        class TestRow
        {
            public string first { get; set; }
            public string last { get; set; }
        }

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
        public void Reorder()
        {
            MutableDataTable dt = GetTable();

            dt.ReorderColumn("first", 1);

            AnalyzeTests.AssertEquals(
@"last,first
Smith,Bob
Jones,Fred
", dt);
        }

        [Fact]
        public void ReorderOutOfRange()
        {
            MutableDataTable dt = GetTable();

            Assert.Throws<ArgumentOutOfRangeException>(
                () => dt.ReorderColumn("first", 2));

            Assert.Throws<ArgumentOutOfRangeException>(
                () => dt.ReorderColumn("first", -1));            
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
        public void KeepColumnsDoNotThrow()
        {
            MutableDataTable dt = GetTable();

            dt.KeepColumns(false, "last", "first", "made up column name");

            AnalyzeTests.AssertEquals(
@"last,first
Smith,Bob
Jones,Fred
", dt);
        }

        [Fact]
        public void KeepColumnsThrowOnMissing()
        {
            MutableDataTable dt = GetTable();

            bool wasErrorThrown = false;
            try
            {
                dt.KeepColumns(true, "last", "first", "made up column name");
            }
            catch (InvalidOperationException)
            {
                wasErrorThrown = true;
            }

            Assert.True(wasErrorThrown);
            // not testing contents of the table since we are assuming that an Exception was thrown
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
        public void CreateColumn2()
        {
            MutableDataTable dt = GetTable();

            dt.CreateColumns<TestRow>(row => new
                {
                    fullname = row.last + "_" + row.first,
                    len = row.first.Length
                });

            AnalyzeTests.AssertEquals(
@"first,last,fullname,len
Bob,Smith,Smith_Bob,3
Fred,Jones,Jones_Fred,4
", dt);
        }

        // $$$ Test cases:
        // - parser has errors, 
        // - ensure table is consistent even if parser throws. 
        // - what if every row fails? How do we get the row1/dummy object?

        [Fact]
        public void CreateColumn2WithNull()
        {
            MutableDataTable dt = GetTable();

            dt.CreateColumns<TestRow>(row => new
            {
                fullname = row.last + "_" + row.first,
                len = row.first.Length
            });

            AnalyzeTests.AssertEquals(
@"first,last,fullname,len
Bob,Smith,Smith_Bob,3
Fred,Jones,Jones_Fred,4
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
        public void RenameMissing()
        {
            MutableDataTable dt = GetTable();

            dt.RenameColumn("missing", "FName", throwOnMissing : false);
            // No change

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
