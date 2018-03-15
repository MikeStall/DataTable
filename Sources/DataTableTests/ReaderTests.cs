using DataAccess;
using Xunit;
using System.IO;
using System.Linq;
using System;

namespace DataTableTests
{
    public class ReaderTests
    {
        [Fact]
        public void TestSemicolon()
        {
            TestDelimeter(';');
        }

        [Fact]
        public void TestTab()
        {
            TestDelimeter('\t');
        }

        [Fact]
        public void TestComma()
        {
            TestDelimeter(',');
        }

        private void TestDelimeter(char ch)
        {
            string content =
@"first{0}last
Bob{0}Smith";
            content = string.Format(content, ch);

            TextReader tr = new StringReader(content);
            MutableDataTable dt = DataTable.New.Read(tr, ch);

            AnalyzeTests.AssertEquals(
@"first,last
Bob,Smith
", dt);
        }

        [Fact]
        public void Split()
        {            
            string[] parts = Reader.split("abc, \"d,\", efg", ',');

            Assert.Equal(3, parts.Length);
            Assert.Equal("abc", parts[0]);
            Assert.Equal("d,", parts[1]);
            Assert.Equal("efg", parts[2]);
        }

        [Fact]
        public void Split_SpaceAtFront_Trim()
        {
            string[] parts = Reader.split(" abc,def", ',', true);

            Assert.Equal(2, parts.Length);
            Assert.Equal("abc", parts[0]);
            Assert.Equal("def", parts[1]);
        }

        [Fact]
        public void Split_SpaceAtFront_NoTrim()
        {
            string[] parts = Reader.split(" abc,def", ',', false);

            Assert.Equal(2, parts.Length);
            Assert.Equal(" abc", parts[0]);
            Assert.Equal("def", parts[1]);
        }

        [Fact]
        public void Split_SpaceInFrontOfEscapedQuote()
        {
            string[] parts = Reader.split("abc, \"d,\", efg", ',');

            Assert.Equal(3, parts.Length);
            Assert.Equal("abc", parts[0]);
            Assert.Equal("d,", parts[1]);
            Assert.Equal("efg", parts[2]);
        }

        [Fact]
        public void Split_SpaceAfterEscapedQuote()
        {
            string[] parts = Reader.split("abc,\" d,\",efg", ',', false);

            Assert.Equal(3, parts.Length);
            Assert.Equal("abc", parts[0]);
            Assert.Equal(" d,", parts[1]);
            Assert.Equal("efg", parts[2]);
        }

        [Fact]
        public void Split_SpaceAfterEscapedQuote_NoTrim()
        {
            string[] parts = Reader.split("abc,\" d,\", efg", ',', true);

            Assert.Equal(3, parts.Length);
            Assert.Equal("abc", parts[0]);
            Assert.Equal("d,", parts[1]);
            Assert.Equal("efg", parts[2]);
        }

        [Fact]
        public void Split_SemiColonSeparator()
        {
            string[] parts = Reader.split("abc;d;efg", ';');

            Assert.Equal(3, parts.Length);
            Assert.Equal("abc", parts[0]);
            Assert.Equal("d", parts[1]);
            Assert.Equal("efg", parts[2]);
        }

        [Fact]
        public void Split_FieldOfOnlyDoubleQuotes()
        {
            string[] parts = Reader.split("abc;\"\"", ';');

            Assert.Equal(2, parts.Length);
            Assert.Equal("abc", parts[0]);
            Assert.Equal("", parts[1]);
        }

        [Fact]
        public void Split_EscapedDoubleQuotesWithinAField()
        {
            string[] parts = Reader.split("abc,\"d=\"\"efg\"\"\",h", ',');
            Assert.Equal(3, parts.Length);
            Assert.Equal("abc", parts[0]);
            Assert.Equal("d=\"efg\"", parts[1]);
            Assert.Equal("h", parts[2]);
        }
            
        [Fact]
        public void Split_OneEscapedDoubleQuote()
        {
            string[] parts = Reader.split("abc,\"d=\"\"efg\",h", ',');
            Assert.Equal(3, parts.Length);
            Assert.Equal("abc", parts[0]);
            Assert.Equal("d=\"efg", parts[1]);
            Assert.Equal("h", parts[2]);
        }

        [Fact]
        public void Split_EmptyFields()
        {
            string[] parts = Reader.split("abc,,efg", ',');
            Assert.Equal(3, parts.Length);
            Assert.Equal("abc", parts[0]);
            Assert.Equal("", parts[1]);
            Assert.Equal("efg", parts[2]);
        }

        [Fact]
        public void Split_DoNotTrim()
        {
            string[] parts = Reader.split(" abc ,efg , hi", ',', false);
            Assert.Equal(3, parts.Length);
            Assert.Equal(" abc ", parts[0]);
            Assert.Equal("efg ", parts[1]);
            Assert.Equal(" hi", parts[2]);
        }

        // [Fact] // $$$ Renenable this test.
        public void Split_UnescapedQuote()
        {
            Assert.Throws<AssertException>(
                delegate
                {
                    string[] parts = Reader.split("a\"", ',');
                });
        }

        [Fact]
        public void Split_MissingClosingQuote()
        {
            // Technically an error case. But it happens in the wild. 
            // So make a best effort to parse it. 
            string[] parts = Reader.split("abc,\"\"d\",xyz", ',');
            Assert.Equal(3, parts.Length);
            Assert.Equal("abc", parts[0]);
            Assert.Equal("xyz", parts[2]);
        }

        // $$$ More tests:
        // - split on 2 columns
        // - other ctors (not ReadLAzy). Anything that calls Split is subject.

        [Fact]
        public void ReadWithNewline()
        {

            // Beware of GIT checkouts and editors can replace newlines, so be explicit
            string content = 
"abc,def, xyz\r\n" +
"1,\"1a\r\n1b\", 1c\n" + // has \r\n split
"2, \"2a\n2b\",2c\n" +  // just has \n split
"3, 3ab, 3c";

            var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));

            DataTable dt = DataTable.New.ReadLazy(stream);

            var rows = dt.Rows.ToArray();
            Assert.Equal(3, rows.Length);

            Assert.Equal("1", rows[0].Values[0]);
            Assert.Equal("1a\r\n1b", rows[0].Values[1]);
            Assert.Equal("1c", rows[0].Values[2]);
            
            Assert.Equal("2", rows[1].Values[0]);
            Assert.Equal("2a\n2b", rows[1].Values[1]);
            Assert.Equal("2c", rows[1].Values[2]);

            Assert.Equal("3", rows[2].Values[0]);
            Assert.Equal("3ab", rows[2].Values[1]);
            Assert.Equal("3c", rows[2].Values[2]);
        }

        [Fact]
        public void ReadThrowsAssertExceptionIfAllowMismatchFalseAndLinesContainMismatch()
        {
            // Make reader tolerant

            string content =
@"aaa,bbb,ccc,ddd
111,111,111,111
222,222,222
333,333,333,333";

            var textReader = new StringReader(content);
            MutableDataTable dt = Reader.Read(textReader);

            var s = dt.SaveToString();

            // Round-trip 
            // - Extra comma at row with missing value. 
            // - newline after all rows, even the last one
            Assert.Equal(
@"aaa,bbb,ccc,ddd
111,111,111,111
222,222,222,
333,333,333,333
", s);
        }

        [Fact]
        public void ReadWithDefaultColumnsShouldHandleFirstRowAsRowData()
        {
            var lines = new[] { "not,a,row,header", "second,row,is,data", "third,row,is,data" };
            var content = string.Join("\n", lines);
            var reader = new StringReader(content);
            var headers = new[] { "header1", "header2", "header3", "header4" };
            var data = Reader.Read(reader, defaultColumns: headers);

            Assert.Equal(headers, data.ColumnNames);
            var enumerator = data.Rows.GetEnumerator();
            int rowCount = 0;
            foreach (var expectedRow in lines)
            {
                enumerator.MoveNext();
                var value = string.Join(",", enumerator.Current.Values);
                Assert.Equal(expectedRow, value);
                rowCount++;
            }

            Assert.Equal(lines.Length, rowCount);
        }

    
        [Fact]
        public void ReadFromStreamWithDefaultColumnsShouldHandleFirstRowAsRowData()
        {
            DataTableBuilder builder = new DataTableBuilder();
            var stream = new MemoryStream();
            var sw = new StreamWriter(stream);
            var rows = new[] { "first,row,is,data", "second,row,is,johnny", "second,row,was,laura", };
            foreach (var row in rows)
            {
                sw.WriteLine(row);
            }

            sw.Flush();
            stream.Seek(0, SeekOrigin.Begin);
            try
            {
                var lazy = builder.ReadLazy(stream, rows[0].Split(','));
                Assert.Equal(rows[0].Split(','), lazy.ColumnNames);
                var rowEnumerator = rows.Skip(0).GetEnumerator();
                rowEnumerator.MoveNext();
                var rowCount = 0;
                foreach (var row in lazy.Rows)
                {
                    Assert.Equal(rowEnumerator.Current, string.Join(",", row.Values));
                    rowEnumerator.MoveNext();
                    rowCount++;
                }

                Assert.Equal(rows.Length, rowCount);
            }
            finally
            {
                sw.Dispose();
                stream.Dispose();
            }
        }

        [Fact]
        public void ReadFromTextReaderWithDefaultColumnsShouldHandleFirstRowAsRowData()
        {
            // arrange
            var tmpFile = Path.GetTempFileName();

            var rows = new[] { "first,row,is,data", "second,row,is,johnny", "second,row,was,laura", };
            
            using (var sw = new StreamWriter(tmpFile))
            {
                foreach (var row in rows)
                {
                    sw.WriteLine(row);
                }

                sw.Flush();
            }

            // act
            try
            {
                var builder = new DataTableBuilder();
                var lazy = builder.ReadLazy(tmpFile, rows[0].Split(','));
                Assert.Equal(rows[0].Split(','), lazy.ColumnNames);
                var rowEnumerator = rows.Skip(0).GetEnumerator();
                rowEnumerator.MoveNext();
                var rowCount = 0;

                // assert
                foreach (var row in lazy.Rows)
                {
                    Assert.Equal(rowEnumerator.Current, string.Join(",", row.Values));
                    rowEnumerator.MoveNext();
                    rowCount++;
                }

                Assert.Equal(rows.Length, rowCount);
            }
            finally
            {
                // cleanup
                File.Delete(tmpFile);
            }
        }

        // Check to see if inputs with multiple Windows new line characters works
        [Fact]
        public void ReadWithMultipleNewLines()
        {
            // Beware of GIT checkouts and editors can replace newlines, so be explicit
            string content = "abc,def, xyz\r\n" +
                             "1,\"1a\r\n\r\n\r\n\r\n1b\", 1c\n" +
                             "2, \"2a\n\n\n\n2b\",2c\n" +
                             "3, 3ab, 3c";

            var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));

            DataTable dt = DataTable.New.ReadLazy(stream);

            var rows = dt.Rows.ToArray();
            Assert.Equal(3, rows.Length);

            Assert.Equal("1", rows[0].Values[0]);
            Assert.Equal("1a\r\n\r\n\r\n\r\n1b", rows[0].Values[1]);
            Assert.Equal("1c", rows[0].Values[2]);

            Assert.Equal("2", rows[1].Values[0]);
            Assert.Equal("2a\n\n\n\n2b", rows[1].Values[1]);
            Assert.Equal("2c", rows[1].Values[2]);

            Assert.Equal("3", rows[2].Values[0]);
            Assert.Equal("3ab", rows[2].Values[1]);
            Assert.Equal("3c", rows[2].Values[2]);
        }
    }
}