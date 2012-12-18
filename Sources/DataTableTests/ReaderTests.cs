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
            Assert.Throws<AssertException>(
                delegate
                {
                    string[] parts = Reader.split("abc,\"\"d", ',');
                });
        }


        // $$$ More tests:
        // - split on 2 columns
        // - other ctors (not ReadLAzy). Anything that calls Split is subject.

        [Fact]
        public void ReadWithNewline()
        {

            string content = @"abc,def, xyz
1,'1a
1b', 1c
2, 2ab,2c
".Replace('\'', '"');
            var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));

            DataTable dt = DataTable.New.ReadLazy(stream);

            var rows = dt.Rows.ToArray();
            Assert.Equal(2, rows.Length);

            Assert.Equal("1", rows[0].Values[0]);
            Assert.Equal("1a" + Environment.NewLine + "1b", rows[0].Values[1]);
            Assert.Equal("1c", rows[0].Values[2]);
            
            Assert.Equal("2", rows[1].Values[0]);
            Assert.Equal("2ab", rows[1].Values[1]);
            Assert.Equal("2c", rows[1].Values[2]);
        }
    }
}
