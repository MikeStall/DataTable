using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using DataAccess;
using System.IO;

namespace DataTableTests
{
    class ReaderTests
    {
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
            MutableDataTable dt = DataTable.New.Read(tr);

            AnalyzeTests.AssertEquals(
@"first,last
Bob,Smith
", dt);
            
        }

        [Fact]
        public void TestCustomDelimeter()
        {
            char ch = 'X';
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
    }
}
