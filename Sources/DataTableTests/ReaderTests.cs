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
            //Reader.ReadCSV(new StringReader("a
            string[] parts = Reader.split("abc, \"d\", efg", ',');

            Assert.Equal(3, parts.Length);
            Assert.Equal("abc", parts[0]);
            Assert.Equal("d", parts[1]);
            Assert.Equal("efg", parts[2]);
        }
    }
}
