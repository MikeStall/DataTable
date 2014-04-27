using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using DataAccess;
using System.IO;

namespace DataTableTests
{
    public class DataTableStreamLookupTests
    {
        public class TestRow
        {         
            public string h1 { get; set ;}
            public string h2 { get; set ;}

            public string GetKey() { return h1 + "+" + h2; }
        }

        [Fact]
        public void LookupTest()
        {
            // Explicit escaped string so we can control whitespace and newlines. 
            string src = "h1,h2\na1 ,a2\r\n b1 ,b2";

            var bytes = Encoding.ASCII.GetBytes(src);
            var input = new MemoryStream(bytes);
            DataTableStreamLookup<TestRow> lookup = new DataTableStreamLookup<TestRow>(input);

            long offsetA = 6;
            string keyA = "a1+a2";
            long offsetB = 14;
            string keyB = "b1+b2";

            List<Tuple<long, string>> list = new List<Tuple<long, string>>();
            lookup.GetOffsetsForRow((row, offset) =>
            {
                string key = row.GetKey();
                list.Add(Tuple.Create(offset, key));
            });
            Assert.Equal(2, list.Count);
            Assert.Equal(offsetA, list[0].Item1);
            Assert.Equal(keyA, list[0].Item2);
            Assert.Equal(offsetB, list[1].Item1);
            Assert.Equal(keyB, list[1].Item2);
            
            var rowA = lookup.ReadAtOffset(offsetA);
            Assert.Equal(keyA, rowA.GetKey());

            
            var rowB = lookup.ReadAtOffset(offsetB);
            Assert.Equal(keyB, rowB.GetKey());
        }        
    }
}