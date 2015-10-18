using DataAccess;
using Xunit;
using System.IO;
using System.Linq;
using System;

namespace DataTableTests
{
    public class WriterTests
    {
        [Fact]
        public void WriteWithNewLine()
        {
            string content =
"abc,def, xyz\r\n" +
"1,\"1a\r\n1b\", 1c\n" + // has \r\n split
"2, \"2a\n2b\",2c\n" +  // just has \n split
"3, 3ab, 3c\r\n"  + 
"4,\"4b,b\",4c";

            DataTable dt = DataTable.New.ReadFromString(content);

            var s = dt.SaveToString();
            
            Assert.Equal(
            "abc,def,xyz\r\n" +
            "1,\"1a\r\n1b\",1c\r\n" + 
            "2,\"2a\n2b\",2c\r\n" +  
            "3,3ab,3c\r\n" + 
            "4,\"4b,b\",4c\r\n"
            , s);            
        }

    }
}