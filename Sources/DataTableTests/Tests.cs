using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using DataAccess;
using System.IO;

namespace DataTableTests
{
    
    public class Scenarios
    {
        [Fact]
        public void Test()
        {
            TextReader tr = new StringReader(
@"name, age,    favorite fruit
Bob, 20, apples
Ed, 65, prunes
Sarah, 40, cherries");

            DataTable dt = Reader.ReadCSV(tr);

            Assert.Equal(3, dt.NumRows);
            
            // Whitespace has been stripped
            Assert.True(dt.HasColumnName("name"));
            Assert.True(dt.HasColumnName("age"));



        }
    }
}
