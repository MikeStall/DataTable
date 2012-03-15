using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using DataAccess;

namespace DataTableTests
{
    public class Dictionary2dTests
    {
        [Fact]
        public void Simple()
        {
            Dictionary2d<string, string, int> d = new Dictionary2d<string, string, int>();

            d["jan", "white"] = 15;
            d["jan", "black"] = 3;
            d["mar", "white"] = 12;
            d["april", "black"] = 13;
            d["april", "black"]++;

            Assert.Equal(4, d.Count);

            Assert.Equal(new string[] { "april", "jan", "mar"}, d.Key1);
            Assert.Equal(new string[] { "black", "white" }, d.Key2);

            Assert.Equal(15, d["jan", "white"]);
            Assert.Equal(3, d["jan", "black"]);
            Assert.Equal(12, d["mar", "white"]);
            Assert.Equal(13 + 1, d["april", "black"]);

            Assert.Equal(0, d["missing", "missing"]);
            

        }
    }
}
