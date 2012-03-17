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

        static MutableDataTable GetTable()
        {
            TextReader tr = new StringReader(
@"name, age,    favorite fruit
Bob, 20, apples
Ed, 65, prunes
Sarah, 40, cherries");

            MutableDataTable dt = Reader.ReadCSV(tr);
            return dt;
        }

        [Fact]
        public void Columns()
        {
            MutableDataTable dt = GetTable();

            // Whitespace has been stripped
            Assert.True(dt.HasColumnName("name"));
            Assert.True(dt.HasColumnName("age"));
            Assert.True(dt.HasColumnName("AGE")); // Case insensitive
            Assert.True(dt.HasColumnName("favorite fruit"));

            Assert.Equal(new string[] { "name", "age", "favorite fruit" },
                dt.ColumnNames);

            Assert.Null(dt.GetColumn("missing")); // missing columns return null            
        }

        [Fact]
        public void GetValuesFromRows()
        {
            MutableDataTable dt = GetTable();

            Assert.Equal(3, dt.NumRows);
            Row[] rows = dt.Rows.ToArray();

            Assert.Equal("Bob", rows[0]["name"]); // index by column name
            Assert.Equal("cherries", rows[2]["favorite fruit"]); // index by column name


            Assert.Equal(new string[] { "Ed", "65", "prunes" },
                rows[1].Values);
        }

        [Fact]
        public void RowWriteLine()
        {
            MutableDataTable dt = GetTable();

            Row row = dt.GetRow(1);
            StringWriter sw = new StringWriter();
            row.WriteCsv(sw);

            // Test writing a single row back to a stream.
            // Spacing is arbitrary. The write won't emit extra spaces.
            Assert.Equal("Ed,65,prunes\r\n", sw.ToString());
        }


        class Point
        {
            public int x { get; set; }
            public int y { get; set; }
        }

        [Fact]
        public void ArrayToTable()
        {
            Point[] ps = new Point[] { 
                new Point { x= 11, y=12},
                new Point { x= 21, y=22},
                new Point { x=31, y=32}
            };

            // Tests converting an array of structs into a table
            MutableDataTable dt = Utility.ToTable(ps);


            StringWriter sw = new StringWriter();
            dt.SaveToStream(sw);
            Assert.Equal(
@"x,y
11,12
21,22
31,32
", sw.ToString());

        }
    }
}
