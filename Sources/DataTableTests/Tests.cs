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

        public static MutableDataTable GetTable()
        {
            TextReader tr = new StringReader(
@"name, age,    favorite fruit
Bob, 20, apples
Ed, 65, prunes
Sarah, 40, cherries");

            MutableDataTable dt = DataTable.New.Read(tr);
            return dt;
        }

        [Fact]
        public void ReadStronglyTypedRows()
        {
            var dt = GetTable();
            var rows = dt.RowsAs<RowType>().ToArray();

            Assert.Equal(3, rows.Length);

            Assert.Equal("Bob", rows[0].Name);
            Assert.Equal(20, rows[0].Age);
            Assert.Equal(RowType.Fruit.apples, rows[0].FavoriteFruit);

            Assert.Equal("Ed", rows[1].Name);
            Assert.Equal(65, rows[1].Age);
            Assert.Equal(RowType.Fruit.prunes, rows[1].FavoriteFruit);

            Assert.Equal("Sarah", rows[2].Name);
            Assert.Equal(40, rows[2].Age);
            Assert.Equal(RowType.Fruit.cherries, rows[2].FavoriteFruit);
        }

        class RowType
        {
            public string Name { get; set; }
            public int Age { get; set; }

            public enum Fruit 
            { 
                apples,
                prunes, 
                cherries,
            }
                        
            // $$$: Need to sort out name problem. column name has a space.
            public Fruit FavoriteFruit { get; set; }
        }

        [Fact]
        public void Columns()
        {
            MutableDataTable dt = GetTable();

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
                rows[1].Values.ToArray());
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
            MutableDataTable dt = DataTable.New.FromEnumerable(ps);


            StringWriter sw = new StringWriter();
            dt.SaveToStream(sw);
            AnalyzeTests.AssertEquals(
@"x,y
11,12
21,22
31,32
", sw.ToString());

        }

        [Fact]
        public void TableFromLinqExpressionWithAnonymousType()
        {
            var x = from i in Enumerable.Range(1, 5) select new { N = i, NSquared = i*i };
            var dt = DataTable.New.FromEnumerable(x);

            AnalyzeTests.AssertEquals(
@"N,NSquared
1,1
2,4
3,9
4,16
5,25
", dt);
        }


        [Fact]
        public void ToMutable()
        {
            var x = from i in Enumerable.Range(1, 5) select new { N = i, NSquared = i * i };
            DataTable dt = DataTable.New.FromEnumerable(x);

            MutableDataTable dt2 = DataTable.New.GetMutableCopy(dt);

            AnalyzeTests.AssertEquals(
@"N,NSquared
1,1
2,4
3,9
4,16
5,25
", dt2);
        }

        [Fact]
        public void QueryWithRowAgainstTable()
        {
            var x = from i in Enumerable.Range(1, 5) select new { N = i, NSquared = i * i };
            var dt = DataTable.New.FromEnumerable(x);

            // Test using the row object to lookup
            var y = from row in dt.Rows where row["N"] == "3" select row["NSquared"];

            Assert.Equal(1, y.Count());
            Assert.Equal("9", y.First());           
        }

        // Enumerable where we should only ask for the first and then stop.
        static IEnumerable<int> TestEnumerable()
        {
            yield return 1;
            
            // Reaching here means we didn't lazily read the enumerable.
            Assert.True(false, "Should not have reached here");
        }

        [Fact]
        public void LazyEnumerable()
        {
            //var dt = DataTable.New.FromEnumerableLazy(TestEnumerable());
            var dt = DataTable.New.FromEnumerableLazy(TestEnumerable());

            // Test using the row object to lookup
            var y = from row in dt.Rows 
                    let i = int.Parse(row["value"]) 
                    select i;

            // Just taking the first element should succeed since we read lazily.
            // If we accidentally read the whole enumeration, we'd fail.
            Assert.Equal(1, y.First());

            // Verify that we can read it a second time. 
            var sample = Analyze.SampleTopN(dt, 1);

            AnalyzeTests.AssertEquals(
@"value
1
", sample);
        }
    }
}
