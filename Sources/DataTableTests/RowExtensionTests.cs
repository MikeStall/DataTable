using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using DataAccess;
using System.IO;

namespace DataTableTests
{
    using System.Globalization;
    using System.Threading;

    class Customer
    {
        public string Name { get; set; }
        public int Age { get; set; }
    }


    // $$$ case sensitive
    public class RowExtensionTests
    {
        [Fact]
        public void Parser()
        {
            //var parser = StrongTypeBinder.BuildMethod<Customer>();
            //var values = new Dictionary<string, string> { { "Name", "Bob" }, { "Age", "15" } };
            //var c = parser(values);

            Row r = new MockRow(new Dictionary<string, string> { 
                {"name", "bob"},
                {"age", "15" }
            });
            var parser = StrongTypeBinder.BuildMethod<Customer>(r.ColumnNames);
            var c = parser(r);
        }

        [Fact]
        public void ConvertNormal()
        {
            DateTime d = new DateTime(1990, 1, 2);
            string g = "E0B9A23E-29C4-42B5-A6EB-1BEB6EE42BEC";

            Row r = new MockRow(new Dictionary<string,string> { 
                {"name", "bob"},
                {"age", "15" },
                {"date", d.ToShortDateString() },
                {"guid", g },
                {"fruit", Fruit.Banana.ToString() }
            });

            RowType result = r.As<RowType>();

            Assert.Equal("bob", result.Name);
            Assert.Equal(15, result.Age);
            Assert.Equal(d, result.Date);
            Assert.Equal(g, result.Guid.ToString().ToUpper());
            Assert.Equal(Fruit.Banana, result.Fruit);
        }

        [Fact]
        public void ConvertCornerCases()
        {                        
            Row r = new MockRow(new Dictionary<string,string> 
            {                 
                {"Field", "abc"},
                {"Private", "abc"},
                {"Readonly", "abc"} // can't be set.
            });

            RowType result = r.As<RowType>();

            Assert.Equal(0, result.Age); // missing 
            Assert.Equal(string.Empty, result.Missing);
            Assert.Null(result.GetPrivate());
            Assert.Null(result.Field);
        }

        [Fact]
        public void ConvertSingleColumn()
        {
            DateTime d = new DateTime(1990, 1, 2);
            Row r = new MockRow(new Dictionary<string, string> { 
                {"value", "32"}
            });
                        
            int result = r.As<int>();

            Assert.Equal(32, result);
        }

        [Fact]
        public void ConvertDouble()
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("en-US");
            Thread.CurrentThread.CurrentUICulture = new CultureInfo("en-US");

            DateTime d = new DateTime(1990, 1, 2);

            Func<string, Row> fpMake = input => new MockRow(new Dictionary<string, string> { 
                {"value", input}
            });


            Assert.Equal(0.32, fpMake(".32").As<double>());
            Assert.Equal(0.32, fpMake("32%").As<double>());
            Assert.Equal(double.NaN, fpMake("32%%").As<double>()); // error
        }


        enum Fruit {
            Apple,
            Pear,
            Banana,
        }
        class RowType
        {
            public string Name { get; set; }
            public int Age { get; set; }
            public DateTime Date { get; set; }
            public Guid Guid { get; set; }
            public Fruit Fruit { get; set; }
            
            public string Missing { get; set; } // not in CSV, defaults to empty

            public string ReadOnly { get { return "readonly"; } } // no setter.

            public string Field; // fields are ignored. 
            private string Private { get; set; } // private properties are ignored

            public string GetPrivate() { return this.Private; }
        }


        // Regression test for single column with a complex type.
        // https://github.com/MikeStall/DataTable/issues/2
        public class RowData
        {
            public string Column1 { get; set; }
            public string Column2 { get; set; }
        }

        [Fact]
        public void TestSingleColumn()
        {
            string data = @"column2
row1
row2
";
            using (var rd = new StringReader(data))
            {
                var dt = DataTable.New.Read(rd);
                var rows = dt.RowsAs<RowData>().ToArray();

                Assert.Equal(2, rows.Length);

                Assert.True(rows[0].Column1 == string.Empty);
                Assert.True(rows[0].Column2 == "row1");

                Assert.True(rows[1].Column1 == string.Empty);
                Assert.True(rows[1].Column2 == "row2");

            }
        }
    }

    class MockRow : Row
    {
        IDictionary<string, string> _values;
        public MockRow(IDictionary<string, string> values)
        {
            _values = values;
        }
        public override IList<string> Values
        {
            get { return _values.Values.ToArray(); }
        }

        public override IEnumerable<string> ColumnNames
        {
            get { return _values.Keys.ToArray(); }
        }
    }
}
