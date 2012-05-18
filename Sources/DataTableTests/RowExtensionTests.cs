using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using DataAccess;

namespace DataTableTests
{
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
            var parser = StrongTypeBinder.BuildMethod<Customer>();
            var values = new Dictionary<string, string> { { "Name", "Bob" }, { "Age", "15" } };
            var c = parser(values);
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
