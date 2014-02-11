using Xunit;
using DataAccess;
using System.Collections.Generic;
using System;

namespace DataTableTests
{
    // Test flatten function. Used when converting IEnumerable<T> to a table.
    public class FlattenTests
    {
        [Fact]
        public void FlattenInt()
        {
            string[] vals = Utility.Flatten<int>(12);
            Assert.Equal(new string[] { "12" }, vals);
        }

        [Fact]
        public void FlattenAnonymousType()
        {
            string[] vals = Utility.Flatten(new { X = 12, Y = 13 });
            Assert.Equal(new string[] { "12", "13" }, vals);
        }


        enum Fruit { Apple, Banana, Orange };
        
        [Fact]
        public void FlattenEnum()
        {
            // Enums use name, not numerical value
            string[] vals = Utility.Flatten(Fruit.Banana);
            Assert.Equal(new string[] { "Banana" } , vals);
        }

        [Fact]
        public void FlattenKeyValue()
        {
            string[] vals = Utility.Flatten(new KeyValuePair<int, string>(5, "five"));
            Assert.Equal(new string[] { "5", "five" }, vals);
        }


        class ComplexType
        {
            public int Age { get; set; }
            public string Name { get; set; }
            public DateTime Birthday { get; set; }
            public CustomType Custom { get; set; }
        }
        [Fact]
        public void FlattenComplexType()
        {
            DateTime date = new DateTime(1900, 6, 15);
            string[] vals = Utility.Flatten(new ComplexType
            {
                Age = 15,
                Name = "Bob",
                Birthday = date,
                Custom = new CustomType { _value = "abc" }
            });

            // All the properties are are simple types,  
            Assert.Equal(new string[] { "15", "Bob", date.ToString(), "abc" }, vals);            
        }

        [Fact]
        public void FlattenComplexTypeColumnNames()
        {
            var columnNames = Utility.InferColumnNames<ComplexType>();
            Assert.Equal(new string[] { "Age", "Name", "Birthday", "Custom" }, columnNames);
        }
    }

    // If a type has TryParse, then treat it as a simple type.
    // Don't enumerate the types.
    public class CustomType
    {
        public int Length
        {
            get { return _value.Length; }
        }
        public string _value;
        public static bool TryParse(string value, out CustomType x)
        {
            x = new CustomType();
            x._value = value;
            return true;
        }

        public override string ToString()
        {
            return _value;
        }
    }

}