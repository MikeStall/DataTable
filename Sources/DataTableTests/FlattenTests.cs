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
        }
        [Fact]
        public void FlattenComplexType()
        {
            DateTime date = new DateTime(1900, 6, 15);
            string[] vals = Utility.Flatten(new ComplexType { Age = 15, Name = "Bob", Birthday = date });

            Assert.Equal(new string[] { "15", "Bob", date.ToString() }, vals);
        }
    }
}