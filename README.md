##C# project for reading and writing CSVs 

Libraries for easy reading, writing, and manipulation of CSV files. Is able to handle: 

* Linq
* Creating tables from `IEnumerable<T>`
* Dictionaries
* In-memory mutable tables 
* Streaming through large tables.

It's an easier data table than `System.Data.DataTable`.

The following nuget packages are available:

* [CsvTools](https://www.nuget.org/packages/CsvTools) - the base library
* [CsvTools.Excel](https://www.nuget.org/packages/CsvTools.Excel) - dependency on CsvTools and the OpenXml SDK 

A few quick examples:

> **Download as CsvTools from Nuget to include in your C# project**: 

    using DataAccess;
    var dt = DataTable.New.ReadCsv(filename); // load a CSV from disk

> **Linq against the rows**: 

    var y = from row in dt.Rows where row["N"] == "3" select row["NSquared"];

> **Linq with strongly-typed parsing, using `RowAs<T>()` method**:

    class Entry
    {
      public int N { get; set; }
      public int NSquared { get; set; }
    }
    int y = (from row in dt.RowsAs<Entry>() where row.N == 3 select row.NSquared).First();

> **Create a table around an `IEnumerable` and then save back as a CSV**:

    var x = from i in Enumerable.Range(1, 5) select new { N = i, NSquared = i * i };
    DataTable dt = DataTable.New.FromEnumerable(x);
    dt.SaveToStream(Console.Out); // write back out as a CSV

> **Also includes support for reading an excel file (.xlsx)**: 

    var dt = DataTable.New.ReadExcel(@"c:\temp\foo.xlsx");
    var names = from row in dt.Rows where int.Parse(row["age"]) > 10 select row["Name"];
    
[See here](http://blogs.msdn.com/b/jmstall/archive/2012/04/24/excel-on-azure.aspx) for more about reading excel. 
