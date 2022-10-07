## DataTable

Fast streaming CSV parser. Libraries for easy reading, writing, and manipulation of CSV files. Is able to handle: 

* CSV file format corner cases including escaped values, newlines, and error recovery
* Linq and binding CSV rows directly to .NET objects
* Creating tables from `IEnumerable<T>`
* In-memory mutable tables 
* Streaming through large tables.
* Dictionaries
* 2D-Dictionary support and converting to CSVs. Great for sparse-arrays. 

It's an easier data table than `System.Data.DataTable`.

<hr>

The following nuget packages are available:

* [CsvTools](https://www.nuget.org/packages/CsvTools) - the base library
* [CsvTools.Excel](https://www.nuget.org/packages/CsvTools.Excel) - dependency on CsvTools and the OpenXml SDK 

<hr>

A few quick examples:

> **Download as CsvTools from Nuget to include in your C# project**: 
```cs
using DataAccess; // See methods on DataTable.New for loading a DataTable.
var dt = DataTable.New.ReadLazy(filename); // Fast streaming load a CSV from disk. 
```

> **Get a mutable datatable**:
```cs
MutableDataTable dt = DataTable.New.ReadCsv(filename); // load entire CSV into memory for mutation 
int totalRows = dt.NumRows;

// Includes mutation methods like:
//  CreateColumn, ReorderColumn, KeepColumns, RenameColumn, 
//  GetRow(int rowIndex), KeepRows(Func<T, bool> predicate)

dt.SaveCsv(filename); // write back out
```

> **Linq against the rows**: 
```cs
var y = from row in dt.Rows where row["N"] == "3" select row["NSquared"];
```

> **Linq with strongly-typed parsing, using `RowAs<T>()` method**:
```
class Entry
{
  public int N { get; set; }
  public int NSquared { get; set; }
}
int y = (from row in dt.RowsAs<Entry>() where row.N == 3 select row.NSquared).First();
```

> **Create a table around an `IEnumerable` and then save back as a CSV**:
```cs
var x = from i in Enumerable.Range(1, 5) select new { N = i, NSquared = i * i };
DataTable dt = DataTable.New.FromEnumerable(x);
dt.SaveToStream(Console.Out); // write back out as a CSV
```

> **Also includes support for reading an excel file (.xlsx)**: 
```cs
var dt = DataTable.New.ReadExcel(@"c:\temp\foo.xlsx");
var names = from row in dt.Rows where int.Parse(row["age"]) > 10 select row["Name"];
```
[See here](http://blogs.msdn.com/b/jmstall/archive/2012/04/24/excel-on-azure.aspx) for more about reading excel. 
