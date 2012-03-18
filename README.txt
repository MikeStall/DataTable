C# project for reading and writing CSVs.

Libaries for easy reading, writing, and manipulation of CSV files. Handles linq, creating tables from IEnumerable<T>, dictionaries, in-memory mutable tables,  streaming through large tables.

It's an easier data table than System.Data.DataTable.

This was extremely handy in a few projects. So I thought I'd put it up on GitHub and polish it up.


A few quick examples:

// Download as CsvTools from Nuget to include in your C# project.
using DataAccess;

var dt = DataTable.New.ReadCsv(filename); // load a CSV from disk

// Linq against the rows
var y = from row in dt.Rows where row["N"] == "3" select row["NSquared"];


// Create a table around an IEnumerable and then save back as a CSV
var x = from i in Enumerable.Range(1, 5) select new { N = i, NSquared = i * i };
DataTable dt = DataTable.New.FromEnumerable(x);
dt.SaveToStream(Console.Out); // write back out as a CSV