using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Linq;

namespace DataAccess
{
    // Empty class. Just exists to hang extension methods off.
    public class DataTableBuilder
    {
    }

    /// <summary>
    /// Provide extension methods for creating tables.
    /// Use extensions methods (instead of ctors) because they're discoverable and extendable. 
    /// All extensions methods should follow the convention:
    ///  - returning a table.
    ///  - Use "Lazy" if the table is not in-memory.
    ///  
    /// Example usage:
    ///   DataTable dt = DataTable.New.FromCsv()
    /// </summary>
    public static class DataTableBuilderExtensions
    {
        // Read an entire CSV file into memory. 
        public static MutableDataTable ReadCsv(this DataTableBuilder builder, string filename)
        {
            return ReadAll(builder, filename);
        }
        public static MutableDataTable ReadAll(this DataTableBuilder builder, string filename)
        {
            return Reader.Read(filename);
        }

        // Read an entire stream into memory.
        public static MutableDataTable ReadAll(this DataTableBuilder builder, TextReader stream)
        {
            return Reader.Read(stream);
        }

        // Copy a non-mutable into a mutable.
        public static MutableDataTable GetMutableCopy(this DataTableBuilder builder, DataTable source)
        {
            return Utility.ToMutable(source);
        }

                
        /// <summary>
        /// Return an in-memory table that contains the topN rows from the table in the filename.
        /// </summary>        
        public static MutableDataTable ReadSampleTopN(this DataTableBuilder builder, string filename, int topN = 100)
        {
            DataTable source = new StreamingDataTable(filename);
            MutableDataTable dt = Analyze.SampleTopN(source, topN);
            return dt;
        }

        /// <summary>
        /// Return a streaming data table over a file. This just reads a row at a time and avoids reading the whole
        /// table into memory. But it only provides sequential read-only access.
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="filename">filename of CSV to read</param>
        /// <returns>a streaming data table for the given filename</returns>
        public static DataTable ReadLazy(this DataTableBuilder builder, string filename)
        {
            return new StreamingDataTable(filename);
        }

        
        /// <summary>
        /// Create an in-memory table with 2 columns (key and value), where each row is a KeyValuePair from the dictionary. 
        /// </summary>
        public static MutableDataTable FromDictionary<TKey, TValue>(this DataTableBuilder builder, IDictionary<TKey, TValue> dict, string keyName, string valName)
        {
            MutableDataTable d = new MutableDataTable();

            int count = dict.Count;
            Column cKeys = new Column(keyName, count);
            Column cVals = new Column(valName, count);

            d.Columns = new Column[] { cKeys, cVals };

            int i = 0;
            foreach (var kv in dict)
            {
                cKeys.Values[i] = kv.Key.ToString();
                cVals.Values[i] = kv.Value.ToString();
                i++;
            }
            return d;
        }

        /// <summary>
        /// Copy the 2d-dictionary into a in-memory table. This is ideal for creating a sparse table from a dictionary.
        /// Column names are inferred from key values.
        /// </summary>        
        public static MutableDataTable From2dDictionary<TKeyRow, TKeyColumn, TValue>(Dictionary2d<TKeyRow, TKeyColumn, TValue> dict)
        {
            return Utility.ToTable(dict);
        }

        /// <summary>
        /// Create an in-memory table from the tuple collection. 
        /// Pass in column names since Tuple properties are just named Item1 and Item2.
        /// </summary>        
        public static MutableDataTable FromTuple<T1, T2>(Tuple<T1, T2>[] a, string columnName1, string columnName2)
        {
            return Utility.ToTable(a, columnName1, columnName2);
        }

        /// <summary>
        /// Create an in-memory table where each row is an item in the enumeration.
        /// The columns are from the "flattened" properties of the T (not fields).
        /// The column names are inferred from T's property names.
        /// </summary>        
        public static MutableDataTable FromEnumerable<T>(this DataTableBuilder builder, IEnumerable<T> a)
        {
            string[] columnNames = Utility.InferColumnNames<T>();

            return Utility.ToTableX<T>(a, columnNames);
        }

        /// <summary>
        /// Create a lazy table around the enumeration. 
        /// </summary>        
        public static DataTable FromEnumerableLazy<T>(this DataTableBuilder builder, IEnumerable<T> items)
        {
            return new EnumerableDataTable<T>(items);
        }
    }
}