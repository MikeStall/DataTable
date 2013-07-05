using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Linq;

namespace DataAccess
{
    /// <summary>
    /// Empty class. Just exists to hang extension methods off. 
    /// </summary>
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
        /// <summary>
        /// Read an entire CSV file into memory.  
        /// </summary>
        /// <param name="builder">ignored</param>
        /// <param name="filename">filename of CSV file to load</param>
        /// <returns>a mutable in-memory DataTable for the given CSV file</returns>
        public static MutableDataTable ReadCsv(this DataTableBuilder builder, string filename)
        {
            Debug.Assert(builder != null);
            if (filename == null)
            {
                throw new ArgumentNullException("filename");
            }
            return Read(builder, filename);
        }

        /// <summary>
        /// Read a file into memory. 
        /// Infer the schema from the header row. Biased to CSV, but may handle tab delimeters too. 
        /// </summary>
        /// <param name="builder">ignored</param>
        /// <param name="filename">filename to load</param>
        /// <returns>a new in-memory table</returns>
        public static MutableDataTable Read(this DataTableBuilder builder, string filename)
        {
            Debug.Assert(builder != null);
            if (filename == null)
            {
                throw new ArgumentNullException("filename");
            }

            return Reader.Read(filename);
        }

        /// <summary>
        /// Read a table from the stream into memory. 
        /// Infer the schema from the header row. Biased to CSV, but may handle tab delimeters too. 
        /// </summary>
        /// <param name="builder">ignored</param>
        /// <param name="stream">input stream to read from</param>
        /// <returns>a new in-memory table</returns>
        public static MutableDataTable Read(this DataTableBuilder builder, TextReader stream)
        {
            return Read(builder, stream, ',');
        }

        public static MutableDataTable Read(this DataTableBuilder builder, TextReader stream, string[] columns)
        {
            return Reader.Read(stream, ',', columns);
        }

        /// <summary>
        /// Read a table from the stream into memory. 
        /// Infer the schema from the header row. Biased to CSV, but may handle tab delimeters too. 
        /// </summary>
        /// <param name="builder">ignored</param>
        /// <param name="stream">input stream to read from</param>
        /// <param name="delimiter">delimiter characeter to use for separatior</param>
        /// <returns>a new in-memory table</returns>
        public static MutableDataTable Read(this DataTableBuilder builder, TextReader stream, char delimiter)
        {
            Debug.Assert(builder != null);
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            return Reader.Read(stream, delimiter);
        }



        /// <summary>
        /// Gets a mutable in-memory copy of the given data table.
        /// </summary>
        /// <param name="builder">ignored</param>
        /// <param name="source">source table that will get copied</param>
        /// <returns>a new table</returns>
        public static MutableDataTable GetMutableCopy(this DataTableBuilder builder, DataTable source)
        {
            Debug.Assert(builder != null);
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }

            return Utility.ToMutable(source);
        }

        /// <summary>
        /// Return an in-memory table that contains the topN rows from the table in the filename.
        /// </summary>
        /// <param name="builder">ignored</param>
        /// <param name="filename">filename of table to load. Schema is inferred from header row.</param>
        /// <returns>a in-memory table containing the topN rows from the supplied file.</returns>
        public static MutableDataTable ReadSampleTopN(this DataTableBuilder builder, string filename)
        {
            return ReadSampleTopN(builder, filename, 100);
        }
        
        /// <summary>
        /// Return an in-memory table that contains the topN rows from the table in the filename.
        /// </summary>
        /// <param name="builder">ignored</param>
        /// <param name="filename">filename of table to load. Schema is inferred from header row.</param>
        /// <param name="topN">reads the topN rows from the table.</param>
        /// <returns>a in-memory table containing the topN rows from the supplied file.</returns>
        public static MutableDataTable ReadSampleTopN(this DataTableBuilder builder, string filename, int topN = 100)
        {
            Debug.Assert(builder != null);
            if (filename == null)
            {
                throw new ArgumentNullException("filename");
            }

            DataTable source = new FileStreamingDataTable(filename);
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
            Debug.Assert(builder != null);

            return new FileStreamingDataTable(filename) { Name = filename };
        }

        /// <summary>
        /// Return a streaming data table over a stream. This just reads a row at a time and avoids reading the whole
        /// table into memory. But it only provides sequential read-only access.
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="inputStream">input stream. Must be seekable and readable</param>
        /// <returns>a streaming data table for the given filename</returns>
        public static DataTable ReadLazy(this DataTableBuilder builder, Stream inputStream)
        {
            Debug.Assert(builder != null);

            return new StreamingDataTable(inputStream);
        }

        
        /// <summary>        
        /// Create an in-memory table with 2 columns (key and value), where each row is a KeyValuePair from the dictionary.         
        /// </summary>
        /// <typeparam name="TKey">TKey of dictionary</typeparam>
        /// <typeparam name="TValue">TValue of dictionary</typeparam>
        /// <param name="builder">ignored</param>
        /// <param name="dict">source of data</param>
        /// <param name="keyName">name for column that holds the dictionary keys</param>
        /// <param name="valName">name for column that holds the dictionary values</param>
        /// <returns>an in-memory table</returns>
        public static MutableDataTable FromDictionary<TKey, TValue>(this DataTableBuilder builder, IDictionary<TKey, TValue> dict, string keyName, string valName)
        {
            Debug.Assert(builder != null);

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
        /// This adds a new column (in position 0) to label TKeyRow. 
        /// </summary>        
        /// <param name="newColumnName">Name of the new column added which corresponds to TKeyRow.</param>
        public static MutableDataTable From2dDictionary<TKeyRow, TKeyColumn, TValue>(this DataTableBuilder builder, Dictionary2d<TKeyRow, TKeyColumn, TValue> dict, string newColumnName = null)
        {
            Debug.Assert(builder != null);

            return Utility.ToTable(dict, newColumnName);
        }

        /// <summary>
        /// Create an in-memory table from the tuple collection. 
        /// Pass in column names since Tuple properties are just named Item1 and Item2.
        /// </summary>        
        public static MutableDataTable FromTuple<T1, T2>(this DataTableBuilder builder, Tuple<T1, T2>[] a, string columnName1, string columnName2)
        {
            Debug.Assert(builder != null);

            return Utility.ToTable(a, columnName1, columnName2);
        }

        /// <summary>
        /// Create an in-memory table where each row is an item in the enumeration.
        /// The columns are from the "flattened" properties of the T (not fields).
        /// The column names are inferred from T's property names.
        /// </summary>        
        public static MutableDataTable FromEnumerable<T>(this DataTableBuilder builder, IEnumerable<T> a)
        {
            Debug.Assert(builder != null);
            string[] columnNames = Utility.InferColumnNames<T>();

            return Utility.ToTableX<T>(a, columnNames);
        }

        /// <summary>
        /// Create a lazy table around the enumeration. 
        /// </summary>        
        public static DataTable FromEnumerableLazy<T>(this DataTableBuilder builder, IEnumerable<T> items)
        {
            Debug.Assert(builder != null);
            return new EnumerableDataTable<T>(items);
        }
    }
}