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
    /// Represents a table of data. 
    /// This is primary an IEnumerable{Row} collection. 
    /// The table may be just read-only streaming over the rows, which is ideal for large files of millions of rows. 
    /// Or it may have loaded the entire table into memory, which can be ideal for mutation. 
    /// </summary>
    public abstract class DataTable
    {
        /// <summary>
        /// Name of this data table. The semantics of the name are determined by the function that 
        /// creates the table. Name can also be empty.
        /// It could be a filename, an excel sheet name, a URL, or even a human readable description 
        /// of how the table was created. 
        /// Name is primarily a debugging tool. You can't programaticaly rely on the name property unless you created the table.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Name of columns in the table. Columns should be case-insensitive.
        /// If this is a mutable table, columns may be added, removed, or reordered.
        /// </summary>
        public abstract IEnumerable<string> ColumnNames { get; }

        /// <summary>
        /// Enumeration of rows in the table.
        /// Each row has a (possibly empty) value for each column.
        /// </summary>
        public abstract IEnumerable<Row> Rows { get; }

        /// <summary>
        /// Enumeration of rows as strongly types. The default implementation here is 
        /// to just to parse the results of <see cref="Rows"/>.
        /// </summary>
        /// <typeparam name="T">Target object type to parse.</typeparam>
        /// <returns>enumeration of rows as strongly typed object</returns>
        public virtual IEnumerable<T> RowsAs<T>() where T : class, new()
        {
            Func<Row, T> parser;

            // Get cached version of the function. 
            // This is optimized assuming that all reads are for the same schema.
            // This code should be thread safe. 
            {
                parser = _parserFunc as Func<Row, T>;
                if (parser == null)
                {
                    parser = StrongTypeBinder.BuildMethod<T>(this.ColumnNames);
                    _parserFunc = parser;
                }
            }

            // Use the local parser function, not the field, in case the field is switched on us by another thread.
            var result = from row in Rows select parser(row);
            return result;
        }

        // Cache the parser function. 
        private object _parserFunc;

        private readonly static DataTableBuilder _builder = new DataTableBuilder();

        /// <summary>
        /// Provides access to extension methods for creating a table. Tables can be created in many ways, such as reading CSV files,
        /// building around .NET objects, filtering existing tables, etc. 
        /// </summary>
        public static DataTableBuilder New
        {
            get { return _builder; }
        }

        /// <summary>
        /// Return true if the table has the given column name. Comparison is case insensitive.
        /// </summary>
        /// <param name="name">name of column to look for.</param>
        /// <returns>true iff the column is present. False if name is null.</returns>
        public bool HasColumnName(string name)
        {
            if (name == null)
                return false;

            foreach (var c in this.ColumnNames)
            {
                if (string.Compare(name, c, true) == 0)
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Get the index for the column. This can be used for an optimized column lookup when streaming across rows. 
        /// </summary>
        /// <param name="columnName">name of column to look for</param>
        /// <param name="throwOnMissing">If the column is nmissing, either throw an exception or return index of -1</param>
        /// <returns></returns>
        public int GetColumnIndex(string columnName, bool throwOnMissing = true)
        {
            int i = 0;
            foreach (string x in this.ColumnNames)
            {
                if (string.Compare(x, columnName, true) == 0)
                {
                    return i;
                }
                i++;
            }
            if (throwOnMissing)
            {
                throw new InvalidOperationException("Column '" + columnName + "' is not found.");
            }
            return -1;
        }
                                
        /// <summary>
        /// Save the table to the given stream, using a CSV format. The first line will be the headers, and then each subsequent line will be a row.
        /// This will escape characters as needed.
        /// </summary>
        /// <param name="output">textwrite to write out to.</param>                
        public virtual void SaveToStream(TextWriter output)
        {
            using (var writer = new CsvWriter(output, this.ColumnNames))
            {
                foreach (var row in this.Rows)
                {
                    writer.WriteRow(row);
                }
            }
        }               

        /// <summary>
        /// Save the table as a CSV to the given filename
        /// </summary>
        /// <param name="outputFilename">filename on disk to save to.</param>
        public void SaveCSV(string outputFilename)
        {
            using (StreamWriter sw = new StreamWriter(outputFilename))
            {
                SaveToStream(sw);
            }
        }
    }
}