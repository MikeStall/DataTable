using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Linq;

namespace DataAccess
{
    // Represents a data table without loading it into memory. 
    // This is primarily an IEnumerable<Row> collection.    
    public abstract class DataTable
    {
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

        private readonly static DataTableBuilder _builder = new DataTableBuilder();
        public static DataTableBuilder New
        {
            get { return _builder; }
        }

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

        // $$$ may add overload to just save a project of columns?

        // Write this table out to the stream, in a CSV format.
        // Provide an non-optimized basic version.,
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

        // Write back out to a csv
        // Saves both with + without warning column.
        public void SaveCSV(string output)
        {
            using (StreamWriter sw = new StreamWriter(output))
            {
                SaveToStream(sw);
            }
        }
    }
}