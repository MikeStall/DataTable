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
    public abstract class DataTableReference
    {
        public abstract IEnumerable<string> ColumnNames { get; }
        public abstract IEnumerable<Row> Rows { get; }

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

    // This is ROW major order, since it reads each row one at a time.
    public class DataTableStream : DataTableReference
    {
        private readonly string _filename;
        private string[] _names;

        public DataTableStream(string filename)
        {
            _filename = filename;
        }
        
        public override IEnumerable<string> ColumnNames
        {
            get
            {
                if (_names == null)
                {
                    using (TextReader sr = this.OpenText())
                    {
                        // First get columns.
                        string header = sr.ReadLine();
                        char ch = Reader.GuessSeparateFromHeaderRow(header);
                        _names = Reader.split(header, ch);
                    }
                }
                return _names;
            }
        }

        private TextReader OpenText()
        {
            return new StreamReader(_filename);
        }

        public override IEnumerable<Row> Rows
        {
            get
            {
                int columnCount = this.ColumnNames.Count();
                TextReader sr = this.OpenText();

                string header = sr.ReadLine(); // skip past header
                char chSeparator = Reader.GuessSeparateFromHeaderRow(header);

                 string line;
                 while ((line = sr.ReadLine()) != null)
                 {
                     string[] parts = Reader.split(line, chSeparator);
                     if (parts.Length != columnCount)
                     {
                         continue; // skip malformed input
                     }

                     yield return new RowFromStreamingTable(parts, this);
                 }

            }
            
        }       


    }

    public class RowFromStreamingTable : Row
    {
        readonly string[] _values;
        readonly DataTableReference _table;

        internal RowFromStreamingTable(string[] values, DataTableReference table)
        {
            _values = values;
            _table = table;
        }
        public override string[] Values
        {
            get
            {
                return _values;
            }
        }

        public override IEnumerable<string> ColumnNames
        {
            get { return _table.ColumnNames; }
        }
    }
}