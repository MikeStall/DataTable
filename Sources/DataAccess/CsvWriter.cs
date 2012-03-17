using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Linq;

namespace DataAccess
{
    // Helper to write a CSV file. 
    // Ensures normalized view.
    public class CsvWriter : IDisposable {
        TextWriter _tw;
        readonly string[] _ColumnNames;

        // Write out to the given stream.
        public CsvWriter(TextWriter writer, IEnumerable<string> columnNames)
        {
            this._ColumnNames = columnNames.ToArray();
            this._tw = writer;

            // Write header
            RawWriteLine(columnNames, _tw); 
        }

        // Will overwrite
        public CsvWriter(string outputFilename, IEnumerable<string> columnNames) 
            : this(CreateWriterForFile(outputFilename), columnNames)
        {   
        }

        static TextWriter CreateWriterForFile(string outputFilename)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(outputFilename));
            return new StreamWriter(outputFilename);
        }

        // Write out specific values for the row. 
        // - This still ensure's they're escaped
        // - places commas
        // - validates count matches header.
        public void WriteRow(string[] values)
        {
            Utility.Assert(values.Length == _ColumnNames.Length, "Number of items in row doesn't match header count");
            RawWriteLine(values, _tw);
        }

        

        // Write a row (which may be from an data-set) into this csv file.
        // Only take the columns specified by the ctor. If row doesn't have a column, output blank.
        // This ensures we maintain the right schema.        
        public void WriteRow(Row r) {
            RawWriteLine(r.GetValuesOrEmpty(this._ColumnNames), _tw);
        }

        public void WriteRow(Row r, IDictionary<string, string> extra) {
            RawWriteLine(ValueHelper(r, extra), _tw);
        }
        IEnumerable<string> ValueHelper(Row r, IDictionary<string, string> extra) {
            foreach (var name in _ColumnNames) {
                //if (r.m_parent.HasColumnName(name)) 
                if (true) // $$$ fix
                {
                    yield return r[name];
                    continue;
                }

                string value = null;
                if (extra.TryGetValue(name, out value)) {
                    // It's in the extra dictionary
                    yield return value;
                } else {
                    yield return string.Empty;
                }
            }
        }

        // Write a single line to a CSV
        public static void RawWriteLine(IEnumerable<string> values, TextWriter tw)
        {
            bool first = true;
            foreach (var c in values)
            {
                if (!first)
                {
                    tw.Write(',');
                }
                first = false;
                tw.Write(Escape(c));
            }
            tw.WriteLine();
        }

        // Escape a value for writing to CSVs
        // - Enclose it in quotes if the value has a comma
        private static string Escape(string s)
        {
            if (s == null)
                return string.Empty;

            if (s.IndexOf(',') >= 0)
            {
                return "\"" + s + "\"";
            }
            return s;
        }

        // Don't close the underlying stream, we don't own it. But we can flush it.
        public void Flush() {
            if (_tw != null)
            {
                _tw.Flush();
            }
        }
    
        #region IDisposable Members

        public void  Dispose()
        {
            this.Flush();
        }

        #endregion
    } // end class
} // end namespace