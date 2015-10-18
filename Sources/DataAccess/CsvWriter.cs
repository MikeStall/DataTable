using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Linq;
using System.Diagnostics.CodeAnalysis;

namespace DataAccess
{
    // Make this private. We don't need to expose a CSV writer since users can easily just create a DataTable and save that. 
    // Helper to write a CSV file. 
    // Ensures normalized view.
    internal class CsvWriter : IDisposable {
        TextWriter _tw;
        readonly string[] _ColumnNames;

        bool _ownsStream = false;

        // Write out to the given stream.
        public CsvWriter(TextWriter writer, IEnumerable<string> columnNames)
        {
            this._ColumnNames = columnNames.ToArray();
            this._tw = writer;

            // Write header
            RawWriteLine(columnNames, _tw); 
        }

        // Will overwrite
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "setting _ownsStream flag means caller will dispose")]
        public CsvWriter(string outputFilename, IEnumerable<string> columnNames) 
            : this(CreateWriterForFile(outputFilename), columnNames)
        {
            _ownsStream = true;
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
            RawWriteLine(r.Values, _tw);
            //RawWriteLine(r.GetValuesOrEmpty(this._ColumnNames), _tw); // $$$ This is very slow, but it matches column names.
        }

        public void WriteRow(Row r, IDictionary<string, string> extra) {
            RawWriteLine(ValueHelper(r, extra), _tw);
        }
        IEnumerable<string> ValueHelper(Row r, IDictionary<string, string> extra) {
            foreach (var name in _ColumnNames) {
                //if (Utility.GetColumnIndexFromName(r.ColumnNames, name) // throws
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

            if (s.IndexOfAny(_escapeChars) >= 0)
            {
                return "\"" + s + "\"";
            }
            return s;
        }
        static readonly char[] _escapeChars = new char[] { ',', '\r', '\n' };

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
            if (_ownsStream)
            {
                if (_tw != null)
                {
                    _tw.Close();
                    _tw = null;
                }
            }
        }

        #endregion
    } // end class
} // end namespace
