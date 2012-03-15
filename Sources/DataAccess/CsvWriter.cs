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
    // $$$ Use this for Save? (not as optimized since each row is a validation)
    public class CsvWriter : IDisposable {
        TextWriter _tw;
        readonly string[] _ColumnNames;

#if false
        public static void WriteDict<TKey, TValue>(string outputFilename, Dictionary<TKey, TValue> dict)
        {
            using (CsvWriter w = new CsvWriter("outputFilename", new string[] { "name", "count" } ))
            {
                foreach (var kv in counts)
                {
                    w.WriteRow(
                }
            }
        }
#endif

        // Will overwrite
        public CsvWriter(string outputFilename, IEnumerable<string> ColumnNames) {
            Directory.CreateDirectory(Path.GetDirectoryName(outputFilename));

            this._ColumnNames = ColumnNames.ToArray();
            this._tw = new StreamWriter(outputFilename);

            // Write header
            DataTable.RawWriteLine(ColumnNames, _tw); 
        }

        // Write out specific values for the row. 
        // - This still ensure's they're escaped
        // - places commas
        // - validates count matches header.
        public void WriteRow(string[] values)
        {
            Utility.Assert(values.Length == _ColumnNames.Length, "Number of items in row doesn't match header count");
            DataTable.RawWriteLine(values, _tw);
        }

        

        // Write a row (which may be from an data-set) into this csv file.
        // Only take the columns specified by the ctor. If row doesn't have a column, output blank.
        // This ensures we maintain the right schema.        
        public void WriteRow(Row r) {
            DataTable.RawWriteLine(r.GetValuesOrEmpty(this._ColumnNames), _tw);
        }

        public void WriteRow(Row r, IDictionary<string, string> extra) {
            DataTable.RawWriteLine(ValueHelper(r, extra), _tw);
        }
        IEnumerable<string> ValueHelper(Row r, IDictionary<string, string> extra) {
            foreach (var name in _ColumnNames) {
                if (r.m_parent.HasColumnName(name)) {
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


        public void Close() {
            _tw.Close();
            this.Dispose();
        }
    
        #region IDisposable Members

        public void  Dispose()
        {
            if (_tw != null) {
                _tw.Dispose();
                _tw = null;
            }
        }

        #endregion
    } // end class
} // end namespace