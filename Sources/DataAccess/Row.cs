using System.Diagnostics;
using System.Collections.Generic;
using System.IO;

namespace DataAccess
{
    

    // Represent a row.
    // This spans columns in the dataset.     
    public class Row {
        internal int m_row;
        readonly internal DataTable m_parent;

        public Row(DataTable parent, int row) {
            m_row = row;
            m_parent = parent;

            Debug.Assert(parent != null);
            Debug.Assert(row >= 0 && row < m_parent.NumRows);
        }

        // Get the value for the given column name.
        public string this[string name] {
            get {
                Column c = m_parent.GetColumn(name);
                return c.Values[m_row];
            }
        }

        // Return string or empty if missing
        public string GetValueOrEmpty(string columnName) {
            Column c = m_parent.GetColumn(columnName);
            if (c == null) {
                return string.Empty;
            }
            return c.Values[m_row];
        }

        // Plural version of GetValueOrEmpty()
        // Return order is same as input order.
        public IEnumerable<string> GetValuesOrEmpty(IEnumerable<string> columnName) {
            foreach (var c in columnName) {
                string val = this.GetValueOrEmpty(c);
                yield return val;
            }
        }

        public string[] DebugValues {
            get {
                int numColumns = m_parent.Columns.Length;
                var result = new string[numColumns];
                for (int i = 0; i < numColumns; i++) {
                    string name = m_parent.Columns[i].Name;
                    string value = m_parent.Columns[i].Values[m_row];

                    result[i] = string.Format("{0}={1}", name, value);
                }
                return result;
            }
        }

        public IEnumerable<string> Values {
            get {
                int numColumns = m_parent.Columns.Length;
                for (int i = 0; i < numColumns; i++) {
                    string value = m_parent.Columns[i].Values[m_row];
                    yield return value;
                }
            }
        }
        public IEnumerable<string> ColumnNames {
            get {
                return this.m_parent.ColumnNames;
            }
        }

        // Write this single row to a CSV file
        public void WriteCsv(TextWriter tw) {
            /*
            int numColumns = m_parent.Columns.Length;
            bool first = true;
            for (int i = 0; i < numColumns; i++) {
                if (!first) {
                    tw.Write(", ");
                }
                first = false;

                string value = this.m_parent.Columns[i].Values[this.m_row];
                tw.Write(RawData.Escape(value));
            }
            tw.WriteLine();
             */
            DataTable.RawWriteLine(this.Values, tw);
        }
    }

}