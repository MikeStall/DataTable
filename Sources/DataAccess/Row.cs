using System.Diagnostics;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DataAccess
{
    public abstract class Row
    {
        public abstract string[] Values { get ; }

        public abstract IEnumerable<string> ColumnNames { get; }
        
        // Write this single row to a CSV file
        public void WriteCsv(TextWriter tw)
        {
            DataTable.RawWriteLine(this.Values, tw);
        }

        public string[] DebugValues
        {
            get
            {
                string[] columnnNames = ColumnNames.ToArray();

                int numColumns = columnnNames.Length;
                var result = new string[numColumns];
                for (int i = 0; i < numColumns; i++)
                {
                    string name = columnnNames[i];
                    string value = this.Values[i];

                    result[i] = string.Format("{0}={1}", name, value);
                }
                return result;
            }
        }

        // Get the value for the given column name.
        public virtual string this[string name]
        {
            get
            {
                int idx = GetColumnIndex(name); // $$$ throw on not found?
                return Values[idx];
            }
        }

        // $$$ Should be in base table.
        // -1 on not found
        protected int GetColumnIndex(string columnName)
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
            return -1;
        }

        // Return string or empty if missing
        public string GetValueOrEmpty(string columnName)
        {
            int idx = GetColumnIndex(columnName);
            if (idx == -1)            
            {
                return string.Empty;
            }
            return Values[idx];
        }

        // Plural version of GetValueOrEmpty()
        // Return order is same as input order.
        public IEnumerable<string> GetValuesOrEmpty(IEnumerable<string> columnName)
        {
            foreach (var c in columnName)
            {
                string val = this.GetValueOrEmpty(c);
                yield return val;
            }
        }
    }

    // Represent a row.
    // This spans columns in the dataset.     
    public class RowInMemory : Row {
        internal int m_row;
        readonly internal DataTable m_parent;

        public RowInMemory(DataTable parent, int row)
        {
            m_row = row;
            m_parent = parent;

            Debug.Assert(parent != null);
            Debug.Assert(row >= 0 && row < m_parent.NumRows);
        }

        public override string[] Values {
            get {
                int numColumns = m_parent.Columns.Length;
                string[] vals = new string[numColumns];

                for (int i = 0; i < numColumns; i++) {
                    string value = m_parent.Columns[i].Values[m_row];
                    vals[i] = value;
                }
                return vals;
            }
        }
        public override IEnumerable<string> ColumnNames {
            get {
                return this.m_parent.ColumnNames;
            }
        }
    }

}