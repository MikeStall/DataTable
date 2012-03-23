using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace DataAccess
{
    // Cheaper than reflection. Could be removed and we could get the information declaratively.
    public interface IColumnSet
    {
        string[] GetFields();
        string GetValue(int idx);
        string Warning { get; }
    }

    // Tabular data (like from a CSV file)
    // While each row is independent, 
    // All operations are on entire columns, including add, remove, reorder.
    // Table is very mutable.
    public class MutableDataTable : DataTable {

        public Column[] Columns { get; set; }

        // Helper to print a single row. 
        // Useful to see example of each type of data. 
        public void DebugPrintRow() {
            // May disable because it's very verbose
#if VERBOSE
            int row = 0;
            Console.WriteLine("---------------");
            foreach (var c in this.Columns) {
                var oldColor = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.Write("{0}:", c.Name);
                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine("{0}", c.Values[row]);
                Console.ForegroundColor = oldColor;
            }
#endif
        }

        // Remove all columns except for the ones listed. 
        // Allows case insensitive matching.
        // Also reorders to match names.
        public void KeepColumns(params string[] names) {
            int len = names.Length;
            var keep = new Column[len];
            for (int i = 0; i < len; i++) {
                string name = names[i];
                keep[i] = GetColumn(name);
                Utility.Assert(keep[i] != null, "No column:" + name);
            }
            this.Columns = keep;

            // Always keep the warning column.
            // This assumes warning was not in the original name list
            if (m_warning != null) {
                this.AddColumnFirst(m_warning);
            }
        }

        public void RemoveColumn(int index)
        {
            List<Column> cs = new List<Column>(this.Columns);
            cs.RemoveAt(index);
            this.Columns = cs.ToArray();

            // Always keep the warning column.
            // This assumes warning was not in the original name list
            if (m_warning != null)
            {
                this.AddColumnFirst(m_warning);
            }
        }

        //public void RemoveColumns(params string names) {

        //}


        Column m_warning = null;
        const string WarningName = "Warnings";

        // $$$
        // For warnings, have a special column.
        // Technically a normal column, so normal operations work on it (like save)
        public Column GetWarningColumn() {
            if (m_warning == null) {
                m_warning = new Column(WarningName, this.NumRows);
                this.AddColumnFirst(m_warning); // for cosmetic purposes, add to leftmost
            }
            return m_warning;
        }

        // Case-insensitive name lookup of a column.
        public Column GetColumn(string name) {
            foreach (var c in this.Columns) {
                if (string.Compare(c.Name, name, true) == 0) {
                    return c;
                }
            }
            // Column not found
            // $$$ 
            return null;
        }

        // Retrieve multiple columns matching the given names
        public Column[] GetColumns(params string[] names) {
            int len = names.Length;
            var c = new Column[len];
            for (int i = 0; i < len; i++) {
                c[i] = GetColumn(names[i]);
            }
            return c;
        }

        public override IEnumerable<string> ColumnNames {
            get {
                foreach (var c in this.Columns) {
                    yield return c.Name;
                }
            }
        }

        static public IEnumerable<string> GetColumnNames(IEnumerable<Column> columns) {
            foreach (var c in columns) {
                yield return c.Name;
            }
        }

        /// <summary>
        /// Create a new column, and initialize the values for each row using the supplied function.
        /// </summary>
        /// <param name="newColumnName">Name of the new column</param>
        /// <param name="fpComputeNewValue">function to compute the value for this cell</param>
        /// <returns>returns newly created column</returns>
        public Column CreateColumn(string newColumnName, Func<Row, string> fpComputeNewValue)
        {
            int numRows = this.NumRows;
            Column cNew = new Column(newColumnName, numRows);

            int iRow = 0;
            foreach (Row row in this.Rows)
            {
                string newValue = fpComputeNewValue(row);
                cNew.Values[iRow] = newValue;
                iRow++;
            }

            AddColumnFirst(cNew);

            return cNew;
        }

        // Take a single column and split it into multiple.
        public void CreateColumnFromSplit<T>(Func<Row, T> fpSplit)
            where T : IColumnSet, new() {

            T dummy = new T();

            string[] fields = dummy.GetFields();

            // Allocate new columns
            Column[] newColumns = new Column[fields.Length];
            for (int i = 0; i < fields.Length; i++) {
                newColumns[i] = new Column(fields[i], this.NumRows);
                this.AddColumn(newColumns[i]);
            }


            // Apply splitter function 
            int rows = this.NumRows;
            for (int r = 0; r < rows; r++) {
                Row row = new RowInMemory(this, r);

                var result = fpSplit(row);

                if (result.Warning != null) {
                    AppendWarning(r, result.Warning);
                }

                // Place results into new columns
                for (int i = 0; i < fields.Length; i++) {
                    newColumns[i].Values[r] = result.GetValue(i);
                }
            }
        }

        // Append a warning to the row in the warning column.
        // Be sure to append since we can have multiple warnings in a row.
        public void AppendWarning(int row, string message) {
            var values = this.GetWarningColumn().Values;
            if (string.IsNullOrEmpty(values[row])) {
                values[row] = message;
            } else {
                values[row] = values[row] + ";" + message;
            }
        }

        // Merge each column into a new column. Use space as join character.
        public void CreateColumnFromMerging(string newName, params string[] old) {

            var parts = GetColumns(old);
            int rows = this.NumRows;
            var c = new Column(newName, rows);

            // Do the merge operation
            for (int i = 0; i < rows; i++) {
                StringBuilder sb = new StringBuilder();
                bool first = true;
                foreach (var part in parts) {
                    if (!first) {
                        sb.Append(" ");
                    }
                    first = false;
                    sb.Append(part.Values[i]);
                }
                c.Values[i] = sb.ToString();
            }

            AddColumn(c);
        }
        
        // Add a column on the leftmost posiiton
        void AddColumnFirst(Column c) {
            Utility.Assert(!HasColumnName(c.Name), "Already has a column '" + c.Name + "'");
            int len = this.Columns.Length;
            var x = new Column[len + 1];
            x[0] = c;
            Array.Copy(this.Columns, 0, x, 1, len);
            this.Columns = x;
        }

        void AddColumn(Column c) {
            Utility.Assert(!HasColumnName(c.Name), "Already has a column '" + c.Name + "'");
            int len = this.Columns.Length;
            var x = new Column[len + 1];
            x[len] = c;
            Array.Copy(this.Columns, x, len);
            this.Columns = x;
        }

        // rename a column from an old name to the new name
        public void RenameColumn(string oldName, string newName) {
            Utility.Assert(HasColumnName(oldName), "Can't rename column '" + oldName + "' because it doesn't exist.");

            if (Utility.Compare(oldName, newName))
            {
                return;
            }

            Utility.Assert(!HasColumnName(newName), "Can't rename column to '" + newName + "' because there's an existing column with that name.");

            var c = GetColumn(oldName);
            c.Name = newName;
        }

        public int NumRows {
            get {
                return this.Columns[0].Values.Length;
            }
        }

        // Get all columns except the warning column
        static IEnumerable<Column> NoWarning(IEnumerable<Column> columns) {
            foreach (var c in columns) {
                if (c.Name == WarningName)
                    continue;
                yield return c;
            }
        }

        public override IEnumerable<Row> Rows {
            get {
                int rows = this.NumRows;
                for (int r = 0; r < rows; r++) {
                    yield return new RowInMemory(this, r);
                }
            }
        }

        // Get a specific row by row-index. This works for in-memory, but
        // can't be done on streaming. 
        public Row GetRow(int rowIndex)
        {
            return new RowInMemory(this, rowIndex);
        }

        // Only keep rows where the predicate returns true
        public void KeepRows(Func<Row, bool> predicate) {
            // Want to avoid multiple memory allocations

            List<int> index = new List<int>();

            // take a first pass through to evaluate the predicate and track
            // which rows to keep
            int rows = this.NumRows;
            for (int r = 0; r < rows; r++) {
                Row row = new RowInMemory(this, r);

                bool keep = predicate(row);
                if (keep) {
                    index.Add(r);
                }
            }

            int rows2 = index.Count; // new number of rows.

            int numColumns = this.Columns.Length;

            // Now allocate the new columns lengths 
            var columns = new Column[numColumns];
            for (int i = 0; i < numColumns; i++) {
                columns[i] = new Column(this.Columns[i].Name, rows2);
            }

            // Copy the the rows we decided to keep.
            for (int r = 0; r < rows2; r++) {
                int rOld = index[r];

                for (int i = 0; i < numColumns; i++) {
                    columns[i].Values[r] = this.Columns[i].Values[rOld];
                }
            }

            this.Columns = columns;
            Utility.Assert(this.NumRows == rows2);

        }

        void WarnIfNotEmpty(string name) {
            var c = this.GetColumn(name);
            for(int row = 0; row < c.Values.Length; row++){
                string v = c.Values[row];
                if (!string.IsNullOrEmpty(v)) {
                    this.GetWarningColumn();
                    this.AppendWarning(row, string.Format("Column '{0}' had value '{1}'.", name, v));
                }
            }
        }

        // Remove columns with given names
        public void DeleteColumns(params string[] names) {            
            // add a warning if not empty
            foreach (var name in names) {
                WarnIfNotEmpty(name);
            }

            int numColumnsOld = this.Columns.Length;

            int idxNew = 0;            
            int numColumnsNew = numColumnsOld - names.Length;
            var newColumns = new Column[numColumnsNew];

            for(int i = 0; i < numColumnsOld; i++)
            {
                var c = this.Columns[i];
                bool keep = true;
                foreach(var name in names) {
                    if (string.Compare(c.Name, name, true) == 0) {
                        keep = false;
                        break;
                    }
                }

                if (keep) {
                    newColumns[idxNew] = c;
                    idxNew++;
                }
            }

            Utility.Assert(idxNew == numColumnsNew);

            this.Columns = newColumns;
        }

        // Apply a given function to every entry in a column
        // This transforms the column in place.
        public void ApplyToColumn(string name, Func<string, string> func) {
            var c = this.GetColumn(name);
            Utility.Assert(c != null);

            for (int i = 0; i < c.Values.Length; i++) {
                string oldValue = c.Values[i];
                string newValue = func(oldValue);
                c.Values[i] = newValue;
            }
        }

        // $$$ Duplicate function?
        public void CreateNewColumn(Func<Row, string> func, string newColumnName) {
            var c = new Column(newColumnName, this.NumRows);
            this.AddColumn(c);
                        
            for (int r = 0; r < c.Values.Length; r++) {
                Row row = new RowInMemory(this, r);
                var newValue = func(row);
                c.Values[r] = newValue;
            }
        }

        // Represent a row for this data table implementation.
        // The storage here is column-major, so the row provides a view across columns.
        private class RowInMemory : Row, IList<string>
        {
            internal int m_row;
            readonly internal MutableDataTable m_parent;

            public RowInMemory(MutableDataTable parent, int row)
            {
                m_row = row;
                m_parent = parent;

                Debug.Assert(parent != null);
                Debug.Assert(row >= 0 && row < m_parent.NumRows);
            }

            /// <summary>
            /// Mutable implementation. Setting values can change original storage, just like columns.
            /// </summary>
            public override IList<string> Values
            {
                get
                {
                    return this;
                }
            }
            public override IEnumerable<string> ColumnNames
            {
                get
                {
                    return this.m_parent.ColumnNames;
                }
            }



            #region IList<string> Members

            int IList<string>.IndexOf(string item)
            {
                throw new NotImplementedException();
            }

            void IList<string>.Insert(int index, string item)
            {
                throw new NotImplementedException();
            }

            void IList<string>.RemoveAt(int index)
            {
                throw new NotImplementedException();
            }

            string IList<string>.this[int index]
            {
                get
                {
                    Column c = this.m_parent.Columns[index];
                    return c.Values[m_row];
                }
                set
                {
                    Column c = this.m_parent.Columns[index];
                    c.Values[m_row] = value;
                }
            }

            #endregion

            #region ICollection<string> Members

            void ICollection<string>.Add(string item)
            {
                throw new NotImplementedException();
            }

            void ICollection<string>.Clear()
            {
                throw new NotImplementedException();
            }

            bool ICollection<string>.Contains(string item)
            {
                throw new NotImplementedException();
            }

            void ICollection<string>.CopyTo(string[] array, int arrayIndex)
            {
                for (int i = 0; i < this.m_parent.Columns.Length; i++)
                {
                    Column c = this.m_parent.Columns[i];
                    string value = c.Values[m_row];
                    array[i + arrayIndex] = value;
                }
            }

            int ICollection<string>.Count
            {
                get { return this.m_parent.Columns.Length; }
            }

            bool ICollection<string>.IsReadOnly
            {
                get { return false;  }
            }

            bool ICollection<string>.Remove(string item)
            {
                throw new NotImplementedException();
            }

            #endregion

            #region IEnumerable<string> Members

            IEnumerator<string> IEnumerable<string>.GetEnumerator()
            {
                return GetEnumeratorWorker();
            }

            IEnumerator<string> GetEnumeratorWorker()
            {
                for (int i = 0; i < this.m_parent.Columns.Length; i++)
                {
                    Column c = this.m_parent.Columns[i];
                    yield return c.Values[m_row];
                }
            }

            #endregion

            #region IEnumerable Members

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumeratorWorker();
            }

            #endregion
        }
    }
}