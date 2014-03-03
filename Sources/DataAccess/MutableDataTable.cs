using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Linq;

namespace DataAccess
{
    // Cheaper than reflection. Could be removed and we could get the information declaratively.
    internal interface IColumnSet
    {
        string[] GetFields();
        string GetValue(int idx);
        string Warning { get; }
    }
        
    /// <summary>
    /// Mutable tabular data. Entire table is loaded in memory, and exposes both column and row access.
    /// Table is stored in column major format, so supports efficient column operations like add, remove, and reorder.
    /// Also exposes row enumeration. Table can be mutated through either row or column views.
    /// </summary>
    public class MutableDataTable : DataTable {

        /// <summary>
        /// Return the set of columnns in this mutable table. 
        /// Column represent the direct storage and are mutable.
        /// </summary>
        public Column[] Columns { get; set; }

        /// <summary>
        /// Remove all columns except for the ones listed. 
        /// Allows case insensitive matching.
        /// Also reorders to match names.
        /// </summary>
        /// <param name="throwOnMissing">throw an exception if one of the columns is missing</param>
        /// <param name="names">names of columns to match</param>
        public void KeepColumns(bool throwOnMissing, params string[] names) {
            int len = names.Length;
            var keep = new List<Column>();
            foreach (var name in names)
            {
                var col = GetColumn(name);
                if (col == null)
                {
                    if (throwOnMissing)
                    {
                        throw new InvalidOperationException("No column named:" + name);
                    }
                    continue;
                }
                col.Name = name;
                keep.Add(col);
            }
            this.Columns = keep.ToArray();
        }

        /// <summary>
        /// Remove all columns except for the ones listed. 
        /// Allows case insensitive matching.
        /// Also reorders to match names.
        /// </summary>
        /// <param name="names">names of columns to match</param>
        public void KeepColumns(params string[] names)
        {
            KeepColumns(true, names);
        }

        private void VerifyColumnIndex(int position)
        {
            if ((position < 0) || (position > this.Columns.Length))
            {
                throw new ArgumentOutOfRangeException("position");
            }
        }

        /// <summary>
        /// Case-insensitive name lookup of a column. 
        /// </summary>
        /// <param name="name">name of column to look for</param>
        /// <param name="throwOnMissing">determines behavior when the column name is not found. </param>
        /// <returns>null if column is not found</returns>
        public Column GetColumn(string name, bool throwOnMissing = false)
        {
            foreach (var c in this.Columns) {
                if (string.Compare(c.Name, name, true) == 0) {
                    return c;
                }
            }

            if (throwOnMissing)
            {
                throw new ArgumentException("Column '" + name + "' is not found.");
            }
            // Column not found
            return null;
        }

        /// <summary>
        /// Reorder a column.
        /// </summary>
        /// <param name="columnName">name of column</param>
        /// <param name="position">new position for column</param>
        /// /// <param name="throwOnMissing">True means throw an exception if the column is missing. Else, nop if the column is missing</param>
        public void ReorderColumn(string columnName, int position, bool throwOnMissing = true)
        {
            int idx = this.GetColumnIndex(columnName, throwOnMissing);
            if (idx == -1)
            {
                return;
            }
            VerifyColumnIndex(position);
            var c = this.Columns[idx];

            List<Column> l = new List<Column>(this.Columns);
            l.RemoveAt(idx);
            l.Insert(position, c);

            this.Columns = l.ToArray();
        }

        /// <summary>
        /// Retrieve multiple columns matching the given names 
        /// </summary>
        /// <param name="names">column names to lookup</param>
        /// <returns>columns correpsonding to provided names</returns>
        public Column[] GetColumns(params string[] names) {
            int len = names.Length;
            var c = new Column[len];
            for (int i = 0; i < len; i++) {
                c[i] = GetColumn(names[i]);
            }
            return c;
        }

        /// <summary>
        /// Get the names of the columns, in the order they appear.
        /// </summary>
        public override IEnumerable<string> ColumnNames {
            get {
                foreach (var c in this.Columns) {
                    yield return c.Name;
                }
            }
        }

        /// <summary>
        /// Create a new column at the end of the table, with empty initial values. 
        /// </summary>
        /// <param name="newColumnName">Name of the new column</param>
        /// <returns>returns newly created column</returns>
        public Column CreateColumn(string newColumnName)
        {
            int numRows = this.NumRows;
            Column cNew = new Column(newColumnName, numRows);

            AddColumnLast(cNew);
            return cNew;
        }

        /// <summary>
        /// Create a new column at the end of the table, and initialize the values for each row using the supplied function.
        /// </summary>
        /// <param name="newColumnName">Name of the new column</param>
        /// <param name="fpComputeNewValue">function to compute the value for this cell</param>
        /// <returns>returns newly created column</returns>
        public Column CreateColumn(string newColumnName, Func<Row, string> fpComputeNewValue)
        {
            Column cNew = CreateColumn(newColumnName);

            int iRow = 0;
            foreach (Row row in this.Rows)
            {
                string newValue = fpComputeNewValue(row);
                cNew.Values[iRow] = newValue;
                iRow++;
            }

            return cNew;
        }


        // New column names infererred from properties on target object
        // Maps to object to support anonymous objects. 
        // if fpNewColumns returns null, then that row is skipped and associated columns remain empty. 
        public void CreateColumns<T>(Func<T, object> fpNewColumns)
        {
            if (fpNewColumns == null)
            {
                throw new ArgumentNullException("fpNewColumns");
            }

            var parser = GetParserFunction<T>();

            var row1 = this.Rows.First();
            var dummy = fpNewColumns(parser(row1));


            string[] fields = Utility.InferColumnNames(dummy.GetType());

            // Allocate new columns
            Column[] newColumns = new Column[fields.Length];
            for (int i = 0; i < fields.Length; i++)
            {
                newColumns[i] = new Column(fields[i], this.NumRows);                
            }

            // apply 
            List<string> x = new List<string>(fields.Length);

            int iRow = 0;
            foreach (var row in this.Rows)
            {
                var val = fpNewColumns(parser(row));

                if (val != null)
                {
                    // apply T back to columns. Inverse parser function

                    x.Clear();
                    Utility.FlattenWorker(val, x);
                    if (x.Count != fields.Length)
                    {
                        // Didn't flatten consistently 
                        string msg = string.Format("Row {0} flattened to {1} fields. Expected {2} fields.", iRow, x.Count, fields.Length);
                        throw new InvalidOperationException(msg);
                    }

                    for (int i = 0; i < fields.Length; i++)
                    {
                        newColumns[i].Values[iRow] = x[i];
                    }
                }

                iRow++;
            }

            // Now that we successfully have new data, mutate original table
            // Wait until the end in case there were errors so that table stays in a consistent state.
            foreach(var newColumn in newColumns)
            {
                this.AddColumnLast(newColumn);
            }
        }
             
        /// <summary>
        /// Merge each column into a new column. Use space as join character. 
        /// This adds a new column. The existing columns are not removed.
        /// </summary>
        /// <param name="newName">name of the new column</param>
        /// <param name="columnNamesToMerge">names of columns to merge. </param>
        /// <returns>the newly created column </returns>
        public Column CreateColumnFromMerging(string newName, params string[] columnNamesToMerge) {

            var parts = GetColumns(columnNamesToMerge);
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

            AddColumnLast(c);

            return c;
        }
        
        /// <summary>
        /// rename a column from an old name to the new name 
        /// </summary>
        /// <param name="oldName">existing column in the table</param>
        /// <param name="newName">new name for the column. Must be a unique name</param>
        /// <param name="throwOnMissing">should an exception be thrown if the column is not in the table</param>
        /// <remarks>This can also be used to change casing on a column.</remarks>
        public void RenameColumn(string oldName, string newName, bool throwOnMissing = true) 
        {            
            if (!HasColumnName(oldName))
            {
                if (throwOnMissing)
                {
                    throw new InvalidOperationException("Can't rename column '" + oldName + "' because it doesn't exist.");
                }
                else 
                {
                    return;
                }

            }

            if (HasColumnName(newName))
            {
                // If we're renaming an exisitng column to itself, assume we're just normalizing casing. 
                if (!Utility.Compare(oldName, newName))
                {
                    throw new InvalidOperationException("Can't rename column to '" + newName + "' because there's an existing column with that name.");
                }
            }

            var c = GetColumn(oldName);
            c.Name = newName;
        }

        /// <summary>
        /// Return total number of rows in the table. 
        /// </summary>
        public int NumRows {
            get {
                return this.Columns[0].Values.Length;
            }
        }

        /// <summary>
        /// Enumerate the rows in the table. The rows provide mutable access to the underlying storage
        /// </summary>
        public override IEnumerable<Row> Rows {
            get {
                int rows = this.NumRows;
                for (int r = 0; r < rows; r++) {
                    yield return new RowInMemory(this, r);
                }
            }
        }
                
        /// <summary>
        /// Get a specific row by row-index. 
        /// </summary>
        /// <param name="rowIndex">0-based index of row to lookup</param>
        /// <returns>row at the given index</returns>
        public Row GetRow(int rowIndex)
        {
            return new RowInMemory(this, rowIndex);
        }

        /// <summary>
        /// Only keep rows where the predicate returns true
        /// </summary>
        /// <param name="predicate">predicate to execute on each row</param>
        public void KeepRows<T>(Func<T, bool> predicate)
        {
            if (predicate == null)
            {
                throw new ArgumentNullException("predicate");
            }

            Func<Row, T> parser = GetParserFunction<T>();

            // $$$ What to do on parser failure for a row? Throw, Keep, Remove?
            KeepRows(row => predicate(parser(row)));
        }

        /// <summary>
        /// Only keep rows where the predicate returns true
        /// </summary>
        /// <param name="predicate">predicate to execute on each row</param>
        public void KeepRows(Func<Row, bool> predicate) {
            if (predicate == null)
            {
                throw new ArgumentNullException("predicate");
            }
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

            int expectedNumRows = index.Count; // new number of rows.

            int numColumns = this.Columns.Length;

            // Now allocate the new columns lengths 
            var columns = new Column[numColumns];
            for (int i = 0; i < numColumns; i++) {
                columns[i] = new Column(this.Columns[i].Name, expectedNumRows);
            }

            // Copy the the rows we decided to keep.
            for (int r = 0; r < expectedNumRows; r++) {
                int rOld = index[r];

                for (int i = 0; i < numColumns; i++) {
                    columns[i].Values[r] = this.Columns[i].Values[rOld];
                }
            }

            this.Columns = columns;
            Utility.Assert(this.NumRows == expectedNumRows, "Incorrect number of rows returns by KeepRows function. Expected: " + expectedNumRows);
        }

        /// <summary>
        /// Remove column with the given index
        /// </summary>
        /// <param name="position">0-based index into column collection</param>
        public void DeleteColumn(int position)
        {
            VerifyColumnIndex(position);

            List<Column> cs = new List<Column>(this.Columns);
            cs.RemoveAt(position);
            this.Columns = cs.ToArray();
        }

        /// <summary>
        /// Delete columns, ignores any column names that are missing.
        /// </summary>
        /// <param name="names">names of rows to delete. Ignores if any of the names are missing. </param>
        public void DeleteColumnsAllowMissing(params string[] names)
        {
            // Would have been nice to take an optional thowOnMissing parameter, 
            // but that would conflict with the names params . 
            foreach (string name in names)
            {
                if (this.HasColumnName(name))
                {
                    this.DeleteColumns(name);
                }
            }
        }

        /// <summary>
        /// Remove columns with given names. This is the opposite of <see cref="KeepColumns"/> 
        /// Throws exception if any of the names are missing. 
        /// </summary>
        /// <param name="names">Names of columns to delete. Throws exception if any of the names are missing. </param>
        public void DeleteColumns(params string[] names) {            
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

            Utility.Assert(idxNew == numColumnsNew, "Did find all expected columns to delete");

            this.Columns = newColumns;
        }
                
        /// <summary>
        /// Apply a given function to every entry in a column
        /// This transforms the column in place.
        /// </summary>
        /// <param name="name">column name to apply to</param>
        /// <param name="func">function called once for each column value, replaces each cell in the column</param>
        public void ApplyToColumn(string name, Func<string, string> func) 
        {
            Column c = this.GetColumn(name);
            Utility.Assert(c != null, string.Format("Cannot apply function to column [{0}]: column does not exist", name));

            for (int i = 0; i < c.Values.Length; i++) {
                string oldValue = c.Values[i];
                string newValue = func(oldValue);
                c.Values[i] = newValue;
            }
        }

        // Add a column on the leftmost posiiton
        private void AddColumnFirst(Column c)
        {
            Utility.Assert(!HasColumnName(c.Name), "Already has a column '" + c.Name + "'");
            int len = this.Columns.Length;
            var x = new Column[len + 1];
            x[0] = c;
            Array.Copy(this.Columns, 0, x, 1, len);
            this.Columns = x;
        }

        // Append a column at the end. 
        private void AddColumnLast(Column c)
        {
            Utility.Assert(!HasColumnName(c.Name), "Already has a column '" + c.Name + "'");
            int len = this.Columns.Length;
            var x = new Column[len + 1];
            x[len] = c;
            Array.Copy(this.Columns, x, len);
            this.Columns = x;
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