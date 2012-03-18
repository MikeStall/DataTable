using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Linq;

namespace DataAccess
{
    // Easy for constructing a table in-memory by adding rows.
    // Useful when a table is a filter over another table (such as a where-clause or sample)
    // Optimized to avoid parsing and string operations.
    internal class TableWriter
    {
        private ViewTable _table;

        public TableWriter(DataTable source)
        {
            _table = new ViewTable();
            _table._ColumnNames = source.ColumnNames.ToArray(); // copy in case source is mutable
        }

        public void AddRow(Row row)
        {
            if (_table == null)
            {
                throw new InvalidOperationException("Can't add rows after table is finished construction");
            }
            // $$$ Verify row is in source, mainly so that we know columns match

            // Since Rows have backpointer to table, must clone the row.
            // But we can keep the same data.
            Row newRow = new RowFromStreamingTable(row.Values, _table);

            _table._rows.Add(newRow);
        }

        // Get the table we've built up.
        public DataTable CloseAndGetTable()
        {
            DataTable result = _table;
            _table = null; // mark as closed, prevents future rows.
            return result;
        }

        // Expose filter results as a datatable.
        // This is still a streaming version. Caller can make this mutable if they want.
        private class ViewTable : DataTable
        {
            public readonly List<Row> _rows = new List<Row>();
            public IEnumerable<string> _ColumnNames;

            public override IEnumerable<string> ColumnNames
            {
                get { return _ColumnNames; }
            }
            public override IEnumerable<Row> Rows
            {
                get { return _rows; }
            }
        }

    }
}