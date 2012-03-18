using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Linq;

namespace DataAccess
{
    // Representation of a row when the values are already computed (perhaps from a stream reader)
    internal class RowFromStreamingTable : Row
    {
        readonly string[] _values;
        readonly DataTable _table;

        internal RowFromStreamingTable(string[] values, DataTable table)
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