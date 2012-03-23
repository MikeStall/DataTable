using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Linq;
using System.Collections.ObjectModel;

namespace DataAccess
{
    // Representation of a row when the values are already computed (perhaps from a stream reader)
    internal class RowFromStreamingTable : Row
    {
        readonly IList<string> _values;
        readonly DataTable _table;

        internal RowFromStreamingTable(IList<string> values, DataTable table)
        {
            _values = values;
            _table = table;
        }
        public override IList<string> Values
        {
            get
            {
                return new ReadOnlyCollection<string>(_values);
            }
        }

        public override IEnumerable<string> ColumnNames
        {
            get { return _table.ColumnNames; }
        }
    }
}