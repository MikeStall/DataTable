using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Linq;

namespace DataAccess
{
    /// <summary>
    /// Create a streaming data table around an Ienumerable 
    /// </summary>
    internal class EnumerableDataTable<T> : DataTable
    {
        private readonly IEnumerable<T> _source;
        private readonly string[] _columnNames;

        public EnumerableDataTable(IEnumerable<T> source)
        {
            _source = source;
            _columnNames = Utility.InferColumnNames<T>();
        }
        public override IEnumerable<string> ColumnNames
        {
            get { return _columnNames; }
        }

        public override IEnumerable<Row> Rows
        {
            get
            {

                foreach (T item in _source)
                {
                    string[] values = Utility.Flatten<T>(item);
                    yield return new RowFromStreamingTable(values, this);
                }
            }
        }

    }
}