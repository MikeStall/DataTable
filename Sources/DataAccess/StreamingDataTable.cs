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
    ///  Stream rows from a file. This is ideal for large read-only files.
    /// </summary>
    internal class StreamingDataTable : DataTable
    {
        private readonly string _filename;
        private string[] _names;

        public StreamingDataTable(string filename)
        {
            _filename = filename;
        }
        
        public override IEnumerable<string> ColumnNames
        {
            get
            {
                if (_names == null)
                {
                    using (TextReader sr = this.OpenText())
                    {
                        // First get columns.
                        string header = sr.ReadLine();
                        char ch = Reader.GuessSeparateFromHeaderRow(header);
                        _names = Reader.split(header, ch);
                    }
                }
                return _names;
            }
        }

        private TextReader OpenText()
        {
            return new StreamReader(_filename);
        }

        public override IEnumerable<Row> Rows
        {
            get
            {
                int columnCount = this.ColumnNames.Count();
                TextReader sr = this.OpenText();

                string header = sr.ReadLine(); // skip past header
                char chSeparator = Reader.GuessSeparateFromHeaderRow(header);

                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    string[] parts = Reader.split(line, chSeparator);
                    if (parts.Length != columnCount)
                    {
                        continue; // skip malformed input
                    }

                    yield return new RowFromStreamingTable(parts, this);
                }

            }
        }
    }
}