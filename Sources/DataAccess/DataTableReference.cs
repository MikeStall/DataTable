using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Linq;

namespace DataAccess
{
    // Represents a data table without loading it into memory. 
    // This is ROW major order, since it reads each row one at a time.
    public class DataTableReference
    {
        private readonly string _filename;
        private string[] _names;

        public DataTableReference(string filename)
        {
            _filename = filename;
        }
        
        public IEnumerable<string> ColumnNames
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

        public IEnumerable<RowBase> Rows
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

                     yield return new RowBase(parts, this);
                 }

            }
            
        }       


    }

    public class RowBase
    {
        readonly string[] _values;
        readonly DataTableReference _table;

        internal RowBase(string[] values, DataTableReference table)
        {
            _values = values;
            _table = table;
        }
        public string[] Values
        {
            get
            {
                return _values;
            }
        }
    }
}