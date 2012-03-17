using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;

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

        public TextReader OpenText()
        {
            return new StreamReader(_filename);
        }


    }
}