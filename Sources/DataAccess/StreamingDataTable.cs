using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Linq;

namespace DataAccess
{
    // Use Stream instead of TextReader because it must be seekable.
    // This assumes exclusive access ot the stream. 
    // If another instance comes in, they'll trash each others position.
    internal class StreamingDataTable : TextReaderDataTable
    {
        internal readonly Stream _input;
                
        public StreamingDataTable(Stream input, string[] columns = null)
            : base(columns)
        {
            // We could optimize to avoid requiring CanSeek if we failed on attempts
            // to read the the rows multiple times. 
            if (!input.CanSeek || !input.CanRead)
            {
                throw new ArgumentException("Input stream must be seekable and readable");
            }

            _input = input;
        }

        protected override TextReader OpenText()
        {
            _input.Position = 0;

            return new StreamReader(_input);
        }
        protected override void CloseText(TextReader reader)
        {
            // Beware, disposing a StreamReader will get dispose the underlying stream.
            // So just nop here since we don't own the stream.
        }      
    }

    internal class FileStreamingDataTable : TextReaderDataTable
    {
        private readonly string _filename;

        public FileStreamingDataTable(string filename, string[] columns = null)
            : base(columns)
        {
            _filename = filename;
        }

        protected override TextReader OpenText()
        {
            return new StreamReader(_filename);            
        }   

        protected override void CloseText(TextReader reader)
        {
            reader.Dispose();
        }
    }

    /// <summary>
    ///  Stream rows from a file. This is ideal for large read-only files.
    /// </summary>
    internal abstract class TextReaderDataTable : DataTable
    {
        private string[] _columnNames;

        // Is the first row the headers or data?
        private bool _firstRowIsHeaders;

        protected TextReaderDataTable(string[] columnNames)
        {
            _columnNames = columnNames;

            // No column names provided, so assume they're in the first row
            _firstRowIsHeaders = columnNames == null;
        }
       
        public override IEnumerable<string> ColumnNames
        {
            get
            {
                if (_columnNames == null)
                {
                    // Read the column names as the first row. 
                    TextReader sr = null;
                    try
                    {
                        sr = this.OpenText();                    
                        // First get columns.
                        string header = sr.ReadLine();
                        char ch = Reader.GuessSeparateFromHeaderRow(header);
                        _columnNames = Reader.split(header, ch);
                    }
                    finally
                    {
                        if (sr != null)
                        {
                            this.CloseText(sr);
                        }
                    }
                }
                return _columnNames;
            }
        }

        protected abstract TextReader OpenText();

        // called on reader from OpenText
        // Don't call dispose because that can close streams. 
        protected abstract void CloseText(TextReader reader);

        public override IEnumerable<Row> Rows
        {
            get
            {
                int columnCount = this.ColumnNames.Count();
                TextReader sr = null;
                                                
                try
                {                    
                    sr = this.OpenText();

                    char[] buffer = new char[10*1000];

                    Reader r = new Reader();
                    r.StartRow();

                    while (true)
                    {
                        int read = sr.ReadBlock(buffer, 0, buffer.Length);
                        if (read == 0)
                        {
                            // At end of file. 
                            var values = r.ProcessEndOfFile(trim: true); // signal end
                            if (values != null)
                            {
                                Row row = new RowFromStreamingTable(values, this);
                                yield return row;
                            }
                            break;
                        }

                        for (int i = 0; i < read; i++)
                        {
                            char ch = buffer[i];

                            var values = r.ProcessChar(ch, trim: true);
                            if (values != null)
                            {
                                if (!_firstRowIsHeaders || r.rowCount > 1)
                                {
                                    Row row = new RowFromStreamingTable(values, this);
                                    yield return row;
                                }
                            }
                        }                      
                    } // loop back
                }
                finally
                {
                    if (sr != null)
                    {
                        this.CloseText(sr);
                    }
                }
            }
        }
    }
}