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
        readonly Stream _input;

        public StreamingDataTable(Stream input)
        {
            // We could optimize to avoid requiring CanSeek if we failed on attemps
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
        
        public FileStreamingDataTable(string filename)
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
        private string[] _names;

        
        public override IEnumerable<string> ColumnNames
        {
            get
            {
                if (_names == null)
                {
                    TextReader sr = null;
                    try
                    {
                        sr = this.OpenText();                    
                        // First get columns.
                        string header = sr.ReadLine();
                        char ch = Reader.GuessSeparateFromHeaderRow(header);
                        _names = Reader.split(header, ch);
                    }
                    finally
                    {
                        if (sr != null)
                        {
                            this.CloseText(sr);
                        }
                    }
                }
                return _names;
            }
        }

        protected abstract TextReader OpenText();

        // called on reader from OpenText
        // Don't call dipose because that can close streams. 
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