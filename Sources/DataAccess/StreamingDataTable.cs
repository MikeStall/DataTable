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

        private readonly string[] _columns;

        public StreamingDataTable(Stream input, string[] columns = null)
        {
            // We could optimize to avoid requiring CanSeek if we failed on attemps
            // to read the the rows multiple times. 
            if (!input.CanSeek || !input.CanRead)
            {
                throw new ArgumentException("Input stream must be seekable and readable");
            }

            _input = input;
            this._columns = columns;
        }

        protected override TextReader OpenText()
        {
            _input.Position = 0;
            
            return new IgnoreFirstReadLineStreamReader(_input, _columns != null);
        }
        protected override void CloseText(TextReader reader)
        {
            // Beware, disposing a StreamReader will get dispose the underlying stream.
            // So just nop here since we don't own the stream.
        }
        public override IEnumerable<string> ColumnNames
        {
            get
            {
                return this._columns ?? base.ColumnNames;
            }
        }
    }

    internal class IgnoreFirstReadLineStreamReader : StreamReader
    {
        private readonly bool ignore;

        private bool ignored;

        private string firstLine;

        public override string ReadLine()
        {
            if (ignore && !ignored)
            {
                ignored = true;
                return firstLine = base.ReadLine();
            }

            if (firstLine != null)
            {
                var tmp = firstLine;
                firstLine = null;
                return tmp;
            }
            return base.ReadLine();
        }

        public IgnoreFirstReadLineStreamReader(Stream stream, bool ignore = false)
            :base(stream)
        {
            this.ignore = ignore;
        }
        public IgnoreFirstReadLineStreamReader(string filename, bool ignore = false)
            : base(filename)
        {
            this.ignore = ignore;
        }
    }


    internal class FileStreamingDataTable : TextReaderDataTable
    {
        private readonly string _filename;

        private string[] _columns;

        public FileStreamingDataTable(string filename, string[] columns = null)
        {
            _filename = filename;
            this._columns = columns;
        }

        protected override TextReader OpenText()
        {
            if (_columns == null)
            {
                return new StreamReader(_filename);
            }
            return new IgnoreFirstReadLineStreamReader(_filename, true);
        }
        public override IEnumerable<string> ColumnNames
        {
            get
            {
                return _columns ?? base.ColumnNames;
            }
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

                    StreamReader underlyingStream = sr as StreamReader;

                    string header = sr.ReadLine(); // skip past header
                    char chSeparator = Reader.GuessSeparateFromHeaderRow(header);

                    int illegal = 0;
                    long offsetOld = -1;

                    string line;
                    while ((line = sr.ReadLine()) != null)
                    {
                        if (underlyingStream != null)
                        {
                            offsetOld = underlyingStream.BaseStream.Position;
                        }

                        RowFromStreamingTable row = null;
                        try
                        {

                            string[] parts = Reader.split(line, chSeparator);
                            
                            // $$$ Major hack for dealing with newlines in quotes strings.
                            // The better fix here would be to switch to a streaming interface.
                            if (parts.Length != columnCount)
                            {
                                string line2 = sr.ReadLine();
                                line += Environment.NewLine + line2;

                                parts = Reader.split(line, chSeparator);
                            }

                            row = new RowFromStreamingTable(parts, this);
                        }
                        catch (AssertException)
                        {
                            // Something really corrupt about this row. Ignore it. 
                            illegal++;
                        }
                        if (row != null)
                        {
                            yield return row;
                        }

                        if (underlyingStream != null)
                        {
                            if (underlyingStream.BaseStream.Position != offsetOld)
                            {
                                underlyingStream.DiscardBufferedData();
                            }
                        }
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