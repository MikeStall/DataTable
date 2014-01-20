﻿using System;
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

        public StreamingDataTable(Stream input, char columnSeparator)
            : base(columnSeparator)
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

        public FileStreamingDataTable(string filename, char columnSeparator)
            : base(columnSeparator)
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
        private readonly char columnSeparator;

        private string[] _names;

        protected TextReaderDataTable(char columnSeparator)
        {
            this.columnSeparator = columnSeparator;
        }

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
                        char ch = this.columnSeparator == default(char) ? Reader.GuessSeparateFromHeaderRow(header) : this.columnSeparator;
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
                    char chSeparator = this.columnSeparator == default(char) ? Reader.GuessSeparateFromHeaderRow(header) : this.columnSeparator;

                    int illegal = 0;
                    string line;
                    while ((line = sr.ReadLine()) != null)
                    {
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