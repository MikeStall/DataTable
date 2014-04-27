using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text;

namespace DataAccess
{
    /// <summary>
    /// Type safe wrapper over table lookup.
    /// </summary>
    /// <typeparam name="TRow"></typeparam>
    public class DataTableStreamLookup<TRow>
    {
        private readonly DataTableStreamLookup _lookup;
        private readonly Func<Row, TRow> _parser;

        public DataTableStreamLookup(Stream input)
        {
            _lookup = new DataTableStreamLookup(input);
            _parser = Row.BuildMethod<TRow>(_lookup._table.ColumnNames);
        }

        public void GetOffsetsForRow(Action<TRow, long> callback)
        {
            _lookup.GetOffsetsForRow((row, offset) => callback(_parser(row), offset));
        }

        public TRow ReadAtOffset(long offset)
        {
            Row row = _lookup.ReadAtOffset(offset);
            return _parser(row);
        }
    }

    /// <summary>
    /// Provide fast index-based lookup into a data table.
    /// </summary>
    public class DataTableStreamLookup
    {
        internal readonly StreamingDataTable _table;

        /// <summary>
        /// Create a lookup over the stream. This class does not own the stream, 
        /// although it will change the stream's position. 
        /// </summary>
        /// <param name="input"></param>
        public DataTableStreamLookup(Stream input)
        {
            _table = new StreamingDataTable(input);
        }

        /// <summary>
        /// Provides a mapping of Rows to Offset. Caller can save this map and then use it with ReadAtOffset() to 
        /// retrieve the Row later
        /// </summary>
        /// <param name="callback">callback invoked with (Row, Offset) to save map</param>
        public void GetOffsetsForRow(Action<Row, long> callback)
        {
            // Beware the difference between stream's byte offset and the character offset. 
            var input = _table._input;
            input.Position = 0;

            Reader r = new Reader();
            r.StartRow();

            long startPosition = 0;
            long position = -1;
            int x = 0;
            while (x != -1)
            {
                position++;
                x = input.ReadByte();
                char ch = (char)x; // This is assuming non-unicode characters

                var values = r.ProcessChar(ch, trim: true);
                if (values != null)
                {
                    if (r.rowCount > 1)
                    {
                        // We now have a Row, Offset pair. 
                        var row = new RowFromStreamingTable(values, _table);
                        callback(row, startPosition);
                    }
                    startPosition = position + 1; // next char
                }
            }         
        }

        /// <summary>
        /// Return a Row from the given byte offset into the stream. Offset should be retrieved via GetOffsetForRow. 
        /// </summary>
        /// <param name="offset">offset into stream for where this row starts</param>
        /// <returns>the row at this offset.</returns>
        public Row ReadAtOffset(long offset)
        {
            var input = _table._input;
            input.Position = offset;

            Reader r = new Reader();
            r.StartRow();

            int x = 0;
            while (x != -1)
            {
                x = input.ReadByte();
                char ch = (char)x;

                var values = r.ProcessChar(ch, trim: true);
                if (values != null)
                {
                    // We now have a Row, Offset pair. 
                    return new RowFromStreamingTable(values, _table);
                }
            }

            return null;
        }
    }
}