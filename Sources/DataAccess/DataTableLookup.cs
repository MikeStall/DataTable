using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DataAccess
{
    // Provide a fast lookup over a datatable.
    // Index is in-memory
    // This can be useful for tables that are larger than memory
    public class DataTableLookup : IDataTableLookup
    {
        // $$$ Still too large. 271MB working set to read in a 500 MB file.  (3.7 million entries)
        // USe SortedList<> and it drops to 179MB. 
        // Map from a string key to a offset into the file.
        IDictionary<string, long> _mapping;

        Stream _input;
        StreamingDataTable _dt;
        IEnumerator<Row> _rows;

        string[] _headers;

        // $$$ Just used for building the map
        int _columnToIndex = -1;
        private char Delimeter = '\t';

        private DataTableLookup() // Force builder function 
        {
        }

        public static DataTableLookup BuildFromDataTable(string file, string columnNameToIndex)
        {
            var dtl = new DataTableLookup();
            dtl._input = new FileStream(file, FileMode.Open);
            dtl.BuildMap(columnNameToIndex); // This will read the stream
            dtl._input.Position = 0;

            dtl._dt = new StreamingDataTable(dtl._input);
            dtl._rows = dtl._dt.Rows.GetEnumerator();
            dtl._rows.MoveNext(); // Read past headers
            
            return dtl;
        }

        void BuildMap(string columnNameToIndex)
        {
            _headers = ReadHeaders(_input);
            _columnToIndex = GetColumnIndex(columnNameToIndex);

            var d = new Dictionary<string, long>();

            int rowNumber = 0;
            while (true)
            {
                rowNumber++;

                long offset = _input.Position;
                string value = GetValueFromRow(_input);
                if (value == null)
                {
                    break;
                }

                d[value] = offset;
            }

            // SortedList is more memory effecient.
            _mapping = new SortedList<string, long>(d);
        }

        // $$$ Should be shared somewhere. 
        int GetColumnIndex(string columnName)
        {
            for (int i = 0; i < _headers.Length; i++)
            {
                if (string.Compare(_headers[i], columnName, true) == 0)
                {
                    return i;
                }
            }
            throw new InvalidOperationException("No column '" + columnName + "' in the table.");
        }

        static void SkipWhitespace(Stream source)
        {
            while (true)
            {
                long offset = source.Position;
                int i = source.ReadByte();
                if (i != '\r' && i != '\n')
                {
                    source.Position = offset;
                    return;
                }
            }
        }

        string[] ReadHeaders(Stream source)
        {
            string header = ReadOneLine(source);
            Delimeter = Reader.GuessSeparateFromHeaderRow(header);

            string[] parts = Reader.split(header, Delimeter);
            return parts;
        }

        // No extra buffering
        static string ReadOneLine(Stream source)
        {
            StringBuilder sb = new StringBuilder();
            while (true)
            {
                int i = source.ReadByte();
                if (i == -1)
                {
                    return null;
                }
                if (i == '\r' || i == '\n')
                {
                    break;
                }
                sb.Append((char)i);
            }
            SkipWhitespace(source);
            return sb.ToString();
        }        

        StringBuilder _stringBuffer = new StringBuilder();

        // Super fast 
        // Only extract a single string from the row (avoids string overhead)
        // $$$ Still broken, doesn't do quote handling properly.
        string GetValueFromRow(Stream source)
        {
            int column = 0;

            _stringBuffer.Clear();
            while (true)
            {
                int i = source.ReadByte(); // $$$ This is still slow
                if (i == -1)
                {
                    return null;
                }
                if (i == '\r' || i == '\n')
                {
                    break;
                }

                char ch = (char)i;
                if (ch == Delimeter)
                {
                    column++;
                    continue;
                }

                if (column == _columnToIndex)
                {
                    _stringBuffer.Append((char)i);
                }
            }
            SkipWhitespace(source);

            return _stringBuffer.ToString();
        }

        public static DataTableLookup Load(string file)
        {
            throw new NotImplementedException();
        }

        // Save this index to a file. Retrieve via Load()
        public void Save(string filename)
        {
            throw new NotImplementedException();
        }

        public Row LookupRow(string key)
        {
            long offset;
            if (!_mapping.TryGetValue(key, out offset))
            {
                return null;
            }

            _input.Position = offset;
            _rows.MoveNext();

            var val = _rows.Current;

            return val;
        }
    }

    public interface IDataTableLookup
    {
        Row LookupRow(string key);
    }

    // $$$ Is TKey just ToString?
    public interface IDataTableLookup<TKey>
    {
        Row LookupRow(TKey key);
    }

    public interface IDataTableLookup<TKey, TRow>
    {
        TRow LookupRow(TKey key);
    }
}
