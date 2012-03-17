using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace DataAccess
{

    // General reader utilities. 
    public static class Reader
    {

        static string Intern(string value) {
            //return string.Intern(value);
            return value;
        }
        // split the string, and then trim each part
        // items can be quoted. 
        // A, B, C
        // A, "B1, B2", C        
        // - trim whitespace 
        public static string[] split(string input, char separator) {
            return split(input, separator, true);
        }
        public static string[] split(string input, char separator, bool trim) {
            List<string> parts = new List<string>();

            bool fEscapeMode = false;
            StringBuilder sb = new StringBuilder();
            foreach (char ch in input) {
                if (fEscapeMode && (ch == '\"')) {
                    // next iteration will pick up the comma
                    fEscapeMode = false;
                    continue;
                }
                if (!fEscapeMode && (ch == separator)) {
                    fEscapeMode = false;
                    // Terminator
                    string x = sb.ToString();
                    if (trim) 
                        x = x.Trim();
                    parts.Add(Intern(x));

                    sb.Length = 0; // reset

                    continue;
                }
                if (ch == '\"') {
                    fEscapeMode = true;
                    continue;
                }
                sb.Append(ch);
            }
            Utility.Assert(!fEscapeMode, "missing closing quote");

            // add leftovers
            string lastItem = sb.ToString();
            if (trim)
                lastItem = lastItem.Trim();
            parts.Add(Intern(lastItem));

            return parts.ToArray();
        }

        public static MutableDataTable ReadTab(string filename) {
            return Read(filename, '\t');
        }
        public static MutableDataTable ReadCSV(string filename) {
            return Read(filename, ',');
        }

         
        public static MutableDataTable ReadCSV(TextReader stream)
        {
            List<string> lines = new List<string>();
            while (true)
            {
                string line = stream.ReadLine();
                if (line == null)
                {
                    return ReadArray(lines, ',', false);
                }
                lines.Add(line);
            }            
        }

        // For large files, just read a few rows. Useful
        public static MutableDataTable ReadSample(TextReader stream, int size)
        {
            char chSeparator = '\0';
            // Guess separator based on header row.
            List<string> lines = new List<string>();
            while (true)
            {
                string line = stream.ReadLine();
                if (line == null || lines.Count > size)
                {
                    return ReadArray(lines, chSeparator, false);
                }

                if (lines.Count == 0)
                {
                    chSeparator = GuessSeparateFromHeaderRow(line);
                }

                lines.Add(line);
            } 
        }

        public static char GuessSeparateFromHeaderRow(string header)
        {
            if (header.Contains("\t"))
            {
                return '\t';
            }
            
            return ',';            
        }

        // Read in a Ascii file that uses the given separate characters.
        // Like CSV. 
        // Supports quotes to escape commas
        public static MutableDataTable Read(string filename, char separator = '\0', bool fAllowMismatch = false) {
            var lines = File.ReadAllLines(filename);

            if (separator == '\0')
            {
                separator = GuessSeparateFromHeaderRow(lines[0]);
            }

            return ReadArray(lines, separator, fAllowMismatch);
        }

        private static MutableDataTable ReadArray(IList<string> lines, char separator, bool fAllowMismatch = false)
        {
            int numRows = lines.Count - 1;
            // First row is a header

            string[] names = split(lines[0], separator);

            int numColumns = names.Length;

            var columns = new Column[numColumns];
            for (int i = 0; i < numColumns; i++) {
                columns[i] = new Column(names[i], numRows);

            }

            // Parse each row into data set
            for (int i = 1; i < lines.Count; i++) {
                string line = lines[i];
                int row = i - 1;
                string[] parts = split(line, separator);

                if (parts.Length < numColumns) {
                    // Deal with possible extra commas at the end. 
                    // Excel handles this. 
                    for (int c = 0; c < parts.Length; c++) {
                        columns[c].Values[row] = parts[c];
                    }
                    for (int c = parts.Length; c < numColumns; c++) {
                        columns[c].Values[row] = String.Empty;
                    }

                    continue;
                }

                if (!fAllowMismatch) {
                    // If mismatch allowed, then treat this row as garbage rather
                    // than throw an exception
                    Utility.Assert(parts.Length == names.Length);
                }
                for (int c = 0; c < numColumns; c++) {
                    columns[c].Values[row] = parts[c];
                }
            }

            MutableDataTable data = new MutableDataTable();
            data.Columns = columns;


            data.DebugPrintRow();

            return data;
        }
    }
        

      





}