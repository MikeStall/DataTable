using System.Diagnostics;

namespace DataAccess {

    /// <summary>
    /// Column from an in-memory data table. Columns know their length and directly expose their values as a mutable array.
    /// </summary>
    public class Column {
        
        /// <summary>
        /// Name of the column. Operations on column names are case-insensitive.
        /// </summary>
        public string Name { get; set; } 

        /// <summary>
        /// Values in this column. 
        /// </summary>
        public string[] Values { get; set; } 
        
        /// <summary>
        /// Create a new column. Caller must still add this into a table.
        /// </summary>
        /// <param name="name">name of the column</param>
        /// <param name="numRows">number of rows in the column. This will set the length of the Values array</param>
        public Column(string name, int numRows) {
            this.Name = name;
            this.Values = new string[numRows];
        }

        /// <summary>
        /// Provide summary string representation for this column
        /// </summary>
        /// <returns></returns>
        public override string ToString() {
            int count = 0;
            if (this.Values != null) {
                count = this.Values.Length;
            }
            return string.Format("{0} ({1} entries)", this.Name, count);
        }

        /// <summary>
        /// Check if this column has any data in it. 
        /// </summary>
        /// <returns>True iff all of the values are empty</returns>
        public bool CheckIsEmpty() {
            foreach (var v in this.Values) {
                if (!string.IsNullOrEmpty(v)) {
                    return false;
                }
            }
            return true;
        }
    }



}