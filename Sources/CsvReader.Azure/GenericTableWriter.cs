using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Services.Client;
using System.Xml.Linq;
using System.Xml;
using System.Data.Services.Common;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;
using System.Text.RegularExpressions;

namespace DataAccess
{
    // Bridge between a DataTable Row and a AzureTable TableServiceEntity
    [DataServiceKey("PartitionKey", "RowKey")]
    internal class GenericWriterEntity : TableServiceEntity
    {
        // Use a custom write hook to convert from a Row to EDM properties.
        public Row _source;        
    }

    internal class GenericTableWriter
    {
        private string[] _edmTypeNames;
        private string[] _columnNames;

        private static bool Compare(string a, string b)
        {
            return string.Compare(a, b, StringComparison.OrdinalIgnoreCase) == 0;
        }

        private static bool IsSpecialColumnName(string columnName)
        { 
            // Case-insensitive compare
            return Compare(columnName, "PartitionKey") || Compare(columnName, "RowKey") || Compare(columnName, "Timestampe");
        }

        // $$$ Should be some common helper. This is protected on Row; but should be on table. 
        private static int GetColumnIndex(string columnName, string[] columnNames)
        {
            for (int i = 0; i < columnNames.Length; i++)
            {
                if (Compare(columnNames[i], columnName))
                {
                    return i;
                }
            }
            return -1;
        }

        // Azure table names are very restrictive, so sanity check upfront to give a useful error.
        // http://msdn.microsoft.com/en-us/library/windowsazure/dd179338.aspx
        private static void ValidateAzureTableName(string tableName)
        {
            if (!Regex.IsMatch(tableName, "^[A-Za-z][A-Za-z0-9]{2,62}$"))
            {
                throw new InvalidOperationException(string.Format("{0} is not a valid name for an azure table", tableName));
            }
        }

        // Write a DataTable to an AzureTable.
        // DataTable's Rows are an unstructured property bag.
        // columnTypes - type of the column, or null if column should be skipped. Length of columnTypes should be the same as number of columns.
        public static void SaveToAzureTable(DataTable table, CloudStorageAccount account, string tableName, Type[] columnTypes, Func<int, Row, ParitionRowKey> funcComputeKeys)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }
            if (account == null)
            {
                throw new ArgumentNullException("account");
            }
            if (columnTypes == null)
            {
                throw new ArgumentNullException("columnTypes");
            }
            if (tableName == null)
            {
                throw new ArgumentNullException("tableName");
            }
            ValidateAzureTableName(tableName);

            // Azure tables have "special" columns. 
            // We can skip these by settings columnType[i] to null, which means don't write that column
            string[] columnNames = table.ColumnNames.ToArray();
            if (columnNames.Length != columnTypes.Length)
            {
                throw new ArgumentException(string.Format("columnTypes should have {0} elements", columnNames.Length), "columnTypes");
            }
            for (int i = 0; i < columnNames.Length; i++)
            {
                if (IsSpecialColumnName(columnNames[i]))
                {
                    columnTypes[i] = null;
                }
            }

            if (funcComputeKeys == null)
            {
                // If incoming table has columns named "PartitionKey" and "RowKey", then use those. 
                int iPartitionKey = GetColumnIndex("PartitionKey", columnNames);
                int iRowKey = GetColumnIndex("RowKey", columnNames);
                if (iPartitionKey >= 0 && iRowKey  >= 0)
                {
                    funcComputeKeys = (rowIndex, row) => 
                        new ParitionRowKey 
                        { 
                            PartitionKey = row.Values[iPartitionKey],
                            RowKey = row.Values[iRowKey]
                        };
                }
                else
                {                    
                    funcComputeKeys = DefaultPartionRow;
                }
            }

            // Validate columnTypes 
            string [] edmTypeNames = Array.ConvertAll(columnTypes, 
                 columnType => {
                     if (columnType == null)
                     {
                         return null;
                     }
                     string edmTypeName;
                     _edmNameMapping.TryGetValue(columnType, out edmTypeName);
                     if (edmTypeName == null)
                     {
                         // Unsupported type!
                         throw new InvalidOperationException(string.Format("Type '{0}' is not a supported type on azure tables", columnType.FullName));
                     }
                     return edmTypeName;
                 });


            CloudTableClient tableClient = account.CreateCloudTableClient();

            tableClient.DeleteTableIfExist(tableName);
            tableClient.CreateTableIfNotExist(tableName);
            
            
            GenericTableWriter w = new GenericTableWriter 
            {
                _edmTypeNames = edmTypeNames,
                _columnNames = table.ColumnNames.ToArray()
            };
            
            TableServiceContext ctx = null;

            int rowCounter = 0;
            foreach (Row row in table.Rows)
            {
                GenericWriterEntity entity = new GenericWriterEntity { _source = row };
                // Compute row and partition keys too. 
                var partRow = funcComputeKeys(rowCounter, row);
                entity.PartitionKey = partRow.PartitionKey;
                entity.RowKey = partRow.RowKey;

                if (ctx == null)
                {
                    ctx = tableClient.GetDataServiceContext();
                    ctx.WritingEntity += new EventHandler<ReadingWritingEntityEventArgs>(w.ctx_WritingEntity);
                }

                ctx.AddObject(tableName, entity);
                rowCounter++;

                if (rowCounter % 50 == 0)
                {
                    ctx.SaveChangesWithRetries(SaveChangesOptions.Batch | SaveChangesOptions.ReplaceOnUpdate);
                    ctx = null;
                }
            }

            if (ctx != null)
            {
                ctx.SaveChangesWithRetries(SaveChangesOptions.Batch | SaveChangesOptions.ReplaceOnUpdate);
            }
        }

        // default row,partition key creator. Everything in a single partition, and rows increment. 
        static ParitionRowKey DefaultPartionRow(int rowIndex, Row row)
        {
            // format rowkey so that when sorted alpanumerically, it's still ascending
            return new ParitionRowKey { PartitionKey = "1", RowKey = rowIndex.ToString("D8") };
        }

        private void ctx_WritingEntity(object sender, ReadingWritingEntityEventArgs args)
        {
            GenericWriterEntity entity = args.Entity as GenericWriterEntity;
            if (entity == null)
            {
                return;
            }

            XElement properties = args.Data.Descendants(GenericTableReader.MetadataNamespace + "properties").First();

            for(int iColumnn = 0; iColumnn < _edmTypeNames.Length; iColumnn++)
            {
                string edmTypeName = _edmTypeNames[iColumnn];
                if (edmTypeName == null)
                {
                    continue;
                }

                string value = entity._source.Values[iColumnn];                
                string columnName = _columnNames[iColumnn];

                // framework will handle row + partition keys. 
                XElement e = new XElement(GenericTableReader.DataNamespace + columnName, value);
                e.Add(new XAttribute(GenericTableReader.MetadataNamespace + "type", edmTypeName));

                properties.Add(e);
            }            
        }
     
        // Mapping of .NET types to EDM types.
        static Dictionary<Type, string> _edmNameMapping = new Dictionary<Type, string> { 
            { typeof(string), "Edm.String" },
            { typeof(byte), "Edm.Byte" },
            { typeof(sbyte), "Edm.SByte" },
            { typeof(short), "Edm.Int16" },
            { typeof(int), "Edm.Int32" },
            { typeof(long), "Edm.Int64" },
            { typeof(double), "Edm.Double" }, 
            { typeof(float), "Edm.Single" },
            { typeof(bool), "Edm.Boolean" },
            { typeof(decimal), "Edm.Decimal" },
            { typeof(DateTime), "Edm.DateTime" },
            { typeof(Guid), "Edm.Guid" }
        };

    }
}