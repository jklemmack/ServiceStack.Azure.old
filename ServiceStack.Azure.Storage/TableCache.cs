using System.Collections.Generic;
using System.Linq;
using System;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Caching;
using ServiceStack.Logging;
using Microsoft.WindowsAzure.Storage;
using ServiceStack.Support;
using ServiceStack.Text;
using Microsoft.WindowsAzure.Storage.Table;
using ServiceStack.DataAnnotations;

namespace ServiceStack.Azure.Storage
{
    public class AzureTableCacheClient : AdapterBase, ICacheClient, ICacheClientExtended
    {
        static readonly DateTime EXPIRY_DATE = new DateTime(2100, 1, 1);

        TableCacheEntry CreateTableEntry(string rowKey, string data = null,
            DateTime? created = null, DateTime? expires = null)
        {
            var createdDate = created ?? DateTime.UtcNow;
            return new TableCacheEntry(rowKey)
            {
                Data = data,
                ExpiryDate = expires,
                CreatedDate = createdDate,
                ModifiedDate = createdDate,
            };
        }


        protected override ILog Log { get { return LogManager.GetLogger(GetType()); } }
        public bool FlushOnDispose { get; set; }

        string connectionString;
        string partitionKey = "";
        CloudTable table = null;

        public AzureTableCacheClient(string ConnectionString, string tableName = "Cache")
        {
            connectionString = ConnectionString;
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            table = tableClient.GetTableReference(tableName);
            table.CreateIfNotExists();
        }


        private bool TryGetValue(string key, out TableCacheEntry entry)
        {
            entry = null;

            TableOperation op = TableOperation.Retrieve<TableCacheEntry>(partitionKey, key);
            TableResult retrievedResult = table.Execute(op);

            if (retrievedResult.Result != null)
            {
                entry = retrievedResult.Result as TableCacheEntry;
                return true;
            }
            else
                return false;
        }


        public void Dispose()
        {
            if (!FlushOnDispose) return;

            FlushAll();
        }


        public bool Add<T>(string key, T value)
        {
            return Add<T>(key, value, EXPIRY_DATE);
        }

        public bool Add<T>(string key, T value, TimeSpan expiresIn)
        {
            return Add<T>(key, value, DateTime.UtcNow.Add(expiresIn));
        }

        public bool Add<T>(string key, T value, DateTime expiresAt)
        {
            string sVal = JsonSerializer.SerializeToString<T>(value);
            TableCacheEntry entry = CreateTableEntry(key, sVal, null, expiresAt);
            TableOperation op = TableOperation.Insert(entry);
            TableResult result = table.Execute(op);
            return result.HttpStatusCode == 200;    // ???
        }

        public long Decrement(string key, uint amount)
        {
            throw new NotImplementedException();
        }

        public void FlushAll()
        {
            throw new NotImplementedException();
        }

        public T Get<T>(string key)
        {
            TableCacheEntry entry = null;
            if (TryGetValue(key, out entry))
            {
                return JsonSerializer.DeserializeFromString<T>(entry.Data);
            }
            return default(T);
        }

        public IDictionary<string, T> GetAll<T>(IEnumerable<string> keys)
        {
            throw new NotImplementedException();
        }

        public long Increment(string key, uint amount)
        {
            throw new NotImplementedException();
        }

        public bool Remove(string key)
        {
            TableCacheEntry entry = CreateTableEntry(key);
            entry.ETag = "*";
            TableOperation op = TableOperation.Delete(entry);
            TableResult result = table.Execute(op);
            return result.HttpStatusCode == 200;
        }

        public void RemoveAll(IEnumerable<string> keys)
        {
            throw new NotImplementedException();
        }

        public bool Replace<T>(string key, T value)
        {
            return Replace(key, value, EXPIRY_DATE);
        }

        public bool Replace<T>(string key, T value, TimeSpan expiresIn)
        {
            return Replace(key, value, DateTime.UtcNow.Add(expiresIn));
        }

        public bool Replace<T>(string key, T value, DateTime expiresAt)
        {
            string sVal = JsonSerializer.SerializeToString<T>(value);
            TableCacheEntry entry = null;
            if (TryGetValue(key, out entry))
            {
                entry = CreateTableEntry(key, sVal, null, expiresAt);
                TableOperation op = TableOperation.Replace(entry);
                TableResult result = table.Execute(op);
                return result.HttpStatusCode == 200;
            }
            return false;
        }

        public bool Set<T>(string key, T value)
        {
            return Set(key, value, EXPIRY_DATE);
        }

        public bool Set<T>(string key, T value, TimeSpan expiresIn)
        {
            return Set(key, value, DateTime.UtcNow.Add(expiresIn));
        }

        public bool Set<T>(string key, T value, DateTime expiresAt)
        {
            string sVal = JsonSerializer.SerializeToString<T>(value);
            TableCacheEntry entry = CreateTableEntry(key, sVal, null, expiresAt);
            TableOperation op = TableOperation.InsertOrReplace(entry);
            TableResult result = table.Execute(op);
            return result.HttpStatusCode == 200;    // ???
        }

        public void SetAll<T>(IDictionary<string, T> values)
        {
            throw new NotImplementedException();
        }

        public TimeSpan? GetTimeToLive(string key)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> GetKeysByPattern(string pattern)
        {
            // Construct the query operation for all customer entities where PartitionKey="Smith".
            TableQuery<TableCacheEntry> query = new TableQuery<TableCacheEntry>()
                ;

            return table.ExecuteQuery<TableCacheEntry>(query)
                .Select(q => q.RowKey);
        }

        public class TableCacheEntry : TableEntity
        {

            public TableCacheEntry(string key)
            {
                this.PartitionKey = "";
                this.RowKey = key;
            }

            public TableCacheEntry() { }


            [StringLength(1024 * 2014 /* 1 MB max */
                - 1024 /* partition key max size*/
                - 1024 /* row key max size */
                - 64   /* timestamp size */
                - 64 * 3 /* 3 datetime fields */

                // - 8 * 1024 /* ID */
                )]

            public string Data { get; set; }

            public DateTime? ExpiryDate { get; set; }

            public DateTime CreatedDate { get; set; }

            public DateTime ModifiedDate { get; set; }
        }
    }
}
