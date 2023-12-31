﻿using DBreeze;
using DBreeze.DataTypes;
using DBreeze.Transactions;
using KVDB;

namespace Stratis.Bitcoin.Database
{
    public class BatchContext : IDisposable
    {
        public Transaction transaction { get; private set; }

        private bool canDispose;

        public BatchContext(Transaction transaction, bool canDispose)
        {
            this.transaction = transaction;
            this.canDispose = canDispose;
        }

        public void Dispose()
        {
            if (this.canDispose)
                this.transaction.Dispose();
        }
    }

    /// <summary>A minimal DBreeze wrapper that makes it compliant with the <see cref="IDb"/> interface.</summary>
    public class DBreezeDb : Db
    {
        private Dictionary<int, Transaction> transactions = new Dictionary<int, Transaction>();

        private string dbPath;

        private DBreezeEngine? db;

        private Dictionary<byte, string> tableNames;

        public DBreezeDb(Dictionary<byte, string> tableNames)
        {
            this.tableNames = tableNames;
            this.dbPath = string.Empty;
        }

        private DBreezeEngine Db()
        {
            if (this.db == null)
                throw new InvalidOperationException("Database is not open.");

            return this.db;
        }

        public override IDbIterator GetIterator(byte table)
        {
            return new DBreezeIterator(this, this.tableNames[table]);
        }

        public override IDbIterator GetIterator()
        {
            return new DBreezeIterator(this, "default");
        }

        public string GetTableName(byte table)
        {
            return this.tableNames[table];
        }

        public override void Open(string dbPath)
        {
            this.dbPath = dbPath;
            this.db = new DBreezeEngine(dbPath);
        }

        public override void Clear()
        {
            this.db = Db();
            this.db.Dispose();
            Directory.Delete(this.dbPath, true);
            this.db = new DBreezeEngine(this.dbPath);
        }

        public override IDbBatch GetWriteBatch(params byte[] tables) => new DBreezeBatch(this, tables);

        private (Transaction transaction, bool canDispose) GetTransaction()
        {
            int threadId = Thread.CurrentThread.ManagedThreadId;

            // DBreeze does not allow nested transactions on the same thread.
            // Re-use any existing transaction.
            if (this.transactions.TryGetValue(threadId, out Transaction? currentTransaction))
            {
                var disposedField = currentTransaction.GetType().GetField("disposed", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
                if (!(bool)disposedField!.GetValue(currentTransaction)!)
                    return (currentTransaction, false);
                this.transactions.Remove(threadId);
            }

            var transaction = Db().GetTransaction(eTransactionTablesLockTypes.EXCLUSIVE);

            this.transactions[threadId] = transaction;

            return (transaction, true);
        }

        public BatchContext GetBatchContext(params byte[] tables)
        {
            (Transaction transaction, bool canDispose) = this.GetTransaction();

            if (tables.Length != 0)
                transaction.SynchronizeTables(tables.Select(t => this.tableNames[t]).ToArray());

            return new BatchContext(transaction, canDispose);
        }

        public byte[]? Get(byte table, byte[] key)
        {
            using (BatchContext ctx = this.GetBatchContext())
            {
                return ctx.transaction.Select<byte[], byte[]>(this.tableNames[table], key)?.Value;
            }
        }

        public override byte[]? Get(byte[] key)
        {
            using (BatchContext ctx = this.GetBatchContext())
            {
                return ctx.transaction.Select<byte[], byte[]>("default", key)?.Value;
            }
        }

        public override void Dispose()
        {
            if (this.db != null)
            {
                this.db.Dispose();
                this.db = null;
            }
        }
    }

    /// <summary>A minimal DBreezeDb wrapper that makes it compliant with the <see cref="IDbBatch"/> interface.</summary>
    public class DBreezeBatch : IDbBatch
    {
        private DBreezeDb db;
        private BatchContext context;

        public DBreezeBatch(DBreezeDb db, params byte[] tables) : base()
        {
            this.db = db;
            this.context = db.GetBatchContext(tables);
        }

        // Methods when using tables.
        public IDbBatch Put(byte table, byte[] key, byte[] value)
        {
            this.context.transaction.Insert(this.db.GetTableName(table), key, value);
            return this;
        }

        public IDbBatch Delete(byte table, byte[] key)
        {
            this.context.transaction.RemoveKey(this.db.GetTableName(table), key);
            return this;
        }

        // Table-less operations.

        public IDbBatch Put(byte[] key, byte[] value)
        {
            this.context.transaction.Insert("default", key, value);
            return this;
        }

        public IDbBatch Delete(byte[] key)
        {
            this.context.transaction.RemoveKey("default", key);
            return this;
        }

        public void Write()
        {
            this.context.transaction.Commit();
        }

        public void Dispose()
        {
            this.context.Dispose();
        }
    }

    /// <summary>A minimal DBreezeDb wrapper that makes it compliant with the <see cref="IDbIterator"/> interface.</summary>
    public class DBreezeIterator : IDbIterator
    {
        private BatchContext context;
        private string tableName;
        private Row<byte[], byte[]>? current;

        internal DBreezeIterator(DBreezeDb db, string tableName)
        {
            this.context = db.GetBatchContext();
            this.tableName = tableName;
        }

        public void Seek(byte[] key)
        {
            this.current = this.context.transaction.SelectForwardStartFrom<byte[], byte[]>(this.tableName, key, includeStartFromKey: true, AsReadVisibilityScope: true).FirstOrDefault();
        }

        public void SeekToLast()
        {
            this.current = this.context.transaction.SelectBackward<byte[], byte[]>(this.tableName, AsReadVisibilityScope: true).FirstOrDefault();
        }

        public void Next()
        {
            this.current = this.context.transaction.SelectForwardStartFrom<byte[], byte[]>(this.tableName, this.current!.Key, includeStartFromKey: false, AsReadVisibilityScope: true).FirstOrDefault();
        }

        public void Prev()
        {
            this.current = this.context.transaction.SelectBackwardStartFrom<byte[], byte[]>(this.tableName, this.current!.Key, includeStartFromKey: false, AsReadVisibilityScope: true).FirstOrDefault();
        }

        public bool IsValid() => this.current?.Exists ?? false;

        public byte[] Key() => this.current!.Key;

        public byte[] Value() => this.current!.Value;

        public void Dispose() => this.context.Dispose();
    }
}