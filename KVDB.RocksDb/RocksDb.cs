using RocksDbSharp;

namespace KVDB.RocksDb
{
    /// <summary>A minimal RocksDb wrapper that makes it compliant with the <see cref="IDb"/> interface.</summary>
    public class RocksDb : IDb
    {
        private string dbPath;

        private RocksDbSharp.RocksDb? db;

        public RocksDb()
        {
            this.dbPath = string.Empty;
        }

        private RocksDbSharp.RocksDb Db()
        {
            if (this.db == null)
                throw new InvalidOperationException("Database is not open.");

            return this.db;
        }

        public IDbIterator GetIterator()
        {
            return new RocksDbIterator(Db().NewIterator());
        }

        public IDbIterator GetIterator(byte table)
        {
            return new RocksDbIterator(table, Db().NewIterator());
        }

        public void Open(string dbPath)
        {
            this.dbPath = dbPath;
            this.db = RocksDbSharp.RocksDb.Open(new DbOptions().SetCreateIfMissing(), dbPath);
        }

        public void Clear()
        {
            this.db = Db();
            this.db.Dispose();
            System.IO.Directory.Delete(this.dbPath, true);
            this.db = RocksDbSharp.RocksDb.Open(new DbOptions().SetCreateIfMissing(), this.dbPath);
        }

        public IDbBatch GetWriteBatch(params byte[] tables) => new RocksDbBatch(Db());

        public byte[] Get(byte table, byte[] key)
        {
            return Db().Get(new[] { table }.Concat(key).ToArray());
        }

        public byte[] Get(byte[] key)
        {
            return Db().Get(key);
        }

        public void Dispose()
        {
            if (this.db != null)
            {
                this.db.Dispose();
                this.db = null;
            }
        }
    }

    /// <summary>A minimal RocksDb wrapper that makes it compliant with the <see cref="IDbBatch"/> interface.</summary>
    public class RocksDbBatch : WriteBatch, IDbBatch
    {
        private RocksDbSharp.RocksDb db;

        public RocksDbBatch(RocksDbSharp.RocksDb db)
        {
            this.db = db;
        }

        public IDbBatch Put(byte table, byte[] key, byte[] value)
        {
            return this.Put(new[] { table }.Concat(key).ToArray(), value);
        }

        public IDbBatch Delete(byte table, byte[] key)
        {
            return this.Delete(new[] { table }.Concat(key).ToArray());
        }

        public IDbBatch Put(byte[] key, byte[] value)
        {
            base.Put(key, value);
            return this;
        }

        public IDbBatch Delete(byte[] key)
        {
            base.Delete(key);
            return this;
        }

        public void Write()
        {
            this.db.Write(this);
        }
    }

    /// <summary>A minimal RocksDb wrapper that makes it compliant with the <see cref="IDbIterator"/> interface.</summary>
    public class RocksDbIterator : IDbIterator
    {
        private byte? table;
        private Iterator iterator;

        public RocksDbIterator(Iterator iterator)
        {
            this.iterator = iterator;
        }

        public RocksDbIterator(byte table, Iterator iterator)
        {
            this.table = table;
            this.iterator = iterator;
        }

        public void Seek(byte[] key)
        {
            this.iterator.Seek(this.table.HasValue ? new[] { this.table.Value }.Concat(key).ToArray() : key);
        }

        public void SeekToLast()
        {
            if (!this.table.HasValue)
            {
                this.iterator.SeekToLast();
                return;
            }

            if (this.table != 255)
            {
                // First seek past the last record in the table by attempting to seek to the start of the next table (if any).
                this.iterator.Seek(new[] { (byte)(this.table + 1) });

                // If we managed to seek to the start of the next table then go back one record to arrive at the last record of 'table'.
                if (this.iterator.Valid())
                {
                    this.iterator.Prev();
                    return;
                }
            }

            // If there is no next table then simply seek to the last record in the db as that will be the last record of 'table'.
            this.iterator.SeekToLast();
        }

        public void Next()
        {
            this.iterator.Next();
        }

        public void Prev()
        {
            this.iterator.Prev();
        }

        public bool IsValid()
        {
            return this.iterator.Valid() && (!this.table.HasValue || this.iterator.Key()[0] == this.table);
        }

        public byte[] Key()
        {
            return this.table.HasValue ? this.iterator.Key().Skip(1).ToArray() : this.iterator.Key();
        }

        public byte[] Value()
        {
            return this.iterator.Value();
        }

        public void Dispose()
        {
            this.iterator.Dispose();
        }
    }
}