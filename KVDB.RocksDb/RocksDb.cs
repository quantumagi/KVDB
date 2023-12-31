﻿using RocksDbSharp;

namespace KVDB.RocksDb
{
    /// <summary>A minimal RocksDb wrapper that makes it compliant with the <see cref="IDb"/> interface.</summary>
    public class RocksDb : Db
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

        public override IDbIterator GetIterator()
        {
            return new RocksDbIterator(Db().NewIterator());
        }

        public override void Open(string dbPath)
        {
            this.dbPath = dbPath;
            this.db = RocksDbSharp.RocksDb.Open(new DbOptions().SetCreateIfMissing(), dbPath);
        }

        public override void Clear()
        {
            this.db = Db();
            this.db.Dispose();
            Directory.Delete(this.dbPath, true);
            this.db = RocksDbSharp.RocksDb.Open(new DbOptions().SetCreateIfMissing(), this.dbPath);
        }

        public override IDbBatch GetWriteBatch(params byte[] tables) => new RocksDbBatch(Db());

        public override byte[] Get(byte[] key)
        {
            return Db().Get(key);
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

    /// <summary>A minimal RocksDb wrapper that makes it compliant with the <see cref="IDbBatch"/> interface.</summary>
    public class RocksDbBatch : WriteBatch, IDbBatch
    {
        private RocksDbSharp.RocksDb db;

        public RocksDbBatch(RocksDbSharp.RocksDb db)
        {
            this.db = db;
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
        private Iterator iterator;

        public RocksDbIterator(Iterator iterator)
        {
            this.iterator = iterator;
        }

        public void Seek(byte[] key) => this.iterator.Seek(key);

        public void SeekToLast() => this.iterator.SeekToLast();

        public void Next() => this.iterator.Next();

        public void Prev() => this.iterator.Prev();

        public bool IsValid() => this.iterator.Valid();

        public byte[] Key() => this.iterator.Key();

        public byte[] Value() => this.iterator.Value();

        public void Dispose() => this.iterator.Dispose();
    }
}