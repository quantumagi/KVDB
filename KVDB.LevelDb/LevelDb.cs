using LevelDB;

namespace KVDB.LevelDb
{
    /// <summary>A minimal LevelDb wrapper that makes it compliant with the <see cref="IDb"/> interface.</summary>
    public class LevelDb : Db
    {
        private string dbPath;

        private DB? db;

        public LevelDb()
        {
            this.dbPath = string.Empty;
        }

        public override IDbIterator GetIterator()
        {
            return new LevelDbIterator(Db().CreateIterator());
        }

        public override void Open(string dbPath)
        {
            this.dbPath = dbPath;
            this.db = new DB(new Options() { CreateIfMissing = true }, dbPath);
        }

        private DB Db()
        {
            if (this.db == null)
                throw new InvalidOperationException("Database is not open.");
            
            return this.db;
        }

        public override void Clear()
        {
            this.db = Db();
            this.db.Dispose();
            Directory.Delete(this.dbPath, true);
            this.db = new DB(new Options() { CreateIfMissing = true }, this.dbPath);
        }

        public override IDbBatch GetWriteBatch(params byte[] tables) => new LevelDbBatch(Db());

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

    /// <summary>A minimal LevelDb wrapper that makes it compliant with the <see cref="IDbBatch"/> interface.</summary>
    public class LevelDbBatch : WriteBatch, IDbBatch
    {
        private DB db;

        public LevelDbBatch(DB db)
        {
            this.db = db;
        }

        public new IDbBatch Put(byte[] key, byte[] value)
        {
            base.Put(key, value);
            return this;
        }

        public new IDbBatch Delete(byte[] key)
        {
            base.Delete(key);
            return this;
        }

        public void Write()
        {
            this.db.Write(this, new WriteOptions() { Sync = true });
        }
    }

    /// <summary>A minimal LevelDb wrapper that makes it compliant with the <see cref="IDbIterator"/> interface.</summary>
    public class LevelDbIterator : IDbIterator
    {
        private Iterator iterator;

        // Table-less constructor.
        public LevelDbIterator(Iterator iterator)
        {
            this.iterator = iterator;
        }

        public void Seek(byte[] key) => this.iterator.Seek(key);

        public void SeekToLast() => this.iterator.SeekToLast();

        public void Next() => this.iterator.Next();

        public void Prev() => this.iterator.Prev();

        public bool IsValid() => this.iterator.IsValid();

        public byte[] Key() => this.iterator.Key();

        public byte[] Value() => this.iterator.Value();

        public void Dispose()
        {
            this.iterator.Dispose();
        }
    }
}