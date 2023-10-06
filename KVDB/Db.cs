namespace KVDB
{
    public abstract class Db : IDb
    {
        public abstract void Open(string dbPath);

        public abstract byte[]? Get(byte[] key);

        public virtual byte[]? Get(byte table, byte[] key)
        {
            return this.Get(new[] { table }.Concat(key).ToArray());
        }

        public abstract IDbIterator GetIterator();

        public virtual IDbIterator GetIterator(byte table)
        {
            return new DbTableIterator(new byte[] { table }, GetIterator());
        }

        public abstract IDbBatch GetWriteBatch(params byte[] tables);

        public abstract void Clear();

        public abstract void Dispose();
    }
}
