namespace KVDB
{
    /// <summary>A minimal LevelDb wrapper that makes it compliant with the <see cref="IDbIterator"/> interface.</summary>
    public class DbTableIterator : IDbIterator
    {
        private byte[] table;
        private IDbIterator iterator;

        public DbTableIterator(byte[] table, IDbIterator iterator)
        {
            this.table = table;
            this.iterator = iterator;
        }

        public void Seek(byte[] key)
        {
            this.iterator.Seek(this.table.Length > 0 ? (this.table.Concat(key).ToArray()) : key);
        }

        private bool IsLastTable(byte[] table)
        {
            return !table.Any(b => b != 255);
        }

        private byte[] NextTable(byte[] table)
        {
            byte[] nextTable = (byte[])table.Clone();
            for (int i = 0; i < nextTable.Length; i++)
            {
                if (nextTable[i] != 255)
                {
                    nextTable[i]++;
                    break;
                }

                nextTable[i] = 0;
                continue;
            }

            return nextTable;
        }

        public void SeekToLast()
        {
            if (this.table.Length != 0 && !IsLastTable(this.table))
            {
                // First seek past the last record in the table by attempting to seek to the start of the next table (if any).
                this.iterator.Seek(NextTable(this.table));

                // If we managed to seek to the start of the next table then go back one record to arrive at the last record of 'table'.
                if (this.iterator.IsValid())
                {
                    this.iterator.Prev();
                    return;
                }
            }

            this.iterator.SeekToLast();
            return;
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
            if (this.table.Length != 0)
            {
                byte[] key = this.iterator.Key();

                for (int i = 0; i < this.table.Length; i++)
                {
                    if (this.table[i] != key[i])
                        return false;
                }
            }

            return this.iterator.IsValid();
        }

        public byte[] Key()
        {
            return this.table.Length != 0 ? this.iterator.Key().Skip(this.table.Length).ToArray() : this.iterator.Key();
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
