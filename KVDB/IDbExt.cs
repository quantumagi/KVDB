namespace KVDB
{
    public static class IDbExt
    {
        /*
        /// <summary>
        /// Gets the value associated with a table and key.
        /// </summary>
        /// <param name="table">The table identifier.</param>
        /// <param name="key">The key of the value to retrieve.</param>
        /// <returns>The value for the specified table and key.</returns>
        public static byte[]? Get(this IDb db, byte table, byte[] key)
        {
            return db.Get(new[] { table }.Concat(key).ToArray());
        }
        */

        /// <summary>
        /// Gets a <see cref="ReadWriteBatch"/>.
        /// </summary>
        /// <param name="db">The database to get the batch for.</param>
        /// <returns>The <see cref="ReadWriteBatch"/>.</returns>
        public static ReadWriteBatch GetReadWriteBatch(this IDb db, params byte[] tables)
        {
            return new ReadWriteBatch(db, tables);
        }
    }
}
