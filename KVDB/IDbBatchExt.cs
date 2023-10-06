namespace KVDB
{
    public static class IDbBatchExt
    {
        /// <summary>
        /// Records a value that will be written to the database when the <see cref="Write"/> method is invoked.
        /// </summary>
        /// <param name="table">The table that will be updated.</param>
        /// <param name="key">The table key that identifies the value to be updated.</param>
        /// <param name="value">The value to be written to the table.</param>
        /// <returns>This class for fluent operations.</returns>
        public static IDbBatch Put(this IDbBatch batch, byte table, byte[] key, byte[] value)
        {
            return batch.Put(new[] { table }.Concat(key).ToArray(), value);
        }


        /// <summary>
        /// Records a key that will be deleted from the database when the <see cref="Write"/> method is invoked.
        /// </summary>
        /// <param name="table">The table that will be updated.</param>
        /// <param name="key">The table key that will be removed.</param>
        /// <returns>This class for fluent operations.</returns>
        public static IDbBatch Delete(this IDbBatch batch, byte table, byte[] key)
        {
            return batch.Delete(new[] { table }.Concat(key).ToArray());
        }
    }
}
