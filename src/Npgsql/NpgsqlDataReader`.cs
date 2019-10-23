using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

using Npgsql.PostgresTypes;
using Npgsql.Schema;
using NpgsqlTypes;

namespace Npgsql
{
    /// <summary>
    /// Reads a forward-only stream of rows from a data source.
    /// </summary>
    /// <remarks>
    /// Abstract base class to allow <see cref="NpgsqlDereferencingReader"/> to be a genuine <see cref="NpgsqlDataReader"/>.
    /// </remarks>
    public abstract class NpgsqlDataReader : DbDataReader
    {
        internal NpgsqlCommand Command { get; private protected set; } = default!;

        #region Cleanup / Dispose

        /// <summary>
        /// Is raised whenever Close() is called.
        /// </summary>
        public abstract event EventHandler? ReaderClosed;

        internal abstract Task Close(bool connectionClosing, bool async);

        internal abstract Task Cleanup(bool async, bool connectionClosing = false);

        /// <summary>
        /// Closes the <see cref="NpgsqlDataReader"/> reader, allowing a new command to be executed.
        /// </summary>
        public override void Close() => Close(connectionClosing: false, async: false).GetAwaiter().GetResult();

        /// <summary>
        /// Closes the <see cref="NpgsqlStandardDataReader"/> reader, allowing a new command to be executed.
        /// </summary>
#if !NET461 && !NETSTANDARD2_0
        public override Task CloseAsync()
#else
        public Task CloseAsync()
#endif
            => Close(connectionClosing: false, async: true);

        /// <summary>
        /// Releases the resources used by the <see cref="NpgsqlDataReader"/>.
        /// </summary>
        protected override void Dispose(bool disposing) => Close();

#if !NET461 && !NETSTANDARD2_0
        /// <summary>
        /// Releases the resources used by the <see cref="NpgsqlDataReader">NpgsqlStandardDataReader</see>.
        /// </summary>
        public override ValueTask DisposeAsync()
        {
            using (NoSynchronizationContextScope.Enter())
                return new ValueTask(Close(connectionClosing: false, async: true));
        }
#endif

        #endregion

        /// <summary>
        /// Returns details about each statement that this reader will or has executed.
        /// </summary>
        /// <remarks>
        /// Note that some fields (i.e. rows and oid) are only populated as the reader
        /// traverses the result.
        ///
        /// For commands with multiple queries, this exposes the number of rows affected on
        /// a statement-by-statement basis, unlike <see cref="NpgsqlStandardDataReader.RecordsAffected"/>
        /// which exposes an aggregation across all statements.
        /// </remarks>
        public abstract IReadOnlyList<NpgsqlStatement> Statements { get; }

        /// <summary>
        /// Indicates whether the reader is currently positioned on a row, i.e. whether reading a
        /// column is possible.
        /// This property is different from <see cref="DbDataReader.HasRows"/> in that <see cref="DbDataReader.HasRows"/> will
        /// return true even if attempting to read a column will fail, e.g. before <see cref="DbDataReader.Read()"/>
        /// has been called
        /// </summary>
        [PublicAPI]
        public abstract bool IsOnRow { get; }

        #region Provider-specific simple type getters

        /// <summary>
        /// Gets the value of the specified column as an <see cref="NpgsqlDate"/>,
        /// Npgsql's provider-specific type for dates.
        /// </summary>
        /// <remarks>
        /// PostgreSQL's date type represents dates from 4713 BC to 5874897 AD, while .NET's DateTime
        /// only supports years from 1 to 1999. If you require years outside this range use this accessor.
        /// The standard <see cref="DbDataReader.GetProviderSpecificValue"/> method will also return this type, but has
        /// the disadvantage of boxing the value.
        /// See http://www.postgresql.org/docs/current/static/datatype-datetime.html
        /// </remarks>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public abstract NpgsqlDate GetDate(int ordinal);

        /// <summary>
        /// Gets the value of the specified column as a TimeSpan,
        /// </summary>
        /// <remarks>
        /// PostgreSQL's interval type has has a resolution of 1 microsecond and ranges from
        /// -178000000 to 178000000 years, while .NET's TimeSpan has a resolution of 100 nanoseconds
        /// and ranges from roughly -29247 to 29247 years.
        /// See http://www.postgresql.org/docs/current/static/datatype-datetime.html
        /// </remarks>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public abstract TimeSpan GetTimeSpan(int ordinal);

        /// <summary>
        /// Gets the value of the specified column as an <see cref="NpgsqlTimeSpan"/>,
        /// Npgsql's provider-specific type for time spans.
        /// </summary>
        /// <remarks>
        /// PostgreSQL's interval type has has a resolution of 1 microsecond and ranges from
        /// -178000000 to 178000000 years, while .NET's TimeSpan has a resolution of 100 nanoseconds
        /// and ranges from roughly -29247 to 29247 years. If you require values from outside TimeSpan's
        /// range use this accessor.
        /// The standard ADO.NET <see cref="DbDataReader.GetProviderSpecificValue"/> method will also return this
        /// type, but has the disadvantage of boxing the value.
        /// See http://www.postgresql.org/docs/current/static/datatype-datetime.html
        /// </remarks>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public abstract NpgsqlTimeSpan GetInterval(int ordinal);

        /// <summary>
        /// Gets the value of the specified column as an <see cref="NpgsqlDateTime"/>,
        /// Npgsql's provider-specific type for date/time timestamps. Note that this type covers
        /// both PostgreSQL's "timestamp with time zone" and "timestamp without time zone" types,
        /// which differ only in how they are converted upon input/output.
        /// </summary>
        /// <remarks>
        /// PostgreSQL's timestamp type represents dates from 4713 BC to 5874897 AD, while .NET's DateTime
        /// only supports years from 1 to 1999. If you require years outside this range use this accessor.
        /// The standard <see cref="DbDataReader.GetProviderSpecificValue"/> method will also return this type, but has
        /// the disadvantage of boxing the value.
        /// See http://www.postgresql.org/docs/current/static/datatype-datetime.html
        /// </remarks>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public abstract NpgsqlDateTime GetTimeStamp(int ordinal);

        #endregion

        #region Special binary getters

        /// <summary>
        /// Retrieves data as a <see cref="Stream"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>The returned object.</returns>
        public abstract Task<Stream> GetStreamAsync(int ordinal, CancellationToken cancellationToken = default);

        #endregion

        #region Special text getters

        /// <summary>
        /// Retrieves data as a <see cref="TextReader"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>The returned object.</returns>
        public abstract Task<TextReader> GetTextReaderAsync(int ordinal, CancellationToken cancellationToken = default);

        #endregion

        #region Other public accessors

        /// <summary>
        /// Gets a representation of the PostgreSQL data type for the specified field.
        /// The returned representation can be used to access various information about the field.
        /// </summary>
        /// <param name="ordinal">The zero-based column index.</param>
        [PublicAPI]
        public abstract PostgresType GetPostgresType(int ordinal);

        /// <summary>
        /// Gets the OID for the PostgreSQL type for the specified field, as it appears in the pg_type table.
        /// </summary>
        /// <remarks>
        /// This is a PostgreSQL-internal value that should not be relied upon and should only be used for
        /// debugging purposes.
        /// </remarks>
        /// <param name="ordinal">The zero-based column index.</param>
        public abstract uint GetDataTypeOID(int ordinal);

        /// <summary>
        /// Returns schema information for the columns in the current resultset.
        /// </summary>
        /// <returns></returns>
        public abstract ReadOnlyCollection<NpgsqlDbColumn> GetColumnSchema();

        #endregion
    }
}
