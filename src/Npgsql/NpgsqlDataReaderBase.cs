using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.Logging;
using Npgsql.PostgresTypes;
using Npgsql.Schema;
using Npgsql.TypeHandlers;
using Npgsql.TypeHandling;
using Npgsql.Util;
using NpgsqlTypes;
using static Npgsql.Util.Statics;

#pragma warning disable CA2222 // Do not decrease inherited member visibility
namespace Npgsql
{
    /// <summary>
    /// Reads a forward-only stream of rows from a data source.
    /// </summary>
    /// <remarks>
    /// Due to C# abstract class syntax limitations, all internal fields and internal properties with private
    /// setters must be in this base class.
    /// </remarks>
#pragma warning disable CA1010
    public abstract class NpgsqlDataReader : DbDataReader
#pragma warning restore CA1010
#if !NET461
        , IDbColumnSchemaGenerator
#endif
    {
        internal NpgsqlCommand Command { get; private protected set; } = default!;
        internal NpgsqlConnector Connector { get; }

        internal ReaderState State;

        /// <summary>
        /// Holds the list of statements being executed by this reader.
        /// </summary>
        protected List<NpgsqlStatement> _statements = default!;

        /// <summary>
        /// The RowDescription message for the current resultset being processed
        /// </summary>
        internal RowDescriptionMessage? RowDescription;

        /// <summary>
        /// Number of records affected
        /// </summary>
        protected ulong? _recordsAffected;

        /// <summary>
        /// Whether the current result set has rows
        /// </summary>
        protected bool _hasRows;

        /// <summary>
        /// Is raised whenever Close() is called.
        /// </summary>
        public event EventHandler? ReaderClosed;

        internal NpgsqlDataReader(NpgsqlConnector connector)
        {
            Connector = connector;
        }

        #region Read

        /// <summary>
        /// Advances the reader to the next record in a result set.
        /// </summary>
        /// <returns><b>true</b> if there are more rows; otherwise <b>false</b>.</returns>
        /// <remarks>
        /// The default position of a data reader is before the first record. Therefore, you must call Read to begin accessing data.
        /// </remarks>
        public sealed override bool Read()
        {
            CheckClosed();
            var fastRead = TryFastRead();
            return fastRead.HasValue
                ? fastRead.Value
                : Read(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// This is the asynchronous version of <see cref="Read()"/> The cancellation token is currently ignored.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public sealed override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            CheckClosed();

            if (cancellationToken.IsCancellationRequested)
                return Task.FromCanceled<bool>(cancellationToken);

            var fastRead = TryFastRead();
            if (fastRead.HasValue)
                return fastRead.Value ? PGUtil.TrueTask : PGUtil.FalseTask;

            using (NoSynchronizationContextScope.Enter())
                return Read(true, cancellationToken);
        }

        /// <summary>
        /// This is an optimized execution path that avoids calling any async methods for the (usual)
        /// case where the next row (or CommandComplete) is already in memory.
        /// </summary>
        /// <returns></returns>
        internal abstract bool? TryFastRead();

        /// <summary>
        /// Internal implementation of Read
        /// </summary>
        /// <param name="async"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        protected abstract Task<bool> Read(bool async, CancellationToken cancellationToken = default);

        #endregion

        #region NextResult

        /// <summary>
        /// Advances the reader to the next result when reading the results of a batch of statements.
        /// </summary>
        /// <returns></returns>
        public abstract override bool NextResult();

        /// <summary>
        /// This is the asynchronous version of NextResult.
        /// The <paramref name="cancellationToken"/> parameter is currently ignored.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public abstract override Task<bool> NextResultAsync(CancellationToken cancellationToken);

        #endregion

        /// <summary>
        /// Gets a value indicating the depth of nesting for the current row.  Always returns zero.
        /// </summary>
        public sealed override int Depth => 0;

        /// <summary>
        /// Gets a value indicating whether the data reader is closed.
        /// </summary>
        public sealed override bool IsClosed => State == ReaderState.Closed;

        /// <summary>
        /// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
        /// </summary>
        public sealed override int RecordsAffected => _recordsAffected.HasValue ? (int)_recordsAffected.Value : -1;

        /// <summary>
        /// Returns details about each statement that this reader will or has executed.
        /// </summary>
        /// <remarks>
        /// Note that some fields (i.e. rows and oid) are only populated as the reader
        /// traverses the result.
        ///
        /// For commands with multiple queries, this exposes the number of rows affected on
        /// a statement-by-statement basis, unlike <see cref="DbDataReader.RecordsAffected"/>
        /// which exposes an aggregation across all statements.
        /// </remarks>
        public IReadOnlyList<NpgsqlStatement> Statements => _statements.AsReadOnly();

        /// <summary>
        /// Gets a value that indicates whether this DbDataReader contains one or more rows.
        /// </summary>
        public sealed override bool HasRows => State == ReaderState.Closed
            ? throw new InvalidOperationException("Invalid attempt to call HasRows when reader is closed.")
            : _hasRows;

        /// <summary>
        /// Indicates whether the reader is currently positioned on a row, i.e. whether reading a
        /// column is possible.
        /// This property is different from <see cref="HasRows"/> in that <see cref="HasRows"/> will
        /// return true even if attempting to read a column will fail, e.g. before <see cref="Read()"/>
        /// has been called
        /// </summary>
        [PublicAPI]
        public bool IsOnRow => State == ReaderState.InResult;

        /// <summary>
        /// Gets the name of the column, given the zero-based column ordinal.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The name of the specified column.</returns>
        public sealed override string GetName(int ordinal) => CheckRowDescriptionAndGetField(ordinal).Name;

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        public sealed override int FieldCount
        {
            get
            {
                CheckClosed();
                return RowDescription?.NumFields ?? 0;
            }
        }

        #region Cleanup / Dispose

        /// <summary>
        /// Consumes all result sets for this reader, leaving the connector ready for sending and processing further
        /// queries
        /// </summary>
        protected abstract Task Consume(bool async);

        internal async Task Close(bool connectionClosing, bool async)
        {
            if (State == ReaderState.Closed)
                return;

            switch (Connector.State)
            {
                case ConnectorState.Broken:
                case ConnectorState.Closed:
                    // This may have happen because an I/O error while reading a value, or some non-safe
                    // exception thrown from a type handler. Or if the connection was closed while the reader
                    // was still open
                    State = ReaderState.Closed;
                    Command.State = CommandState.Idle;
                    ReaderClosed?.Invoke(this, EventArgs.Empty);
                    return;
            }

            if (State != ReaderState.Consumed)
                await Consume(async);

            await Cleanup(async, connectionClosing);
        }

        internal async Task Cleanup(bool async, bool connectionClosing = false)
        {
            await _Cleanup(async, connectionClosing);

            if (ReaderClosed != null)
            {
                ReaderClosed(this, EventArgs.Empty);
                ReaderClosed = null;
            }
        }

        internal abstract Task _Cleanup(bool async, bool connectionClosing = false);

        #endregion

        #region Simple value getters

        /// <summary>
        /// Gets the value of the specified column as a Boolean.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override bool GetBoolean(int ordinal) => GetFieldValue<bool>(ordinal);

        /// <summary>
        /// Gets the value of the specified column as a byte.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override byte GetByte(int ordinal) => GetFieldValue<byte>(ordinal);

        /// <summary>
        /// Gets the value of the specified column as a single character.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override char GetChar(int ordinal) => GetFieldValue<char>(ordinal);

        /// <summary>
        /// Gets the value of the specified column as a 16-bit signed integer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override short GetInt16(int ordinal) => GetFieldValue<short>(ordinal);

        /// <summary>
        /// Gets the value of the specified column as a 32-bit signed integer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override int GetInt32(int ordinal) => GetFieldValue<int>(ordinal);

        /// <summary>
        /// Gets the value of the specified column as a 64-bit signed integer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override long GetInt64(int ordinal) => GetFieldValue<long>(ordinal);

        /// <summary>
        /// Gets the value of the specified column as a <see cref="DateTime"/> object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override DateTime GetDateTime(int ordinal) => GetFieldValue<DateTime>(ordinal);

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="string"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override string GetString(int ordinal) => GetFieldValue<string>(ordinal);

        /// <summary>
        /// Gets the value of the specified column as a <see cref="decimal"/> object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override decimal GetDecimal(int ordinal) => GetFieldValue<decimal>(ordinal);

        /// <summary>
        /// Gets the value of the specified column as a double-precision floating point number.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override double GetDouble(int ordinal) => GetFieldValue<double>(ordinal);

        /// <summary>
        /// Gets the value of the specified column as a single-precision floating point number.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override float GetFloat(int ordinal) => GetFieldValue<float>(ordinal);

        /// <summary>
        /// Gets the value of the specified column as a globally-unique identifier (GUID).
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override Guid GetGuid(int ordinal) => GetFieldValue<Guid>(ordinal);

        /// <summary>
        /// Populates an array of objects with the column values of the current row.
        /// </summary>
        /// <param name="values">An array of Object into which to copy the attribute columns.</param>
        /// <returns>The number of instances of <see cref="object"/> in the array.</returns>
        public sealed override int GetValues(object[] values)
        {
            if (values == null)
                throw new ArgumentNullException(nameof(values));
            CheckResultSet();

            var count = Math.Min(FieldCount, values.Length);
            for (var i = 0; i < count; i++)
                values[i] = GetValue(i);
            return count;
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="object"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override object this[int ordinal] => GetValue(ordinal);

        #endregion

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
        public NpgsqlDate GetDate(int ordinal) => GetFieldValue<NpgsqlDate>(ordinal);

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
        public TimeSpan GetTimeSpan(int ordinal) => GetFieldValue<TimeSpan>(ordinal);

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
        public NpgsqlTimeSpan GetInterval(int ordinal) => GetFieldValue<NpgsqlTimeSpan>(ordinal);

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
        public NpgsqlDateTime GetTimeStamp(int ordinal) => GetFieldValue<NpgsqlDateTime>(ordinal);

        #endregion

        #region Special binary getters

        /// <summary>
        /// Reads a stream of bytes from the specified column, starting at location indicated by dataOffset, into the buffer, starting at the location indicated by bufferOffset.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="dataOffset">The index within the row from which to begin the read operation.</param>
        /// <param name="buffer">The buffer into which to copy the data.</param>
        /// <param name="bufferOffset">The index with the buffer to which the data will be copied.</param>
        /// <param name="length">The maximum number of characters to read.</param>
        /// <returns>The actual number of bytes read.</returns>
        public abstract override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length);

        /// <summary>
        /// Retrieves data as a <see cref="Stream"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The returned object.</returns>
        public sealed override Stream GetStream(int ordinal) => GetStream(ordinal, false).Result;

        /// <summary>
        /// Retrieves data as a <see cref="Stream"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>The returned object.</returns>
        public Task<Stream> GetStreamAsync(int ordinal, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
                return Task.FromCanceled<Stream>(cancellationToken);
            using (NoSynchronizationContextScope.Enter())
                return GetStream(ordinal, true).AsTask();
        }

        ValueTask<Stream> GetStream(int ordinal, bool async)
        {
            var fieldDescription = CheckRowAndGetField(ordinal);
            if (!(fieldDescription.Handler is ByteaHandler))
                throw new InvalidCastException($"GetStream() not supported for type {fieldDescription.Handler.PgDisplayName}");

            return GetStreamInternal(ordinal, async);
        }

        /// <summary>
        /// Internal implementation of GetStream
        /// </summary>
        /// <param name="ordinal"></param>
        /// <param name="async"></param>
        /// <returns></returns>
        internal abstract ValueTask<Stream> GetStreamInternal(int ordinal, bool async);

        #endregion

        #region Special text getters

        /// <summary>
        /// Reads a stream of characters from the specified column, starting at location indicated by dataOffset, into the buffer, starting at the location indicated by bufferOffset.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="dataOffset">The index within the row from which to begin the read operation.</param>
        /// <param name="buffer">The buffer into which to copy the data.</param>
        /// <param name="bufferOffset">The index with the buffer to which the data will be copied.</param>
        /// <param name="length">The maximum number of characters to read.</param>
        /// <returns>The actual number of characters read.</returns>
        public abstract override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length);

        /// <summary>
        /// Retrieves data as a <see cref="TextReader"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The returned object.</returns>
        public sealed override TextReader GetTextReader(int ordinal)
            => GetTextReader(ordinal, false).Result;

        /// <summary>
        /// Retrieves data as a <see cref="TextReader"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>The returned object.</returns>
        public Task<TextReader> GetTextReaderAsync(int ordinal, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
                return Task.FromCanceled<TextReader>(cancellationToken);
            using (NoSynchronizationContextScope.Enter())
                return GetTextReader(ordinal, true).AsTask();
        }

        async ValueTask<TextReader> GetTextReader(int ordinal, bool async)
        {
            var fieldDescription = CheckRowAndGetField(ordinal);
            if (!(fieldDescription.Handler is ITextReaderHandler handler))
                throw new InvalidCastException($"GetTextReader() not supported for type {fieldDescription.Handler.PgDisplayName}");

            var stream = async
                ? await GetStreamInternal(ordinal, async)
                : GetStreamInternal(ordinal, async).Result;

            return handler.GetTextReader(stream);
        }

        #endregion

        #region GetFieldValue

        /// <summary>
        /// Asynchronously gets the value of the specified column as a type.
        /// </summary>
        /// <typeparam name="T">The type of the value to be returned.</typeparam>
        /// <param name="ordinal">The type of the value to be returned.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns></returns>
        public abstract override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken);

        /// <summary>
        /// Synchronously gets the value of the specified column as a type.
        /// </summary>
        /// <typeparam name="T">Synchronously gets the value of the specified column as a type.</typeparam>
        /// <param name="ordinal">The column to be retrieved.</param>
        /// <returns>The column to be retrieved.</returns>
        public abstract override T GetFieldValue<T>(int ordinal);

        #endregion

        #region GetValue

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="object"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public abstract override object GetValue(int ordinal);

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="object"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public abstract override object GetProviderSpecificValue(int ordinal);

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="object"/>.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override object this[string name] => GetValue(GetOrdinal(name));

        #endregion

        #region IsDBNull

        /// <summary>
        /// Gets a value that indicates whether the column contains nonexistent or missing values.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns><b>true</b> if the specified column is equivalent to <see cref="DBNull"/>; otherwise <b>false</b>.</returns>
        public abstract override bool IsDBNull(int ordinal);

        /// <summary>
        /// An asynchronous version of <see cref="IsDBNull(int)"/>, which gets a value that indicates whether the column contains non-existent or missing values.
        /// The <paramref name="cancellationToken"/> parameter is currently ignored.
        /// </summary>
        /// <param name="ordinal">The zero-based column to be retrieved.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns><b>true</b> if the specified column value is equivalent to <see cref="DBNull"/> otherwise <b>false</b>.</returns>
        public abstract override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken);

        #endregion

        #region Other public accessors

        /// <summary>
        /// Gets the column ordinal given the name of the column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>The zero-based column ordinal.</returns>
        public sealed override int GetOrdinal(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("name cannot be empty", nameof(name));
            if (State == ReaderState.Closed)
                throw new InvalidOperationException("The reader is closed");
            if (RowDescription is null)
                throw new InvalidOperationException("No resultset is currently being traversed");
            return RowDescription.GetFieldIndex(name);
        }

        /// <summary>
        /// Gets a representation of the PostgreSQL data type for the specified field.
        /// The returned representation can be used to access various information about the field.
        /// </summary>
        /// <param name="ordinal">The zero-based column index.</param>
        [PublicAPI]
        public PostgresType GetPostgresType(int ordinal) => CheckRowDescriptionAndGetField(ordinal).PostgresType;

        /// <summary>
        /// Gets the data type information for the specified field.
        /// This will be the PostgreSQL type name (e.g. double precision), not the .NET type
        /// (see <see cref="GetFieldType"/> for that).
        /// </summary>
        /// <param name="ordinal">The zero-based column index.</param>
        public override string GetDataTypeName(int ordinal) => CheckRowDescriptionAndGetField(ordinal).TypeDisplayName;

        /// <summary>
        /// Gets the OID for the PostgreSQL type for the specified field, as it appears in the pg_type table.
        /// </summary>
        /// <remarks>
        /// This is a PostgreSQL-internal value that should not be relied upon and should only be used for
        /// debugging purposes.
        /// </remarks>
        /// <param name="ordinal">The zero-based column index.</param>
        public uint GetDataTypeOID(int ordinal) => CheckRowDescriptionAndGetField(ordinal).TypeOID;

        /// <summary>
        /// Gets the data type of the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The data type of the specified column.</returns>
        public override Type GetFieldType(int ordinal)
            => Command.ObjectResultTypes?[ordinal]
               ?? CheckRowDescriptionAndGetField(ordinal).FieldType;

        /// <summary>
        /// Returns the provider-specific field type of the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The Type object that describes the data type of the specified column.</returns>
        public override Type GetProviderSpecificFieldType(int ordinal)
        {
            var fieldDescription = CheckRowDescriptionAndGetField(ordinal);
            return fieldDescription.Handler.GetProviderSpecificFieldType(fieldDescription);
        }

        /// <summary>
        /// Gets all provider-specific attribute columns in the collection for the current row.
        /// </summary>
        /// <param name="values">An array of Object into which to copy the attribute columns.</param>
        /// <returns>The number of instances of <see cref="object"/> in the array.</returns>
        public sealed override int GetProviderSpecificValues(object[] values)
        {
            if (values == null)
                throw new ArgumentNullException(nameof(values));
            if (State != ReaderState.InResult)
                throw new InvalidOperationException("No row is available");

            var count = Math.Min(FieldCount, values.Length);
            for (var i = 0; i < count; i++)
                values[i] = GetProviderSpecificValue(i);
            return count;
        }

        /// <summary>
        /// Returns an <see cref="IEnumerator"/> that can be used to iterate through the rows in the data reader.
        /// </summary>
        /// <returns>An <see cref="IEnumerator"/> that can be used to iterate through the rows in the data reader.</returns>
        public sealed override IEnumerator GetEnumerator()
            => new DbEnumerator(this);

        /// <summary>
        /// Returns schema information for the columns in the current resultset.
        /// </summary>
        /// <returns></returns>
        public abstract ReadOnlyCollection<NpgsqlDbColumn> GetColumnSchema();

#if !NET461
        ReadOnlyCollection<DbColumn> IDbColumnSchemaGenerator.GetColumnSchema()
            => new ReadOnlyCollection<DbColumn>(GetColumnSchema().Cast<DbColumn>().ToList());
#endif

        #endregion

        #region Schema metadata table

        /// <summary>
        /// Returns a System.Data.DataTable that describes the column metadata of the DataReader.
        /// </summary>
#nullable disable
        public sealed override DataTable GetSchemaTable()
#nullable restore
        {
            if (FieldCount == 0) // No resultset
                return null;

            var table = new DataTable("SchemaTable");

            // Note: column order is important to match SqlClient's, some ADO.NET users appear
            // to assume ordering (see #1671)
            table.Columns.Add("ColumnName", typeof(string));
            table.Columns.Add("ColumnOrdinal", typeof(int));
            table.Columns.Add("ColumnSize", typeof(int));
            table.Columns.Add("NumericPrecision", typeof(int));
            table.Columns.Add("NumericScale", typeof(int));
            table.Columns.Add("IsUnique", typeof(bool));
            table.Columns.Add("IsKey", typeof(bool));
            table.Columns.Add("BaseServerName", typeof(string));
            table.Columns.Add("BaseCatalogName", typeof(string));
            table.Columns.Add("BaseColumnName", typeof(string));
            table.Columns.Add("BaseSchemaName", typeof(string));
            table.Columns.Add("BaseTableName", typeof(string));
            table.Columns.Add("DataType", typeof(Type));
            table.Columns.Add("AllowDBNull", typeof(bool));
            table.Columns.Add("ProviderType", typeof(int));
            table.Columns.Add("IsAliased", typeof(bool));
            table.Columns.Add("IsExpression", typeof(bool));
            table.Columns.Add("IsIdentity", typeof(bool));
            table.Columns.Add("IsAutoIncrement", typeof(bool));
            table.Columns.Add("IsRowVersion", typeof(bool));
            table.Columns.Add("IsHidden", typeof(bool));
            table.Columns.Add("IsLong", typeof(bool));
            table.Columns.Add("IsReadOnly", typeof(bool));
            table.Columns.Add("ProviderSpecificDataType", typeof(Type));
            table.Columns.Add("DataTypeName", typeof(string));

            foreach (var column in GetColumnSchema())
            {
                var row = table.NewRow();

                row["ColumnName"] = column.ColumnName;
                row["ColumnOrdinal"] = column.ColumnOrdinal ?? -1;
                row["ColumnSize"] = column.ColumnSize ?? -1;
                row["NumericPrecision"] = column.NumericPrecision ?? 0;
                row["NumericScale"] = column.NumericScale ?? 0;
                row["IsUnique"] = column.IsUnique == true;
                row["IsKey"] = column.IsKey == true;
                row["BaseServerName"] = "";
                row["BaseCatalogName"] = column.BaseCatalogName;
                row["BaseColumnName"] = column.BaseColumnName;
                row["BaseSchemaName"] = column.BaseSchemaName;
                row["BaseTableName"] = column.BaseTableName;
                row["DataType"] = column.DataType;
                row["AllowDBNull"] = (object?)column.AllowDBNull ?? DBNull.Value;
                row["ProviderType"] = column.NpgsqlDbType ?? NpgsqlDbType.Unknown;
                row["IsAliased"] = column.IsAliased == true;
                row["IsExpression"] = column.IsExpression == true;
                row["IsIdentity"] = column.IsIdentity == true;
                row["IsAutoIncrement"] = column.IsAutoIncrement == true;
                row["IsRowVersion"] = false;
                row["IsHidden"] = column.IsHidden == true;
                row["IsLong"] = column.IsLong == true;
                row["DataTypeName"] = column.DataTypeName;

                table.Rows.Add(row);
            }

            return table;
        }

        #endregion Schema metadata table

        #region Checks

        /// <summary>
        /// Exception if result set is not valid
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void CheckResultSet()
        {
            switch (State)
            {
                case ReaderState.BeforeResult:
                case ReaderState.InResult:
                    break;
                case ReaderState.Closed:
                    throw new InvalidOperationException("The reader is closed");
                default:
                    throw new InvalidOperationException("No resultset is currently being traversed");
            }
        }

        /// <summary>
        /// Get field description, exception if request is not valid
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected FieldDescription CheckRowAndGetField(int column)
        {
            switch (State)
            {
                case ReaderState.InResult:
                    break;
                case ReaderState.Closed:
                    throw new InvalidOperationException("The reader is closed");
                default:
                    throw new InvalidOperationException("No row is available");
            }

            if (column < 0 || column >= RowDescription!.NumFields)
                throw new IndexOutOfRangeException($"Column must be between {0} and {RowDescription!.NumFields - 1}");

            return RowDescription[column];
        }

        /// <summary>
        /// Checks that we have a RowDescription, but not necessary an actual resultset
        /// (for operations which work in SchemaOnly mode.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected FieldDescription CheckRowDescriptionAndGetField(int column)
        {
            if (RowDescription == null)
                throw new InvalidOperationException("No resultset is currently being traversed");

            if (column < 0 || column >= RowDescription.NumFields)
                throw new IndexOutOfRangeException($"Column must be between {0} and {RowDescription.NumFields - 1}");

            return RowDescription[column];
        }

        /// <summary>
        /// Exception if reader is closed
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void CheckClosed()
        {
            if (State == ReaderState.Closed)
                throw new InvalidOperationException("The reader is closed");
        }

        #endregion
    }

    enum ReaderState
    {
        BeforeResult,
        InResult,
        BetweenResults,
        Consumed,
        Closed,
    }
}
