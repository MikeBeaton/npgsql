using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

using Npgsql.Logging;
using Npgsql.PostgresTypes;
using Npgsql.Schema;
using NpgsqlTypes;

namespace Npgsql
{
    /// <summary>
    /// Cursor dereferencing data reader derived originally from removed Npgsql v2 code (see
    /// https://github.com/npgsql/npgsql/issues/438); but now with safer, more consistent behaviour.
    /// </summary>
    /// <remarks>
    /// Safer:
    ///  - No longer FETCH ALL by default as this is dangerous for large result sets (http://stackoverflow.com/q/42292341/);
    ///    though this can be enabled with DereferenceFetchSize=-1 and will be more efficient for small to medium result sets.
    /// More consistent:
    ///  - Now works for multiple cursors in any arrangement (n x 1; 1 x n; n x m).
    /// Cf Oracle:
    ///  - Oracle does the equivalent of this automatic dereferencing (on by default) in their driver.
    ///  - So in Oracle you cannot read a cursor from a result set directly; however you can still code additonal
    ///    SQL with output parameters in order to get at the original cursors.
    ///  - See note below about copying Oracle behaviour when only some of the original result set are cursors
    ///    (since this will presumably be the most useful thing to do for cross-db developers).
    /// </remarks>
    public sealed class NpgsqlDereferencingReader : NpgsqlDataReader
    {
        // internally to this class, zero or any negative value will FETCH ALL; externally in settings, -1 is the currently chosen trigger value
        private int _fetchSize;
        private NpgsqlConnector _connector;
        private NpgsqlDataReader _originalReader;
        private CommandBehavior _behavior;

        // current FETCH reader
        private NpgsqlDataReader FetchReader = default!;

        // # read on current FETCH
        private int Count;

        // cursors to dereference
        private readonly List<string> Cursors = new List<string>();

        // current cursor index
        private int CursorIndex = 0;

        // current cursor
        private string? Cursor = null;

        static readonly NpgsqlLogger Log = NpgsqlLogManager.CreateLogger(nameof(NpgsqlDereferencingReader));

        /// <summary>
        /// Is raised whenever Close() is called.
        /// </summary>
        public override event EventHandler? ReaderClosed;

        /// <summary>
        /// Create a safe, sensible dereferencing reader; <see cref="CanDereference"/> has already been called to check
        /// that there are at least some cursors to dereference before this constructor is called.
        /// </summary>
        /// <param name="reader">The original reader for the undereferenced query.</param>
        /// <param name="behavior">The required <see cref="CommandBehavior"/></param>
        /// <param name="connector">The connector to use</param>
        /// <remarks>
        /// FETCH ALL is genuinely useful in some situations (if using cursors to return small or medium sized multiple
        /// result sets then we can and do save one round trip to the database overall: n_cursors round trips, rather
        /// than n_cursors + 1), but since it is badly problematic in the case of large cursors we force the user to
        /// request it explicitly.
        /// https://github.com/npgsql/npgsql/issues/438
        /// http://stackoverflow.com/q/42292341/
        /// </remarks>
        internal NpgsqlDereferencingReader(NpgsqlStandardDataReader reader, CommandBehavior behavior, NpgsqlConnector connector)
        {
            _originalReader = reader;
            _behavior = behavior;
            _connector = connector;
            _fetchSize = connector.Settings.DereferenceFetchSize;
            Command = reader.Command;
        }

        /// <summary>
        /// True iff current reader has cursors in its output types.
        /// </summary>
        /// <param name="reader">The reader to check</param>
        /// <returns>Are there cursors?</returns>
        /// <remarks>Really a part of NpgsqlDereferencingReader</remarks>
        static public bool CanDereference(DbDataReader reader)
        {
            var hasCursors = false;
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (reader.GetDataTypeName(i) == "refcursor")
                {
                    hasCursors = true;
                    break;
                }
            }
            return hasCursors;
        }

        /// <summary>
        /// Initialise the reader
        /// </summary>
        /// <returns></returns>
        internal async Task Init(bool async, CancellationToken cancellationToken)
        {
            // Behavior is only saved to be used here in this Init method; we don't need to check or enforce it again
            // elsewhere since the read logic below is already enforcing it.
            // For SingleRow we additionally rely on the user to only read one row and then dispose of everything.
            var earlyQuit = (_behavior == CommandBehavior.SingleResult || _behavior == CommandBehavior.SingleRow);

            // we're saving all the cursors from the original reader here, then disposing of it
            // (we've already checked in CanDereference that there are at least some cursors)
            using (_originalReader)
            {
                // Supports 1x1 1xN Nx1 and NXM patterns of cursor data.
                // If just some values are cursors we follow the pre-existing pattern set by the Oracle drivers, and dereference what we can.
                while (async ? await _originalReader.ReadAsync(cancellationToken) : _originalReader.Read())
                {
                    for (var i = 0; i < _originalReader.FieldCount; i++)
                    {
                        if (_originalReader.GetDataTypeName(i) == "refcursor")
                        {
                            // cursor name can potentially contain " so stop that breaking us
                            // TO DO: document how/why
                            Cursors.Add(_originalReader.GetString(i).Replace(@"""", @""""""));
                            if (earlyQuit) break;
                        }
                    }
                    if (earlyQuit) break;
                }
            }

            // initialize
            if (async)
                await NextResultAsync(cancellationToken);
            else
                NextResult();
        }

        /// <summary>
        /// Fetch next N rows from current cursor.
        /// </summary>
        /// <param name="closePreviousSQL">
        /// SQL to prepend to close the previous cursor in a single round trip (send <see cref="string.Empty"/> when
        /// not required).
        /// </param>
        /// <param name="async">True to operate asynchronously.</param>
        /// <param name="cancellationToken">Async <see cref="CancellationToken"/>.</param>
        async Task FetchNextNFromCursor(string closePreviousSQL, bool async, CancellationToken cancellationToken)
        {
            // close and dispose previous fetch reader for this cursor
            if (FetchReader != null && !FetchReader.IsClosed)
            {
                FetchReader.Dispose();
            }

            // fetch next n from cursor;
            // optionally close previous cursor;
            // iff we're fetching all, we can close this cursor in this command
            using (var fetchCmd = CreateCommand(closePreviousSQL + FetchSQL() + (_fetchSize <= 0 ? CloseSQL() : "")))
            {
                if (async)
                    FetchReader = await fetchCmd.ExecuteReaderAsync(CommandBehavior.SingleResult, cancellationToken);
                else
                    FetchReader = fetchCmd.ExecuteReader(CommandBehavior.SingleResult);

                // closing the wrapped reader invokes any close events on this reader
                FetchReader.ReaderClosed += (sender, args) =>
                    ReaderClosed?.Invoke(this, EventArgs.Empty);
            }

            Count = 0;
        }

        #region NextResult

        /// <summary>
        /// Advances the reader to the next result when reading the results of a batch of statements.
        /// </summary>
        /// <returns></returns>
        public override bool NextResult()
        {
            return NextResult(false, default(CancellationToken))
                .GetAwaiter().GetResult();
        }

        /// <summary>
        /// This is the asynchronous version of NextResult.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <remarks>Note: the <paramref name="cancellationToken"/> parameter need not be and is not ignored in this variant.</remarks>
        /// <returns>A task representing the asynchronous operation.</returns>
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            using (NoSynchronizationContextScope.Enter())
                return NextResult(true, cancellationToken);
        }

        async Task<bool> NextResult(bool async, CancellationToken cancellationToken)
        {
            var closeSql = CloseCursor(CursorIndex >= Cursors.Count);
            if (CursorIndex >= Cursors.Count)
            {
                return false;
            }
            Cursor = Cursors[CursorIndex++];
            await FetchNextNFromCursor(closeSql, async, cancellationToken);
            return true;
        }

        #endregion

        #region Read

        /// <summary>
        /// Advances the reader to the next record in a result set.
        /// </summary>
        /// <returns><b>true</b> if there are more rows; otherwise <b>false</b>.</returns>
        /// <remarks>
        /// The default position of a data reader is before the first record. Therefore, you must call Read to begin accessing data.
        /// </remarks>
        public override bool Read()
        {
            return Read(false, default(CancellationToken))
                .GetAwaiter().GetResult();
        }

        /// <summary>
        /// This is the asynchronous version of <see cref="Read()"/> The cancellation token is currently ignored.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            using (NoSynchronizationContextScope.Enter())
                return Read(true, cancellationToken);
        }

        async Task<bool> Read(bool async, CancellationToken cancellationToken)
        {
            if (FetchReader != null)
            {
                var cursorHasNextRow = async ? await FetchReader.ReadAsync(cancellationToken) : FetchReader.Read();
                if (cursorHasNextRow)
                {
                    Count++;
                    return true;
                }

                // if we did FETCH ALL or if rows ended before requested count, there is nothing more to fetch on this cursor
                if (_fetchSize <= 0 || Count < _fetchSize)
                {
                    return false;
                }
            }

            // NB if rows ended at requested count, there may or may not be more rows
            await FetchNextNFromCursor(string.Empty, async, cancellationToken);

            // recursive self-call
            return await Read(async, cancellationToken);
        }

        #endregion

        /// <summary>
        /// SQL to fetch required count from current cursor
        /// </summary>
        /// <returns>SQL</returns>
        private string FetchSQL()
        {
            return string.Format(@"FETCH {0} FROM ""{1}"";", (_fetchSize <= 0 ? "ALL" : _fetchSize.ToString()), Cursor);
        }

        /// <summary>
        /// SQL to close current cursor
        /// </summary>
        /// <returns>SQL</returns>
        private string CloseSQL()
        {
            return string.Format(@"CLOSE ""{0}"";", Cursor);
        }

        /// <summary>
        /// Close current FETCH cursor on the database
        /// </summary>
        /// <param name="ExecuteNow">Iff false then return the SQL but don't execute the command</param>
        /// <returns>The SQL to close the cursor, if there is one and this has not already been executed.</returns>
        private string CloseCursor(bool ExecuteNow = true)
        {
            // close and dispose current fetch reader for this cursor
            if (FetchReader != null && !FetchReader.IsClosed)
            {
                FetchReader.Dispose();
            }
            // close cursor itself
            if (_fetchSize > 0 && !string.IsNullOrEmpty(Cursor))
            {
                var closeSql = CloseSQL();
                if (!ExecuteNow)
                {
                    return closeSql;
                }
                using (var closeCmd = CreateCommand(closeSql))
                {
                    closeCmd.ExecuteNonQuery();
                }
                Cursor = null;
            }
            return "";
        }

        private NpgsqlCommand CreateCommand(string sql)
        {
            var command = _connector.Connection!.CreateCommand();
            command.CommandText = sql;
            return command;
        }

        #region DbDataReader abstract interface

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public override object this[string name] =>
            FetchReader[name];

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override object this[int i] =>
            FetchReader[i];

        /// <summary>
        /// 
        /// </summary>
        public override int Depth =>
            FetchReader.Depth;

        /// <summary>
        /// 
        /// </summary>
        public override int FieldCount =>
            FetchReader.FieldCount;

        /// <summary>
        /// 
        /// </summary>
        public override bool HasRows =>
            FetchReader.HasRows; 

        /// <summary>
        /// 
        /// </summary>
        public override bool IsClosed =>
            FetchReader.IsClosed;

        /// <summary>
        /// 
        /// </summary>
        public override int RecordsAffected =>
            FetchReader.RecordsAffected;

        /// <summary>
        /// 
        /// </summary>
        public override void Close()
        {
            CloseCursor();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override bool GetBoolean(int i) =>
            FetchReader.GetBoolean(i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override byte GetByte(int i) =>
            FetchReader.GetByte(i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <param name="fieldOffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferoffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public override long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) =>
            FetchReader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override char GetChar(int i) =>
            FetchReader.GetChar(i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <param name="fieldoffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferoffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public override long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) =>
            FetchReader.GetChars(i, fieldoffset, buffer, bufferoffset, length);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override string GetDataTypeName(int i) =>
            FetchReader.GetDataTypeName(i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override DateTime GetDateTime(int i) =>
            FetchReader.GetDateTime(i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override decimal GetDecimal(int i) =>
            FetchReader.GetDecimal(i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override double GetDouble(int i) =>
            FetchReader.GetDouble(i);

        /// <summary>
        /// Returns an <see cref="IEnumerator"/> that can be used to iterate through the rows in the data reader.
        /// </summary>
        /// <returns>An <see cref="IEnumerator"/> that can be used to iterate through the rows in the data reader.</returns>
        public override IEnumerator GetEnumerator()
            => new DbEnumerator(this);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override Type GetFieldType(int i) =>
            FetchReader.GetFieldType(i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override float GetFloat(int i) =>
            FetchReader.GetFloat(i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override Guid GetGuid(int i) =>
            FetchReader.GetGuid(i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override short GetInt16(int i) =>
            FetchReader.GetInt16(i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override int GetInt32(int i) =>
            FetchReader.GetInt32(i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override long GetInt64(int i) =>
            FetchReader.GetInt64(i);
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override string GetName(int i) =>
            FetchReader.GetName(i);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override DataTable GetSchemaTable() =>
            FetchReader.GetSchemaTable();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public override int GetOrdinal(string name) =>
            FetchReader.GetOrdinal(name);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override string GetString(int i) =>
            FetchReader.GetString(i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override object GetValue(int i) =>
            FetchReader.GetValue(i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public override int GetValues(object[] values) =>
            FetchReader.GetValues(values);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override bool IsDBNull(int i) =>
            FetchReader.IsDBNull(i);

        #endregion

        #region Cleanup / Dispose

        internal override async Task Close(bool connectionClosing, bool async)
        {
            await FetchReader.Close(connectionClosing, async);
        }

        internal override async Task Cleanup(bool async, bool connectionClosing = false)
        {
            await FetchReader.Cleanup(async, connectionClosing);

            if (ReaderClosed != null)
            {
                ReaderClosed(this, EventArgs.Empty);
                ReaderClosed = null;
            }
        }

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
        public override IReadOnlyList<NpgsqlStatement> Statements =>
            FetchReader.Statements;

        /// <summary>
        /// Indicates whether the reader is currently positioned on a row, i.e. whether reading a
        /// column is possible.
        /// This property is different from <see cref="DbDataReader.HasRows"/> in that <see cref="DbDataReader.HasRows"/> will
        /// return true even if attempting to read a column will fail, e.g. before <see cref="DbDataReader.Read()"/>
        /// has been called
        /// </summary>
        [PublicAPI]
        public override bool IsOnRow =>
            FetchReader.IsOnRow;

        #region GetFieldValue

        /// <summary>
        /// Synchronously gets the value of the specified column as a type.
        /// </summary>
        /// <typeparam name="T">Synchronously gets the value of the specified column as a type.</typeparam>
        /// <param name="ordinal">The column to be retrieved.</param>
        /// <returns>The column to be retrieved.</returns>
        public override T GetFieldValue<T>(int ordinal) =>
           FetchReader.GetFieldValue<T>(ordinal);

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
        public override NpgsqlDate GetDate(int ordinal) => GetFieldValue<NpgsqlDate>(ordinal);

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
        public override TimeSpan GetTimeSpan(int ordinal) => GetFieldValue<TimeSpan>(ordinal);

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
        public override NpgsqlTimeSpan GetInterval(int ordinal) => GetFieldValue<NpgsqlTimeSpan>(ordinal);

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
        public override NpgsqlDateTime GetTimeStamp(int ordinal) => GetFieldValue<NpgsqlDateTime>(ordinal);

        #endregion

        #region Special binary getters

        /// <summary>
        /// Retrieves data as a <see cref="Stream"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>The returned object.</returns>
        public override Task<Stream> GetStreamAsync(int ordinal, CancellationToken cancellationToken = default) =>
            FetchReader.GetStreamAsync(ordinal, cancellationToken);

        #endregion

        #region Special text getters

        /// <summary>
        /// Retrieves data as a <see cref="TextReader"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>The returned object.</returns>
        public override Task<TextReader> GetTextReaderAsync(int ordinal, CancellationToken cancellationToken = default) =>
            FetchReader.GetTextReaderAsync(ordinal, cancellationToken);

        #endregion

        #region Other public accessors

        /// <summary>
        /// Gets a representation of the PostgreSQL data type for the specified field.
        /// The returned representation can be used to access various information about the field.
        /// </summary>
        /// <param name="ordinal">The zero-based column index.</param>
        [PublicAPI]
        public override PostgresType GetPostgresType(int ordinal) =>
            FetchReader.GetPostgresType(ordinal);

        /// <summary>
        /// Gets the OID for the PostgreSQL type for the specified field, as it appears in the pg_type table.
        /// </summary>
        /// <remarks>
        /// This is a PostgreSQL-internal value that should not be relied upon and should only be used for
        /// debugging purposes.
        /// </remarks>
        /// <param name="ordinal">The zero-based column index.</param>
        public override uint GetDataTypeOID(int ordinal) =>
            FetchReader.GetDataTypeOID(ordinal);

        /// <summary>
        /// Returns schema information for the columns in the current resultset.
        /// </summary>
        /// <returns></returns>
        public override ReadOnlyCollection<NpgsqlDbColumn> GetColumnSchema() =>
            FetchReader.GetColumnSchema();
        #endregion
    }
}
