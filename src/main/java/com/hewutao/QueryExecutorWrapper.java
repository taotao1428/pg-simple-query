package com.hewutao;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.postgresql.core.Field;
import org.postgresql.core.PGStream;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.ResultHandler;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.PSQLWarning;
import org.postgresql.util.ServerErrorMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class QueryExecutorWrapper {
    private PGStream pgStream;
    private QueryExecutor executor;


    public QueryExecutorWrapper(Connection conn, Statement stat) throws Exception {
        Object protoConn = FieldUtils.readField(conn, "protoConnection", true);

        pgStream = (PGStream) FieldUtils.readField(protoConn, "pgStream", true);

        executor = (QueryExecutor) FieldUtils.readField(protoConn, "executor", true);
    }

    public void sendSimpleQuery(String sql) throws Exception {
        byte[] data = sql.getBytes(StandardCharsets.UTF_8);
        int len = 4 + data.length + 1;

        pgStream.SendChar('Q');
        pgStream.SendInteger4(len);
        pgStream.Send(data);
        pgStream.SendChar(0);
        pgStream.SendChar('S');     // Sync
        pgStream.SendInteger4(4); // Length
        pgStream.flush();
    }


     public void processResults(ResultHandler handler, int flags) throws IOException {
        boolean noResults = (flags & QueryExecutor.QUERY_NO_RESULTS) != 0;
        boolean bothRowsAndStatus = (flags & QueryExecutor.QUERY_BOTH_ROWS_AND_STATUS) != 0;

        List tuples = null;

        int len;
        int c;
        boolean endQuery = false;

        // At the end of a command execution we have the CommandComplete
        // message to tell us we're done, but with a describeOnly command
        // we have no real flag to let us know we're done.  We've got to
        // look for the next RowDescription or NoData message and return
        // from there.
        boolean doneAfterRowDescNoData = false;

        int parseIndex = 0;
        int describeIndex = 0;
        int describePortalIndex = 0;
        int bindIndex = 0;
        int executeIndex = 0;

        Field[] fieldsCache = null;

        while (!endQuery)
        {
            c = pgStream.ReceiveChar();
            switch (c)
            {
                case 'A':  // Asynchronous Notify
                    throw new IOException("Unexpected packet type: " + c);

                case '1':    // Parse Complete (response to Parse)
                    throw new IOException("Unexpected packet type: " + c);

                case 't':    // ParameterDescription
                    throw new IOException("Unexpected packet type: " + c);

                case '2':    // Bind Complete  (response to Bind)
                    throw new IOException("Unexpected packet type: " + c);

                case '3':    // Close Complete (response to Close)
                    pgStream.ReceiveInteger4(); // len, discarded
                    break;

                case 'n':    // No Data        (response to Describe)
                    throw new IOException("Unexpected packet type: " + c);

                case 's':    // Portal Suspended (end of Execute)
                    // nb: this appears *instead* of CommandStatus.
                    // Must be a SELECT if we suspended, so don't worry about it.

                    throw new IOException("Unexpected packet type: " + c);

                case 'C':  // Command Status (end of Execute)
                    // Handle status.
                    String status = receiveCommandStatus();

                {

                    Field[] fields = fieldsCache;
                    if (fields != null && !noResults && tuples == null)
                        tuples = new ArrayList();

                    if (fields != null || tuples != null)
                    { // There was a resultset.
                        handler.handleResultRows(new QueryImpl("sql"), fields, tuples, null);
                        tuples = null;
                        fieldsCache = null;

                        if (bothRowsAndStatus)
                            interpretCommandStatus(status, handler);
                    }
                    else
                    {
                        interpretCommandStatus(status, handler);
                    }
                }
                break;

                case 'D':  // Data Transfer (ongoing Execute response)
                    byte[][] tuple = null;
                    try {
                        tuple = pgStream.ReceiveTupleV3();
                    } catch(OutOfMemoryError oome) {
                        if (!noResults) {
                            handler.handleError(new PSQLException(GT.tr("Ran out of memory retrieving query results."), PSQLState.OUT_OF_MEMORY, oome));
                        }
                    }


                    if (!noResults)
                    {
                        if (tuples == null)
                            tuples = new ArrayList();
                        tuples.add(tuple);
                    }

                    break;

                case 'E':  // Error Response (response to pretty much everything; backend then skips until Sync)
                    SQLException error = receiveErrorResponse();
                    handler.handleError(error);

                    // keep processing
                    break;

                case 'I':  // Empty Query (end of Execute)
                    pgStream.ReceiveInteger4();
                    handler.handleCommandStatus("EMPTY", 0, 0);

                    break;

                case 'N':  // Notice Response
                    SQLWarning warning = receiveNoticeResponse();
                    handler.handleWarning(warning);
                    break;

                case 'S':    // Parameter Status
                    throw new IOException("Unexpected packet type: " + c);

                case 'T':  // Row Description (response to Describe)
                    Field[] fields = receiveFields();
                    tuples = new ArrayList();

                    fieldsCache = fields;
                    break;

                case 'Z':    // Ready For Query (eventual response to Sync)
                    receiveRFQ();
                    endQuery = true;

                    break;

                case 'G':  // CopyInResponse
                    throw new IOException("Unexpected packet type: " + c);

                case 'H':  // CopyOutResponse
                    throw new IOException("Unexpected packet type: " + c);

                case 'c':  // CopyDone
                    throw new IOException("Unexpected packet type: " + c);

                case 'd':  // CopyData
                    throw new IOException("Unexpected packet type: " + c);

                default:
                    throw new IOException("Unexpected packet type: " + c);
            }

        }
    }

    /**
     * Ignore the response message by reading the message length and skipping
     * over those bytes in the communication stream.
     */
    private void skipMessage() throws IOException {
        int l_len = pgStream.ReceiveInteger4();
        // skip l_len-4 (length includes the 4 bytes for message length itself
        pgStream.Skip(l_len - 4);
    }

    private String receiveCommandStatus() throws IOException {
        //TODO: better handle the msg len
        int l_len = pgStream.ReceiveInteger4();
        //read l_len -5 bytes (-4 for l_len and -1 for trailing \0)
        String status = pgStream.ReceiveString(l_len - 5);
        //now read and discard the trailing \0
        pgStream.Receive(1);


        return status;
    }

    private void interpretCommandStatus(String status, ResultHandler handler) {
        int update_count = 0;
        long insert_oid = 0;

        if (status.startsWith("INSERT") || status.startsWith("UPDATE") || status.startsWith("DELETE") || status.startsWith("MOVE"))
        {
            try
            {
                long updates = Long.parseLong(status.substring(1 + status.lastIndexOf(' ')));

                // deal with situations where the update modifies more than 2^32 rows
                if ( updates > Integer.MAX_VALUE )
                    update_count = Statement.SUCCESS_NO_INFO;
                else
                    update_count = (int)updates;

                if (status.startsWith("INSERT"))
                    insert_oid = Long.parseLong(status.substring(1 + status.indexOf(' '),
                            status.lastIndexOf(' ')));
            }
            catch (NumberFormatException nfe)
            {
                handler.handleError(new PSQLException(GT.tr("Unable to interpret the update count in command completion tag: {0}.", status), PSQLState.CONNECTION_FAILURE));
                return ;
            }
        }

        handler.handleCommandStatus(status, update_count, insert_oid);
    }

    private SQLException receiveErrorResponse() throws IOException {
        // it's possible to get more than one error message for a query
        // see libpq comments wrt backend closing a connection
        // so, append messages to a string buffer and keep processing
        // check at the bottom to see if we need to throw an exception

        int elen = pgStream.ReceiveInteger4();
        String totalMessage = pgStream.ReceiveString(elen - 4);
        ServerErrorMessage errorMsg = new ServerErrorMessage(totalMessage, 0);

        return new PSQLException(errorMsg);
    }

    private SQLWarning receiveNoticeResponse() throws IOException {
        int nlen = pgStream.ReceiveInteger4();
        ServerErrorMessage warnMsg = new ServerErrorMessage(pgStream.ReceiveString(nlen - 4), 0);

        return new PSQLWarning(warnMsg);
    }

    private Field[] receiveFields() throws IOException
    {
        int l_msgSize = pgStream.ReceiveInteger4();
        int size = pgStream.ReceiveInteger2();
        Field[] fields = new Field[size];

        for (int i = 0; i < fields.length; i++)
        {
            String columnLabel = pgStream.ReceiveString();
            int tableOid = pgStream.ReceiveInteger4();
            short positionInTable = (short)pgStream.ReceiveInteger2();
            int typeOid = pgStream.ReceiveInteger4();
            int typeLength = pgStream.ReceiveInteger2();
            int typeModifier = pgStream.ReceiveInteger4();
            int formatType = pgStream.ReceiveInteger2();
            fields[i] = new Field(columnLabel,
                    "",  /* name not yet determined */
                    typeOid, typeLength, typeModifier, tableOid, positionInTable);
            fields[i].setFormat(formatType);
        }

        return fields;
    }

    private void receiveRFQ() throws IOException {
        if (pgStream.ReceiveInteger4() != 5)
            throw new IOException("unexpected length of ReadyForQuery message");

        char tStatus = (char)pgStream.ReceiveChar();

        // Update connection state.
        switch (tStatus)
        {
            case 'I':
//                protoConnection.setTransactionState(ProtocolConnection.TRANSACTION_IDLE);
                break;
            case 'T':
//                protoConnection.setTransactionState(ProtocolConnection.TRANSACTION_OPEN);
                break;
            case 'E':
//                protoConnection.setTransactionState(ProtocolConnection.TRANSACTION_FAILED);
                break;
            default:
                throw new IOException("unexpected transaction state in ReadyForQuery message: " + (int)tStatus);
        }
    }
}
