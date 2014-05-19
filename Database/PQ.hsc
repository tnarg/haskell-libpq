{-# LANGUAGE ForeignFunctionInterface, EmptyDataDecls, OverloadedStrings, ScopedTypeVariables #-}

-- | This is a binding to libpq: the C application programmer's
-- interface to PostgreSQL. libpq is a set of library functions that
-- allow client programs to pass queries to the PostgreSQL backend
-- server and to receive the results of these queries.
module Database.PQ
    (
    -- * Database Connection Control Functions
    -- $dbconn
      Connection
    , connectdb
    , connectStart
    , connectPoll
    --, conndefaults
    --, conninfoParse
    , reset
    , resetStart
    , resetPoll
    , PollingStatus(..)
    , finish

    -- * Connection Status Functions
    -- $connstatus
    , db
    , user
    , pass
    , host
    , port
    , options
    , ConnStatus(..)
    , status
    , TransactionStatus(..)
    , transactionStatus
    , parameterStatus
    , protocolVersion
    , serverVersion
    , errorMessage
    , socket
    , backendPID
    , connectionNeedsPassword
    , connectionUsedPassword
    --, getssl


    -- * Command Execution Functions
    -- $commandexec
    , Result
    , exec
    , Format(..)
    , Oid
    , execParams
    , prepare
    , execPrepared
    , describePrepared
    , describePortal
    , ExecStatus(..)
    , resultStatus
    , resStatus
    , resultErrorMessage
    , FieldCode(..)
    , resultErrorField

    -- * Retrieving Query Result Information
    -- $queryresultinfo
    , ntuples
    , nfields
    , Row
    , Column
    , toRow
    , toColumn
    , fname
    , fnumber
    , ftable
    , ftablecol
    , fformat
    , ftype
    , fmod
    , fsize
    , getvalue
    , getvalue'
    , getisnull
    , getlength
    , nparams
    , paramtype
    , print
    , PrintOpt(..)
    , defaultPrintOpt

    -- Retrieving Result Information for Other Commands
    -- $othercommands
    , cmdStatus
    , cmdTuples
    , oidValue

    -- * Escaping Strings for Inclusion in SQL Commands
    , escapeStringConn

    -- * Escaping Binary Strings for Inclusion in SQL Commands
    , escapeByteaConn
    , unescapeBytea

    -- * Asynchronous Command Processing
    -- $asynccommand
    , sendQuery
    , sendQueryParams
    , sendPrepare
    , sendQueryPrepared
    , sendDescribePrepared
    , sendDescribePortal
    , getResult
    , consumeInput
    , isBusy
    , setnonblocking
    , isnonblocking
    , FlushStatus(..)
    , flush

    -- * Cancelling Queries in Progress
    -- $cancel
    , Cancel
    , getCancel
    , cancel

    -- * Asynchronous Notification
    -- $asyncnotification
    , Notify(..)
    , notifies

    -- * Control Functions
    -- $control
    , clientEncoding
    , setClientEncoding
    , Verbosity(..)
    , setErrorVerbosity
    , trace
    , untrace
    )
where

#include <libpq-fe.h>

import Prelude hiding ( print )
import Foreign
import Foreign.C.Types
import Foreign.C.String
import qualified Foreign.Concurrent as FC
import GHC.Conc ( -- threadWaitRead
                 threadWaitWrite)
import System.Posix.Types ( Fd(..) )
import Data.List ( foldl' )
import System.IO ( Handle )

#if __GLASGOW_HASKELL__ >= 611
import GHC.IO.Handle ( hDuplicate )
#else
import GHC.Handle ( hDuplicate )
#endif
import System.Posix.IO ( handleToFd )
import Control.Concurrent.MVar
import System.Posix.Types ( CPid )

import Data.ByteString.Char8 ()
import qualified Data.ByteString.Unsafe as B
import qualified Data.ByteString.Internal as B ( fromForeignPtr
                                               , c_strlen
                                               )
import qualified Data.ByteString as B

-- $dbconn
-- The following functions deal with making a connection to a
-- PostgreSQL backend server. An application program can have several
-- backend connections open at one time. (One reason to do that is to
-- access more than one database.) Each connection is represented by a
-- 'Connection', which is obtained from the function 'connectdb', or
-- 'connectStart'. The 'status' function should be called to check
-- whether a connection was successfully made before queries are sent
-- via the connection object.

-- | 'Connection' encapsulates a connection to the backend.
newtype Connection = Conn (MVar (Maybe (ForeignPtr PGconn))) deriving Eq
data PGconn


-- | Makes a new connection to the database server.
--
-- This function opens a new database connection using the parameters
--  taken from the string conninfo. Its nonblocking analogues are
--  'connectStart' and 'connectPoll'.
--
-- The passed string can be empty to use all default parameters, or it
-- can contain one or more parameter settings separated by
-- whitespace. Each parameter setting is in the form keyword =
-- value. Spaces around the equal sign are optional. To write an empty
-- value or a value containing spaces, surround it with single quotes,
-- e.g., keyword = 'a value'. Single quotes and backslashes within the
-- value must be escaped with a backslash, i.e., \' and \\.
connectdb :: B.ByteString -- ^ Connection Info
          -> IO Connection
connectdb conninfo =
    do connPtr <- B.useAsCString conninfo c_PQconnectdb
       if connPtr == nullPtr
           then fail "libpq failed to allocate a PGconn structure"
           else do fp <- newForeignPtr p_PQfinish connPtr
                   mvar <- newMVar $ Just fp
                   return $ Conn mvar


-- | Make a connection to the database server in a nonblocking manner.
connectStart :: B.ByteString -- ^ Connection Info
             -> IO Connection
connectStart connStr =
    do connPtr <- B.useAsCString connStr c_PQconnectStart
       if connPtr == nullPtr
           then fail "libpq failed to allocate a PGconn structure"
           else do fp <- newForeignPtr p_PQfinish connPtr
                   mvar <- newMVar $ Just fp
                   return $ Conn mvar


-- | If 'connectStart' succeeds, the next stage is to poll libpq so
-- that it can proceed with the connection sequence. Use 'socket' to
-- obtain the 'Fd' of the socket underlying the database
-- connection. Loop thus: If 'connectPoll' last returned
-- 'PollingReading', wait until the socket is ready to read (as
-- indicated by select(), poll(), or similar system function). Then
-- call 'connectPoll' again. Conversely, if 'connectPoll' last
-- returned 'PollingWriting', wait until the socket is ready to write,
-- then call 'connectPoll' again. If you have yet to call
-- 'connectPoll', i.e., just after the call to 'connectStart', behave
-- as if it last returned 'PollingWriting'. Continue this loop until
-- 'connectPoll' returns 'PollingFailed', indicating the connection
-- procedure has failed, or 'PollingOk', indicating the connection has
-- been successfully made.
connectPoll :: Connection
            -> IO PollingStatus
connectPoll = pollHelper c_PQconnectPoll


-- PQconndefaults
-- Returns the default connection options.

-- PQconninfoOption *PQconndefaults(void);

-- typedef struct
-- {
--     char   *keyword;   /* The keyword of the option */
--     char   *envvar;    /* Fallback environment variable name */
--     char   *compiled;  /* Fallback compiled in default value */
--     char   *val;       /* Option's current value, or NULL */
--     char   *label;     /* Label for field in connect dialog */
--     char   *dispchar;  /* Indicates how to display this field
--                           in a connect dialog. Values are:
--                           ""        Display entered value as is
--                           "*"       Password field - hide value
--                           "D"       Debug option - don't show by default */
--     int     dispsize;  /* Field size in characters for dialog */
-- } PQconninfoOption;
-- Returns a connection options array. This can be used to determine all possible PQconnectdb options and their current default values. The return value points to an array of PQconninfoOption structures, which ends with an entry having a null keyword pointer. The null pointer is returned if memory could not be allocated. Note that the current default values (val fields) will depend on environment variables and other context. Callers must treat the connection options data as read-only.

-- After processing the options array, free it by passing it to PQconninfoFree. If this is not done, a small amount of memory is leaked for each call to PQconndefaults.

-- PQconninfoParse
-- Returns parsed connection options from the provided connection string.

-- PQconninfoOption *PQconninfoParse(const char *conninfo, char **errmsg);
-- Parses a connection string and returns the resulting options as an array; or returns NULL if there is a problem with the connection string. This can be used to determine the PQconnectdb options in the provided connection string. The return value points to an array of PQconninfoOption structures, which ends with an entry having a null keyword pointer.

-- Note that only options explicitly specified in the string will have values set in the result array; no defaults are inserted.

-- If errmsg is not NULL, then *errmsg is set to NULL on success, else to a malloc'd error string explaining the problem. (It is also possible for *errmsg to be set to NULL even when NULL is returned; this indicates an out-of-memory situation.)

-- After processing the options array, free it by passing it to PQconninfoFree. If this is not done, some memory is leaked for each call to PQconninfoParse. Conversely, if an error occurs and errmsg is not NULL, be sure to free the error string using PQfreemem.


-- | Resets the communication channel to the server.
--
-- This function will close the connection to the server and attempt
-- to reestablish a new connection to the same server, using all the
-- same parameters previously used. This might be useful for error
-- recovery if a working connection is lost.
reset :: Connection
      -> IO ()
reset connection = withConn connection c_PQreset


-- | Reset the communication channel to the server, in a nonblocking manner.
resetStart :: Connection
           -> IO Bool
resetStart connection =
    enumFromConn connection c_PQresetStart


-- | To initiate a connection reset, call 'resetStart'. If it returns
-- 'False', the reset has failed. If it returns 'True', poll the reset
-- using 'resetPoll' in exactly the same way as you would create the
-- connection using 'connectPoll'.
resetPoll :: Connection
          -> IO PollingStatus
resetPoll = pollHelper c_PQresetPoll

data PollingStatus
    = PollingFailed
    | PollingReading
    | PollingWriting
    | PollingOk deriving (Eq, Show)

pollHelper :: (Ptr PGconn -> IO CInt)
           -> Connection
           -> IO PollingStatus
pollHelper poller connection =
    do code <- withConn connection poller
       case code of
         (#const PGRES_POLLING_READING) -> return PollingReading
         (#const PGRES_POLLING_OK)      -> return PollingOk
         (#const PGRES_POLLING_WRITING) -> return PollingWriting
         (#const PGRES_POLLING_FAILED)  -> return PollingFailed
         _ -> error $ "unexpected polling status " ++ show code


-- | Closes the connection to the server.
--
-- Note that the 'Connection' must not be used again after 'finish'
-- has been called.
finish :: Connection
       -> IO ()
finish (Conn mvar) =
    do mFp <- takeMVar mvar
       case mFp of
         Nothing -> putMVar mvar Nothing
         Just fp -> do finalizeForeignPtr fp
                       putMVar mvar Nothing


-- $connstatus
-- These functions can be used to interrogate the status of an
-- existing database connection object.


-- | Returns the database name of the connection.
db :: Connection
   -> IO (Maybe B.ByteString)
db = statusString c_PQdb


-- | Returns the user name of the connection.
user :: Connection
     -> IO (Maybe B.ByteString)
user = statusString c_PQuser


-- | Returns the password of the connection.
pass :: Connection
     -> IO (Maybe B.ByteString)
pass = statusString c_PQpass


-- | Returns the server host name of the connection.
host :: Connection
     -> IO (Maybe B.ByteString)
host = statusString c_PQhost


-- | Returns the port of the connection.
port :: Connection
     -> IO (Maybe B.ByteString)
port = statusString c_PQport


-- | Returns the command-line options passed in the connection request.
options :: Connection
        -> IO (Maybe B.ByteString)
options = statusString c_PQoptions


-- | Helper function that checks for nullPtrs and returns the empty
-- string.
statusString :: (Ptr PGconn -> IO CString)
             -> Connection
             -> IO (Maybe B.ByteString)
statusString f connection =
    withConn connection $ \ptr ->
        do cstr <- f ptr
           if cstr == nullPtr
             then return Nothing
             else Just `fmap` B.packCString cstr


data ConnStatus
    = ConnectionOk                 -- ^ The 'Connection' is ready.
    | ConnectionBad                -- ^ The connection procedure has failed.
    | ConnectionStarted            -- ^ Waiting for connection to be made.
    | ConnectionMade               -- ^ Connection OK; waiting to send.
    | ConnectionAwaitingResponse   -- ^ Waiting for a response from the server.
    | ConnectionAuthOk             -- ^ Received authentication;
                                   -- waiting for backend start-up to
                                   -- finish.
    | ConnectionSetEnv             -- ^ Negotiating environment-driven
                                   -- parameter settings.
    | ConnectionSSLStartup         -- ^ Negotiating SSL encryption.
      deriving (Eq, Show)


-- | Returns the status of the connection.
--
-- The status can be one of a number of values. However, only two of
-- these are seen outside of an asynchronous connection procedure:
-- 'ConnectionOk' and 'ConnectionBad'. A good connection to the
-- database has the status 'ConnectionOk'. A failed connection attempt
-- is signaled by status 'ConnectionBad'. Ordinarily, an OK status
-- will remain so until 'finish', but a communications failure might
-- result in the status changing to 'ConnectionBad' prematurely. In
-- that case the application could try to recover by calling 'reset'.
--
-- See the entry for 'connectStart' and 'connectPoll' with regards to
-- other status codes that might be seen.
status :: Connection
       -> IO ConnStatus
status connection = do
  stat <- withConn connection c_PQstatus
  return $ case stat of
             (#const CONNECTION_OK)               -> ConnectionOk
             (#const CONNECTION_BAD)              -> ConnectionBad
             (#const CONNECTION_STARTED)          -> ConnectionStarted
             (#const CONNECTION_MADE)             -> ConnectionMade
             (#const CONNECTION_AWAITING_RESPONSE)-> ConnectionAwaitingResponse
             (#const CONNECTION_AUTH_OK)          -> ConnectionAuthOk
             (#const CONNECTION_SETENV)           -> ConnectionSetEnv
             (#const CONNECTION_SSL_STARTUP)      -> ConnectionSSLStartup
             --(#const CONNECTION_NEEDED)           -> ConnectionNeeded
             c -> error $ "Unknown connection status " ++ show c


data TransactionStatus = TransIdle    -- ^ currently idle
                       | TransActive  -- ^ a command is in progress
                       | TransInTrans -- ^ idle, in a valid transaction block
                       | TransInError -- ^ idle, in a failed transaction block
                       | TransUnknown -- ^ the connection is bad
                         deriving (Eq, Show)

-- | Returns the current in-transaction status of the server.
--
-- 'TransActive' is reported only when a query has been sent to the
-- server and not yet completed.
transactionStatus :: Connection
                  -> IO TransactionStatus
transactionStatus connection = do
    stat <- withConn connection c_PQtransactionStatus
    return $ case stat of
               (#const PQTRANS_IDLE)    -> TransIdle
               (#const PQTRANS_ACTIVE)  -> TransActive
               (#const PQTRANS_INTRANS) -> TransInTrans
               (#const PQTRANS_INERROR) -> TransInError
               (#const PQTRANS_UNKNOWN) -> TransUnknown
               c -> error $ "Unknown transaction status " ++ show c


-- | Looks up a current parameter setting of the server.
--
-- Certain parameter values are reported by the server automatically
-- at connection startup or whenever their values
-- change. 'parameterStatus' can be used to interrogate these
-- settings. It returns the current value of a parameter if known, or
-- 'Nothing' if the parameter is not known.
parameterStatus :: Connection
                -> B.ByteString -- ^ paramName
                -> IO (Maybe B.ByteString)
parameterStatus connection paramName =
    withConn connection $ \connPtr ->
        B.useAsCString paramName $ \paramNamePtr ->
        do cstr <- c_PQparameterStatus connPtr paramNamePtr
           if cstr == nullPtr
             then return Nothing
             else Just `fmap` B.packCString cstr


-- | Interrogates the frontend/backend protocol being used.
--
-- Applications might wish to use this to determine whether certain
-- features are supported. Currently, the possible values are 2 (2.0
-- protocol), 3 (3.0 protocol), or zero (connection bad). This will
-- not change after connection startup is complete, but it could
-- theoretically change during a connection reset. The 3.0 protocol
-- will normally be used when communicating with PostgreSQL 7.4 or
-- later servers; pre-7.4 servers support only protocol 2.0. (Protocol
-- 1.0 is obsolete and not supported by libpq.)
protocolVersion :: Connection
                -> IO Int
protocolVersion connection =
    fmap fromIntegral $ withConn connection c_PQprotocolVersion


-- | Returns an integer representing the backend version.
--
-- Applications might use this to determine the version of the
-- database server they are connected to. The number is formed by
-- converting the major, minor, and revision numbers into
-- two-decimal-digit numbers and appending them together. For example,
-- version 8.1.5 will be returned as 80105, and version 8.2 will be
-- returned as 80200 (leading zeroes are not shown). Zero is returned
-- if the connection is bad.
serverVersion :: Connection
              -> IO Int
serverVersion connection =
    fmap fromIntegral $ withConn connection c_PQserverVersion


-- | Returns the error message most recently generated by an operation
-- on the connection.
--
-- Nearly all libpq functions will set a message for 'errorMessage' if
-- they fail. Note that by libpq convention, a nonempty 'errorMessage'
-- result can be multiple lines, and will include a trailing
-- newline. The result string should not be expected to remain the
-- same across operations on the 'Connection'.
errorMessage :: Connection
             -> IO (Maybe B.ByteString)
errorMessage = flip maybeBsFromConn c_PQerrorMessage


-- | Obtains the file descriptor number of the connection socket to
-- the server. (This will not change during normal operation, but
-- could change during connection setup or reset.)
socket :: Connection
       -> IO (Maybe Fd)
socket connection =
    do cFd <- withConn connection c_PQsocket
       return $ case cFd of
                  -1 -> Nothing
                  _ -> Just $ Fd cFd


-- | Returns the process 'CPid' of the backend server process
-- handling this connection.
--
-- The backend PID is useful for debugging purposes and for comparison
-- to NOTIFY messages (which include the PID of the notifying backend
-- process). Note that the PID belongs to a process executing on the
-- database server host, not the local host!
backendPID :: Connection
           -> IO CPid
backendPID connection =
    fmap fromIntegral $ withConn connection c_PQbackendPID


-- | Returns 'True' if the connection authentication method required a
-- password, but none was available. Returns 'False' if not.
--
-- This function can be applied after a failed connection attempt to
-- decide whether to prompt the user for a password.
connectionNeedsPassword :: Connection
                        -> IO Bool
connectionNeedsPassword connection =
    enumFromConn connection c_PQconnectionNeedsPassword


-- | Returns 'True' if the connection authentication method used a
-- password. Returns 'False' if not.
--
-- This function can be applied after either a failed or successful
-- connection attempt to detect whether the server demanded a
-- password.
connectionUsedPassword :: Connection
                       -> IO Bool
connectionUsedPassword connection =
    enumFromConn connection c_PQconnectionUsedPassword


-- TODO: getSSL :: Connection -> IO SSL


-- $commandexec
-- Once a connection to a database server has been successfully
-- established, the functions described here are used to perform SQL
-- queries and commands.

-- | 'Result' encapsulates the result of a query (or more precisely,
-- of a single SQL command --- a query string given to 'sendQuery' can
-- contain multiple commands and thus return multiple instances of
-- 'Result'.
newtype Result = Result (ForeignPtr PGresult) deriving (Eq, Show)
data PGresult

data Format = Text | Binary deriving (Enum, Eq, Show)

type Oid = CUInt


-- | Submits a command to the server and waits for the result.
--
-- Returns a 'Result' or possibly 'Nothing'. A 'Result' will generally
-- be returned except in out-of-memory conditions or serious errors
-- such as inability to send the command to the server. If a 'Nothing'
-- is returned, it should be treated like a 'FatalError' result. Use
-- 'errorMessage' to get more information about such errors.
--
-- It is allowed to include multiple SQL commands (separated by
-- semicolons) in the command string. Multiple queries sent in a
-- single 'exec' call are processed in a single transaction, unless
-- there are explicit BEGIN/COMMIT commands included in the query
-- string to divide it into multiple transactions. Note however that
-- the returned 'Result' structure describes only the result of the
-- last command executed from the string. Should one of the commands
-- fail, processing of the string stops with it and the returned
-- 'Result' describes the error condition.
exec :: Connection        -- ^ connection
     -> B.ByteString      -- ^ statement
     -> IO (Maybe Result) -- ^ result
exec connection query =
    resultFromConn connection $ \p ->
        B.useAsCString query $ c_PQexec p


-- | Submits a command to the server and waits for the result, with
-- the ability to pass parameters separately from the SQL command
-- text.
--
-- 'execParams' is like 'exec', but offers additional functionality:
-- parameter values can be specified separately from the command
-- string proper, and query results can be requested in either text or
-- binary format. 'execParams' is supported only in protocol 3.0 and
-- later connections; it will fail when using protocol 2.0.
--
-- The primary advantage of 'execParams' over 'exec' is that parameter
-- values can be separated from the command string, thus avoiding the
-- need for tedious and error-prone quoting and escaping.
--
-- Unlike 'exec', 'execParams' allows at most one SQL command in the
-- given string. (There can be semicolons in it, but not more than one
-- nonempty command.) This is a limitation of the underlying protocol,
-- but has some usefulness as an extra defense against SQL-injection
-- attacks.
--
-- Tip: Specifying parameter types via OIDs is tedious, particularly
-- if you prefer not to hard-wire particular OID values into your
-- program. However, you can avoid doing so even in cases where the
-- server by itself cannot determine the type of the parameter, or
-- chooses a different type than you want. In the SQL command text,
-- attach an explicit cast to the parameter symbol to show what data
-- type you will send. For example:
-- SELECT * FROM mytable WHERE x = $1::bigint;
-- This forces parameter $1 to be treated as bigint, whereas by
-- default it would be assigned the same type as x. Forcing the
-- parameter type decision, either this way or by specifying a numeric
-- type OID, is strongly recommended when sending parameter values in
-- binary format, because binary format has less redundancy than text
-- format and so there is less chance that the server will detect a
-- type mismatch mistake for you.
execParams :: Connection                          -- ^ connection
           -> B.ByteString                        -- ^ statement
           -> [Maybe (Oid, B.ByteString, Format)] -- ^ parameters
           -> Format                              -- ^ result format
           -> IO (Maybe Result)                   -- ^ result
execParams connection statement params rFmt =
    do let (oids, values, lengths, formats) =
               foldl' accum ([],[],[],[]) $ reverse params
           c_lengths = map toEnum lengths :: [CInt]
           n = toEnum $ length params
           f = toEnum $ fromEnum rFmt
       resultFromConn connection $ \c ->
           B.useAsCString statement $ \s ->
               withArray oids $ \ts ->
                   withMany (maybeWith B.useAsCString) values $ \c_values ->
                       withArray c_values $ \vs ->
                           withArray c_lengths $ \ls ->
                               withArray formats $ \fs ->
                                   c_PQexecParams c s n ts vs ls fs f

    where
      accum (a,b,c,d) Nothing = ( 0:a
                                , Nothing:b
                                , 0:c
                                , 0:d
                                )
      accum (a,b,c,d) (Just (t,v,f)) = ( t:a
                                       , (Just v):b
                                       , (B.length v):c
                                       , (toEnum $ fromEnum f):d
                                       )


-- | Submits a request to create a prepared statement with the given
-- parameters, and waits for completion.
--
-- 'prepare' creates a prepared statement for later execution with
-- 'execPrepared'. This feature allows commands that will be used
-- repeatedly to be parsed and planned just once, rather than each
-- time they are executed. 'prepare' is supported only in protocol 3.0
-- and later connections; it will fail when using protocol 2.0.
--
-- The function creates a prepared statement named stmtName from the
-- query string, which must contain a single SQL command. stmtName can
-- be \"\" to create an unnamed statement, in which case any
-- pre-existing unnamed statement is automatically replaced; otherwise
-- it is an error if the statement name is already defined in the
-- current session. If any parameters are used, they are referred to
-- in the query as $1, $2, etc. paramTypes specifies, by 'Oid', the
-- data types to be assigned to the parameter symbols. If paramTypes
-- is 'Nothing', or any particular element in the array is zero, the
-- server assigns a data type to the parameter symbol in the same way
-- it would do for an untyped literal string. Also, the query can use
-- parameter symbols with numbers higher than the length of
-- paramTypes; data types will be inferred for these symbols as
-- well. (See 'describePrepared' for a means to find out what data
-- types were inferred.)
--
-- As with 'exec', the result is normally a 'Result' whose contents
-- indicate server-side success or failure. A 'Nothing' result
-- indicates out-of-memory or inability to send the command at
-- all. Use 'errorMessage' to get more information about such errors.
--
-- Prepared statements for use with 'execPrepared' can also be created
-- by executing SQL PREPARE statements. (But 'prepare' is more
-- flexible since it does not require parameter types to be
-- pre-specified.) Also, although there is no libpq function for
-- deleting a prepared statement, the SQL DEALLOCATE statement can be
-- used for that purpose.
prepare :: Connection        -- ^ connection
        -> B.ByteString      -- ^ stmtName
        -> B.ByteString      -- ^ query
        -> Maybe [Oid]       -- ^ paramTypes
        -> IO (Maybe Result) -- ^ result
prepare connection stmtName query mParamTypes =
    resultFromConn connection $ \c ->
        B.useAsCString stmtName $ \s ->
            B.useAsCString query $ \q ->
                maybeWith withArray mParamTypes $ \o ->
                    let l = maybe 0 (toEnum . length) mParamTypes
                    in c_PQprepare c s q l o


-- | Sends a request to execute a prepared statement with given
-- parameters, and waits for the result.
--
-- 'execPrepared' is like 'execParams', but the command to be executed
-- is specified by naming a previously-prepared statement, instead of
-- giving a query string. This feature allows commands that will be
-- used repeatedly to be parsed and planned just once, rather than
-- each time they are executed. The statement must have been prepared
-- previously in the current session. 'execPrepared' is supported only
-- in protocol 3.0 and later connections; it will fail when using
-- protocol 2.0.
--
-- The parameters are identical to 'execParams', except that the name
-- of a prepared statement is given instead of a query string, and the
-- paramTypes parameter is not present (it is not needed since the
-- prepared statement's parameter types were determined when it was
-- created).
execPrepared :: Connection                     -- ^ connection
             -> B.ByteString                   -- ^ stmtName
             -> [Maybe (B.ByteString, Format)] -- ^ parameters
             -> Format                         -- ^ result format
             -> IO (Maybe Result)              -- ^ result
execPrepared connection stmtName mPairs rFmt =
    do let (values, lengths, formats) = foldl' accum ([],[],[]) $ reverse mPairs
           c_lengths = map toEnum lengths :: [CInt]
           n = toEnum $ length mPairs
           f = toEnum $ fromEnum rFmt
       resultFromConn connection $ \c ->
           B.useAsCString stmtName $ \s ->
               withMany (maybeWith B.useAsCString) values $ \c_values ->
                   withArray c_values $ \vs ->
                       withArray c_lengths $ \ls ->
                           withArray formats $ \fs ->
                               c_PQexecPrepared c s n vs ls fs f

    where
      accum (a,b,c) Nothing       = ( Nothing:a
                                    , 0:b
                                    , 0:c
                                    )
      accum (a,b,c) (Just (v, f)) = ( (Just v):a
                                    , (B.length v):b
                                    , (toEnum $ fromEnum f):c
                                    )


-- | Submits a request to obtain information about the specified
-- prepared statement, and waits for completion.
--
-- 'describePrepared' allows an application to obtain information
-- about a previously prepared statement. 'describePrepared' is
-- supported only in protocol 3.0 and later connections; it will fail
-- when using protocol 2.0.
--
-- stmtName can be empty to reference the unnamed statement, otherwise
-- it must be the name of an existing prepared statement. On success,
-- a 'Result' with status 'CommandOk' is returned. The functions
-- 'nparams' and 'paramtype' can be applied to this 'Result' to obtain
-- information about the parameters of the prepared statement, and the
-- functions 'nfields', 'fname', 'ftype', etc provide information
-- about the result columns (if any) of the statement.
describePrepared :: Connection
                 -> B.ByteString -- ^ stmtName
                 -> IO (Maybe Result)
describePrepared connection stmtName =
    resultFromConn connection $ \c ->
        B.useAsCString stmtName $ \s -> c_PQdescribePrepared c s


-- | Submits a request to obtain information about the specified
-- portal, and waits for completion.
--
-- 'describePortal' allows an application to obtain information about
-- a previously created portal. (libpq does not provide any direct
-- access to portals, but you can use this function to inspect the
-- properties of a cursor created with a DECLARE CURSOR SQL command.)
-- 'describePortal' is supported only in protocol 3.0 and later
-- connections; it will fail when using protocol 2.0.
--
-- portalName can be empty to reference the unnamed portal, otherwise
-- it must be the name of an existing portal. On success, a 'Result'
-- with status 'CommandOk' is returned. The functions 'nfields',
-- 'fname', 'ftype', etc can be applied to the 'Result' to obtain
-- information about the result columns (if any) of the portal.
describePortal :: Connection
               -> B.ByteString -- ^ portalName
               -> IO (Maybe Result)
describePortal connection portalName =
    resultFromConn connection $ \c ->
        B.useAsCString portalName $ \p ->
            c_PQdescribePortal c p


data ExecStatus = EmptyQuery    -- ^ The string sent to the server was empty.
                | CommandOk     -- ^ Successful completion of a
                                -- command returning no data.
                | TuplesOk      -- ^ Successful completion of a
                                -- command returning data (such as a
                                -- SELECT or SHOW).
                | CopyOut       -- ^ Copy Out (from server) data
                                -- transfer started.
                | CopyIn        -- ^ Copy In (to server) data transfer
                                -- started.
                | BadResponse   -- ^ The server's response was not understood.
                | NonfatalError -- ^ A nonfatal error (a notice or
                                -- warning) occurred.
                | FatalError    -- ^ A fatal error occurred.
                  deriving (Eq, Show)

instance Enum ExecStatus where
    toEnum (#const PGRES_EMPTY_QUERY)    = EmptyQuery
    toEnum (#const PGRES_COMMAND_OK)     = CommandOk
    toEnum (#const PGRES_TUPLES_OK)      = TuplesOk
    toEnum (#const PGRES_COPY_OUT)       = CopyOut
    toEnum (#const PGRES_COPY_IN)        = CopyIn
    toEnum (#const PGRES_BAD_RESPONSE)   = BadResponse
    toEnum (#const PGRES_NONFATAL_ERROR) = NonfatalError
    toEnum (#const PGRES_FATAL_ERROR)    = FatalError
    toEnum _ = error "Database.PQ.Enum.ExecStatus.toEnum: bad argument"

    fromEnum EmptyQuery    = (#const PGRES_EMPTY_QUERY)
    fromEnum CommandOk     = (#const PGRES_COMMAND_OK)
    fromEnum TuplesOk      = (#const PGRES_TUPLES_OK)
    fromEnum CopyOut       = (#const PGRES_COPY_OUT)
    fromEnum CopyIn        = (#const PGRES_COPY_IN)
    fromEnum BadResponse   = (#const PGRES_BAD_RESPONSE)
    fromEnum NonfatalError = (#const PGRES_NONFATAL_ERROR)
    fromEnum FatalError    = (#const PGRES_FATAL_ERROR)

-- | Returns the result status of the command.
resultStatus :: Result
             -> IO ExecStatus
resultStatus result = enumFromResult result c_PQresultStatus


-- | Converts the 'ExecStatus' returned by 'resultStatus' into a
-- string describing the status code. The caller should not
-- free the result.
resStatus :: ExecStatus
          -> IO B.ByteString
resStatus es =
    do cstr <- c_PQresStatus $ fromIntegral $ fromEnum es
       len <- B.c_strlen cstr
       fp <- newForeignPtr_ $ castPtr cstr
       return $ B.fromForeignPtr fp 0 $ fromIntegral len


-- | Returns the error message most recently generated by an operation
-- on the connection.
resultErrorMessage :: Result
                   -> IO (Maybe B.ByteString)
resultErrorMessage = flip maybeBsFromResult c_PQresultErrorMessage


data FieldCode = DiagSeverity
               -- ^ The severity; the field contents are ERROR, FATAL,
               -- or PANIC (in an error message), or WARNING, NOTICE,
               -- DEBUG, INFO, or LOG (in a notice message), or a
               -- localized translation of one of these. Always
               -- present.

               | DiagSqlstate
               -- ^ The SQLSTATE code for the error. The SQLSTATE code
               -- identifies the type of error that has occurred; it
               -- can be used by front-end applications to perform
               -- specific operations (such as error handling) in
               -- response to a particular database error. For a list
               -- of the possible SQLSTATE codes, see Appendix A. This
               -- field is not localizable, and is always present.

               | DiagMessagePrimary
               -- ^ The primary human-readable error message
               -- (typically one line). Always present.

               | DiagMessageDetail
               -- ^ Detail: an optional secondary error message
               -- carrying more detail about the problem. Might run to
               -- multiple lines.

               | DiagMessageHint
               -- ^ Hint: an optional suggestion what to do about the
               -- problem. This is intended to differ from detail in
               -- that it offers advice (potentially inappropriate)
               -- rather than hard facts. Might run to multiple lines.

               | DiagStatementPosition
               -- ^ A string containing a decimal integer indicating
               -- an error cursor position as an index into the
               -- original statement string. The first character has
               -- index 1, and positions are measured in characters
               -- not bytes.

               | DiagInternalPosition
               -- ^ This is defined the same as the
               -- 'DiagStatementPosition' field, but it is used when
               -- the cursor position refers to an internally
               -- generated command rather than the one submitted by
               -- the client. The 'DiagInternalQuery' field will
               -- always appear when this field appears.

               | DiagInternalQuery
               -- ^ The text of a failed internally-generated
               -- command. This could be, for example, a SQL query
               -- issued by a PL/pgSQL function.

               | DiagContext
               -- ^ An indication of the context in which the error
               -- occurred. Presently this includes a call stack
               -- traceback of active procedural language functions
               -- and internally-generated queries. The trace is one
               -- entry per line, most recent first.

               | DiagSourceFile
               -- ^ The file name of the source-code location where
               -- the error was reported.

               | DiagSourceLine
               -- ^ The line number of the source-code location where
               -- the error was reported.

               | DiagSourceFunction
               -- ^ The name of the source-code function reporting the
               -- error.

               deriving (Eq, Show)


instance Enum FieldCode where
    toEnum (#const PG_DIAG_SEVERITY)           = DiagSeverity
    toEnum (#const PG_DIAG_SQLSTATE)           = DiagSqlstate
    toEnum (#const PG_DIAG_MESSAGE_PRIMARY)    = DiagMessagePrimary
    toEnum (#const PG_DIAG_MESSAGE_DETAIL)     = DiagMessageDetail
    toEnum (#const PG_DIAG_MESSAGE_HINT)       = DiagMessageHint
    toEnum (#const PG_DIAG_STATEMENT_POSITION) = DiagStatementPosition
    toEnum (#const PG_DIAG_INTERNAL_POSITION)  = DiagInternalPosition
    toEnum (#const PG_DIAG_INTERNAL_QUERY)     = DiagInternalQuery
    toEnum (#const PG_DIAG_CONTEXT)            = DiagContext
    toEnum (#const PG_DIAG_SOURCE_FILE)        = DiagSourceFile
    toEnum (#const PG_DIAG_SOURCE_LINE)        = DiagSourceLine
    toEnum (#const PG_DIAG_SOURCE_FUNCTION)    = DiagSourceFunction
    toEnum _ = error "Database.PQ.Enum.FieldCode.toEnum: bad argument"

    fromEnum DiagSeverity          = (#const PG_DIAG_SEVERITY)
    fromEnum DiagSqlstate          = (#const PG_DIAG_SQLSTATE)
    fromEnum DiagMessagePrimary    = (#const PG_DIAG_MESSAGE_PRIMARY)
    fromEnum DiagMessageDetail     = (#const PG_DIAG_MESSAGE_DETAIL)
    fromEnum DiagMessageHint       = (#const PG_DIAG_MESSAGE_HINT)
    fromEnum DiagStatementPosition = (#const PG_DIAG_STATEMENT_POSITION)
    fromEnum DiagInternalPosition  = (#const PG_DIAG_INTERNAL_POSITION)
    fromEnum DiagInternalQuery     = (#const PG_DIAG_INTERNAL_QUERY)
    fromEnum DiagContext           = (#const PG_DIAG_CONTEXT)
    fromEnum DiagSourceFile        = (#const PG_DIAG_SOURCE_FILE)
    fromEnum DiagSourceLine        = (#const PG_DIAG_SOURCE_LINE)
    fromEnum DiagSourceFunction    = (#const PG_DIAG_SOURCE_FUNCTION)


-- | Returns an individual field of an error report.
--
-- fieldcode is an error field identifier; see the symbols listed
-- below. 'Nothing' is returned if the PGresult is not an error or
-- warning result, or does not include the specified field. Field
-- values will normally not include a trailing newline.
--
-- The client is responsible for formatting displayed information to
-- meet its needs; in particular it should break long lines as
-- needed. Newline characters appearing in the error message fields
-- should be treated as paragraph breaks, not line breaks.
--
-- Errors generated internally by libpq will have severity and primary
-- message, but typically no other fields. Errors returned by a
-- pre-3.0-protocol server will include severity and primary message,
-- and sometimes a detail message, but no other fields.
--
-- Note that error fields are only available from 'Result' objects,
-- not 'Connection' objects; there is no errorField function.
resultErrorField :: Result
                 -> FieldCode
                 -> IO (Maybe B.ByteString)
resultErrorField (Result fp) fieldcode =
    maybeBsFromForeignPtr fp $ \res ->
        c_PQresultErrorField res $ fromIntegral $ fromEnum fieldcode


-- $queryresultinfo
-- These functions are used to extract information from a 'Result'
-- that represents a successful query result (that is, one that has
-- status 'TuplesOk'). They can also be used to extract information
-- from a successful Describe operation: a Describe's result has all
-- the same column information that actual execution of the query
-- would provide, but it has zero rows. For objects with other status
-- values, these functions will act as though the result has zero rows
-- and zero columns.

-- | Returns the number of rows (tuples) in the query result. Because
-- it returns an integer result, large result sets might overflow the
-- return value on 32-bit operating systems.
ntuples :: Result
        -> IO Int
ntuples (Result res) = withForeignPtr res (return . fromEnum . c_PQntuples)


-- | Returns the number of columns (fields) in each row of the query
-- result.
nfields :: Result
        -> IO Int
nfields (Result res) = withForeignPtr res (return . fromEnum . c_PQnfields)


newtype Column = Col CInt
newtype Row = Row CInt

toColumn :: (Integral a) => a -> Column
toColumn = Col . fromIntegral

toRow :: (Integral a) => a -> Row
toRow = Row . fromIntegral


-- | Returns the column name associated with the given 'Column'
-- number. Column numbers start at 0.
fname :: Result
      -> Column
      -> IO (Maybe B.ByteString)
fname result (Col colNum) =
    maybeBsFromResult result $ \fp ->
        c_PQfname fp colNum


-- | Returns the column number associated with the given column name.
fnumber :: Result
        -> B.ByteString
        -> IO (Maybe Int)
fnumber (Result res) columnName =
    do num <- withForeignPtr res $ \resPtr ->
              B.useAsCString columnName $ \columnNamePtr ->
                  c_PQfnumber resPtr columnNamePtr
       return $ if num == -1
                  then Nothing
                  else Just $ fromIntegral num


-- | Returns the OID of the table from which the given column was
-- fetched. Column numbers start at 0.
ftable :: Result
       -> Column
       -> IO Oid
ftable result (Col colNum) = withResult result $ \ptr -> c_PQftable ptr colNum


-- | Returns the column number (within its table) of the column making
-- up the specified query result column. Query-result column numbers
-- start at 0, but table columns have nonzero numbers.
ftablecol :: Result
          -> Column
          -> IO Column
ftablecol result (Col colNum) =
    fmap Col $ withResult result $ \p -> c_PQftablecol p colNum


-- | Returns the 'Format' of the given column. Column numbers start at
-- 0.
fformat :: Result
        -> Column
        -> IO Format
fformat result (Col colNum) =
    enumFromResult result $ \ptr -> c_PQfformat ptr colNum


-- | Returns the data type associated with the given column
-- number. The 'Oid' returned is the internal OID number of the
-- type. Column numbers start at 0.
--
-- You can query the system table pg_type to obtain the names and
-- properties of the various data types. The OIDs of the built-in data
-- types are defined in the file src/include/catalog/pg_type.h in the
-- source tree.
ftype :: Result
      -> Column
      -> IO Oid
ftype result (Col colNum) = withResult result $ \ptr -> c_PQftype ptr colNum


-- | Returns the type modifier of the column associated with the given
-- column number. Column numbers start at 0.
--
-- The interpretation of modifier values is type-specific; they
-- typically indicate precision or size limits. The value -1 is used
-- to indicate "no information available". Most data types do not use
-- modifiers, in which case the value is always -1.
fmod :: Result
     -> Column
     -> IO Int
fmod result (Col colNum) = numFromResult result $ \ptr -> c_PQfmod ptr colNum


-- | Returns the size in bytes of the column associated with the given
-- column number. Column numbers start at 0.
--
-- 'fsize' returns the space allocated for this column in a database
-- row, in other words the size of the server's internal
-- representation of the data type. (Accordingly, it is not really
-- very useful to clients.) A negative value indicates the data type
-- is variable-length.
fsize :: Result
      -> Column
      -> IO Int
fsize result (Col colNum) = numFromResult result $ \ptr -> c_PQfsize ptr colNum


-- | Returns a single field value of one row of a PGresult. Row and
-- column numbers start at 0.
--
-- For convenience, this binding uses 'getisnull' and 'getlength' to
-- help construct the result.
--
-- Note: The 'ByteString' returned holds a reference to the Result. As
-- long as ByteString is live, the Result will not be garbage
-- collected. 'getvalue'' returns a copy of the data.
getvalue :: Result
         -> Row
         -> Column
         -> IO (Maybe B.ByteString)
getvalue (Result fp) (Row rowNum) (Col colNum) =
    withForeignPtr fp $ \ptr -> do
      isnull <- c_PQgetisnull ptr rowNum colNum
      if toEnum $ fromIntegral isnull
        then return $ Nothing

        else do cstr <- c_PQgetvalue ptr rowNum colNum
                l <- c_PQgetlength ptr rowNum colNum
                fp' <- FC.newForeignPtr (castPtr cstr) finalizer
                return $ Just $ B.fromForeignPtr fp' 0 $ fromIntegral l

    where
      finalizer = touchForeignPtr fp


-- | Returns a copy of a single field value of one row of a
-- PGresult. Row and column numbers start at 0.
--
-- For convenience, this binding uses 'getisnull' and 'getlength' to
-- help construct the result.
getvalue' :: Result
          -> Row
          -> Column
          -> IO (Maybe B.ByteString)
getvalue' (Result fp) (Row rowNum) (Col colNum) =
    withForeignPtr fp $ \ptr -> do
      isnull <- c_PQgetisnull ptr rowNum colNum
      if toEnum $ fromIntegral isnull
        then return $ Nothing

        else do cstr <- c_PQgetvalue ptr rowNum colNum
                l <- fromIntegral `fmap` c_PQgetlength ptr rowNum colNum
                Just `fmap` B.packCStringLen (cstr, l)


-- | Tests a field for a null value. Row and column numbers start at
-- 0.
getisnull :: Result
          -> Row
          -> Column
          -> IO Bool
getisnull result (Row rowNum) (Col colNum) =
    enumFromResult result $ \ptr ->
        c_PQgetisnull ptr rowNum colNum


-- | Returns the actual length of a field value in bytes. Row and
-- column numbers start at 0.
--
-- This is the actual data length for the particular data value, that
-- is, the size of the object pointed to by 'getvalue'. For text data
-- format this is the same as strlen(). For binary format this is
-- essential information. Note that one should not rely on 'fsize' to
-- obtain the actual data length.
getlength :: Result
          -> Row
          -> Column
          -> IO Int
getlength result (Row rowNum) (Col colNum) =
    numFromResult result $ \ptr ->
      c_PQgetlength ptr rowNum colNum


-- | Returns the number of parameters of a prepared statement.
--
-- This function is only useful when inspecting the result of
-- PQdescribePrepared. For other types of queries it will return zero.
nparams :: Result
        -> IO Int
nparams result = numFromResult result c_PQnparams


-- | Returns the data type of the indicated statement
-- parameter. Parameter numbers start at 0.
--
-- This function is only useful when inspecting the result of
-- 'describePrepared'. For other types of queries it will return zero.
paramtype :: Result
          -> Int -- ^ param_number
          -> IO Oid
paramtype result param_number =
    withResult result $ \p -> c_PQparamtype p $ fromIntegral param_number


data PrintOpt = PrintOpt {
      poHeader     :: Bool -- ^ print output field headings and row count
    , poAlign      :: Bool -- ^ fill align the fields
    , poStandard   :: Bool -- ^ old brain dead format
    , poHtml3      :: Bool -- ^ output HTML tables
    , poExpanded   :: Bool -- ^ expand tables
    , poPager      :: Bool -- ^ use pager for output if needed
    , poFieldSep   :: B.ByteString   -- ^ field separator
    , poTableOpt   :: B.ByteString   -- ^ attributes for HTML table element
    , poCaption    :: B.ByteString   -- ^ HTML table caption
    , poFieldName  :: [B.ByteString] -- ^ list of replacement field names
    }


defaultPrintOpt :: PrintOpt
defaultPrintOpt = PrintOpt True True False False False False "|" "" "" []


#let alignment t = "%lu", (unsigned long)offsetof(struct {char x__; t (y__); }, y__)
instance Storable PrintOpt where
  sizeOf _ = #{size PQprintOpt}

  alignment _ = #{alignment PQprintOpt}

  peek ptr = do
      a <- fmap pqbool $ #{peek PQprintOpt, header  } ptr
      b <- fmap pqbool $ #{peek PQprintOpt, align   } ptr
      c <- fmap pqbool $ #{peek PQprintOpt, standard} ptr
      d <- fmap pqbool $ #{peek PQprintOpt, html3   } ptr
      e <- fmap pqbool $ #{peek PQprintOpt, expanded} ptr
      f <- fmap pqbool $ #{peek PQprintOpt, pager   } ptr
      g <- B.packCString =<< #{peek PQprintOpt, fieldSep} ptr
      h <- B.packCString =<< #{peek PQprintOpt, tableOpt} ptr
      i <- B.packCString =<< #{peek PQprintOpt, caption} ptr
      j <- #{peek PQprintOpt, fieldName} ptr
      j' <- peekArray0 nullPtr j
      j'' <- mapM B.packCString j'
      return $ PrintOpt a b c d e f g h i j''
      where
        pqbool :: CChar -> Bool
        pqbool = toEnum . fromIntegral

  poke ptr (PrintOpt a b c d e f g h i j) =
      B.useAsCString g $ \g' -> do
        B.useAsCString h $ \h' -> do
          B.useAsCString i $ \i' -> do
            withMany B.useAsCString j $ \j' ->
              withArray0 nullPtr j' $ \j'' -> do
                let a' = (fromIntegral $ fromEnum a)::CChar
                    b' = (fromIntegral $ fromEnum b)::CChar
                    c' = (fromIntegral $ fromEnum c)::CChar
                    d' = (fromIntegral $ fromEnum d)::CChar
                    e' = (fromIntegral $ fromEnum e)::CChar
                    f' = (fromIntegral $ fromEnum f)::CChar
                #{poke PQprintOpt, header}    ptr a'
                #{poke PQprintOpt, align}     ptr b'
                #{poke PQprintOpt, standard}  ptr c'
                #{poke PQprintOpt, html3}     ptr d'
                #{poke PQprintOpt, expanded}  ptr e'
                #{poke PQprintOpt, pager}     ptr f'
                #{poke PQprintOpt, fieldSep}  ptr g'
                #{poke PQprintOpt, tableOpt}  ptr h'
                #{poke PQprintOpt, caption}   ptr i'
                #{poke PQprintOpt, fieldName} ptr j''


-- | Prints out all the rows and, optionally, the column names to the
-- specified output stream.
--
-- This function was formerly used by psql to print query results, but
-- this is no longer the case. Note that it assumes all the data is in
-- text format.
print :: Handle
      -> Result
      -> PrintOpt
      -> IO ()
print h (Result res) po =
    withForeignPtr res $ \resPtr -> do
      B.useAsCString "w" $ \mode -> do
        with po $ \poPtr -> do
          dup_h <- hDuplicate h
          fd <- handleToFd dup_h
          threadWaitWrite fd
          cfile <- c_fdopen (fromIntegral fd) mode
          c_PQprint cfile resPtr poPtr


-- $othercommands
-- These functions are used to extract other information from PGresult
-- objects.

-- | Returns the command status tag from the SQL command that
-- generated the PGresult.
--
-- Commonly this is just the name of the command, but it might include
-- additional data such as the number of rows processed.
cmdStatus :: Result
          -> IO (Maybe B.ByteString)
cmdStatus = flip maybeBsFromResult c_PQcmdStatus


-- | Returns the number of rows affected by the SQL command.
--
-- This function returns a string containing the number of rows
-- affected by the SQL statement that generated the 'Result'. This
-- function can only be used following the execution of a SELECT,
-- CREATE TABLE AS, INSERT, UPDATE, DELETE, MOVE, FETCH, or COPY
-- statement, or an EXECUTE of a prepared query that contains an
-- INSERT, UPDATE, or DELETE statement. If the command that generated
-- the 'Result' was anything else, 'cmdTuples' returns an empty
-- string.
cmdTuples :: Result
          -> IO (Maybe B.ByteString)
cmdTuples = flip maybeBsFromResult c_PQcmdTuples


-- | Returns the 'Oid' of the inserted row, if the SQL command was an
-- INSERT that inserted exactly one row into a table that has OIDs, or
-- a EXECUTE of a prepared query containing a suitable INSERT
-- statement. Otherwise, this function returns 'Nothing'. This
-- function will also return 'Nothing' if the table affected by the
-- INSERT statement does not contain OIDs.
oidValue :: Result
         -> IO (Maybe Oid)
oidValue result =
    withResult result $ \ptr ->
        do oid <- c_PQoidValue ptr
           return $ case oid of
                      (#const InvalidOid) -> Nothing
                      _                   -> Just oid


-- | Escapes a string for use within an SQL command. This is useful
-- when inserting data values as literal constants in SQL
-- commands. Certain characters (such as quotes and backslashes) must
-- be escaped to prevent them from being interpreted specially by the
-- SQL parser.
escapeStringConn :: Connection
                 -> B.ByteString
                 -> IO (Maybe B.ByteString)
escapeStringConn connection bs =
    withConn connection $ \conn ->
        B.unsafeUseAsCStringLen bs $ \(from, bslen) ->
            alloca $ \err -> do
              to <- mallocBytes (bslen*2+1)
              num <- c_PQescapeStringConn conn to from (fromIntegral bslen) err
              stat <- peek err
              case stat of
                0 -> do let i = fromIntegral num
                        tore <- reallocBytes to i
                        tofp <- newForeignPtr finalizerFree tore
                        return $ Just $ B.fromForeignPtr tofp 0 i
                _ -> do free to
                        return Nothing


-- | Escapes binary data for use within an SQL command with the type
-- bytea. As with 'escapeStringConn', this is only used when inserting
-- data directly into an SQL command string.
escapeByteaConn :: Connection
                -> B.ByteString
                -> IO (Maybe B.ByteString)
escapeByteaConn connection bs =
    withConn connection $ \conn ->
        B.unsafeUseAsCStringLen bs $ \(from, bslen) ->
            alloca $ \to_length -> do
              to <- c_PQescapeByteaConn conn from (fromIntegral bslen) to_length
              if to == nullPtr
                then return Nothing
                else do tofp <- newForeignPtr p_PQfreemem to
                        l <- peek to_length
                        return $ Just $ B.fromForeignPtr tofp 0 ((fromIntegral l) - 1)



-- | Converts a 'ByteString' representation of binary data into binary
-- data - the reverse of 'PQescapeByteaConn'. This is needed when
-- retrieving bytea data in text format, but not when retrieving it in
-- binary format.
--
-- The parameter points to a string such as might be returned by
-- 'getvalue' when applied to a bytea column. 'unescapeBytea' converts
-- this string representation into its binary representation. It
-- returns a 'ByteString', or 'Nothing' on error.
--
-- This conversion is not exactly the inverse of 'escapeByteaConn',
-- because the string is not expected to be "escaped" when received
-- from 'getvalue'. In particular this means there is no need for
-- string quoting considerations, and so no need for a 'Connection'
-- parameter.
unescapeBytea :: B.ByteString
              -> IO (Maybe B.ByteString)
unescapeBytea bs =
    B.useAsCString bs $ \from ->
        alloca $ \to_length -> do
          to <- c_PQunescapeBytea from to_length
          if to == nullPtr
            then return Nothing
            else do tofp <- newForeignPtr p_PQfreemem to
                    l <- peek to_length
                    return $ Just $ B.fromForeignPtr tofp 0 $ fromIntegral l


-- $asynccommand
-- The 'exec' function is adequate for submitting commands in normal,
-- synchronous applications. It has a couple of deficiencies, however,
-- that can be of importance to some users:
--
--   * 'exec' waits for the command to be completed. The application
--   might have other work to do (such as maintaining a user
--   interface), in which case it won't want to block waiting for the
--   response.
--
--   * Since the execution of the client application is suspended
--   while it waits for the result, it is hard for the application to
--   decide that it would like to try to cancel the ongoing
--   command. (It can be done from a signal handler, but not
--   otherwise.)
--
--   * 'exec' can return only one 'Result'. If the submitted command
--   string contains multiple SQL commands, all but the last 'Result'
--   are discarded by 'exec'.
--
-- Applications that do not like these limitations can instead use the
-- underlying functions that 'exec' is built from: 'sendQuery' and
-- 'getResult'. There are also 'sendQueryParams', 'sendPrepare',
-- 'sendQueryPrepared', 'sendDescribePrepared', and
-- 'sendDescribePortal', which can be used with 'getResult' to
-- duplicate the functionality of 'execParams', 'prepare',
-- 'execPrepared', 'describePrepared', and 'describePortal'
-- respectively.

-- | Submits a command to the server without waiting for the
-- result(s). 'True' is returned if the command was successfully
-- dispatched and 'False' if not (in which case, use 'errorMessage' to
-- get more information about the failure).
sendQuery :: Connection
          -> B.ByteString
          -> IO Bool
sendQuery connection query =
    enumFromConn connection $ \p ->
        B.useAsCString query $ c_PQsendQuery p


-- | Submits a command and separate parameters to the server without
-- waiting for the result(s).
sendQueryParams :: Connection
                -> B.ByteString
                -> [Maybe (Oid, B.ByteString, Format)]
                -> Format
                -> IO Bool
sendQueryParams connection statement params rFmt =
    do let (oids, values, lengths, formats) =
               foldl' accum ([],[],[],[]) $ reverse params
           c_lengths = map toEnum lengths :: [CInt]
           n = toEnum $ length params
           f = toEnum $ fromEnum rFmt
       enumFromConn connection $ \c ->
           B.useAsCString statement $ \s ->
               withArray oids $ \ts ->
                   withMany (maybeWith B.useAsCString) values $ \c_values ->
                       withArray c_values $ \vs ->
                           withArray c_lengths $ \ls ->
                               withArray formats $ \fs ->
                                   c_PQsendQueryParams c s n ts vs ls fs f

    where
      accum (a,b,c,d) Nothing = ( 0:a
                                , Nothing:b
                                , 0:c
                                , 0:d
                                )
      accum (a,b,c,d) (Just (t,v,f)) = ( t:a
                                       , (Just v):b
                                       , (B.length v):c
                                       , (toEnum $ fromEnum f):d
                                       )


-- | Sends a request to create a prepared statement with the given
-- parameters, without waiting for completion.
sendPrepare :: Connection
            -> B.ByteString
            -> B.ByteString
            -> Maybe [Oid]
            -> IO Bool
sendPrepare connection stmtName query mParamTypes =
    enumFromConn connection $ \c ->
        B.useAsCString stmtName $ \s ->
            B.useAsCString query $ \q ->
                maybeWith withArray mParamTypes $ \o ->
                    let l = maybe 0 (toEnum . length) mParamTypes
                    in c_PQsendPrepare c s q l o


-- | Sends a request to execute a prepared statement with given
-- parameters, without waiting for the result(s).
sendQueryPrepared :: Connection
                  -> B.ByteString
                  -> [Maybe (B.ByteString, Format)]
                  -> Format
                  -> IO Bool
sendQueryPrepared connection stmtName mPairs rFmt =
    do let (values, lengths, formats) = foldl' accum ([],[],[]) $ reverse mPairs
           c_lengths = map toEnum lengths :: [CInt]
           n = toEnum $ length mPairs
           f = toEnum $ fromEnum rFmt
       enumFromConn connection $ \c ->
           B.useAsCString stmtName $ \s ->
               withMany (maybeWith B.useAsCString) values $ \c_values ->
                   withArray c_values $ \vs ->
                       withArray c_lengths $ \ls ->
                           withArray formats $ \fs ->
                               c_PQsendQueryPrepared c s n vs ls fs f

    where
      accum (a,b,c) Nothing       = ( Nothing:a
                                    , 0:b
                                    , 0:c
                                    )
      accum (a,b,c) (Just (v, f)) = ( (Just v):a
                                    , (B.length v):b
                                    , (toEnum $ fromEnum f):c
                                    )


-- | Submits a request to obtain information about the specified
-- prepared statement, without waiting for completion.
--
-- This is an asynchronous version of 'describePrepared': it returns
-- 'True' if it was able to dispatch the request, and 'False' if
-- not. After a successful call, call 'getResult' to obtain the
-- results. The function's parameters are handled identically to
-- 'describePrepared'. Like 'describePrepared', it will not work on
-- 2.0-protocol connections.
sendDescribePrepared :: Connection
                     -> B.ByteString -- ^ stmtName
                     -> IO Bool
sendDescribePrepared connection stmtName =
    enumFromConn connection $ \c ->
        B.useAsCString stmtName $ \s ->
            c_PQsendDescribePrepared c s


-- | Submits a request to obtain information about the specified
-- portal, without waiting for completion.
--
-- This is an asynchronous version of 'describePortal': it returns
-- 'True' if it was able to dispatch the request, and 'False' if
-- not. After a successful call, call 'getResult' to obtain the
-- results. The function's parameters are handled identically to
-- 'describePortal'. Like 'describePortal', it will not work on
-- 2.0-protocol connections.
sendDescribePortal :: Connection
                     -> B.ByteString -- ^ portalName
                     -> IO Bool
sendDescribePortal connection portalName =
    enumFromConn connection $ \c ->
        B.useAsCString portalName $ \p ->
            c_PQsendDescribePortal c p


-- | Waits for the next result from a prior 'sendQuery',
-- 'sendQueryParams', 'sendPrepare', or 'sendQueryPrepared' call, and
-- returns it. A null pointer is returned when the command is complete
-- and there will be no more results.
getResult :: Connection
          -> IO (Maybe Result)
getResult connection =
    do resPtr <- withConn connection c_PQgetResult
       if resPtr == nullPtr
           then return Nothing
           else (Just . Result) `fmap` newForeignPtr p_PQclear resPtr


-- | If input is available from the server, consume it.
--
-- 'consumeInput' normally returns 'True' indicating "no error", but
-- returns 'False' if there was some kind of trouble (in which case
-- 'errorMessage' can be consulted). Note that the result does not say
-- whether any input data was actually collected. After calling
-- 'consumeInput', the application can check 'isBusy' and/or
-- 'notifies' to see if their state has changed.
consumeInput :: Connection
             -> IO Bool
consumeInput connection = enumFromConn connection c_PQconsumeInput


-- | Returns True if a command is busy, that is, getResult would block
-- waiting for input. A False return indicates that getResult can be
-- called with assurance of not blocking.
--
-- 'isBusy' will not itself attempt to read data from the server;
-- therefore 'consumeInput' must be invoked first, or the busy state
-- will never end.
isBusy :: Connection
       -> IO Bool
isBusy connection = enumFromConn connection c_PQisBusy


-- | Sets the nonblocking status of the connection.
setnonblocking :: Connection
               -> Bool
               -> IO Bool
setnonblocking connection blocking =
    do let arg = fromIntegral $ fromEnum blocking
       stat <- withConn connection $ \ptr -> c_PQsetnonblocking ptr arg
       return $ stat == 0


-- | Returns the blocking status of the database connection.
isnonblocking :: Connection
              -> IO Bool
isnonblocking connection = enumFromConn connection c_PQisnonblocking


data FlushStatus = FlushOk
                 | FlushFailed
                 | FlushWriting
                   deriving (Eq, Show)

-- | Attempts to flush any queued output data to the server. Returns
-- 'FlushOk' if successful (or if the send queue is empty),
-- 'FlushFailed' if it failed for some reason, or 'FlushWriting' if it
-- was unable to send all the data in the send queue yet (this case
-- can only occur if the connection is nonblocking).
flush :: Connection
      -> IO FlushStatus
flush connection =
    do stat <- withConn connection c_PQflush
       return $ case stat of
                  0 -> FlushOk
                  1 -> FlushWriting
                  _ -> FlushFailed


-- $cancel
-- A client application can request cancellation of a command that is
-- still being processed by the server, using the functions described
-- in this section.

-- | Contains the information needed to cancel a command issued
-- through a particular database connection.
newtype Cancel = Cancel (ForeignPtr PGcancel) deriving Eq
data PGcancel


-- | Creates a data structure containing the information needed to
-- cancel a command issued through a particular database connection.
--
-- 'getCancel' creates a 'Cancel' object given a 'Connection'. It will
-- return 'Nothing' if the given conn is an invalid connection.

getCancel :: Connection
          -> IO (Maybe Cancel)
getCancel connection =
    withConn connection $ \conn ->
        do ptr <- c_PQgetCancel conn
           if ptr == nullPtr
             then return Nothing
             else do fp <- newForeignPtr p_PQfreeCancel ptr
                     return $ Just $ Cancel fp


-- | Requests that the server abandon processing of the current
-- command.
--
-- The return value is 'Right ()' if the cancel request was
-- successfully dispatched and if not, 'Left B.ByteString' containing
-- an error message explaining why not.
--
-- Successful dispatch is no guarantee that the request will have any
-- effect, however. If the cancellation is effective, the current
-- command will terminate early and return an error result. If the
-- cancellation fails (say, because the server was already done
-- processing the command), then there will be no visible result at
-- all.
cancel :: Cancel
       -> IO (Either B.ByteString ())
cancel (Cancel fp) =
    withForeignPtr fp $ \ptr ->
        do errbuf <- mallocBytes errbufsize
           res <- c_PQcancel ptr errbuf $ fromIntegral errbufsize
           case res of
             1 -> do free errbuf
                     return $ Right ()

             _ -> do l <- fromIntegral `fmap` B.c_strlen errbuf
                     fp' <- newForeignPtr finalizerFree $ castPtr errbuf
                     return $ Left $ B.fromForeignPtr fp' 0 l

    where
      errbufsize = 256


-- $asyncnotification
-- PostgreSQL offers asynchronous notification via the LISTEN and
-- NOTIFY commands. A client session registers its interest in a
-- particular notification channel with the LISTEN command (and can
-- stop listening with the UNLISTEN command). All sessions listening
-- on a particular channel will be notified asynchronously when a
-- NOTIFY command with that channel name is executed by any session. A
-- \"payload\" string can be passed to communicate additional data to
-- the listeners.
--
-- libpq applications submit LISTEN, UNLISTEN, and NOTIFY commands as
-- ordinary SQL commands. The arrival of NOTIFY messages can
-- subsequently be detected by calling 'notifies'.

data Notify = Notify {
      notifyRelname :: B.ByteString -- ^ notification channel name
    , notifyBePid   :: CPid         -- ^ process ID of notifying server process
    , notifyExtra   :: B.ByteString -- ^ notification payload string
    }

instance Storable Notify where
  sizeOf _ = #{size PGnotify}

  alignment _ = #{alignment PGnotify}

  peek ptr = do
      relname <- B.packCString =<< #{peek PGnotify, relname} ptr
      extra   <- B.packCString =<< #{peek PGnotify, extra} ptr
      be_pid  <- fmap f $ #{peek PGnotify, be_pid} ptr
      return $ Notify relname be_pid extra
      where
        f :: CInt -> CPid
        f = fromIntegral

  poke ptr (Notify a b c) =
      B.useAsCString a $ \a' ->
        B.useAsCString c $ \c' ->
            do #{poke PGnotify, relname} ptr a'
               #{poke PGnotify, be_pid}  ptr (fromIntegral b :: CInt)
               #{poke PGnotify, extra}   ptr c'


-- | Returns the next notification from a list of unhandled
-- notification messages received from the server. It returns a
-- 'Nothing' if there are no pending notifications. Once a
-- notification is returned from notifies, it is considered handled
-- and will be removed from the list of notifications.
notifies :: Connection
         -> IO (Maybe Notify)
notifies connection =
    withConn connection $ \ptr ->
        do mn <- c_PQnotifies ptr
           if mn == nullPtr
             then return Nothing
             else Just `fmap` peek mn



-- $control
-- These functions control miscellaneous details of libpq's behavior.

-- | Returns the client encoding.
clientEncoding :: Connection
               -> IO B.ByteString
clientEncoding connection =
    withConn connection $ \ptr ->
        do i <- c_PQclientEncoding ptr
           cstr <- c_pg_encoding_to_char i
           len <- B.c_strlen cstr
           fp <- newForeignPtr_ $ castPtr cstr
           return $ B.fromForeignPtr fp 0 $ fromIntegral len


-- | Sets the client encoding.
setClientEncoding :: Connection -> B.ByteString -> IO Bool
setClientEncoding connection enc =
    do stat <- withConn connection $ \c ->
               B.useAsCString enc $ \s ->
                   c_PQsetClientEncoding c s

       return $ stat == 0


data Verbosity = ErrorsTerse
               | ErrorsDefault
               | ErrorsVerbose deriving (Show, Eq)

instance Enum Verbosity where
    toEnum (#const PQERRORS_TERSE)   = ErrorsTerse
    toEnum (#const PQERRORS_DEFAULT) = ErrorsDefault
    toEnum (#const PQERRORS_VERBOSE) = ErrorsVerbose
    toEnum _ = error "Database.PQ.Enum.Verbosity.toEnum: bad argument"

    fromEnum ErrorsTerse   = (#const PQERRORS_TERSE)
    fromEnum ErrorsDefault = (#const PQERRORS_DEFAULT)
    fromEnum ErrorsVerbose = (#const PQERRORS_VERBOSE)


-- | Determines the verbosity of messages returned by 'errorMessage'
-- and 'resultErrorMessage'.
--
-- 'setErrorVerbosity' sets the verbosity mode, returning the
-- connection's previous setting. In 'ErrorsTerse' mode, returned
-- messages include severity, primary text, and position only; this
-- will normally fit on a single line. The default mode produces
-- messages that include the above plus any detail, hint, or context
-- fields (these might span multiple lines). The 'ErrorsVerbose' mode
-- includes all available fields. Changing the verbosity does not
-- affect the messages available from already-existing 'Result'
-- objects, only subsequently-created ones.
setErrorVerbosity :: Connection
                  -> Verbosity
                  -> IO Verbosity
setErrorVerbosity connection verbosity =
    enumFromConn connection $ \p ->
        c_PQsetErrorVerbosity p $ fromIntegral $ fromEnum verbosity


-- | Enables tracing of the client/server communication to a debugging
-- file stream.
--
-- Note: On Windows, if the libpq library and an application are
-- compiled with different flags, this function call will crash the
-- application because the internal representation of the FILE
-- pointers differ. Specifically, multithreaded/single-threaded,
-- release/debug, and static/dynamic flags should be the same for the
-- library and all applications using that library.
trace :: Connection
      -> Handle
      -> IO ()
trace connection h =
    withConn connection $ \ptr ->
      B.useAsCString "w" $ \mode -> do
        dup_h <- hDuplicate h
        fd <- handleToFd dup_h
        cfile <- c_fdopen (fromIntegral fd) mode
        c_PQtrace ptr cfile


-- | Disables tracing started by PQtrace.
untrace :: Connection
        -> IO ()
untrace connection = withConn connection c_PQuntrace


withConn :: Connection
         -> (Ptr PGconn -> IO b)
         -> IO b
withConn connection f =
    withConn' connection $ flip withForeignPtr f


withConn' :: Connection
          -> (ForeignPtr PGconn -> IO b)
          -> IO b
withConn' (Conn mvar) f =
    withMVar mvar $ \mFp ->
        case mFp of
          Nothing -> error "Database connection has been closed"
          Just fp -> f fp




enumFromConn :: (Integral a, Enum b) => Connection
             -> (Ptr PGconn -> IO a)
             -> IO b
enumFromConn connection f = fmap (toEnum . fromIntegral) $ withConn connection f


resultFromConn :: Connection
               -> (Ptr PGconn -> IO (Ptr PGresult))
               -> IO (Maybe Result)
resultFromConn connection f =
    do resPtr <- withConn connection f
       if resPtr == nullPtr
           then return Nothing
           else (Just . Result) `fmap` newForeignPtr p_PQclear resPtr


withResult :: Result
           -> (Ptr PGresult -> IO b)
           -> IO b
withResult (Result fp) f = withForeignPtr fp f


numFromResult :: (Integral a, Num b) => Result
              -> (Ptr PGresult -> IO a)
              -> IO b
numFromResult result f = fmap fromIntegral $ withResult result f


enumFromResult :: (Integral a, Enum b) => Result
               -> (Ptr PGresult -> IO a)
               -> IO b
enumFromResult result f = fmap (toEnum . fromIntegral) $ withResult result f


-- | Returns a ByteString with a finalizer that touches the ForeignPtr
-- PGconn that \"owns\" the CString to keep it alive.
--
-- The CString must be a null terminated c string. nullPtrs are
-- treated as 'Nothing'.
maybeBsFromConn :: Connection
                -> (Ptr PGconn -> IO CString)
                -> IO (Maybe B.ByteString)
maybeBsFromConn connection f =
    withConn' connection $ \fp -> maybeBsFromForeignPtr fp f


-- | Returns a ByteString with a finalizer that touches the ForeignPtr
-- PGresult that \"owns\" the CString to keep it alive.
--
-- The CString must be a null terminated c string. nullPtrs are
-- treated as 'Nothing'.
maybeBsFromResult :: Result
                  -> (Ptr PGresult -> IO CString)
                  -> IO (Maybe B.ByteString)
maybeBsFromResult (Result res) f = maybeBsFromForeignPtr res f

-- | Returns a ByteString with a finalizer that touches the ForeignPtr
-- that \"owns\" the CString to keep it alive.
--
-- The CString must be a null terminated c string. nullPtrs are
-- treated as 'Nothing'.
maybeBsFromForeignPtr :: ForeignPtr a
                      -> (Ptr a -> IO CString)
                      -> IO (Maybe B.ByteString)
maybeBsFromForeignPtr fp f =
    withForeignPtr fp $ \p ->
        do cstr <- f p
           if cstr == nullPtr
             then return Nothing
             else do l <- fromIntegral `fmap` B.c_strlen cstr
                     fp' <- FC.newForeignPtr (castPtr cstr) finalizer
                     return $ Just $ B.fromForeignPtr fp' 0 l
    where
      finalizer = touchForeignPtr fp

-- -- | Returns a ByteString with a finalizer that touches the ForeignPtr
-- -- that \"owns\" the CStringLen to keep it alive.
-- bsFromForeignPtrLen :: ForeignPtr a
--                     -> (Ptr a -> IO CStringLen)
--                     -> IO B.ByteString
-- bsFromForeignPtrLen fp f =
--     withForeignPtr fp $ \p ->
--         do (cstr, l) <- f p
--            if cstr == nullPtr
--              then return ""
--              else do fp' <- FC.newForeignPtr (castPtr cstr) finalizer
--                      return $ B.fromForeignPtr fp' 0 l
--     where
--       finalizer = touchForeignPtr fp


foreign import ccall safe "libpq-fe.h PQconnectdb"
    c_PQconnectdb :: CString ->IO (Ptr PGconn)

foreign import ccall safe "libpq-fe.h PQconnectStart"
    c_PQconnectStart :: CString ->IO (Ptr PGconn)

foreign import ccall safe "libpq-fe.h PQconnectPoll"
    c_PQconnectPoll :: Ptr PGconn ->IO CInt

foreign import ccall safe "libpq-fe.h PQdb"
    c_PQdb :: Ptr PGconn -> IO CString

foreign import ccall safe "libpq-fe.h PQuser"
    c_PQuser :: Ptr PGconn -> IO CString

foreign import ccall safe "libpq-fe.h PQpass"
    c_PQpass :: Ptr PGconn -> IO CString

foreign import ccall safe "libpq-fe.h PQhost"
    c_PQhost :: Ptr PGconn -> IO CString

foreign import ccall safe "libpq-fe.h PQport"
    c_PQport :: Ptr PGconn -> IO CString

foreign import ccall safe "libpq-fe.h PQoptions"
    c_PQoptions :: Ptr PGconn -> IO CString

foreign import ccall safe "libpq-fe.h PQbackendPID"
    c_PQbackendPID :: Ptr PGconn -> IO CInt

foreign import ccall safe "libpq-fe.h PQconnectionNeedsPassword"
    c_PQconnectionNeedsPassword :: Ptr PGconn -> IO CInt

foreign import ccall safe "libpq-fe.h PQconnectionUsedPassword"
    c_PQconnectionUsedPassword :: Ptr PGconn -> IO CInt

foreign import ccall safe "libpq-fe.h PQstatus"
    c_PQstatus :: Ptr PGconn -> IO CInt

foreign import ccall safe "libpq-fe.h PQtransactionStatus"
    c_PQtransactionStatus :: Ptr PGconn -> IO CInt

foreign import ccall safe "libpq-fe.h PQparameterStatus"
    c_PQparameterStatus :: Ptr PGconn -> CString -> IO CString

foreign import ccall safe "libpq-fe.h PQprotocolVersion"
    c_PQprotocolVersion :: Ptr PGconn -> IO CInt

foreign import ccall safe "libpq-fe.h PQserverVersion"
    c_PQserverVersion :: Ptr PGconn -> IO CInt

foreign import ccall safe "libpq-fe.h PQsocket"
    c_PQsocket :: Ptr PGconn -> IO CInt

foreign import ccall safe "libpq-fe.h PQerrorMessage"
    c_PQerrorMessage :: Ptr PGconn -> IO CString

foreign import ccall safe "libpq-fe.h &PQfinish"
    p_PQfinish :: FunPtr (Ptr PGconn -> IO ())

foreign import ccall safe "libpq-fe.h PQreset"
    c_PQreset :: Ptr PGconn -> IO ()

foreign import ccall safe "libpq-fe.h PQresetStart"
    c_PQresetStart :: Ptr PGconn ->IO CInt

foreign import ccall safe "libpq-fe.h PQresetPoll"
    c_PQresetPoll :: Ptr PGconn ->IO CInt

foreign import ccall safe "libpq-fe.h PQclientEncoding"
    c_PQclientEncoding :: Ptr PGconn -> IO CInt

foreign import ccall safe "libpq-fe.h pg_encoding_to_char"
    c_pg_encoding_to_char :: CInt -> IO CString

foreign import ccall safe "libpq-fe.h PQsetClientEncoding"
    c_PQsetClientEncoding :: Ptr PGconn -> CString -> IO CInt

type PGVerbosity = CInt
foreign import ccall safe "libpq-fe.h PQsetErrorVerbosity"
    c_PQsetErrorVerbosity :: Ptr PGconn -> PGVerbosity -> IO PGVerbosity

foreign import ccall safe "libpq-fe.h PQtrace"
    c_PQtrace :: Ptr PGconn -> Ptr CFile -> IO ()

foreign import ccall safe "libpq-fe.h PQuntrace"
    c_PQuntrace :: Ptr PGconn -> IO ()

foreign import ccall safe "libpq-fe.h PQsendQuery"
    c_PQsendQuery :: Ptr PGconn -> CString ->IO CInt

foreign import ccall safe "libpq-fe.h PQsendQueryParams"
    c_PQsendQueryParams :: Ptr PGconn -> CString -> CInt -> Ptr Oid
                        -> Ptr CString -> Ptr CInt -> Ptr CInt -> CInt
                        -> IO CInt

foreign import ccall safe "libpq-fe.h PQsendPrepare"
    c_PQsendPrepare :: Ptr PGconn -> CString -> CString -> CInt -> Ptr Oid
                    -> IO CInt

foreign import ccall safe "libpq-fe.h PQsendQueryPrepared"
    c_PQsendQueryPrepared :: Ptr PGconn -> CString -> CInt -> Ptr CString
                          -> Ptr CInt -> Ptr CInt -> CInt -> IO CInt

foreign import ccall safe "libpq-fe.h PQsendDescribePrepared"
    c_PQsendDescribePrepared :: Ptr PGconn -> CString -> IO CInt

foreign import ccall safe "libpq-fe.h PQsendDescribePortal"
    c_PQsendDescribePortal :: Ptr PGconn -> CString -> IO CInt

foreign import ccall safe "libpq-fe.h PQflush"
    c_PQflush :: Ptr PGconn -> IO CInt

foreign import ccall safe "libpq-fe.h PQgetCancel"
    c_PQgetCancel :: Ptr PGconn -> IO (Ptr PGcancel)

foreign import ccall safe "libpq-fe.h &PQfreeCancel"
    p_PQfreeCancel :: FunPtr (Ptr PGcancel -> IO ())

foreign import ccall safe "libpq-fe.h PQcancel"
    c_PQcancel :: Ptr PGcancel -> CString -> CInt -> IO CInt

foreign import ccall safe "libpq-fe.h PQnotifies"
    c_PQnotifies :: Ptr PGconn -> IO (Ptr Notify)

foreign import ccall safe "libpq-fe.h PQconsumeInput"
    c_PQconsumeInput :: Ptr PGconn ->IO CInt

foreign import ccall safe "libpq-fe.h PQisBusy"
    c_PQisBusy :: Ptr PGconn -> IO CInt

foreign import ccall safe "libpq-fe.h PQsetnonblocking"
    c_PQsetnonblocking :: Ptr PGconn -> CInt -> IO CInt

foreign import ccall safe "libpq-fe.h PQisnonblocking"
    c_PQisnonblocking :: Ptr PGconn -> IO CInt

foreign import ccall safe "libpq-fe.h PQgetResult"
    c_PQgetResult :: Ptr PGconn -> IO (Ptr PGresult)

foreign import ccall safe "libpq-fe.h PQexec"
    c_PQexec :: Ptr PGconn -> CString -> IO (Ptr PGresult)

foreign import ccall safe "libpq-fe.h PQexecParams"
    c_PQexecParams :: Ptr PGconn -> CString -> CInt -> Ptr Oid
                   -> Ptr CString -> Ptr CInt -> Ptr CInt -> CInt
                   -> IO (Ptr PGresult)

foreign import ccall safe "libpq-fe.h PQprepare"
    c_PQprepare :: Ptr PGconn -> CString -> CString -> CInt -> Ptr Oid
                -> IO (Ptr PGresult)

foreign import ccall safe "libpq-fe.h PQexecPrepared"
    c_PQexecPrepared :: Ptr PGconn -> CString -> CInt -> Ptr CString
                     -> Ptr CInt -> Ptr CInt -> CInt -> IO (Ptr PGresult)

foreign import ccall safe "libpq-fe.h PQdescribePrepared"
    c_PQdescribePrepared :: Ptr PGconn -> CString -> IO (Ptr PGresult)

foreign import ccall safe "libpq-fe.h PQdescribePortal"
    c_PQdescribePortal :: Ptr PGconn -> CString -> IO (Ptr PGresult)

foreign import ccall safe "libpq-fe.h &PQclear"
    p_PQclear :: FunPtr (Ptr PGresult ->IO ())

foreign import ccall safe "libpq-fe.h PQresultStatus"
    c_PQresultStatus :: Ptr PGresult -> IO CInt

foreign import ccall safe "libpq-fe.h PQresStatus"
    c_PQresStatus :: CInt -> IO CString

foreign import ccall safe "libpq-fe.h PQresultErrorMessage"
    c_PQresultErrorMessage :: Ptr PGresult -> IO CString

foreign import ccall safe "libpq-fe.h PQresultErrorField"
    c_PQresultErrorField :: Ptr PGresult -> CInt -> IO CString

foreign import ccall safe "libpq-fe.h PQntuples"
    c_PQntuples :: Ptr PGresult -> CInt

foreign import ccall safe "libpq-fe.h PQnfields"
    c_PQnfields :: Ptr PGresult -> CInt

foreign import ccall safe "libpq-fe.h PQfname"
    c_PQfname :: Ptr PGresult -> CInt -> IO CString

foreign import ccall safe "libpq-fe.h PQfnumber"
    c_PQfnumber :: Ptr PGresult -> CString -> IO CInt

foreign import ccall safe "libpq-fe.h PQftable"
    c_PQftable :: Ptr PGresult -> CInt -> IO Oid

foreign import ccall safe "libpq-fe.h PQftablecol"
    c_PQftablecol :: Ptr PGresult -> CInt -> IO CInt

foreign import ccall safe "libpq-fe.h PQfformat"
    c_PQfformat :: Ptr PGresult -> CInt -> IO CInt

foreign import ccall safe "libpq-fe.h PQftype"
    c_PQftype :: Ptr PGresult -> CInt -> IO Oid

foreign import ccall safe "libpq-fe.h PQfmod"
    c_PQfmod :: Ptr PGresult -> CInt -> IO CInt

foreign import ccall safe "libpq-fe.h PQfsize"
    c_PQfsize :: Ptr PGresult -> CInt -> IO CInt

foreign import ccall safe "libpq-fe.h PQgetvalue"
    c_PQgetvalue :: Ptr PGresult -> CInt -> CInt -> IO CString

foreign import ccall safe "libpq-fe.h PQgetisnull"
    c_PQgetisnull :: Ptr PGresult -> CInt -> CInt -> IO CInt

foreign import ccall safe "libpq-fe.h PQgetlength"
    c_PQgetlength :: Ptr PGresult -> CInt -> CInt -> IO CInt

foreign import ccall safe "libpq-fe.h PQnparams"
    c_PQnparams :: Ptr PGresult -> IO CInt

foreign import ccall safe "libpq-fe.h PQparamtype"
    c_PQparamtype :: Ptr PGresult -> CInt -> IO Oid

foreign import ccall safe "stdio.h fdopen"
    c_fdopen :: CInt -> CString -> IO (Ptr CFile)

foreign import ccall safe "libpq-fe.h PQprint"
    c_PQprint :: Ptr CFile -> Ptr PGresult -> Ptr PrintOpt -> IO ()

foreign import ccall safe "libpq-fe.h PQcmdStatus"
    c_PQcmdStatus :: Ptr PGresult -> IO CString

foreign import ccall safe "libpq-fe.h PQcmdTuples"
    c_PQcmdTuples :: Ptr PGresult -> IO CString

foreign import ccall safe "libpq-fe.h PQoidValue"
    c_PQoidValue :: Ptr PGresult -> IO Oid

foreign import ccall safe "libpq-fe.h PQescapeStringConn"
    c_PQescapeStringConn :: Ptr PGconn
                         -> Ptr Word8 -- Actually (CString)
                         -> CString
                         -> CSize
                         -> Ptr CInt
                         -> IO CSize

foreign import ccall safe "libpq-fe.h PQescapeByteaConn"
    c_PQescapeByteaConn :: Ptr PGconn
                        -> CString -- Actually (Ptr CUChar)
                        -> CSize
                        -> Ptr CSize
                        -> IO (Ptr Word8) -- Actually (IO (Ptr CUChar))

foreign import ccall safe "libpq-fe.h PQunescapeBytea"
    c_PQunescapeBytea :: CString -- Actually (Ptr CUChar)
                      -> Ptr CSize
                      -> IO (Ptr Word8) -- Actually (IO (Ptr CUChar))

foreign import ccall safe "libpq-fe.h &PQfreemem"
    p_PQfreemem :: FunPtr (Ptr a -> IO ())
