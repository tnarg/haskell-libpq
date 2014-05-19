{-# LANGUAGE OverloadedStrings, DeriveDataTypeable, CPP #-}
-- |
-- Module: Database.PQ.Utils
--
-- Utility module that adds error handling and convenient conversion to
-- and from SQL values.
--
-- This module is meant to be used instead of "Database.PQ", with the latter
-- imported qualified if you need its entry points and types:
--
-- >import Database.PQ.Utils
-- >import qualified Database.PQ as PQ

module Database.PQ.Utils (
    -- * Connection
    Connection,
    connectdb,

    -- * Error handling
    SqlError(..),
    sqlError,
    throwSqlError,
    throwPQError,

    -- * Command execution
    execCommand,
    execCommandParams,
    execTuples,
    execTuplesParams,
    withTransaction,
    withTransactionCustom,

    -- * Conversion from SQL results
    -- | Read tuples from 'PQ.Result' structures using a convenient
    -- 'Applicative' interface.  Columns are requested by name.  Column names
    -- are looked up only once per result set.
    --
    -- Example:
    --
    -- >{-# LANGUAGE OverloadedStrings #-}
    -- >
    -- >import Control.Applicative
    -- >import Database.PQ.Utils
    -- >import qualified Data.Vector as V
    -- >
    -- >main :: IO ()
    -- >main = do
    -- >    conn <- connectdb ""
    -- >    res <- execTuples conn "SELECT x, x*x AS y FROM generate_series(1, 10) AS x"
    -- >
    -- >    let row :: ReadResult (Int, Int)
    -- >        row =  (,)
    -- >           <$> columnNotNull "x" fromInt
    -- >           <*> columnNotNull "y" fromInt
    -- >
    -- >    vec <- readResult row res
    -- >    V.forM_ vec print

    ReadResult,
    readResult,

    -- ** Column retrieval
    column,
    column',
    columnNotNull,
    columnNotNull',

    -- ** Value conversion functions
    -- |
    -- These functions assume that the result has been retrieved in text
    -- mode.  If you use the 'execTuples' and 'execTuplesParams' functions from
    -- this module, you don't need to worry about this.  If you use 'PQ.execParams'
    -- from "Database.PQ" and specify Binary as the 'PQ.Format', these functions
    -- will not work correctly on datums retrieved from the 'PQ.Result'.
    fromText,
    fromBytea,
    fromBool,
    fromInt,
    fromFloat,
    fromReadS,

    -- * Conversion to SQL parameters
    PQParam,
    toText,
    toBytea,
    toBool,
    toInt,
    toFloat,

    -- * Miscellaneous
    consume
) where

import Database.PQ (Connection)
import qualified Database.PQ as PQ

import Control.Applicative (Applicative(..), (<$>))
import Data.ByteString (ByteString)
import Control.Exception (Exception)
import Data.Typeable (Typeable)
import Data.Vector (Vector)
import Numeric (readSigned, readDec, readFloat, showSigned, showInt, showFloat)
import System.IO.Unsafe (unsafePerformIO)

import qualified Control.Exception as E
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.UTF8 as UTF8
import qualified Data.Vector as V

data SqlError = SqlError String
    deriving (Show, Typeable)

instance Exception SqlError

-- | Throw a 'SqlError' exception from pure code.
sqlError :: String -> a
sqlError = E.throw . SqlError

-- | Throw a 'SqlError' exception from within the IO monad.
throwSqlError :: String -> IO a
throwSqlError = E.throwIO . SqlError

-- | Like 'throwSqlError', but handle the format returned by 'PQ.errorMessage'
-- and 'PQ.resultErrorMessage'.  For example, these functions might return an
-- empty string if the previous command did not produce an error.  Although
-- using such functions when an error did not occur is a bug, it's confusing
-- when a program fails and prints nothing.
throwPQError :: Maybe ByteString -> IO a
throwPQError errmsg =
    case errmsg of
         Nothing            -> throwSqlError "(null)"
         Just x | B.null x  -> throwSqlError "(empty error message)"
                | otherwise -> throwSqlError $ UTF8.toString x

checkConnection :: Connection -> IO Connection
checkConnection conn = do
    status <- PQ.status conn
    if status == PQ.ConnectionOk
       then return conn
       else throwPQError =<< PQ.errorMessage conn

checkResult :: PQ.ExecStatus -> Connection -> Maybe PQ.Result -> IO PQ.Result
checkResult expectedStatus conn mresult =
    case mresult of
         Nothing     -> throwPQError =<< PQ.errorMessage conn
         Just result -> do
             actualStatus <- PQ.resultStatus result
             if actualStatus == expectedStatus
                then return result
                else let wrongStatus = throwSqlError $
                                       "Expected status " ++ show expectedStatus ++
                                       ", but command produced " ++ show actualStatus
                      in case actualStatus of
                          PQ.EmptyQuery -> throwSqlError "Empty query string"

                          -- Non-error result codes
                          PQ.CommandOk  -> wrongStatus
                          PQ.TuplesOk   -> wrongStatus
                          PQ.CopyOut    -> wrongStatus
                          PQ.CopyIn     -> wrongStatus

                          -- Error / unrecognized status
                          _ -> throwPQError =<< PQ.resultErrorMessage result

-- | Make a new database connection (using 'PQ.connectdb' from "Database.PQ").
-- If the connection fails, throw a 'SqlError' describing the error.
connectdb :: ByteString -> IO Connection
connectdb conninfo = checkConnection =<< PQ.connectdb conninfo

-- | Execute a query that is expected to return no data.
--
-- If the query fails, or does in fact return a result set,
-- a 'SqlError' is thrown.
execCommand :: Connection -> ByteString -> IO PQ.Result
execCommand conn query = checkResult PQ.CommandOk conn =<< PQ.exec conn query

-- | Execute a query that is expected to return data (such as @SELECT@ or @SHOW@).
--
-- If the query fails, or does not return a result set, a 'SqlError' is thrown.
--
-- Bear in mind that in PostgreSQL, result rows are not returned lazily.
-- If you want to read a large result set incrementally, you
-- will need to use a cursor.  See 'consume' for an example.
execTuples :: Connection -> ByteString -> IO PQ.Result
execTuples conn query = checkResult PQ.TuplesOk conn =<< PQ.exec conn query

-- | Run an action within a transaction.
--
-- If an exception arises, the transaction will be rolled back.
withTransaction :: Connection -> IO a -> IO a
withTransaction conn = withTransactionCustom conn "BEGIN"

-- | Like 'withTransaction', but with a custom \"begin\" statement.
--
-- @'withTransaction' conn = 'withTransactionCustom' conn \"BEGIN\"@
withTransactionCustom :: Connection -> ByteString -> IO a -> IO a
withTransactionCustom conn begin action
    = portableMask $ \restore -> do
        _ <- execCommand conn begin
        r <- restore action `E.onException` execCommand conn "ROLLBACK"
        _ <- execCommand conn "COMMIT"
        return r

-- | Like 'E.mask', but backported to base before version 4.3.0.
--
-- Note that the restore callback is monomorphic, unlike in 'E.mask'.
-- This could be fixed by changing the type signature, but it would
-- require us to enable the RankNTypes extension.  The 'withTransactionCustom'
-- function above calls the restore callback only once, so we don't
-- need that polymorphism.
portableMask :: ((IO a -> IO a) -> IO b) -> IO b
#if MIN_VERSION_base(4,3,0)
portableMask io = E.mask $ \restore -> io restore
#else
portableMask io = do
    b <- E.blocked
    E.block $ io $ \m -> if b then m else E.unblock m
#endif

type PQParam = (PQ.Oid, ByteString, PQ.Format)

-- | Like execCommand, but with the ability to pass parameters separately
-- from the SQL command text.
--
execCommandParams :: Connection -> ByteString -> [Maybe PQParam] -> IO PQ.Result
execCommandParams conn query params =
    checkResult PQ.CommandOk conn
        =<< PQ.execParams conn query params PQ.Text

-- | Like execTuples, but with the ability to pass parameters separately
-- from the SQL command text.
--
execTuplesParams :: Connection -> ByteString -> [Maybe PQParam] -> IO PQ.Result
execTuplesParams conn query params =
    checkResult PQ.TuplesOk conn
        =<< PQ.execParams conn query params PQ.Text

-- | Result parser.
newtype ReadResult a = ReadResult {runReadResult :: PQ.Result -> IO (PQ.Row -> IO a)}

instance Functor ReadResult where
    fmap f a = ReadResult $ \res -> do
        ac <- runReadResult a res
        return $ \row -> do
            v <- ac row
            return (f v)

instance Applicative ReadResult where
    pure x = ReadResult $ \_ -> return $ \_ -> return x
    f1 <*> f2 = ReadResult $ \res -> do
        c1 <- runReadResult f1 res
        c2 <- runReadResult f2 res
        return $ \row -> do
            v1 <- c1 row
            v2 <- c2 row
            return (v1 v2)

-- | Execute a 'ReadResult' computation on a 'PQ.Result'.
--
-- Each item in the resulting 'Vector' corresponds to a /row/ of the
-- result set.
--
-- This function is strict in @record@.  As long as @record@ is in turn
-- strict in its members, and as long as those members do not contain
-- original copies of values returned by 'PQ.getvalue', result records
-- should not pin down the entire 'PQ.Result'.
readResult :: ReadResult record -> PQ.Result -> IO (Vector record)
readResult r res = do
    readRowAction <- runReadResult r res
    count <- PQ.ntuples res
    V.generateM count $ \i -> do
        row <- readRowAction (PQ.toRow i)
        return $! row

columnHelper :: String -> (PQ.Result -> PQ.Row -> PQ.Column -> IO a) -> ReadResult a
columnHelper colname f = ReadResult $ \res -> do
    m <- PQ.fnumber res (C.pack colname)
    case m of
         Just col -> return $ \row -> do
             v <- f res row (PQ.toColumn col)
             return $! v
         Nothing  -> throwSqlError $ "Result set does not have column \""
                                      ++ colname ++ "\""

-- | Retrieve a (nullable) column by name, and convert it using the given function.
column :: String -> (ByteString -> a) -> ReadResult (Maybe a)
column colname f = columnHelper colname $ \res row col -> do
    m <- PQ.getvalue res row col
    case m of
         Nothing -> return Nothing
         Just v  -> return $! Just $! f v

-- | Retrieve a column by name and convert it.  If the value is null,
-- throw a 'SqlError'.
columnNotNull :: String -> (ByteString -> a) -> ReadResult a
columnNotNull colname f = columnHelper colname $ \res row col -> do
    m <- PQ.getvalue res row col
    case m of
         Just v  -> return $! f v
         Nothing -> throwSqlError $ "Unexpected null value in column \""
                                    ++ colname ++ "\""

-- | Like 'column', but use 'PQ.getvalue'' instead of 'PQ.getvalue'.
column' :: String -> (ByteString -> a) -> ReadResult (Maybe a)
column' colname f = columnHelper colname $ \res row col -> do
    v <- PQ.getvalue' res row col
    return (f <$> v)

-- | Like 'columnNotNull', but use 'PQ.getvalue'' instead of 'PQ.getvalue'.
columnNotNull' :: String -> (ByteString -> a) -> ReadResult a
columnNotNull' colname f = columnHelper colname $ \res row col -> do
    m <- PQ.getvalue' res row col
    case m of
         Just v  -> return (f v)
         Nothing -> throwSqlError $ "Unexpected null value in column \""
                                    ++ colname ++ "\""

-- | Convert a @TEXT@ datum to a 'ByteString'.  This is just a 'B.copy'.
fromText :: ByteString -> ByteString
fromText = B.copy

-- | Convert a @BYTEA@ datum to a 'ByteString'.  This performs conversion
-- using 'PQ.unescapeBytea', throwing a 'SqlError' on failure.
fromBytea :: ByteString -> ByteString
fromBytea str =
    case unsafePerformIO $ PQ.unescapeBytea str of
         Just bstr -> bstr
         Nothing   -> sqlError "PQunescapeBytea failed"

-- | Read a @BOOL@ datum.  The text representation of a @BOOL@ is a single
-- character: @\'f\'@ or @\'t\'@.
fromBool :: ByteString -> Bool
fromBool str | str == C.singleton 'f' = False
             | str == C.singleton 't' = True
             | otherwise = sqlError "Invalid syntax for fromBool"

-- | Convert a datum by converting its bytes to a 'String' and feeding
-- it to a 'ReadS' parser.
--
-- See 'fromInt' and 'fromFloat' for examples.
fromReadS :: ReadS a        -- ^ Parser
          -> String         -- ^ Error message if parsing fails
          -> ByteString     -- ^ Datum in text format (should consist solely
                            --   of ASCII characters)
          -> a
fromReadS readS errmsg bstr =
    case [x | (x, "") <- readS (C.unpack bstr)] of
         [x] -> x
         []  -> sqlError errmsg
         _   -> sqlError (errmsg ++ " (ambiguous parse)")

-- | Read any datum that can be parsed as an integer.
--
-- @'fromInt' = 'fromReadS' ('readSigned' 'readDec') \"Invalid syntax for fromInt\"@
fromInt :: (Integral a) => ByteString -> a
fromInt = fromReadS (readSigned readDec) "Invalid syntax for fromInt"

-- | Read any datum that can be parsed as a floating point number.
--
-- This function should be able to accept the PostgreSQL text representation
-- of any of the numeric types (except for weird ones like @MONEY@), but this has
-- not been proven.  The @NaN@ and @Infinity@ syntax PostgreSQL uses are
-- recognized by Haskell's 'readFloat'.
--
-- @'fromFloat' = 'fromReadS' ('readSigned' 'readFloat') \"Invalid syntax for fromFloat\"@
fromFloat :: (RealFrac a) => ByteString -> a
fromFloat = fromReadS (readSigned readFloat) "Invalid syntax for fromFloat"

toText :: ByteString -> PQParam
toText txt = (0, txt, PQ.Text)

toBytea :: ByteString -> PQParam
toBytea btxt = (0, btxt, PQ.Binary)

toBool :: Bool -> PQParam
toBool False = toText (C.singleton 'f')
toBool True  = toText (C.singleton 't')

toInt :: (Integral a) => a -> PQParam
toInt n = toText $ C.pack $ showSigned showInt 0 n ""

toFloat :: (RealFloat a) => a -> PQParam
toFloat n = toText $ C.pack $ showFloat n ""

-- | Repeatedly execute a query until it returns zero rows.
--
-- Each non-empty result set is passed to the callback action.
--
-- Example:
--
-- >consumeExample :: Connection -> IO ()
-- >consumeExample conn =
-- >    withTransaction conn $ do
-- >        _ <- execCommand conn
-- >             $  "DECLARE series_cursor NO SCROLL CURSOR FOR"
-- >             ++ " SELECT x, x*x AS y FROM generate_series(1,100) AS x"
-- >
-- >        let parser :: ReadResult (Int, Int)
-- >            parser =  (,)
-- >                  <$> columnNotNull "x" fromInt
-- >                  <*> columnNotNull "y" fromInt
-- >
-- >        consume conn "FETCH FORWARD 7 FROM series_cursor" parser
-- >            $ \set -> do
-- >                putStrLn $ "Chunk " ++ show (V.length set)
-- >                V.forM_ set $ \(x, y) ->
-- >                    putStrLn $ "\t" ++ show x ++ "\t" ++ show y
consume :: Connection               -- ^ Database connection
        -> ByteString               -- ^ Query
        -> ReadResult record        -- ^ Result parser
        -> (Vector record -> IO a)  -- ^ Callback
        -> IO ()
consume conn query parser callback = loop
    where
        loop = do
            res <- execTuples conn query
            ntuples <- PQ.ntuples res
            case ntuples of
                 _ | ntuples > 0 -> do
                     _ <- callback =<< readResult parser res
                     loop
                   | ntuples == 0 -> return ()
                   | otherwise    -> throwSqlError "PQntuples returned a negative number"
