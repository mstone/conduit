{-# LANGUAGE FlexibleContexts, ForeignFunctionInterface #-}
-- | Streaming compression and decompression using conduits.
--
-- Parts of this code were taken from zlib-enum and adapted for conduits.
module Data.Conduit.Zlib (
    -- * Conduits
    compress, decompress, gzip, ungzip, zcat,
    -- * Flushing
    compressFlush, decompressFlush,
    -- * Re-exported from zlib-bindings
    WindowBits (..), defaultWindowBits, ZlibException(..)
) where

import Codec.Zlib
import Data.Conduit hiding (unsafeLiftIO, Source, Sink, Conduit)
import qualified Data.Conduit as C (unsafeLiftIO)
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Control.Exception (try)
import Control.Monad ((<=<), unless, when)
import Control.Monad.Trans.Class (lift)

import Data.IORef
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource (allocate)
import Control.Exception (SomeException, toException)
import Codec.Zlib.Lowlevel
import Foreign.C
import Foreign.Ptr
import Foreign.Marshal.Alloc

-- | Gzip compression with default parameters.
gzip :: (MonadThrow m, MonadUnsafeIO m) => GInfConduit ByteString m ByteString
gzip = compress 1 (WindowBits 31)

-- | Gzip decompression with default parameters.
ungzip :: (MonadUnsafeIO m, MonadThrow m) => GInfConduit ByteString m ByteString
ungzip = decompress (WindowBits 31)

unsafeLiftIO :: (MonadUnsafeIO m, MonadThrow m) => IO a -> m a
unsafeLiftIO =
    either rethrow return <=< C.unsafeLiftIO . try
  where
    rethrow :: MonadThrow m => ZlibException -> m a
    rethrow = monadThrow

-- |
-- Decompress (inflate) a stream of 'ByteString's. For example:
--
-- >    sourceFile "test.z" $= decompress defaultWindowBits $$ sinkFile "test"

decompress
    :: (MonadUnsafeIO m, MonadThrow m)
    => WindowBits -- ^ Zlib parameter (see the zlib-bindings package as well as the zlib C library)
    -> GInfConduit ByteString m ByteString
decompress =
    mapOutput unChunk . mapInput Chunk unChunk' . decompressFlush
  where
    unChunk Flush = S.empty
    unChunk (Chunk bs) = bs

    unChunk' Flush = Nothing
    unChunk' (Chunk bs) = Just bs

-- | Same as 'decompress', but allows you to explicitly flush the stream.
decompressFlush
    :: (MonadUnsafeIO m, MonadThrow m)
    => WindowBits -- ^ Zlib parameter (see the zlib-bindings package as well as the zlib C library)
    -> GInfConduit (Flush ByteString) m (Flush ByteString)
decompressFlush config =
    awaitE >>= either return start
  where
    start input = do
        inf <- lift $ unsafeLiftIO $ initInflate config
        push inf input

    continue inf = awaitE >>= either (close inf) (push inf)

    goPopper popper = do
        mbs <- lift $ unsafeLiftIO popper
        case mbs of
            Nothing -> return ()
            Just bs -> yield (Chunk bs) >> goPopper popper

    push inf (Chunk x) = do
        popper <- lift $ unsafeLiftIO $ feedInflate inf x
        goPopper popper
        continue inf

    push inf Flush = do
        chunk <- lift $ unsafeLiftIO $ flushInflate inf
        unless (S.null chunk) $ yield $ Chunk chunk
        yield Flush
        continue inf

    close inf ret = do
        chunk <- lift $ unsafeLiftIO $ finishInflate inf
        unless (S.null chunk) $ yield $ Chunk chunk
        return ret

-- |
-- Compress (deflate) a stream of 'ByteString's. The 'WindowBits' also control
-- the format (zlib vs. gzip).

compress
    :: (MonadUnsafeIO m, MonadThrow m)
    => Int         -- ^ Compression level
    -> WindowBits  -- ^ Zlib parameter (see the zlib-bindings package as well as the zlib C library)
    -> GInfConduit ByteString m ByteString
compress level =
    mapOutput unChunk . mapInput Chunk unChunk' . compressFlush level
  where
    unChunk Flush = S.empty
    unChunk (Chunk bs) = bs

    unChunk' Flush = Nothing
    unChunk' (Chunk bs) = Just bs

-- | Same as 'compress', but allows you to explicitly flush the stream.
compressFlush
    :: (MonadUnsafeIO m, MonadThrow m)
    => Int         -- ^ Compression level
    -> WindowBits  -- ^ Zlib parameter (see the zlib-bindings package as well as the zlib C library)
    -> GInfConduit (Flush ByteString) m (Flush ByteString)
compressFlush level config =
    awaitE >>= either return start
  where
    start input = do
        def <- lift $ unsafeLiftIO $ initDeflate level config
        push def input

    continue def = awaitE >>= either (close def) (push def)

    goPopper popper = do
        mbs <- lift $ unsafeLiftIO popper
        case mbs of
            Nothing -> return ()
            Just bs -> yield (Chunk bs) >> goPopper popper

    push def (Chunk x) = do
        popper <- lift $ unsafeLiftIO $ feedDeflate def x
        goPopper popper
        continue def

    push def Flush = do
        mchunk <- lift $ unsafeLiftIO $ flushDeflate def
        maybe (return ()) (yield . Chunk) mchunk
        yield Flush
        continue def

    close def ret = do
        mchunk <- lift $ unsafeLiftIO $ finishDeflate def
        case mchunk of
            Nothing -> return ret
            Just chunk -> yield (Chunk chunk) >> close def ret


zBufLen :: Int
zBufLen = 65536

zBufError :: CInt
zBufError = -5

type ZStreamFreeFunction = ZStream' -> IO ()
foreign import ccall "dynamic" mkZStreamFreeFun :: FunPtr ZStreamFreeFunction -> ZStreamFreeFunction

zstreamFreeInflate :: ZStreamFreeFunction
zstreamFreeInflate = mkZStreamFreeFun c_free_z_stream_inflate

-- |
-- Decompress (inflate) a stream of 'ByteString's representing concatenated gzip streams, returning errors inline.

zcat :: (MonadResource m)
     => Pipe l ByteString (Either SomeException ByteString) u m ()
zcat = do
    (_rk_zs, zs)     <- lift $ allocate zstreamNew            zstreamFreeInflate
    (_rk_obuf, obuf) <- lift $ allocate (mallocBytes zBufLen) free
    refFlag <- liftIO $ newIORef True
    liftIO $ inflateInit2 zs (WindowBits 31)
    liftIO $ c_set_avail_out zs obuf $ fromIntegral zBufLen
    go zs obuf refFlag
  where
    go :: (MonadResource m)
        => ZStream'
        -> Ptr CChar
        -> IORef Bool
        -> Pipe l ByteString (Either SomeException ByteString) u m ()
    go zs obuf refFlag = do
        mbs <- await
        case mbs of
            Nothing -> do
                --rest <- liftIO $ finishInflate inf
                -- ...
                return ()
            Just bs -> do
                -- XXX: scope!
                (ibuf, ilen) <- liftIO $ unsafeUseAsCStringLen bs return
                liftIO $ c_set_avail_in zs ibuf $ fromIntegral ilen
                goInflate False zs ibuf ilen obuf zBufLen refFlag
                flag <- liftIO $ readIORef refFlag
                if flag then go zs obuf refFlag else return ()

    goInflate :: (MonadResource m)
              => Bool
              -> ZStream'
              -> Ptr CChar
              -> Int
              -> Ptr CChar
              -> Int
              -> IORef Bool
              -> Pipe l ByteString (Either SomeException ByteString) u m ()
    goInflate reinitFlag zs ibuf ilen obuf olen refFlag = do
        when reinitFlag $ liftIO $ inflateInit2 zs (WindowBits 31)
        res <- liftIO $ c_call_inflate_noflush zs
        -- XXX: check for zNeedDict
        if (res < 0 && res /= zBufError)
            then do
                yield $ Left $ toException $ ZlibException $ fromIntegral res
                liftIO $ writeIORef refFlag False
            else do
                inext <- liftIO $ c_get_next_in zs
                let irest = ilen - (inext `minusPtr` ibuf)
                oavail <- liftIO $ c_get_avail_out zs
                let size = olen - fromIntegral oavail
                obs <- liftIO $ S.packCStringLen (obuf, size)
                liftIO $ c_set_avail_out zs obuf $ fromIntegral olen
                if irest == 0
                    then yield $ Right obs
                    else do
                        (yield $ Right obs)
                        goInflate True zs ibuf ilen obuf olen refFlag
