{-# LANGUAGE ScopedTypeVariables, FlexibleInstances, OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
import Test.Hspec
import qualified Test.Hspec.Core as Hspec
import Test.Hspec.QuickCheck
import Test.QuickCheck
import Test.QuickCheck.Property

import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Zlib as CZ
import Control.Monad.ST (runST)
import Data.Monoid
import qualified Data.ByteString as S
import qualified Data.ByteString.Char8 as S8
import qualified Data.ByteString.Lazy as L
import Data.ByteString.Lazy.Char8 ()
import Control.Monad.Trans.Resource
import Control.Monad.Trans.Control ()
import Control.Exception (SomeException, toException)
import Data.Either (rights)
import Control.Applicative ((<$>))

instance Arbitrary S.ByteString where
    arbitrary = S.pack `fmap` arbitrary

instance Example (IO Bool) where
  evaluateExample mv = do
    v <- mv
    case v of
      True -> return Hspec.Success
      False -> return (Hspec.Fail "")

{- Known-answer test vectors for zcat conduit. -}
a_gz, b_gz, ab_gz, err_gz, abab_gz :: S8.ByteString
a_gz = "\US\139\b\NUL)\DC3%P\STX\ETXK\228\STX\NUL\a\161\234\221\STX\NUL\NUL\NUL"
b_gz = "\US\139\b\NUL)\DC3%P\STX\ETXK\226\STX\NUL\196\242\199\246\STX\NUL\NUL\NUL"
ab_gz = a_gz `S8.append` b_gz
err_gz = ab_gz `S8.append` "HIYA\n"
abab_gz = ab_gz `S8.append` ab_gz

gzipByteString :: S.ByteString -> IO S.ByteString
gzipByteString i = mconcat <$> (C.runResourceT $ C.yield i C.$= CZ.gzip C.$$ CL.consume)

{- zcat . gzip === id -}
downstream_inverse_single :: Property
downstream_inverse_single = do
  let zBufLen = 100
  (i :: S.ByteString) <- resize (2 * zBufLen) arbitrary
  morallyDubiousIOProperty $ do
    z <- gzipByteString i
    C.runResourceT $ do
      os <- C.yield z C.$= CZ.zcat zBufLen C.$$ CL.consume
      return (i == (mconcat $ rights $ os))

{- concat . zcat . concat . map gzip === concat -}
downstream_inverse_concat :: Property
downstream_inverse_concat = do
  let zBufLen = 100
  (is :: [S.ByteString]) <- listOf $ resize (2 * zBufLen) arbitrary
  let i = mconcat is
  morallyDubiousIOProperty $ do
    zs <- mapM gzipByteString is
    C.runResourceT $ do
      os <- CL.sourceList zs C.$= CZ.zcat zBufLen C.$$ CL.consume
      return (i == (mconcat $ rights $ os))

{- known-answer test routine for zcat -}
kat :: String -> S.ByteString -> [Either SomeException S.ByteString] -> Spec
kat name gz expectedResults =
    it name example
  where
    example :: IO Bool
    example = do
        actualResults <- C.runResourceT $ C.yield gz C.$= CZ.zcat 64 C.$$ CL.consume
        let lhs = (mconcat $ rights $ actualResults)
        let rhs = (mconcat $ rights $ expectedResults)
        return (lhs == rhs)

main :: IO ()
main = hspec $ do
    describe "zlib" $ do
        prop "idempotent" $ \bss' -> runST $ do
            let bss = map S.pack bss'
                lbs = L.fromChunks bss
                src = mconcat $ map (CL.sourceList . return) bss
            outBss <- runExceptionT_ $ src C.$= CZ.gzip C.$= CZ.ungzip C.$$ CL.consume
            return $ lbs == L.fromChunks outBss
        prop "flush" $ \bss' -> runST $ do
            let bss = map S.pack $ filter (not . null) bss'
                bssC = concatMap (\bs -> [C.Chunk bs, C.Flush]) bss
                src = mconcat $ map (CL.sourceList . return) bssC
            outBssC <- runExceptionT_
                     $ src C.$= CZ.compressFlush 5 (CZ.WindowBits 31)
                           C.$= CZ.decompressFlush (CZ.WindowBits 31)
                           C.$$ CL.consume
            return $ bssC == outBssC
        it "compressFlush large data" $ do
            let content = L.pack $ map (fromIntegral . fromEnum) $ concat $ ["BEGIN"] ++ map show [1..100000 :: Int] ++ ["END"]
                src = CL.sourceList $ map C.Chunk $ L.toChunks content
            bssC <- src C.$$ CZ.compressFlush 5 (CZ.WindowBits 31) C.=$ CL.consume
            let unChunk (C.Chunk x) = [x]
                unChunk C.Flush = []
            bss <- CL.sourceList bssC C.$$ CL.concatMap unChunk C.=$ CZ.ungzip C.=$ CL.consume
            L.fromChunks bss `shouldBe` content

    describe "zcat" $ do
        kat "a"    a_gz    [Right "a\n"]
        kat "b"    b_gz    [Right "b\n"]
        kat "ab"   ab_gz   [Right "a\nb\n"]
        kat "ab!"  err_gz  [Right "a\nb\n", Left (toException (CZ.ZlibException (-3)))]
        kat "abab" abab_gz [Right "a\nb\na\nb\n"]
        prop "dinv_single" downstream_inverse_single
        prop "dinv_concat" downstream_inverse_concat

