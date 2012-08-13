import Test.Hspec
import Test.Hspec.QuickCheck (prop)

import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Zlib as CZ
import Control.Monad.ST (runST)
import Data.Monoid
import qualified Data.ByteString as S
import qualified Data.ByteString.Char8 as S8
import qualified Data.ByteString.Lazy as L
import Data.ByteString.Lazy.Char8 ()
import Control.Monad.Trans.Resource (runExceptionT_)
import Control.Exception (SomeException, toException)
import Data.Either (rights)

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
        let a_gz = S8.pack "\US\139\b\NUL)\DC3%P\STX\ETXK\228\STX\NUL\a\161\234\221\STX\NUL\NUL\NUL"
            b_gz = S8.pack "\US\139\b\NUL)\DC3%P\STX\ETXK\226\STX\NUL\196\242\199\246\STX\NUL\NUL\NUL"
            ab_gz = a_gz `S.append` b_gz
            err_gz = ab_gz `S.append` (S8.pack "HIYA\n")
            abab_gz = ab_gz `S.append` ab_gz

        let pipeline :: S.ByteString -> C.ResourceT IO [Either SomeException S.ByteString]
            pipeline x = C.yield x C.$= CZ.zcat C.$$ CL.consume

        let go :: String -> S.ByteString -> [Either SomeException S.ByteString] -> Spec
            go name gz expectedResults = do
                it name $ do
                  actualResults <- C.runResourceT $ pipeline gz
                  let lhs = (mconcat $ rights $ actualResults)
                  let rhs = (mconcat $ rights $ expectedResults)
                  lhs `shouldBe` rhs

        go "a"   a_gz     [Right $ S8.pack "a\n"]
        go "b"   b_gz     [Right $ S8.pack "b\n"]
        go "ab"  ab_gz    [Right $ S8.pack "a\nb\n"]
        go "ab!" err_gz   [Right $ S8.pack "a\nb\n", Left (toException (CZ.ZlibException (-3)))]
        go "abab" abab_gz [Right $ S8.pack "a\nb\na\nb\n"]
